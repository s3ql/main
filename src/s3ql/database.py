'''
database.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.


Module Attributes:
-----------

:initsql:      SQL commands that are executed whenever a new
               connection is created.
'''


import collections
import copy
import dataclasses
import logging
import math
import os
import re
import time
from typing import Dict, List, Optional

import apsw

from . import CURRENT_FS_REV, sqlite3ext
from .backends.common import AbstractBackend, NoSuchObject
from .common import freeze_basic_mapping, sha256_fh, thaw_basic_mapping
from .logging import QuietError

log = logging.getLogger(__name__)

# Format for metadata object names. Block number and seq no need
# to be interpolated.
METADATA_OBJ_NAME = 's3ql_metadata_%012x_%010x'
METADATA_OBJ_RE = re.compile(r'^s3ql_metadata_([0-9a-f]+)_([0-9a-f]+)$')


sqlite_ver = tuple([int(x) for x in apsw.sqlitelibversion().split('.')])
if sqlite_ver < (3, 7, 0):
    raise QuietError('SQLite version too old, must be 3.7.0 or newer!\n')


def sqlite3_log(errcode, message):
    if errcode == apsw.SQLITE_NOTICE:
        log.debug('sqlite3: %s', message)
    elif errcode == apsw.SQLITE_WARNING:
        log.warning('sqlite3: %s', message)
    elif errcode == apsw.SQLITE_ERROR:
        log.error('sqlite3: %s', message)
    elif errcode == apsw.SQLITE_OK:
        log.info('sqlite3: %s', message)
    else:
        errstr = apsw.mapping_extended_result_codes.get(
            errcode, apsw.mapping_result_codes[errcode & 255]
        )
        # Interpolate the log message directly so that it is available for pytest_checklogs
        log.warning(f'sqlite3: {message} ({errstr})')


apsw.config(apsw.SQLITE_CONFIG_LOG, sqlite3_log)


@dataclasses.dataclass
class FsAttributes:
    data_block_size: int
    metadata_block_size: int
    seq_no: int = 0
    db_size: Optional[int] = None
    revision: int = CURRENT_FS_REV
    label: str = '<unnamed>'
    needs_fsck: bool = False
    is_mounted: bool = False
    inode_gen: int = 0
    last_fsck: float = 0
    last_modified: float = 0
    db_md5: Optional[str] = None

    def copy(self):
        return copy.copy(self)

    @staticmethod
    def deserialize(buf: bytes, min_seq: Optional[int] = None):
        '''De-serialize *buf* into a `FsAttributes` instance.

        If *min_seq* is specified and the buffer has a sequence number lower than
        that, raise `OutdatedData` exception. When possible, this is raised before doing
        filesystem revision checks.
        '''
        d = thaw_basic_mapping(buf)

        # seq_no is present in all fs revisions, so this check is safe.
        if min_seq is not None and d['seq_no'] < min_seq:
            raise OutdatedData()

        if d['revision'] < CURRENT_FS_REV:
            raise QuietError(
                'File system revision too old, please run `s3qladm upgrade` first.', exitcode=32
            )
        elif d['revision'] > CURRENT_FS_REV:
            raise QuietError(
                'File system revision too new, please update your S3QL installation.', exitcode=33
            )
        return FsAttributes(**d)

    def serialize(self):
        return freeze_basic_mapping(dataclasses.asdict(self))


class OutdatedData(RuntimeError):
    '''Raised by `FsAttributes.deserialize`'''

    pass


class Connection:
    '''
    This class wraps an APSW connection object. It should be used instead of any
    native APSW cursors.

    It provides methods to directly execute SQL commands and creates apsw
    cursors dynamically.

    Instances are not thread safe. They can be passed between threads,
    but must not be called concurrently.

    Attributes
    ----------

    :conn:     apsw connection object
    '''

    def __init__(self, file_: str, blocksize: Optional[int] = None):
        if blocksize:
            sqlite3ext.set_blocksize(blocksize)

            # Need to make sure that SQLite and the write tracker module agree on the canonical path
            # of the file (otherwise write tracking will not be enabled).
            file_ = os.path.realpath(file_)

            self.dirty_blocks = sqlite3ext.track_writes(file_)
            self.conn = apsw.Connection(file_, vfs=sqlite3ext.get_vfsname())
        else:
            self.conn = apsw.Connection(file_)
        self.file = file_
        self.blocksize = blocksize
        self.tx_active = False

        cur = self.conn.cursor()

        cur.execute('PRAGMA locking_mode = EXCLUSIVE')
        res = self.get_val('PRAGMA journal_mode = WAL')
        if res.lower() != 'wal':
            raise RuntimeError(f'Unable to set WAL journaling mode, got: {res}')

        for s in (
            'PRAGMA synchronous = NORMAL',
            'PRAGMA foreign_keys = OFF',
            'PRAGMA recursize_triggers = on',
            'PRAGMA temp_store = FILE',
            'PRAGMA legacy_file_format = off',
        ):
            cur.execute(s)

        # Disable bloom filter optimization when we detect that sqlite is a buggy version
        # https://sqlite.org/forum/forumpost/031e262a89b6a9d2
        if (3, 38, 0) <= sqlite_ver < (3, 38, 4):
            log.warning(
                'Your sqlite version has bugs in its bloom filter optimization. '
                'We disable the usage of some indexes (in fsck.s3ql). '
                'Consider updating sqlite to 3.38.5+.'
            )
            self.fixes = {'dbf': '+'}
            # https://sqlite.org/forum/forumpost/0d3200f4f3bcd3a3
            cur.execute('PRAGMA automatic_index = off')
        else:
            self.fixes = {'dbf': ''}

    def close(self):
        # As recommended in https://www.sqlite.org/lang_analyze.html
        self.execute('PRAGMA analysis_limit=0')
        self.execute('PRAGMA optimize')
        self.conn.close()

    def get_size(self):
        '''Return size of database file'''

        if self.file is not None and self.file not in ('', ':memory:'):
            return os.path.getsize(self.file)
        else:
            return 0

    def query(self, *a, **kw):
        '''Return iterator over results of given SQL statement

        If the caller does not retrieve all rows the iterator's close() method
        should be called as soon as possible to terminate the SQL statement
        (otherwise it may block execution of other statements). To this end,
        the iterator may also be used as a context manager.
        '''

        return ResultSet(self.conn.cursor().execute(*a, **kw))

    def execute(self, *a, **kw):
        '''Execute the given SQL statement. Return number of affected rows'''

        self.conn.cursor().execute(*a, **kw)
        return self.changes()

    def rowid(self, *a, **kw):
        """Execute SQL statement and return last inserted rowid"""

        self.conn.cursor().execute(*a, **kw)
        return self.conn.last_insert_rowid()

    def has_val(self, *a, **kw):
        '''Execute statement and check if it gives result rows'''

        res = self.conn.cursor().execute(*a, **kw)
        try:
            next(res)
        except StopIteration:
            return False
        else:
            # Finish the active SQL statement
            res.close()
            return True

    def checkpoint(self):
        '''Checkpoint the WAL'''

        try:
            row = self.get_row('PRAGMA main.wal_checkpoint(RESTART)')
        except apsw.LockedError:  # https://sqlite.org/forum/forumpost/9e1ad35f07
            row = (1, 0, 0)
        if row[0] != 0 or row[1] == -1:
            log.warning(
                'Unable to checkpoint WAL - please report at https://github.com/s3ql/s3ql/issues'
            )
        else:
            log.debug('checkpoint: wrote %d/%d pages to db file', row[2], row[1])

    def batch(self, dt_target: float):
        return BatchedTransactionManager(self, dt_target)

    def get_val(self, *a, **kw):
        """Execute statement and return first element of first result row.

        If there is no result row, raises `NoSuchRowError`. If there is more
        than one row, raises `NoUniqueValueError`.
        """

        return self.get_row(*a, **kw)[0]

    def get_list(self, *a, **kw):
        """Execute select statement and returns result list"""

        return list(self.query(*a, **kw))

    def get_row(self, *a, **kw):
        """Execute select statement and return first row.

        If there are no result rows, raises `NoSuchRowError`. If there is more
        than one result row, raises `NoUniqueValueError`.
        """

        res = self.conn.cursor().execute(*a, **kw)
        try:
            row = next(res)
        except StopIteration:
            raise NoSuchRowError()
        try:
            next(res)
        except StopIteration:
            # Fine, we only wanted one row
            pass
        else:
            # Finish the active SQL statement
            res.close()
            raise NoUniqueValueError()

        return row

    def last_rowid(self):
        """Return rowid most recently inserted in the current thread"""

        return self.conn.last_insert_rowid()

    def changes(self):
        """Return number of rows affected by most recent sql statement"""

        return self.conn.changes()


class BatchedTransactionManager:
    def __init__(self, conn: Connection, dt_target: float) -> None:
        self.conn = conn
        self.dt_target = dt_target
        self.in_tx = False
        self.stamp = None
        self.batch_size = 100

    def __enter__(self):
        self.start_batch()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.in_tx:
            self.conn.execute('COMMIT')
            self.in_tx = False

    def start_batch(self):
        self.stamp = time.time()
        self.conn.execute('BEGIN TRANSACTION')
        self.in_tx = True

    def finish_batch(self, processed: int):
        '''Return new batch size'''

        if not self.in_tx:
            raise RuntimeError('No active batch')

        self.conn.execute('COMMIT')
        self.in_tx = False
        self.conn.checkpoint()
        dt = time.time() - self.stamp
        self.batch_size = max(
            100, int(processed * self.dt_target / dt)  # make sure to make some progress
        )
        log.debug(
            'Last batch of %d took %.2f s, adjusting batch_size to %d and yielding',
            processed,
            dt,
            self.batch_size,
        )


def create_tables(conn):
    # Table of storage objects
    # Refcount is included for performance reasons
    # phys_size is the number of bytes stored in the backend (i.e., after compression).
    # length is the logical size in the filesystem.
    # phys_size == -1 indicates block has not been uploaded yet
    conn.execute(
        """
    CREATE TABLE objects (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        hash        BLOB(32) UNIQUE,
        refcount    INT NOT NULL,
        phys_size   INT NOT NULL,
        length      INT NOT NULL
    )"""
    )

    # Table with filesystem metadata
    # The number of links `refcount` to an inode can in theory
    # be determined from the `contents` table. However, managing
    # this separately should be significantly faster (the information
    # is required for every getattr!)
    conn.execute(
        """
    CREATE TABLE inodes (
        -- id has to specified *exactly* as follows to become
        -- an alias for the rowid.
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        uid       INT NOT NULL,
        gid       INT NOT NULL,
        mode      INT NOT NULL,
        mtime_ns  INT NOT NULL,
        atime_ns  INT NOT NULL,
        ctime_ns  INT NOT NULL,
        refcount  INT NOT NULL,
        size      INT NOT NULL DEFAULT 0,
        rdev      INT NOT NULL DEFAULT 0,
        locked    BOOLEAN NOT NULL DEFAULT 0
    )"""
    )

    # Further Blocks used by inode (blockno >= 1)
    conn.execute(
        """
    CREATE TABLE inode_blocks (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        blockno   INT NOT NULL,
        obj_id    INTEGER NOT NULL REFERENCES objects(id),
        PRIMARY KEY (inode, blockno)
    )"""
    )

    # Symlinks
    conn.execute(
        """
    CREATE TABLE symlink_targets (
        inode     INTEGER PRIMARY KEY REFERENCES inodes(id),
        target    BLOB NOT NULL
    )"""
    )

    # Names of file system objects
    conn.execute(
        """
    CREATE TABLE names (
        id     INTEGER PRIMARY KEY,
        name   BLOB NOT NULL,
        refcount  INT NOT NULL,
        UNIQUE (name)
    )"""
    )

    # Table of filesystem objects
    # rowid is used by readdir() to restart at the correct position
    conn.execute(
        """
    CREATE TABLE contents (
        rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
        name_id   INT NOT NULL REFERENCES names(id),
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id),

        UNIQUE (parent_inode, name_id)
    )"""
    )

    # Extended attributes
    conn.execute(
        """
    CREATE TABLE ext_attributes (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        name_id   INTEGER NOT NULL REFERENCES names(id),
        value     BLOB NOT NULL,

        PRIMARY KEY (inode, name_id)
    )"""
    )

    # Shortcuts
    conn.execute(
        """
    CREATE VIEW contents_v AS
    SELECT * FROM contents JOIN names ON names.id = name_id
    """
    )
    conn.execute(
        """
    CREATE VIEW ext_attributes_v AS
    SELECT * FROM ext_attributes JOIN names ON names.id = name_id
    """
    )


class DatabaseChecksumError(RuntimeError):
    '''Raised when the downloaded database does not have the expected checksum'''

    def __init__(self, name: str, expected: str, actual: str):
        super().__init__()
        self.name = name
        self.expected = expected
        self.actual = actual

    def __str__(self):
        return f'File {self.name} has checksum {self.actual}, expected {self.expected}'


def get_block_objects(backend: AbstractBackend) -> Dict[int, List[int]]:
    '''Get list of objects holding versions of each block'''

    block_list = collections.defaultdict(list)
    log.debug('Scanning metadata objects...')
    for obj in backend.list('s3ql_metadata_'):
        hit = METADATA_OBJ_RE.match(obj)
        if not hit:
            # _bak objects are leftovers from previous filesystem revisions.
            if not obj.startswith('s3ql_metadata_bak_'):
                log.warning('Unexpected object in backend: %s, ignoring', obj)
            continue
        blockno = int(hit.group(1), base=16)
        seq_no = int(hit.group(2), base=16)
        block_list[blockno].append(seq_no)

    for l in block_list.values():
        l.sort()

    return block_list


def download_metadata(
    backend: AbstractBackend,
    db_file: str,
    params: FsAttributes,
    failsafe=False,
):
    '''Download metadata from backend into *db_file*

    If *failsafe* is True, do not truncate file and do not verify
    checksum.
    '''

    blocksize = params.metadata_block_size
    file_size = params.db_size

    # Determine which objects hold the most recent database snapshot
    block_list = get_block_objects(backend)
    total = len(block_list)
    processed = 0

    with open(db_file, 'w+b', buffering=0) as fh:
        for blockno, candidates in block_list.items():
            off = blockno * blocksize
            if off >= file_size and not failsafe:
                log.debug('download_metadata: skipping obsolete block %d', blockno)
                continue
            try:
                seq_no = first_le_than(candidates, params.seq_no)
            except ValueError:
                # In filesafe mode we do not have accurate file size information, so
                # the file may be shorter than this offset.
                if not failsafe:
                    raise

            processed += 1
            log.info(
                'Downloaded %d/%d metadata blocks (%d%%)',
                processed,
                total,
                processed * 100 / total,
                extra={'rate_limit': 1, 'update_console': True, 'is_last': processed == total},
            )
            obj = METADATA_OBJ_NAME % (blockno, seq_no)
            fh.seek(off)
            backend.readinto_fh(obj, fh)

        if not failsafe:
            log.debug('download_metadata: truncating file to %d bytes', file_size)
            fh.truncate(file_size)

            log.debug('Calculating metadata checksum...')
            digest = sha256_fh(fh).hexdigest()
            log.debug('download_metadata: digest is %s', digest)
            if params.db_md5 != digest:
                raise DatabaseChecksumError(db_file, params.db_md5, digest)

    return Connection(db_file, blocksize)


def first_le_than(l: List[int], threshold: int):
    '''Return first element of *l* less or equal than *threshold*

    Assumes that *l* is sorted in ascending order
    '''

    for e in l[::-1]:
        if e <= threshold:
            return e

    raise ValueError('No element below %d in list of length %d' % (threshold, len(l)))


def get_available_seq_nos(backend: AbstractBackend) -> List[int]:
    nos = []
    for obj in backend.list('s3ql_params_'):
        hit = re.match('^s3ql_params_([0-9a-f]+)$', obj)
        if not hit:
            log.warning('Unexpected object in backend: %s, ignoring', obj)
            continue
        seq_no = int(hit.group(1), base=16)
        nos.append(seq_no)
    return nos


def expire_objects(backend, versions_to_keep=32):
    '''Delete metadata objects that are no longer needed'''

    log.debug('Expiring old metadata backups...')
    block_list = get_block_objects(backend)
    to_remove = []

    metadata_objs = get_available_seq_nos(backend)
    metadata_objs.sort()
    seq_to_keep = metadata_objs[-versions_to_keep:]
    to_remove = ['s3ql_params_%010x' % x for x in metadata_objs[:-versions_to_keep]]

    # Determine which objects we still need
    to_keep = collections.defaultdict(set)
    for seq_no in seq_to_keep:
        # We'll have to figure out what we want to do here when one of the objects
        # has an older filesystem revision...
        params = read_remote_params(backend, seq_no)
        blocksize = params.metadata_block_size

        # math.ceil() calculates the number of blocks we need. To convert to
        # the index, need to subtract one (with one blocks, the max index is 0).
        max_blockno = math.ceil(params.db_size / blocksize) - 1

        for blockno, candidates in block_list.items():
            log.debug('block %d has candidates %s', blockno, candidates)
            if blockno > max_blockno:
                continue
            to_keep[blockno].add(first_le_than(candidates, seq_no))

    # Remove what's no longer needed
    for blockno, candidates in block_list.items():
        for seq_no in candidates:
            if seq_no in to_keep[blockno]:
                continue
            to_remove.append(METADATA_OBJ_NAME % (blockno, seq_no))
    backend.delete_multi(to_remove)


def upload_metadata(
    backend: AbstractBackend,
    db: Connection,
    params: FsAttributes,
    incremental: bool = True,
    update_params: bool = True,
) -> None:
    '''Upload metadata to backend

    If *incremental* is false, upload all objects (rather than just known dirty ones).

    If *update_params* is false, do not calculate checksum and update *params* with current database
    size and checksum.
    '''

    blocksize = db.blocksize
    log.info('Uploading metadata...')
    with open(db.file, 'rb', buffering=0) as fh:
        db_size = fh.seek(0, os.SEEK_END)
        if incremental:
            next_dirty_block = db.dirty_blocks.get_block
            total = db.dirty_blocks.get_count()
        else:
            total = math.ceil(db_size / blocksize)
            all_blocks = iter(range(0, total))

            def next_dirty_block():
                try:
                    return next(all_blocks)
                except StopIteration:
                    raise KeyError()

        processed = 0
        while True:
            try:
                blockno = next_dirty_block()
            except KeyError:
                break

            log.debug('Uploading block %d', blockno)
            processed += 1
            log.info(
                'Uploaded %d out of ~%d dirty blocks (%d%%)',
                processed,
                total,
                processed * 100 / total,
                extra={'rate_limit': 1, 'update_console': True, 'is_last': processed == total},
            )
            obj = METADATA_OBJ_NAME % (blockno, params.seq_no)
            fh.seek(blockno * blocksize)
            backend.write_fh(obj, fh, len_=blocksize)

        if not update_params:
            return

        log.info('Calculating metadata checksum...')
        params.db_size = db_size
        digest = sha256_fh(fh).hexdigest()
        log.debug('upload_metadata: digest is %s', digest)
        params.db_md5 = digest


def upload_params(backend: AbstractBackend, params: FsAttributes):
    buf = params.serialize()
    backend['s3ql_params'] = buf
    backend['s3ql_params_%010x' % params.seq_no] = buf


def write_params(cachepath: str, params: FsAttributes):
    buf = params.serialize()
    filename = cachepath + '.params'
    tmpname = filename + '.tmp'
    with open(tmpname, 'wb') as fh:
        fh.write(buf)
        # Fsync to make sure that the updated sequence number is committed to
        # disk. Otherwise, a crash immediately after mount could result in both
        # the local and remote metadata appearing to be out of date.
        fh.flush()
        os.fsync(fh.fileno())

    # we need to flush the dirents too.
    # stackoverflow.com/a/41362774
    # stackoverflow.com/a/5809073
    os.rename(tmpname, filename)
    dirfd = os.open(os.path.dirname(filename), os.O_DIRECTORY)
    try:
        os.fsync(dirfd)
    finally:
        os.close(dirfd)


def read_cached_params(cachepath: str, min_seq: Optional[int] = None) -> Optional[FsAttributes]:
    filename = cachepath + '.params'
    if not os.path.exists(filename):
        return None

    assert os.path.exists(cachepath + '.db')
    with open(filename, 'rb') as fh:
        buf = fh.read()

    try:
        params = FsAttributes.deserialize(buf, min_seq=min_seq)
    except OutdatedData:
        log.info('Ignoring locally cached metadata (outdated).')
        return None

    return params


def read_remote_params(backend, seq_no: Optional[int] = None) -> FsAttributes:
    if seq_no is None:
        try:
            buf = backend['s3ql_params']
        except NoSuchObject:
            raise QuietError(
                'File system revision too old, please run `s3qladm upgrade` first.', exitcode=32
            )
    else:
        buf = backend['s3ql_params_%010x' % seq_no]
    params = FsAttributes.deserialize(buf)

    if seq_no:
        assert params.seq_no == seq_no

    return params


class NoUniqueValueError(Exception):
    '''Raised if get_val or get_row was called with a query
    that generated more than one result row.
    '''

    def __str__(self):
        return 'Query generated more than 1 result row'


class NoSuchRowError(Exception):
    '''Raised if the query did not produce any result rows'''

    def __str__(self):
        return 'Query produced 0 result rows'


class ResultSet:
    '''
    Provide iteration over encapsulated apsw cursor. Additionally,
    `ResultSet` instances may be used as context managers to terminate
    the query before all result rows have been retrieved.
    '''

    def __init__(self, cur):
        self.cur = cur

    def __next__(self):
        return next(self.cur)

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cur.close()

    def close(self):
        '''Terminate query'''

        self.cur.close()


# Load extension, needs a dummy connection
db = apsw.Connection(':memory:')
db.enableloadextension(True)
db.loadextension(sqlite3ext.__file__[:-3])
db.close()
