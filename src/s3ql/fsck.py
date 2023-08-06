'''
fsck.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import atexit
import itertools
import logging
import os
import re
import shutil
import stat
import sys
import textwrap
import time
from datetime import datetime
from os.path import basename
from tempfile import NamedTemporaryFile

import apsw

from . import CTRL_INODE, CURRENT_FS_REV, ROOT_INODE
from .backends.common import NoSuchObject
from .backends.comprenc import ComprencBackend
from .backends.local import Backend as LocalBackend
from .common import (
    get_backend,
    get_path,
    inode_for_path,
    is_mounted,
    sha256_fh,
    thaw_basic_mapping,
    time_ns,
)
from .database import (
    Connection,
    FsAttributes,
    NoSuchRowError,
    download_metadata,
    expire_objects,
    get_available_seq_nos,
    read_cached_params,
    read_remote_params,
    upload_metadata,
    upload_params,
    write_params,
)
from .logging import QuietError, setup_logging, setup_warnings
from .parse_args import ArgumentParser

log = logging.getLogger(__name__)

S_IFMT = (
    stat.S_IFDIR
    | stat.S_IFREG
    | stat.S_IFSOCK
    | stat.S_IFBLK
    | stat.S_IFCHR
    | stat.S_IFIFO
    | stat.S_IFLNK
)


class Fsck:
    def __init__(self, cachedir_, backend_, param, conn):
        self.cachedir = cachedir_
        self.backend = backend_
        self.expect_errors = False
        self.found_errors = False
        self.uncorrectable_errors = False
        self.max_obj_size = param.data_block_size
        self.conn = conn

        # Set of objects that have been unlinked by check_cache.
        # check_object_refcounts() will not report errors if these objects still
        # exist even though they have refcount=0
        self.unlinked_objects = set()

    def check(self, check_cache=True):
        """Check file system

        If *check_cache* is False, assume that all cache files are clean (aka
        have been uploaded to the backend). If *check_cache* is ``keep``, do not
        remove cache files on exit.

        Sets instance variable `found_errors`.
        """

        # Create indices required for reference checking
        log.info('Creating temporary extra indices...')
        for idx in ('tmp1', 'tmp2', 'tmp3', 'tmp4'):
            self.conn.execute('DROP INDEX IF EXISTS %s' % idx)
        self.conn.execute('CREATE INDEX tmp1 ON inode_blocks(obj_id)')
        self.conn.execute('CREATE INDEX tmp2 ON contents(inode)')
        self.conn.execute('CREATE INDEX tmp3 ON contents(name_id)')
        self.conn.execute('CREATE INDEX tmp4 ON ext_attributes(name_id)')
        try:
            self.check_lof()
            if check_cache:
                self.check_cache(check_cache == 'keep')
            self.check_names_refcount()

            self.check_contents_name()
            self.check_contents_inode()
            self.check_contents_parent_inode()

            self.check_objects_temp()
            self.check_objects_hash()
            self.check_objects_refcount()
            self.check_objects_id()
            self.check_objects_phys_size()
            # TODO: Are there any checks we could do on objects.length?

            self.check_inode_blocks_obj_id()
            self.check_inode_blocks_inode()

            self.check_inodes_refcount()
            self.check_inodes_size()

            self.check_ext_attributes_name()
            self.check_ext_attributes_inode()

            self.check_symlinks_inode()

            self.check_loops()
            self.check_unix()
            self.check_foreign_keys()
        finally:
            log.info('Dropping temporary indices...')
            for idx in ('tmp1', 'tmp2', 'tmp3', 'tmp4'):
                self.conn.execute('DROP INDEX %s' % idx)

    def log_error(self, *a, **kw):
        '''Log file system error if not expected'''

        if self.expect_errors:
            return log.info(*a, **kw)
        else:
            return log.warning(*a, **kw)

    def check_foreign_keys(self):
        '''Check for referential integrity

        Checks that all foreign keys in the SQLite tables actually resolve.
        This is necessary, because we disable runtime checking by SQLite
        for performance reasons.

        Note: any problems should have already been caught by the more
        specific checkers.
        '''

        log.info("Checking referential integrity...")

        for (table,) in self.conn.query("SELECT name FROM sqlite_master WHERE type='table'"):
            for row in self.conn.query('PRAGMA foreign_key_list(%s)' % table):
                sql_objs = {
                    'src_table': table,
                    'dst_table': row[2],
                    'src_col': row[3],
                    'dst_col': row[4],
                    'dbf': self.conn.fixes['dbf'],
                }

                for (val,) in self.conn.query(
                    'SELECT %(src_table)s.%(src_col)s '
                    'FROM %(src_table)s LEFT JOIN %(dst_table)s '
                    'ON %(dbf)s%(src_table)s.%(src_col)s = %(dbf)s%(dst_table)s.%(dst_col)s '
                    'WHERE %(dbf)s%(dst_table)s.%(dst_col)s IS NULL '
                    'AND %(src_table)s.%(src_col)s IS NOT NULL' % sql_objs
                ):
                    self.found_errors = True
                    sql_objs['val'] = val
                    self.log_error(
                        '%(src_table)s.%(src_col)s refers to non-existing key %(val)s '
                        'in %(dst_table)s.%(dst_col)s, deleting.',
                        sql_objs,
                    )
                    log.error('This should not happen, please report a bug.')
                    self.uncorrectable_errors = True

    def check_cache(self, keep_cache=False):
        """Commit uncommitted cache files"""

        log.info("Checking for dirty cache objects...")
        if not os.path.exists(self.cachedir):
            return
        candidates = os.listdir(self.cachedir)

        total = len(candidates)
        for i, filename in enumerate(candidates):
            i += 1  # start at 1

            log.info(
                'Processed %d/%d dirty cache objects (%d%%)',
                i,
                total,
                i / total * 100,
                extra={'rate_limit': 1, 'update_console': True, 'is_last': i == total},
            )

            match = re.match('^(\\d+)-(\\d+)$', filename)
            if match:
                inode = int(match.group(1))
                blockno = int(match.group(2))
            elif re.match('^(\\d+)-(\\d+)\\.tmp$', filename):
                # Temporary file created when downloading object
                self.found_errors = True
                self.log_error("Removing leftover temporary file: " + filename)
                os.unlink(os.path.join(self.cachedir, filename))
                continue
            else:
                raise RuntimeError('Strange file in cache directory: %s' % filename)

            # Calculate block checksum
            with open(os.path.join(self.cachedir, filename), "rb") as fh:
                size = os.fstat(fh.fileno()).st_size
                hash_should = sha256_fh(fh).digest()
            log.debug('%s has checksum %s', filename, hash_should)

            # Check if stored object has same checksum
            try:
                (obj_id, hash_is) = self.conn.get_row(
                    'SELECT obj_id, hash '
                    'FROM inode_blocks JOIN objects ON obj_id = objects.id '
                    'WHERE inode=? AND blockno=?',
                    (
                        inode,
                        blockno,
                    ),
                )
            except NoSuchRowError:
                pass
            else:
                if hash_should == hash_is:
                    if not keep_cache:
                        os.unlink(os.path.join(self.cachedir, filename))
                    continue

            self.found_errors = True
            self.log_error("Writing dirty block %d of inode %d to backend", blockno, inode)

            try:
                obj_id = self.conn.get_val('SELECT id FROM objects WHERE hash=?', (hash_should,))

            except NoSuchRowError:
                obj_id = self.conn.rowid(
                    'INSERT INTO objects (refcount, phys_size, length, hash) '
                    'VALUES(1, -1, ?, ?)',
                    (size, hash_should),
                )

                with open(os.path.join(self.cachedir, filename), "rb") as fh:
                    obj_size = self.backend.write_fh('s3ql_data_%d' % obj_id, fh)

                self.conn.execute('UPDATE objects SET phys_size=? WHERE id=?', (obj_size, obj_id))
            else:
                self.conn.execute('UPDATE objects SET refcount=refcount+1 WHERE id=?', (obj_id,))

            try:
                old_obj_id = self.conn.get_val(
                    'SELECT obj_id FROM inode_blocks WHERE inode=? AND blockno=?',
                    (inode, blockno),
                )
            except NoSuchRowError:
                self.conn.execute(
                    'INSERT INTO inode_blocks (obj_id, inode, blockno) VALUES(?,?,?)',
                    (obj_id, inode, blockno),
                )
            else:
                self.conn.execute(
                    'UPDATE inode_blocks SET obj_id=? WHERE inode=? AND blockno=?',
                    (obj_id, inode, blockno),
                )

                # Decrease the refcount, but don't take any further action
                # because the reference count might be wrong
                self.conn.execute(
                    'UPDATE objects SET refcount=refcount-1 WHERE id=?', (old_obj_id,)
                )
                self.unlinked_objects.add(old_obj_id)

            if not keep_cache:
                os.unlink(os.path.join(self.cachedir, filename))

    def check_lof(self):
        """Ensure that there is a lost+found directory"""

        log.info('Checking lost+found...')

        now_ns = time_ns()
        try:
            (inode_l, name_id) = self.conn.get_row(
                "SELECT inode, name_id FROM contents_v WHERE name=? AND parent_inode=?",
                (b"lost+found", ROOT_INODE),
            )

        except NoSuchRowError:
            self.found_errors = True
            try:
                # A bug in sqlite made fsck.s3ql move lost+found into lost+found. We just move it back to the root.
                (inode_l, name_id) = self.conn.get_row(
                    "SELECT inode, name_id FROM contents_v WHERE name=? AND parent_inode=inode",
                    (b"lost+found",),
                )
                self.log_error("Moving lost+found directory to root")
                self.conn.execute(
                    "UPDATE contents SET parent_inode = ? WHERE inode = ?", (ROOT_INODE, inode_l)
                )
            except NoSuchRowError:
                self.log_error("Recreating missing lost+found directory")
                inode_l = self.create_inode(
                    mode=stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                    atime_ns=now_ns,
                    ctime_ns=now_ns,
                    mtime_ns=now_ns,
                    refcount=1,
                )
                self.conn.execute(
                    "INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)",
                    (self._add_name(b"lost+found"), inode_l, ROOT_INODE),
                )

        mode = self.conn.get_val('SELECT mode FROM inodes WHERE id=?', (inode_l,))
        if not stat.S_ISDIR(mode):
            self.found_errors = True
            self.log_error(
                '/lost+found is not a directory! Old entry will be saved as '
                '/lost+found/inode-%s*',
                inode_l,
            )
            # We leave the old inode unassociated, so that it will be added
            # to lost+found later on.
            inode_l = self.create_inode(
                mode=stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                atime_ns=now_ns,
                ctime_ns=now_ns,
                mtime_ns=now_ns,
                refcount=1,
            )
            self.conn.execute(
                'UPDATE contents SET inode=? WHERE name_id=? AND parent_inode=?',
                (inode_l, name_id, ROOT_INODE),
            )

    def check_contents_name(self):
        """Check contents.name_id"""

        log.info('Checking contents (names)...')

        for rowid, name_id, inode_p, inode in self.conn.query(
            'SELECT contents.rowid, name_id, parent_inode, inode '
            'FROM contents LEFT JOIN names ON %(dbf)sname_id = %(dbf)snames.id '
            'WHERE %(dbf)snames.id IS NULL' % self.conn.fixes
        ):
            self.found_errors = True
            try:
                path = get_path(inode_p, self.conn)[1:]
            except NoSuchRowError:
                newname = ('-%d' % inode).encode()
            else:
                newname = escape(path) + ('-%d' % inode).encode()
            (id_p_new, newname) = self.resolve_free(b"/lost+found", newname)

            self.log_error(
                'Content entry for inode %d refers to non-existing name with id %d, '
                'moving to /lost+found/%s',
                inode,
                name_id,
                to_str(newname),
            )

            self.conn.execute(
                'UPDATE contents SET name_id=?, parent_inode=? WHERE rowid=?',
                (self._add_name(newname), id_p_new, rowid),
            )

    def check_contents_parent_inode(self):
        """Check contents.parent_inode"""

        log.info('Checking contents (parent inodes)...')

        for rowid, inode_p, name_id in self.conn.query(
            'SELECT contents.rowid, parent_inode, name_id '
            'FROM contents LEFT JOIN inodes '
            'ON %(dbf)sparent_inode = %(dbf)sinodes.id '
            'WHERE %(dbf)sinodes.id IS NULL' % self.conn.fixes
        ):
            self.found_errors = True
            name = self.conn.get_val('SELECT name FROM names WHERE id = ?', (name_id,))
            (id_p_new, newname) = self.resolve_free(
                b"/lost+found", ('[%d]-%s' % (inode_p, name)).encode()
            )

            self.log_error(
                'Parent inode %d for "%s" vanished, moving to /lost+found', inode_p, to_str(name)
            )
            self._del_name(name_id)
            self.conn.execute(
                'UPDATE contents SET name_id=?, parent_inode=? WHERE rowid=?',
                (self._add_name(newname), id_p_new, rowid),
            )

    def check_contents_inode(self):
        """Check contents.inode"""

        log.info('Checking contents (inodes)...')

        to_delete = list()
        for rowid, inode_p, inode, name_id in self.conn.query(
            'SELECT contents.rowid, parent_inode, inode, '
            'name_id FROM contents LEFT JOIN inodes '
            'ON %(dbf)sinode = %(dbf)sinodes.id '
            'WHERE %(dbf)sinodes.id IS NULL' % self.conn.fixes
        ):
            self.found_errors = True
            try:
                path = get_path(inode, self.conn)[1:]
            except NoSuchRowError:
                path = '[inode %d, parent %d]' % (inode, inode_p)

            self.log_error('Inode for %s vanished, deleting', to_str(path))
            self._del_name(name_id)
            to_delete.append(rowid)

        for rowid in to_delete:
            self.conn.execute('DELETE FROM contents WHERE rowid=?', (rowid,))

    def check_ext_attributes_name(self):
        """Check ext_attributes.name_id"""

        log.info('Checking extended attributes (names)...')

        for rowid, name_id, inode in self.conn.query(
            'SELECT ext_attributes.rowid, name_id, inode '
            'FROM ext_attributes LEFT JOIN names '
            'ON %(dbf)sname_id = %(dbf)snames.id '
            'WHERE %(dbf)snames.id IS NULL' % self.conn.fixes
        ):
            self.found_errors = True
            for name, id_p in self.conn.query(
                'SELECT name, parent_inode FROM contents_v WHERE inode=?', (inode,)
            ):
                path = get_path(id_p, self.conn, name)
                self.log_error(
                    'Extended attribute %d of %s refers to non-existing name %d, renaming..',
                    rowid,
                    to_str(path),
                    name_id,
                )

            while True:
                name_id = self._add_name('lost+found_%d' % rowid)
                if not self.conn.has_val(
                    "SELECT 1 FROM ext_attributes WHERE name_id=? AND inode=?", (name_id, inode)
                ):
                    self.conn.execute(
                        'UPDATE ext_attributes SET name_id=? WHERE rowid=?', (name_id, rowid)
                    )
                    break
                self._del_name('lost+found_%d' % rowid)
                rowid += 1

    def check_ext_attributes_inode(self):
        """Check ext_attributes.inode"""

        log.info('Checking extended attributes (inodes)...')

        to_delete = list()
        for rowid, inode, name_id in self.conn.query(
            'SELECT ext_attributes.rowid, inode, name_id '
            'FROM ext_attributes LEFT JOIN inodes '
            'ON %(dbf)sinode = %(dbf)sinodes.id '
            'WHERE %(dbf)sinodes.id IS NULL' % self.conn.fixes
        ):
            self.found_errors = True
            self.log_error(
                'Extended attribute %d refers to non-existing inode %d, deleting', rowid, inode
            )
            to_delete.append(rowid)
            self._del_name(name_id)

        for rowid in to_delete:
            self.conn.execute('DELETE FROM ext_attributes WHERE rowid=?', (rowid,))

    def check_loops(self):
        """Ensure that all directories can be reached from root"""

        log.info('Checking directory reachability...')

        self.conn.execute(
            'CREATE TEMPORARY TABLE loopcheck (inode INTEGER PRIMARY KEY, parent_inode INTEGER)'
        )
        self.conn.execute('CREATE INDEX ix_loopcheck_parent_inode ON loopcheck(parent_inode)')
        self.conn.execute(
            'INSERT INTO loopcheck (inode) '
            'SELECT parent_inode FROM contents GROUP BY parent_inode'
        )
        self.conn.execute(
            'UPDATE loopcheck SET parent_inode = '
            '(SELECT contents.parent_inode FROM contents '
            ' WHERE contents.inode = loopcheck.inode LIMIT 1)'
        )
        self.conn.execute('CREATE TEMPORARY TABLE loopcheck2 (inode INTEGER PRIMARY KEY)')
        self.conn.execute('INSERT INTO loopcheck2 (inode) SELECT inode FROM loopcheck')

        def delete_tree(inode_p):
            for (inode,) in self.conn.query(
                "SELECT inode FROM loopcheck WHERE parent_inode=?", (inode_p,)
            ):
                delete_tree(inode)
            self.conn.execute('DELETE FROM loopcheck2 WHERE inode=?', (inode_p,))

        root = ROOT_INODE
        while True:
            delete_tree(root)

            if not self.conn.has_val("SELECT 1 FROM loopcheck2"):
                break

            self.found_errors = True

            # Try obvious culprits first
            try:
                inode = self.conn.get_val(
                    'SELECT loopcheck2.inode FROM loopcheck2 JOIN contents '
                    'ON loopcheck2.inode = contents.inode '
                    'WHERE parent_inode = contents.inode LIMIT 1'
                )
            except NoSuchRowError:
                inode = self.conn.get_val("SELECT inode FROM loopcheck2 ORDER BY inode ASC LIMIT 1")

            (name, name_id) = self.conn.get_row(
                "SELECT name, name_id FROM contents_v WHERE inode=? LIMIT 1", (inode,)
            )
            (id_p, name) = self.resolve_free(b"/lost+found", name)

            self.log_error(
                "Found unreachable filesystem entries, re-anchoring %s [%d] in /lost+found",
                to_str(name),
                inode,
            )
            self.conn.execute(
                'UPDATE contents SET parent_inode=?, name_id=? WHERE inode=? AND name_id=?',
                (id_p, self._add_name(name), inode, name_id),
            )
            self._del_name(name_id)
            self.conn.execute('UPDATE loopcheck SET parent_inode=? WHERE inode=?', (id_p, inode))
            root = inode

        self.conn.execute("DROP TABLE loopcheck")
        self.conn.execute("DROP TABLE loopcheck2")

    def check_inodes_size(self):
        """Check inodes.size"""

        log.info('Checking inodes (sizes)...')

        self.conn.execute(
            'CREATE TEMPORARY TABLE min_sizes '
            '(id INTEGER PRIMARY KEY, min_size INTEGER NOT NULL)'
        )
        try:
            self.conn.execute(
                '''
            INSERT INTO min_sizes (id, min_size)
            SELECT inode, MAX(blockno * ? + length)
            FROM inode_blocks JOIN objects ON obj_id == objects.id
            GROUP BY inode''',
                (self.max_obj_size,),
            )

            self.conn.execute(
                '''
               CREATE TEMPORARY TABLE wrong_sizes AS
               SELECT id, size, min_size
                 FROM inodes JOIN min_sizes USING (id)
                WHERE size < min_size'''
            )

            for id_, size_old, size in self.conn.query('SELECT * FROM wrong_sizes'):
                self.found_errors = True
                self.log_error(
                    "Size of inode %d (%s) does not agree with number of blocks, "
                    "setting from %d to %d",
                    id_,
                    to_str(get_path(id_, self.conn)),
                    size_old,
                    size,
                )
                self.conn.execute("UPDATE inodes SET size=? WHERE id=?", (size, id_))
        finally:
            self.conn.execute('DROP TABLE min_sizes')
            self.conn.execute('DROP TABLE IF EXISTS wrong_sizes')

    def check_inodes_refcount(self):
        """Check inodes.refcount"""

        log.info('Checking inodes (refcounts)...')

        self.conn.execute(
            'CREATE TEMPORARY TABLE refcounts '
            '(id INTEGER PRIMARY KEY, refcount INTEGER NOT NULL)'
        )
        try:
            self.conn.execute(
                'INSERT INTO refcounts (id, refcount) '
                'SELECT inode, COUNT(name_id) FROM contents GROUP BY inode'
            )

            self.conn.execute(
                '''
               CREATE TEMPORARY TABLE wrong_refcounts AS
               SELECT inodes.id, refcounts.refcount, inodes.refcount
                 FROM inodes LEFT JOIN refcounts ON %(dbf)sinodes.id = %(dbf)srefcounts.id
                WHERE inodes.refcount != refcounts.refcount
                   OR %(dbf)srefcounts.refcount IS NULL'''
                % self.conn.fixes
            )

            for id_, cnt, cnt_old in self.conn.query('SELECT * FROM wrong_refcounts'):
                # No checks for root and control
                if id_ in (ROOT_INODE, CTRL_INODE):
                    continue

                self.found_errors = True
                if cnt is None:
                    (id_p, name) = self.resolve_free(b"/lost+found", ("inode-%d" % id_).encode())
                    self.log_error(
                        "Inode %d not referenced, adding as /lost+found/%s", id_, to_str(name)
                    )
                    self.conn.execute(
                        "INSERT INTO contents (name_id, inode, parent_inode) VALUES (?,?,?)",
                        (self._add_name(basename(name)), id_, id_p),
                    )
                    self.conn.execute("UPDATE inodes SET refcount=? WHERE id=?", (1, id_))

                else:
                    self.log_error(
                        "Inode %d (%s) has wrong reference count, setting from %d to %d",
                        id_,
                        to_str(get_path(id_, self.conn)),
                        cnt_old,
                        cnt,
                    )
                    self.conn.execute("UPDATE inodes SET refcount=? WHERE id=?", (cnt, id_))
        finally:
            self.conn.execute('DROP TABLE refcounts')
            self.conn.execute('DROP TABLE IF EXISTS wrong_refcounts')

    def check_inode_blocks_inode(self):
        """Check inode_blocks.inode"""

        log.info('Checking inode_blocks.inode...')

        to_delete = list()
        for rowid, inode, obj_id in self.conn.query(
            'SELECT inode_blocks.rowid, inode, obj_id '
            'FROM inode_blocks LEFT JOIN inodes '
            'ON %(dbf)sinode = %(dbf)sinodes.id '
            'WHERE %(dbf)sinodes.id IS NULL' % self.conn.fixes
        ):
            self.found_errors = True
            self.log_error(
                'Inode-block mapping %d refers to non-existing inode %d, deleting', rowid, inode
            )
            to_delete.append(rowid)

            # Decrease the refcount, but don't take any action because the
            # reference count might be wrong
            self.conn.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?', (obj_id,))
            self.unlinked_objects.add(obj_id)

        for rowid in to_delete:
            self.conn.execute('DELETE FROM inode_blocks WHERE rowid=?', (rowid,))

    def check_inode_blocks_obj_id(self):
        """Check inode_blocks.obj_id"""

        log.info('Checking block-object mapping...')

        to_delete = list()
        for rowid, obj_id, inode in self.conn.query(
            'SELECT inode_blocks.rowid, obj_id, inode FROM inode_blocks '
            'LEFT JOIN objects ON %(dbf)sobj_id = %(dbf)sobjects.id '
            'WHERE %(dbf)sobjects.id IS NULL' % self.conn.fixes
        ):
            self.found_errors = True
            self.log_error(
                'Inode-block mapping for inode %d refers to non-existing object %d', inode, obj_id
            )
            to_delete.append(rowid)

            affected_entries = list(
                self.conn.query(
                    'SELECT name, name_id, parent_inode FROM contents_v WHERE inode=?', (inode,)
                )
            )
            for name, name_id, id_p in affected_entries:
                path = get_path(id_p, self.conn, name)
                self.log_error("File may lack data, moved to /lost+found: %s", to_str(path))
                (lof_id, newname) = self.resolve_free(b"/lost+found", escape(path))

                self.conn.execute(
                    'UPDATE contents SET name_id=?, parent_inode=? '
                    'WHERE name_id=? AND parent_inode=?',
                    (self._add_name(newname), lof_id, name_id, id_p),
                )
                self._del_name(name_id)

        for rowid in to_delete:
            self.conn.execute('DELETE FROM inode_blocks WHERE rowid=?', (rowid,))

    def check_symlinks_inode(self):
        """Check symlinks.inode"""

        log.info('Checking symlinks (inodes)...')

        to_delete = list()
        for rowid, inode in self.conn.query(
            'SELECT symlink_targets.rowid, inode FROM symlink_targets '
            'LEFT JOIN inodes ON %(dbf)sinode = %(dbf)sinodes.id '
            'WHERE %(dbf)sinodes.id IS NULL' % self.conn.fixes
        ):
            self.found_errors = True
            self.log_error('Symlink %d refers to non-existing inode %d, deleting', rowid, inode)
            to_delete.append(rowid)

        for rowid in to_delete:
            self.conn.execute('DELETE FROM symlink_targets WHERE rowid=?', (rowid,))

    def check_objects_hash(self):
        """Check objects.hash"""

        log.info('Checking objects.hash...')

        count = self.conn.get_val('SELECT COUNT(id) FROM objects WHERE hash IS NULL')
        if not count:
            return

        # This should only happen when there was an error during upload, so the
        # object must not have been stored correctly. We cannot just recalculate
        # the hash for the block, because then we may be modifying the contents
        # of the inode that refers to this block.
        self.found_errors = True
        self.log_error("No checksum for %d objects, removing from table...", count)
        self.conn.execute('DELETE FROM objects WHERE hash IS NULL')

        # Orphaned objects will be picked up by check_objects_id(), and missing
        # blocks handled by check_inode_blocks_obj_id().

    def create_inode(
        self,
        mode,
        uid=os.getuid(),
        gid=os.getgid(),
        mtime_ns=None,
        atime_ns=None,
        ctime_ns=None,
        refcount=None,
        size=0,
    ):
        '''Create inode'''

        id_ = self.conn.rowid(
            'INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,'
            'refcount,size) VALUES (?,?,?,?,?,?,?,?)',
            (mode, uid, gid, mtime_ns, atime_ns, ctime_ns, refcount, size),
        )

        return id_

    def check_names_refcount(self):
        """Check names.refcount"""

        log.info('Checking names (refcounts)...')

        self.conn.execute(
            'CREATE TEMPORARY TABLE refcounts '
            '(id INTEGER PRIMARY KEY, refcount INTEGER NOT NULL)'
        )
        try:
            self.conn.execute('INSERT INTO refcounts (id, refcount) SELECT id, 0 FROM names')
            self.conn.execute(
                'UPDATE refcounts SET refcount='
                '(SELECT COUNT(name_id) FROM contents WHERE name_id = refcounts.id)'
                '+ (SELECT COUNT(name_id) FROM ext_attributes '
                '   WHERE name_id = refcounts.id)'
            )

            self.conn.execute(
                '''
               CREATE TEMPORARY TABLE wrong_refcounts AS
               SELECT names.id, refcounts.refcount, names.refcount
                 FROM names LEFT JOIN refcounts ON %(dbf)snames.id = %(dbf)srefcounts.id
                WHERE names.refcount != refcounts.refcount
                   OR %(dbf)srefcounts.refcount IS NULL'''
                % self.conn.fixes
            )

            for id_, cnt, cnt_old in self.conn.query('SELECT * FROM wrong_refcounts'):
                self.found_errors = True
                if cnt is None:
                    self.log_error(
                        "Name %d not referenced, removing (old refcount: %d)", id_, cnt_old
                    )
                    self.conn.execute('DELETE FROM names WHERE id=?', (id_,))
                else:
                    self.log_error(
                        "Name %d has wrong reference count, setting from %d to %d",
                        id_,
                        cnt_old,
                        cnt,
                    )
                    self.conn.execute("UPDATE names SET refcount=? WHERE id=?", (cnt, id_))
        finally:
            self.conn.execute('DROP TABLE refcounts')
            self.conn.execute('DROP TABLE IF EXISTS wrong_refcounts')

    def check_unix(self):
        """Check if file systems for agreement with UNIX conventions

        This means:
        - Only directories should have child entries
        - Only regular files should have data blocks and a size
        - Only symlinks should have a target
        - Only devices should have a device number
        - symlink size is length of target
        - names are not longer than 255 bytes
        - All directory entries have a valid mode

        Note that none of this is enforced by S3QL. However, as long as S3QL
        only communicates with the UNIX FUSE module, none of the above should
        happen (and if it does, it would probably confuse the system quite a
        lot).
        """

        log.info('Checking unix conventions...')

        for inode, mode, size, target, rdev in self.conn.query(
            "SELECT id, mode, size, target, rdev "
            "FROM inodes LEFT JOIN symlink_targets ON id = inode"
        ):
            has_children = self.conn.has_val(
                'SELECT 1 FROM contents WHERE parent_inode=? LIMIT 1', (inode,)
            )

            if stat.S_IFMT(mode) == 0:
                if has_children:
                    mode = mode | stat.S_IFDIR
                    made_to = 'directory'
                else:
                    mode = mode | stat.S_IFREG
                    made_to = 'regular file'

                self.found_errors = True
                self.log_error(
                    'Inode %d (%s): directory entry has no type, changed to %s.',
                    inode,
                    to_str(get_path(inode, self.conn)),
                    made_to,
                )
                self.conn.execute('UPDATE inodes SET mode=? WHERE id=?', (mode, inode))

            if stat.S_ISLNK(mode) and target is None:
                self.found_errors = True
                self.log_error(
                    'Inode %d (%s): symlink does not have target. '
                    'This is probably going to confuse your system!',
                    inode,
                    to_str(get_path(inode, self.conn)),
                )

            if stat.S_ISLNK(mode) and target is not None and size != len(target):
                self.found_errors = True
                self.log_error(
                    'Inode %d (%s): symlink size (%d) does not agree with target '
                    'length (%d). This is probably going to confuse your system!',
                    inode,
                    to_str(get_path(inode, self.conn)),
                    size,
                    len(target),
                )

            if size != 0 and (
                not stat.S_ISREG(mode) and not stat.S_ISLNK(mode) and not stat.S_ISDIR(mode)
            ):
                self.found_errors = True
                self.log_error(
                    'Inode %d (%s) is not regular file but has non-zero size. '
                    'This is may confuse your system!',
                    inode,
                    to_str(get_path(inode, self.conn)),
                )

            if target is not None and not stat.S_ISLNK(mode):
                self.found_errors = True
                self.log_error(
                    'Inode %d (%s) is not symlink but has symlink target. '
                    'This is probably going to confuse your system!',
                    inode,
                    to_str(get_path(inode, self.conn)),
                )

            if rdev != 0 and not (stat.S_ISBLK(mode) or stat.S_ISCHR(mode)):
                self.found_errors = True
                self.log_error(
                    'Inode %d (%s) is not device but has device number. '
                    'This is probably going to confuse your system!',
                    inode,
                    to_str(get_path(inode, self.conn)),
                )

            if has_children and not stat.S_ISDIR(mode):
                self.found_errors = True
                self.log_error(
                    'Inode %d (%s) is not a directory but has child entries. '
                    'This is probably going to confuse your system!',
                    inode,
                    to_str(get_path(inode, self.conn)),
                )

            if not stat.S_ISREG(mode) and self.conn.has_val(
                'SELECT 1 FROM inode_blocks WHERE inode=?', (inode,)
            ):
                self.found_errors = True
                self.log_error(
                    'Inode %d (%s) is not a regular file but has data blocks. '
                    'This is probably going to confuse your system!',
                    inode,
                    to_str(get_path(inode, self.conn)),
                )

        for name, id_p in self.conn.query(
            'SELECT name, parent_inode FROM contents_v WHERE LENGTH(name) > 255'
        ):
            path = get_path(id_p, self.conn, name)
            self.log_error(
                'Entry name %s... in %s has more than 255 characters, this could cause problems',
                to_str(name[:40]),
                to_str(path[: -len(name)]),
            )
            self.found_errors = True

    def check_objects_refcount(self):
        """Check objects.refcount"""

        log.info('Checking objects (reference counts)...')

        self.conn.execute(
            'CREATE TEMPORARY TABLE refcounts '
            '(id INTEGER PRIMARY KEY, refcount INTEGER NOT NULL)'
        )
        try:
            self.conn.execute(
                'INSERT INTO refcounts (id, refcount) '
                'SELECT obj_id, COUNT(obj_id) FROM inode_blocks GROUP BY obj_id'
            )

            self.conn.execute(
                '''
               CREATE TEMPORARY TABLE wrong_refcounts AS
               SELECT objects.id, refcounts.refcount, objects.refcount
                 FROM objects LEFT JOIN refcounts ON %(dbf)sobjects.id = %(dbf)srefcounts.id
                WHERE objects.refcount != refcounts.refcount
                   OR %(dbf)srefcounts.refcount IS NULL'''
                % self.conn.fixes
            )

            for id_, cnt, cnt_old in self.conn.query('SELECT * FROM wrong_refcounts'):
                if cnt is None and id_ in self.unlinked_objects and cnt_old == 0:
                    # Object was unlinked by check_cache
                    self.conn.execute('DELETE FROM objects WHERE id=?', (id_,))
                else:
                    self.found_errors = True
                    self.log_error(
                        "Object %s has invalid refcount, setting from %d to %d",
                        id_,
                        cnt_old,
                        cnt or 0,
                    )

                    if cnt is not None:
                        self.conn.execute("UPDATE objects SET refcount=? WHERE id=?", (cnt, id_))
                    else:
                        # Orphaned object will be picked up by check_objects_id(), dangling
                        # references by check_inode_blocks_obj_id()
                        self.conn.execute('DELETE FROM objects WHERE id=?', (id_,))
        finally:
            self.conn.execute('DROP TABLE refcounts')
            self.conn.execute('DROP TABLE IF EXISTS wrong_refcounts')

        # Delete objects which (correctly had) refcount=0
        for obj_id in self.conn.query('SELECT id FROM objects WHERE refcount = 0'):
            del self.backend['s3ql_data_%d' % obj_id]
        self.conn.execute("DELETE FROM objects WHERE refcount = 0")

    def check_objects_temp(self):
        """Remove temporary objects"""

        # Tests may provide a plain backend directly, but in regular operation
        # we'll always work with a ComprencBackend (even if there is neither
        # compression nor encryption)
        if isinstance(self.backend, ComprencBackend):
            plain_backend = self.backend.backend
        else:
            assert isinstance(self.backend, LocalBackend)
            plain_backend = self.backend

        if not isinstance(plain_backend, LocalBackend):
            return

        log.info('Checking for temporary objects and empty directories (backend)...')

        empty_dirs = 0
        for path, _, filenames in os.walk(plain_backend.prefix, topdown=False):
            for name in filenames:
                if not re.search(r'^[^#]+#[0-9]+--?[0-9]+\.tmp$', name):
                    continue

                self.found_errors = True
                self.log_error("removing temporary file %s", name)
                os.unlink(os.path.join(path, name))

            if path != plain_backend.prefix:
                try:
                    os.rmdir(path)
                    empty_dirs += 1
                except OSError:
                    pass

        log.info('Removed %d empty directories', empty_dirs)

    def check_objects_id(self):
        """Check objects.id"""

        log.info('Checking objects (backend)...')

        # We use this table to keep track of the objects that we have seen
        self.conn.execute("CREATE TEMP TABLE obj_ids (id INTEGER PRIMARY KEY)")
        try:
            for i, obj_name in enumerate(self.backend.list('s3ql_data_')):
                log.info(
                    'Processed %d objects so far',
                    i,
                    extra={'rate_limit': 1, 'update_console': True},
                )

                # We only bother with data objects
                try:
                    obj_id = int(obj_name[10:])
                except ValueError:
                    log.warning("Ignoring unexpected object %r", obj_name)
                    continue

                self.conn.execute('INSERT INTO obj_ids VALUES(?)', (obj_id,))

            for (obj_id,) in self.conn.query(
                'SELECT id FROM obj_ids EXCEPT SELECT id FROM objects'
            ):
                try:
                    if obj_id in self.unlinked_objects:
                        del self.backend['s3ql_data_%d' % obj_id]
                    else:
                        # TODO: Save the data in lost+found instead
                        del self.backend['s3ql_data_%d' % obj_id]
                        self.found_errors = True
                        self.log_error("Deleted spurious object %d", obj_id)
                except NoSuchObject:
                    pass

            self.conn.execute(
                'CREATE TEMPORARY TABLE missing AS '
                'SELECT id FROM objects EXCEPT SELECT id FROM obj_ids'
            )
            for (obj_id,) in self.conn.query('SELECT * FROM missing'):
                if ('s3ql_data_%d' % obj_id) in self.backend:
                    # Object was just not in list yet
                    continue

                self.found_errors = True
                self.log_error(
                    "object %s only exists in table but not in backend, deleting", obj_id
                )
                # Missing references will be picked up by check_inode_blocks_obj_id() later.
                self.conn.execute("DELETE FROM objects WHERE id=?", (obj_id,))

        finally:
            self.conn.execute('DROP TABLE obj_ids')
            self.conn.execute('DROP TABLE IF EXISTS missing')

    def check_objects_phys_size(self):
        """Check objects.phys_size"""

        log.info('Checking objects (physical sizes)...')

        for (obj_id,) in self.conn.query('SELECT id FROM objects WHERE phys_size = -1'):
            self.found_errors = True
            self.log_error("Object %d has no size information, retrieving from backend...", obj_id)
            self.conn.execute(
                'UPDATE objects SET phys_size=? WHERE id=?',
                (self.backend.get_size('s3ql_data_%d' % obj_id), obj_id),
            )

    def resolve_free(self, path, name):
        '''Return parent inode and name of an unused directory entry

        The directory entry will be in `path`. If an entry `name` already
        exists there, we append a numeric suffix.
        '''

        if not isinstance(path, bytes):
            raise TypeError('path must be of type bytes')
        if not isinstance(name, bytes):
            raise TypeError('name must be of type bytes')

        inode_p = inode_for_path(path, self.conn)

        # Debugging http://code.google.com/p/s3ql/issues/detail?id=217
        # and http://code.google.com/p/s3ql/issues/detail?id=261
        if len(name) > 255 - 4:
            name = b''.join((name[0:120], b' ... ', name[-120:]))

        i = 0
        newname = name
        name += b'-'
        try:
            while True:
                self.conn.get_val(
                    "SELECT inode FROM contents_v WHERE name=? AND parent_inode=?",
                    (newname, inode_p),
                )
                i += 1
                newname = name + str(i).encode()

        except NoSuchRowError:
            pass

        return (inode_p, newname)

    def _add_name(self, name):
        '''Get id for *name* and increase refcount

        Name is inserted in table if it does not yet exist.
        '''

        try:
            name_id = self.conn.get_val('SELECT id FROM names WHERE name=?', (name,))
        except NoSuchRowError:
            name_id = self.conn.rowid('INSERT INTO names (name, refcount) VALUES(?,?)', (name, 1))
        else:
            self.conn.execute('UPDATE names SET refcount=refcount+1 WHERE id=?', (name_id,))
        return name_id

    def _del_name(self, name_id):
        '''Decrease refcount for name_id, remove if it reaches 0'''

        self.conn.execute('UPDATE names SET refcount=refcount-1 WHERE id=?', (name_id,))
        self.conn.execute('DELETE FROM names WHERE refcount=0 AND id=?', (name_id,))


def parse_args(args):
    parser = ArgumentParser(description="Checks and repairs an S3QL filesystem.")

    parser.add_log('~/.s3ql/fsck.log')
    parser.add_cachedir()
    parser.add_debug()
    parser.add_quiet()
    parser.add_backend_options()
    parser.add_version()
    parser.add_storage_url()
    parser.add_compress()

    parser.add_argument(
        "--keep-cache",
        action="store_true",
        default=False,
        help="Do not purge locally cached files on exit.",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        default=False,
        help="If user input is required, exit without prompting.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Force checking even if file system is marked clean.",
    )
    parser.add_argument(
        "--force-remote",
        action="store_true",
        default=False,
        help="Force use of remote metadata even when this would likely result in data loss.",
    )
    options = parser.parse_args(args)

    return options


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    setup_warnings()
    options = parse_args(args)
    setup_logging(options)

    # Check if fs is mounted on this computer
    # This is not foolproof but should prevent common mistakes
    if is_mounted(options.storage_url):
        raise QuietError('Can not check mounted file system.', exitcode=40)

    backend = get_backend(options)
    atexit.register(backend.close)

    log.info('Starting fsck of %s', options.storage_url)

    cachepath = options.cachepath
    db = None

    param = read_remote_params(backend)
    local_param = read_cached_params(cachepath, min_seq=param.seq_no)
    if local_param is not None:
        log.info('Using cached metadata.')
        param = local_param
        db = Connection(cachepath + '.db', param.metadata_block_size)
        if param.is_mounted:
            log.info('File system was not unmounted cleanly')
            param.needs_fsck = True
        if local_param.seq_no > param.seq_no:
            log.info('Remote metadata is outdated.')
            param.needs_fsck = True

    if param.is_mounted and local_param is not param:
        print(
            textwrap.fill(
                textwrap.dedent(
                    '''\
              Backend reports that file system is still mounted elsewhere. If this is not
              true,  run fsck on the computer where the file system has been mounted most
              recently.

              You may also continue and use whatever metadata is available in the backend.
              However, in that case YOU MAY LOOSE ALL DATA THAT HAS BEEN UPLOADED OR MODIFIED
              SINCE THE LAST SUCCESSFUL METADATA UPLOAD. Moreover, files and directories that
              you have deleted since then MAY REAPPEAR WITH SOME OF THEIR CONTENT LOST.
              '''
                )
            )
        )

        print(
            'Enter "continue, I know what I am doing" to use the outdated data anyway:',
            '> ',
            sep='\n',
            end='',
        )
        if options.force_remote:
            print('(--force-remote specified, continuing anyway)')
        elif options.batch:
            raise QuietError('(in batch mode, exiting)', exitcode=41)
        elif sys.stdin.readline().strip() != 'continue, I know what I am doing':
            raise QuietError(exitcode=42)
        param.needs_fsck = True

    if (
        not param.needs_fsck and (time.time() - param.last_fsck) < 60 * 60 * 24 * 31
    ):  # last check more than 1 month ago
        if options.force:
            log.info('File system seems clean, checking anyway.')
        else:
            log.info('File system is marked as clean. Use --force to force checking.')
            return

    # When using remote metadata, get rid of outdated local cache (so that we
    # don't accidentally upload it)
    outdated_cachedir = False
    if not db:
        try:
            for name_ in os.listdir(cachepath + '-cache'):
                if name_ not in ('.', '..'):
                    outdated_cachedir = True
                    break
        except FileNotFoundError:
            pass
    if outdated_cachedir and param.needs_fsck:
        for i in itertools.count():
            bak_name = '%s-cache.bak%d' % (cachepath, i)
            if not os.path.exists(bak_name):
                break
        log.warning('Renaming outdated cache directory %s to .bak%d', cachepath + '-cache', i)
        log.warning(
            'You should delete this directory once you are sure that everything is in order.'
        )
        os.rename(cachepath + '-cache', bak_name)
    elif outdated_cachedir:
        log.info("Flushing outdated local cache...")
        shutil.rmtree(cachepath + '-cache')

    # We only read cache files if the filesystem was not unmounted cleanly. On a clean unmount, the
    # cache files can not be dirty.
    if db and param.is_mounted:
        if options.keep_cache:
            check_cache = 'keep'
        else:
            check_cache = True
    else:
        check_cache = False

    if not db:
        # When file system was not unmounted cleanly, the checksum and size may not have been
        # updated.
        log.info('Downloading metadata...')
        db = download_metadata(backend, cachepath + '.db', param, failsafe=param.is_mounted)

        # Don't download the same metadata again in verify_metadata_snapshots
        check_current_metadata = False
    else:
        check_current_metadata = True

    log.info('Checking DB integrity...')
    try:
        # get_list may raise CorruptError itself
        res = db.get_list('PRAGMA integrity_check(20)')
        if res[0][0] != 'ok':
            log.error('\n'.join(x[0] for x in res))
            raise apsw.CorruptError()
    except apsw.CorruptError:
        raise QuietError(
            'Database file (f{cachepath}.db) is corrupted. Restore from a backup or try\n'
            'to repair with the SQLite CLI (cf. https://www.sqlite.org/recovery.html),\n'
            'then re-run fsck.s3ql',
            exitcode=43,
        )

    # To detect bugs in S3QL, make sure we can correctly download the last few metadata snapshots.
    verify_metadata_snapshots(backend, count=5, include_most_recent=check_current_metadata)

    param.is_mounted = True
    param.seq_no += 1
    write_params(cachepath, param)
    upload_params(backend, param)

    fsck = Fsck(cachepath + '-cache', backend, param, db)
    fsck.check(check_cache)

    if fsck.uncorrectable_errors:
        raise QuietError("Uncorrectable errors found, aborting.", exitcode=44 + 128)

    if fsck.found_errors and not param.needs_fsck:
        log.error('File system was marked as clean, yet fsck found problems.')
        log.error(
            'Please report this to the S3QL mailing list, http://groups.google.com/group/s3ql'
        )

    # If the filesystem was not unmounted cleanly, the database may have changes that have not
    # been uploaded yet are not tracked through the VFS, so we need to do a full upload. Just
    # to be sure, we also do a full upload if we found any errors.
    if param.needs_fsck or fsck.found_errors:
        full_upload = True
    else:
        full_upload = False

    param.needs_fsck = False
    param.is_mounted = False
    param.last_fsck = time.time()
    param.last_modified = time.time()

    if full_upload:
        # This is a good time to reduce DB size if possible
        db.execute('VACUUM')
        db.close()
        upload_metadata(backend, db, param, incremental=False)
    else:
        db.close()
        upload_metadata(backend, db, param, incremental=True)

    write_params(cachepath, param)
    upload_params(backend, param)

    expire_objects(backend)

    log.info('Completed fsck of %s', options.storage_url)

    if fsck.found_errors:
        sys.exit(128)
    else:
        sys.exit(0)


def escape(path):
    '''Escape slashes in path so that is usable as a file name'''

    return path[1:].replace(b'_', b'__').replace(b'/', b'_')


def to_str(name):
    '''Decode path name for printing'''

    return str(name, encoding='utf-8', errors='replace')


def verify_metadata_snapshots(backend, count: int = 5, include_most_recent: bool = True):
    '''Verify the last *count* metadata snapshots.'''

    log.info('Verifying consistency of most recent metadata backups:')
    backups = sorted(get_available_seq_nos(backend), reverse=True)

    if not backups:
        raise RuntimeError(
            'No metadata backups found. This should not happen, please report a bug.'
        )

    if include_most_recent:
        to_check = backups[:count]
    else:
        to_check = backups[1 : count + 1]

    for seq_no in to_check:
        # De-serialize directly instead of using read_remote_params() to avoid
        # exceptions on old filesystem revisions.
        d = thaw_basic_mapping(backend['s3ql_params_%010x' % seq_no])
        if d['revision'] != CURRENT_FS_REV:
            break
        params = FsAttributes(**d)
        assert params.seq_no == seq_no
        date = datetime.fromtimestamp(params.last_modified).strftime('%Y-%m-%d %H:%M:%S')
        log.info('Checking backup %010x (from %s))...', seq_no, date)
        with NamedTemporaryFile() as fh:
            conn = download_metadata(backend, fh.name, params)
            res = conn.get_list('PRAGMA integrity_check(20)')
            if res[0][0] != 'ok':
                log.error('\n'.join(x[0] for x in res))
                raise RuntimeError(
                    'Metadata backup is corrupted. Please report this as a bug',
                )
            conn.close()


if __name__ == '__main__':
    main(sys.argv[1:])
