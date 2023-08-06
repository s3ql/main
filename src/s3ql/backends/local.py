'''
local.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import _thread
import logging
import os
import struct
from contextlib import ExitStack
from typing import Any, BinaryIO, Dict, Optional

from ..common import ThawError, copyfh, freeze_basic_mapping, thaw_basic_mapping
from .common import AbstractBackend, CorruptedObjectError, DanglingStorageURLError, NoSuchObject

log = logging.getLogger(__name__)


class Backend(AbstractBackend):
    '''
    A backend that stores data on the local hard disk
    '''

    needs_login = False
    known_options = set()

    def __init__(self, options):
        '''Initialize local backend'''

        # Unused argument
        # pylint: disable=W0613

        super().__init__()
        self.prefix = options.storage_url[len('local://') :].rstrip('/')

        if not os.path.exists(self.prefix):
            raise DanglingStorageURLError(self.prefix)

    @property
    def has_delete_multi(self):
        return True

    def __str__(self):
        return 'local directory %s' % self.prefix

    def is_temp_failure(self, exc):  # IGNORE:W0613
        return False

    def lookup(self, key):
        path = self._key_to_path(key)
        try:
            with open(path, 'rb') as src:
                return _read_meta(src)
        except FileNotFoundError:
            raise NoSuchObject(key)

    def get_size(self, key):
        return os.path.getsize(self._key_to_path(key))

    def readinto_fh(self, key: str, ofh: BinaryIO):
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset.
        '''

        path = self._key_to_path(key)
        with ExitStack() as es:
            try:
                ifh = es.enter_context(open(path, 'rb', buffering=0))
            except FileNotFoundError:
                raise NoSuchObject(key)

            try:
                metadata = _read_meta(ifh)
            except ThawError:
                raise CorruptedObjectError('Invalid metadata')

            copyfh(ifh, ofh)
        return metadata

    def write_fh(
        self,
        key: str,
        fh: BinaryIO,
        metadata: Optional[Dict[str, Any]] = None,
        len_: Optional[int] = None,
    ):
        '''Upload *len_* bytes from *fh* under *key*.

        The data will be read at the current offset. If *len_* is None, reads until the
        end of the file.

        If a temporary error (as defined by `is_temp_failure`) occurs, the operation is
        retried.  Returns the size of the resulting storage object .
        '''

        if metadata is None:
            metadata = dict()

        path = self._key_to_path(key)
        buf = freeze_basic_mapping(metadata)
        if len(buf).bit_length() > 16:
            raise ValueError('Metadata too large')

        # By renaming, we make sure that there are no
        # conflicts between parallel reads, the last one wins
        tmpname = '%s#%d-%d.tmp' % (path, os.getpid(), _thread.get_ident())

        with ExitStack() as es:
            try:
                dest = es.enter_context(open(tmpname, 'wb', buffering=0))
            except FileNotFoundError:
                try:
                    os.makedirs(os.path.dirname(path))
                except FileExistsError:
                    # Another thread may have created the directory already
                    pass
                dest = es.enter_context(open(tmpname, 'wb', buffering=0))

            dest.write(b's3ql_1\n')
            dest.write(struct.pack('<H', len(buf)))
            dest.write(buf)
            copyfh(fh, dest, len_)
            size = dest.tell()
        os.rename(tmpname, path)

        return size

    def contains(self, key):
        path = self._key_to_path(key)
        try:
            os.lstat(path)
        except FileNotFoundError:
            return False
        return True

    def delete_multi(self, keys):
        for i, key in enumerate(keys):
            try:
                self.delete(key)
            except:
                del keys[:i]
                raise

        del keys[:]

    def delete(self, key):
        path = self._key_to_path(key)
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    def list(self, prefix=''):
        if prefix:
            base = os.path.dirname(self._key_to_path(prefix))
        else:
            base = self.prefix

        for path, dirnames, filenames in os.walk(base, topdown=True):
            # Do not look in wrong directories
            if prefix:
                rpath = path[len(self.prefix) :]  # path relative to base
                prefix_l = ''.join(rpath.split('/'))

                dirs_to_walk = list()
                for name in dirnames:
                    prefix_ll = unescape(prefix_l + name)
                    if prefix_ll.startswith(prefix[: len(prefix_ll)]):
                        dirs_to_walk.append(name)
                dirnames[:] = dirs_to_walk

            for name in filenames:
                # Skip temporary files
                if '#' in name:
                    continue

                key = unescape(name)

                if not prefix or key.startswith(prefix):
                    yield key

    def _key_to_path(self, key):
        '''Return path for given key'''

        # NOTE: We must not split the path in the middle of an
        # escape sequence, or list() will fail to work.

        key = escape(key)

        if not key.startswith('s3ql_data_'):
            return os.path.join(self.prefix, key)

        no = key[10:]
        path = [self.prefix, 's3ql_data_']
        for i in range(0, len(no), 3):
            path.append(no[:i])
        path.append(key)

        return os.path.join(*path)


def _read_meta(fh):
    buf = fh.read(9)
    if not buf.startswith(b's3ql_1\n'):
        raise CorruptedObjectError('Invalid object header: %r' % buf)

    len_ = struct.unpack('<H', buf[-2:])[0]
    try:
        return thaw_basic_mapping(fh.read(len_))
    except ThawError:
        raise CorruptedObjectError('Invalid metadata')


def escape(s):
    '''Escape '/', '=' and '.' in s'''

    s = s.replace('=', '=3D')
    s = s.replace('/', '=2F')
    s = s.replace('#', '=23')

    return s


def unescape(s):
    '''Un-Escape '/', '=' and '.' in s'''

    s = s.replace('=2F', '/')
    s = s.replace('=23', '#')
    s = s.replace('=3D', '=')

    return s
