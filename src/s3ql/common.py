'''
common.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, QuietError # Ensure use of custom logger class
from . import BUFSIZE, CTRL_NAME, ROOT_INODE
from dugong import HostnameNotResolvable
from getpass import getpass
from ast import literal_eval
from base64 import b64decode, b64encode
import binascii
import configparser
import re
import stat
import threading
import traceback
import sys
import os
import time
import subprocess
import errno
import hashlib
import llfuse
import posixpath
import functools
import contextlib

log = logging.getLogger(__name__)

file_system_encoding = sys.getfilesystemencoding()
def path2bytes(s):
    return s.encode(file_system_encoding, 'surrogateescape')
def bytes2path(s):
    return s.decode(file_system_encoding, 'surrogateescape')

def get_seq_no(backend):
    '''Get current metadata sequence number'''

    from .backends.common import NoSuchObject

    seq_nos = list(backend.list('s3ql_seq_no_'))
    if not seq_nos:
        # Maybe list result is outdated
        seq_nos = [ 's3ql_seq_no_1' ]

    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in seq_nos ]
    seq_no = max(seq_nos)

    # Make sure that object really exists
    while ('s3ql_seq_no_%d' % seq_no) not in backend:
        seq_no -= 1
        if seq_no == 0:
            raise QuietError('No S3QL file system found at given storage URL.',
                             exitcode=18)
    while ('s3ql_seq_no_%d' % seq_no) in backend:
        seq_no += 1
    seq_no -= 1

    # Delete old seq nos
    for i in [ x for x in seq_nos if x < seq_no - 10 ]:
        try:
            del backend['s3ql_seq_no_%d' % i]
        except NoSuchObject:
            pass # Key list may not be up to date

    return seq_no

def is_mounted(storage_url):
    '''Try to determine if *storage_url* is mounted

    Note that the result may be wrong.. this is really just
    a best-effort guess.
    '''

    match = storage_url + ' '
    if os.path.exists('/proc/mounts'):
        with open('/proc/mounts', 'r') as fh:
            for line in fh:
                if line.startswith(match):
                    return True
            return False

    try:
        for line in subprocess.check_output(['mount'], stderr=subprocess.STDOUT,
                                            universal_newlines=True):
            if line.startswith(match):
                return True
    except subprocess.CalledProcessError:
        log.warning('Warning! Unable to check if file system is mounted '
                    '(/proc/mounts missing and mount call failed)')

    return False

def inode_for_path(path, conn):
    """Return inode of directory entry at `path`

     Raises `KeyError` if the path does not exist.
    """
    from .database import NoSuchRowError

    if not isinstance(path, bytes):
        raise TypeError('path must be of type bytes')

    # Remove leading and trailing /
    path = path.lstrip(b"/").rstrip(b"/")

    # Traverse
    inode = ROOT_INODE
    for el in path.split(b'/'):
        try:
            inode = conn.get_val("SELECT inode FROM contents_v WHERE name=? AND parent_inode=?",
                                 (el, inode))
        except NoSuchRowError:
            raise KeyError('Path %s does not exist' % path)

    return inode

def get_path(id_, conn, name=None):
    """Return a full path for inode `id_`.

    If `name` is specified, it is appended at the very end of the
    path (useful if looking up the path for file name with parent
    inode).
    """

    if name is None:
        path = list()
    else:
        if not isinstance(name, bytes):
            raise TypeError('name must be of type bytes')
        path = [ name ]

    maxdepth = 255
    while id_ != ROOT_INODE:
        # This can be ambiguous if directories are hardlinked
        (name2, id_) = conn.get_row("SELECT name, parent_inode FROM contents_v "
                                    "WHERE inode=? LIMIT 1", (id_,))
        path.append(name2)
        maxdepth -= 1
        if maxdepth == 0:
            raise RuntimeError('Failed to resolve name "%s" at inode %d to path',
                               name, id_)

    path.append(b'')
    path.reverse()

    return b'/'.join(path)


def _escape(s):
    '''Escape '/', '=' and '\0' in s'''

    s = s.replace('=', '=3D')
    s = s.replace('/', '=2F')
    s = s.replace('\0', '=00')

    return s

def get_backend_cachedir(storage_url, cachedir):
    if not os.path.exists(cachedir):
        try:
            os.mkdir(cachedir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        except PermissionError:
            raise QuietError('No permission to create cache directory (%s)' % cachedir,
                             exitcode=45)

    if not os.access(cachedir, os.R_OK | os.W_OK | os.X_OK):
        raise QuietError('No permission to access cache directory (%s)' % cachedir,
                         exitcode=45)

    return os.path.join(cachedir, _escape(storage_url))


def sha256_fh(fh):
    fh.seek(0)

    # Bogus error about hashlib not having a sha256 member
    #pylint: disable=E1101
    sha = hashlib.sha256()

    while True:
        buf = fh.read(BUFSIZE)
        if not buf:
            break
        sha.update(buf)

    return sha.digest()


def assert_s3ql_fs(path):
    '''Raise `QuietError` if *path* is not on an S3QL file system

    Returns name of the S3QL control file.
    '''

    try:
        os.stat(path)
    except FileNotFoundError:
        raise QuietError('%s does not exist' % path)
    except OSError as exc:
        if exc.errno is errno.ENOTCONN:
            raise QuietError('File system appears to have crashed.')
        raise

    ctrlfile = os.path.join(path, CTRL_NAME)
    if not (CTRL_NAME not in llfuse.listdir(path)
            and os.path.exists(ctrlfile)):
        raise QuietError('%s is not on an S3QL file system' % path)

    return ctrlfile


def assert_fs_owner(path, mountpoint=False):
    '''Raise `QuietError` if user is not owner of S3QL fs at *path*

    Implicitly calls `assert_s3ql_fs` first. Returns name of the
    S3QL control file.

    If *mountpoint* is True, also call `assert_s3ql_mountpoint`, i.e.
    fail if *path* is not the mount point of the file system.
    '''

    if mountpoint:
        ctrlfile = assert_s3ql_mountpoint(path)
    else:
        ctrlfile = assert_s3ql_fs(path)

    if os.stat(ctrlfile).st_uid != os.geteuid() and os.geteuid() != 0:
        raise QuietError('Permission denied. %s is was not mounted by you '
                         'and you are not root.' % path)

    return ctrlfile

def assert_s3ql_mountpoint(mountpoint):
    '''Raise QuietError if *mountpoint* is not an S3QL mountpoint

    Implicitly calls `assert_s3ql_fs` first. Returns name of the
    S3QL control file.
    '''

    ctrlfile = assert_s3ql_fs(mountpoint)
    if not posixpath.ismount(mountpoint):
        raise QuietError('%s is not a mount point' % mountpoint)

    return ctrlfile

def get_backend(options, raw=False):
    '''Return backend for given storage-url

    If *raw* is true, don't attempt to unlock and don't wrap into
    ComprencBackend.
    '''

    return get_backend_factory(options.storage_url, options.backend_options,
                               options.authfile, options.cachedir,
                               getattr(options, 'compress', ('lzma', 2)), raw)()

def get_backend_factory(storage_url, backend_options, authfile, cachedir,
                        compress=('lzma', 2), raw=False):
    '''Return factory producing backend objects for given storage-url

    If *raw* is true, don't attempt to unlock and don't wrap into
    ComprencBackend.
    '''

    from .backends import prefix_map
    from .backends.common import (CorruptedObjectError, NoSuchObject, AuthenticationError,
                                  DanglingStorageURLError, AuthorizationError)
    from .backends.comprenc import ComprencBackend

    hit = re.match(r'^([a-zA-Z0-9]+)://', storage_url)
    if not hit:
        raise QuietError('Unable to parse storage url "%s"' % storage_url,
                         exitcode=2)

    backend = hit.group(1)
    try:
        backend_class = prefix_map[backend]
    except KeyError:
        raise QuietError('No such backend: %s' % backend, exitcode=11)

    # Validate backend options
    for opt in backend_options.keys():
        if opt not in backend_class.known_options:
            raise QuietError('Unknown backend option: %s' % opt,
                             exitcode=3)

    # Read authfile
    config = configparser.ConfigParser()
    if os.path.isfile(authfile):
        mode = os.stat(authfile).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            raise QuietError("%s has insecure permissions, aborting." % authfile,
                             exitcode=12)
        config.read(authfile)

    backend_login = None
    backend_passphrase = None
    fs_passphrase = None
    for section in config.sections():
        def getopt(name):
            try:
                return config.get(section, name)
            except configparser.NoOptionError:
                return None

        pattern = getopt('storage-url')

        if not pattern or not storage_url.startswith(pattern):
            continue

        backend_login = getopt('backend-login') or backend_login
        backend_passphrase = getopt('backend-password') or backend_passphrase
        fs_passphrase = getopt('fs-passphrase') or fs_passphrase

    if not backend_login and backend_class.needs_login:
        if sys.stdin.isatty():
            backend_login = getpass("Enter backend login: ")
        else:
            backend_login = sys.stdin.readline().rstrip()

    if not backend_passphrase and backend_class.needs_login:
        if sys.stdin.isatty():
            backend_passphrase = getpass("Enter backend passphrase: ")
        else:
            backend_passphrase = sys.stdin.readline().rstrip()

    backend = None
    try:
        backend = backend_class(storage_url, backend_login, backend_passphrase,
                                backend_options, cachedir)

        # Do not use backend.lookup(), this would use a HEAD request and
        # not provide any useful error messages if something goes wrong
        # (e.g. wrong credentials)
        backend.fetch('s3ql_passphrase')

    except AuthenticationError:
        raise QuietError('Invalid credentials (or skewed system clock?).',
                         exitcode=14)

    except AuthorizationError:
        raise QuietError('No permission to access backend.',
                         exitcode=15)

    except HostnameNotResolvable:
        raise QuietError("Can't connect to backend: unable to resolve hostname",
                         exitcode=19)

    except DanglingStorageURLError as exc:
        raise QuietError(str(exc), exitcode=16)

    except NoSuchObject:
        encrypted = False

    else:
        encrypted = True

    finally:
        if backend is not None:
            backend.close()

    if raw:
        return lambda: backend_class(storage_url, backend_login, backend_passphrase,
                                     backend_options, cachedir)

    if encrypted and not fs_passphrase:
        if sys.stdin.isatty():
            fs_passphrase = getpass("Enter file system encryption passphrase: ")
        else:
            fs_passphrase = sys.stdin.readline().rstrip()
    elif not encrypted:
        fs_passphrase = None

    if fs_passphrase is not None:
        fs_passphrase = fs_passphrase.encode('utf-8')

    if not encrypted:
        return lambda: ComprencBackend(None, compress,
                                    backend_class(storage_url, backend_login,
                                                  backend_passphrase, backend_options,
                                                  cachedir))

    with ComprencBackend(fs_passphrase, compress, backend) as tmp_backend:
        try:
            data_pw = tmp_backend['s3ql_passphrase']
        except CorruptedObjectError:
            raise QuietError('Wrong file system passphrase', exitcode=17)

    # To support upgrade, temporarily store the backend
    # passphrase in every backend object.
    def factory():
        b = ComprencBackend(data_pw, compress,
                            backend_class(storage_url, backend_login,
                                          backend_passphrase, backend_options,
                                          cachedir))
        b.fs_passphrase = fs_passphrase
        return b

    return factory

def pretty_print_size(i):
    '''Return *i* as string with appropriate suffix (MiB, GiB, etc)'''

    if i < 1024:
        return '%d bytes' % i

    if i < 1024**2:
        unit = 'KiB'
        i /= 1024

    elif i < 1024**3:
        unit = 'MiB'
        i /= 1024**2

    elif i < 1024**4:
        unit = 'GiB'
        i /= 1024**3

    else:
        unit = 'TB'
        i /= 1024**4

    if i < 10:
        form = '%.2f %s'
    elif i < 100:
        form = '%.1f %s'
    else:
        form = '%d %s'

    return form % (i, unit)


class ExceptionStoringThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self._exc_info = None
        self._joined = False

    def run_protected(self):
        pass

    def run(self):
        try:
            self.run_protected()
        except:
            # This creates a circular reference chain
            self._exc_info = sys.exc_info()

    def join_get_exc(self):
        self._joined = True
        self.join()
        return self._exc_info

    def join_and_raise(self):
        '''Wait for the thread to finish, raise any occurred exceptions'''

        self._joined = True
        if self.is_alive():
            self.join()

        if self._exc_info is not None:
            # Break reference chain
            exc_info = self._exc_info
            self._exc_info = None
            raise EmbeddedException(exc_info, self.name)

    def __del__(self):
        if not self._joined:
            raise RuntimeError("ExceptionStoringThread instance was destroyed "
                               "without calling join_and_raise()!")

class EmbeddedException(Exception):
    '''Encapsulates an exception that happened in a different thread
    '''

    def __init__(self, exc_info, threadname):
        super().__init__()
        self.exc_info = exc_info
        self.threadname = threadname

    def __str__(self):
        return ''.join(['caused by an exception in thread %s.\n' % self.threadname,
                       'Original/inner traceback (most recent call last): \n' ] +
                       traceback.format_exception(*self.exc_info))


class AsyncFn(ExceptionStoringThread):
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.target = fn
        self.args = args
        self.kwargs = kwargs

    def run_protected(self):
        self.target(*self.args, **self.kwargs)

def split_by_n(seq, n):
    '''Yield elements in iterable *seq* in groups of *n*'''

    while seq:
        yield seq[:n]
        seq = seq[n:]

def handle_on_return(fn):
    '''Provide fresh ExitStack instance in `on_return` argument'''
    @functools.wraps(fn)
    def wrapper(*a, **kw):
        assert 'on_return' not in kw
        with contextlib.ExitStack() as on_return:
            kw['on_return'] = on_return
            return fn(*a, **kw)
    return wrapper

def parse_literal(buf, type_spec):
    '''Try to parse *buf* as *type_spec*

    Raise `ValueError` if *buf* does not contain a valid
    Python literal, or if the literal does not correspond
    to *type_spec*.

    Example use::

      buf = b'[1, 'a', 3]'
      parse_literal(buf, [int, str, int])

    '''

    try:
        obj = literal_eval(buf.decode())
    except UnicodeDecodeError:
        raise ValueError('unable to decode as utf-8')
    except (ValueError, SyntaxError):
        raise ValueError('unable to parse as python literal')

    if (isinstance(type_spec, list) and type(obj) == list
        and [ type(x) for x in obj ] == type_spec):
        return obj
    elif (isinstance(type_spec, tuple) and type(obj) == tuple
        and [ type(x) for x in obj ] == list(type_spec)):
        return obj
    elif type(obj) == type_spec:
        return obj

    raise ValueError('literal has wrong type')

class ThawError(Exception):
    def __str__(self):
        return 'Malformed serialization data'

def thaw_basic_mapping(buf):
    '''Reconstruct dict from serialized representation

    *buf* must be a bytes-like object as created by
    `freeze_basic_mapping`. Raises `ThawError` if *buf* is not a valid
    representation.

    This procedure is safe even if *buf* comes from an untrusted source.
    '''

    try:
        d = literal_eval(buf.decode('utf-8'))
    except (UnicodeDecodeError, SyntaxError, ValueError):
        raise ThawError()

    # Decode bytes values
    for (k,v) in d.items():
        if not isinstance(v, bytes):
            continue
        try:
            d[k] = b64decode(v)
        except binascii.Error:
            raise ThawError()

    return d

def freeze_basic_mapping(d):
    '''Serialize mapping of elementary types

    Keys of *d* must be strings. Values of *d* must be of elementary type (i.e.,
    `str`, `bytes`, `int`, `float`, `complex`, `bool` or None).

    The output is a bytestream that can be used to reconstruct the mapping. The
    bytestream is not guaranteed to be deterministic. Look at
    `checksum_basic_mapping` if you need a deterministic bytestream.
    '''

    els = []
    for (k,v) in d.items():
        if not isinstance(k, str):
            raise ValueError('key %s must be str, not %s' % (k, type(k)))

        if (not isinstance(v, (str, bytes, bytearray, int, float, complex, bool))
            and v is not None):
            raise ValueError('value for key %s (%s) is not elementary' % (k, v))

        # To avoid wasting space, we b64encode non-ascii byte values.
        if isinstance(v, (bytes, bytearray)):
            v = b64encode(v)

        # This should be a pretty safe assumption for elementary types, but we
        # add an assert just to be safe (Python docs just say that repr makes
        # "best effort" to produce something parseable)
        (k_repr, v_repr)  = (repr(k), repr(v))
        assert (literal_eval(k_repr), literal_eval(v_repr)) == (k, v)

        els.append(('%s: %s' % (k_repr, v_repr)))

    buf = '{ %s }' % ', '.join(els)
    return buf.encode('utf-8')

def load_params(cachepath):
    with open(cachepath + '.params' , 'rb') as fh:
        return thaw_basic_mapping(fh.read())

def save_params(cachepath, param):
    with open(cachepath + '.params', 'wb') as fh:
        fh.write(freeze_basic_mapping(param))

        # Fsync to make sure that the updated sequence number is committed to
        # disk. Otherwise, a crash immediately after mount could result in both
        # the local and remote metadata appearing to be out of date.
        fh.flush()
        os.fsync(fh.fileno())

def time_ns():
    return int(time.time() * 1e9)
