'''
common.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import collections
import hashlib
import hmac
import inspect
import itertools
import logging
import os
import random
import re
import ssl
import struct
import textwrap
import threading
import time
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from functools import wraps
from io import BytesIO
from typing import Any, BinaryIO, Callable, Dict, Optional, Union

import trio

from s3ql.http import HTTPConnection, is_temp_network_error

from ..logging import LOG_ONCE, QuietError

log = logging.getLogger(__name__)

Elementary = Union[str, bytes, int, float, complex, bool, None]


class RateTracker:
    '''
    Maintain an average occurrence rate for events over a configurable time
    window. The rate is computed with one second resolution.
    '''

    def __init__(self, window_length):
        if not isinstance(window_length, int):
            raise ValueError('only integer window lengths are supported')

        self.buckets = [0] * window_length
        self.window_length = window_length
        self.last_update = int(time.monotonic())
        self.lock = threading.Lock()

    def register(self, _not_really=False):
        '''Register occurrence of an event.

        The keyword argument is for class-internal use only.
        '''

        buckets = self.buckets
        bucket_count = len(self.buckets)
        now = int(time.monotonic())

        elapsed = min(now - self.last_update, bucket_count)
        for i in range(elapsed):
            buckets[(now - i) % bucket_count] = 0

        if _not_really:
            return

        with self.lock:
            buckets[now % bucket_count] += 1
            self.last_update = now

    def get_rate(self):
        '''Return average rate of event occurrence'''

        self.register(_not_really=True)
        return sum(self.buckets) / len(self.buckets)

    def get_count(self):
        '''Return total number of events in window'''

        self.register(_not_really=True)
        return sum(self.buckets)


@dataclass
class UploadRequest:
    fh: BinaryIO
    len: int
    off: int
    key: str
    callback: Callable[[int], None]
    metadata: Dict[str, Elementary]


# We maintain a (global) running average of temporary errors, so
# that we can log a warning if this number becomes large. We
# use a relatively large window to prevent bogus spikes if
# multiple threads all have to retry after a long period of
# inactivity.
RETRY_TIMEOUT = 60 * 60 * 24


def retry(method, _tracker=RateTracker(60)):
    '''Wrap *method* for retrying on some exceptions

    If *method* raises an exception for which the instance's `is_temp_failure(exc)` method is true,
    the *method* is called again at increasing intervals. If this persists for more than
    `RETRY_TIMEOUT` seconds, the most-recently caught exception is re-raised within a TimeoutError.
    If the method defines a keyword parameter *is_retry*, then this parameter will be set to True
    whenever the function is retried.
    '''

    if inspect.isgeneratorfunction(method):
        raise TypeError('Wrapping a generator function is pointless')

    sig = inspect.signature(method)
    has_is_retry = 'is_retry' in sig.parameters

    @wraps(method)
    def wrapped(*a, **kw):
        self = a[0]
        interval = 1 / 50
        waited = 0
        retries = 0
        while True:
            if has_is_retry:
                kw['is_retry'] = retries > 0
            try:
                return method(*a, **kw)
            except Exception as exc:
                # Access to protected member ok
                # pylint: disable=W0212
                if not self.is_temp_failure(exc):
                    raise

                _tracker.register()
                rate = _tracker.get_rate()
                if rate > 5:
                    log.warning(
                        'Had to retry %d times over the last %d seconds, '
                        'server or network problem?',
                        rate * _tracker.window_length,
                        _tracker.window_length,
                    )
                else:
                    log.debug('Average retry rate: %.2f Hz', rate)

                if waited > RETRY_TIMEOUT:
                    log.error(
                        '%s.%s(*): Timeout exceeded, re-raising %r exception',
                        self.__class__.__name__,
                        method.__name__,
                        exc,
                    )
                    raise TimeoutError() from exc

                retries += 1
                if retries <= 2:
                    log_fn = log.debug
                elif retries <= 4:
                    log_fn = log.info
                else:
                    log_fn = log.warning

                log_fn(
                    'Encountered %s (%s), retrying %s.%s (attempt %d)...',
                    type(exc).__name__,
                    exc,
                    self.__class__.__name__,
                    method.__name__,
                    retries,
                )

                if hasattr(exc, 'retry_after') and exc.retry_after:
                    log.debug('retry_after is %.2f seconds', exc.retry_after)
                    interval = exc.retry_after

            # Add some random variation to prevent flooding the
            # server with too many concurrent requests.
            time.sleep(interval * random.uniform(1, 1.5))
            waited += interval
            interval = min(5 * 60, 2 * interval)

    extend_docstring(
        wrapped,
        'This method has been wrapped and will automatically re-execute in '
        'increasing intervals for up to `s3ql.backends.common.RETRY_TIMEOUT` '
        'seconds if it raises an exception for which the instance\'s '
        '`is_temp_failure` method returns True.',
    )

    return wrapped


def extend_docstring(fun, s):
    '''Append *s* to *fun*'s docstring with proper wrapping and indentation'''

    if fun.__doc__ is None:
        fun.__doc__ = ''

    # Figure out proper indentation
    indent = 60
    for line in fun.__doc__.splitlines()[1:]:
        stripped = line.lstrip()
        if stripped:
            indent = min(indent, len(line) - len(stripped))

    indent_s = '\n' + ' ' * indent
    fun.__doc__ += ''.join(indent_s + line for line in textwrap.wrap(s, width=80 - indent))
    fun.__doc__ += '\n'


class AbstractBackend(object, metaclass=ABCMeta):
    '''Functionality shared between all backends.

    Instances behave similarly to dicts. They can be iterated over and
    indexed into, but raise a separate set of exceptions.
    '''

    needs_login = True

    def __init__(self):
        super().__init__()

    def __getitem__(self, key):
        return self.fetch(key)[0]

    def __setitem__(self, key, value):
        self.store(key, value)

    def __delitem__(self, key):
        self.delete(key)

    def __iter__(self):
        return self.list()

    def __contains__(self, key):
        return self.contains(key)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()
        return False

    def iteritems(self):
        for key in self.list():
            yield (key, self[key])

    @property
    def has_delete_multi(self):
        '''True if the backend supports `delete_multi`.'''

        return False

    def reset(self):
        '''Reset backend

        This resets the backend and ensures that it is ready to process requests. In most cases,
        this method does nothing. However, if e.g. a previous request was not send completely or
        response not read completely because of an exception, the `reset` method will make sure that
        any underlying connection is properly closed.
        '''

        pass

    @abstractmethod
    def readinto_fh(self, key: str, fh: BinaryIO):
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset. If a temporary error (as defined by
        `is_temp_failure`) occurs, the operation is retried.
        '''
        pass

    @abstractmethod
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
        retried.  Returns the size of the resulting storage object (which may be less due
        to compression)
        '''
        pass

    def fetch(self, key):
        """Return data stored under `key`.

        Returns a tuple with the data and metadata. If only the data itself is
        required, ``backend[key]`` is a more concise notation for
        ``backend.fetch(key)[0]``.
        """

        fh = BytesIO()
        metadata = self.readinto_fh(key, fh)
        return (fh.getvalue(), metadata)

    def store(self, key, val, metadata=None):
        """Store data under `key`.

        `metadata` can be mapping with additional attributes to store with the
        object. Keys have to be of type `str`, values have to be of elementary
        type (`str`, `bytes`, `int`, `float` or `bool`).

        If no metadata is required, one can simply assign to the subscripted
        backend instead of using this function: ``backend[key] = val`` is
        equivalent to ``backend.store(key, val)``.
        """

        return self.write_fh(key, BytesIO(val), metadata)

    @abstractmethod
    def is_temp_failure(self, exc):
        '''Return true if exc indicates a temporary error

        Return true if the given exception indicates a temporary problem. Most
        instance methods automatically retry the request in this case, so the
        caller does not need to worry about temporary failures.

        However, in same cases (e.g. when reading or writing an object), the
        request cannot automatically be retried. In these case this method can
        be used to check for temporary problems and so that the request can be
        manually restarted if applicable.
        '''

        pass

    @abstractmethod
    def lookup(self, key):
        """Return metadata for given key.

        If the key does not exist, `NoSuchObject` is raised.
        """

        pass

    @abstractmethod
    def get_size(self, key):
        '''Return size of object stored under *key*'''
        pass

    def contains(self, key):
        '''Check if `key` is in backend'''

        try:
            self.lookup(key)
        except NoSuchObject:
            return False
        else:
            return True

    @abstractmethod
    def delete(self, key):
        """Delete object stored under `key`

        Attempts to delete non-existing objects will silently succeed.
        """
        pass

    def delete_multi(self, keys):
        """Delete objects stored under `keys`

        Deleted objects are removed from the *keys* list, so that the caller can
        determine which objects have not yet been processed if an exception is
        occurs.

        Attempts to delete non-existing objects will silently succeed.
        """

        while keys:
            self.delete(keys[-1])
            keys.pop()

    @abstractmethod
    def list(self, prefix=''):
        '''List keys in backend

        Returns an iterator over all keys in the backend.
        '''
        pass

    def close(self):
        '''Close any opened resources

        This method closes any resources allocated by the backend (e.g. network
        sockets). This method should be called explicitly before a backend
        object is garbage collected. The backend object may be re-used after
        `close` has been called, in this case the necessary resources are
        transparently allocated again.
        '''

        pass


class NoSuchObject(Exception):
    '''Raised if the requested object does not exist in the backend'''

    def __init__(self, key):
        super().__init__()
        self.key = key

    def __str__(self):
        return 'Backend does not have anything stored under key %r' % self.key


class DanglingStorageURLError(Exception):
    '''Raised if the backend can't store data at the given location'''

    def __init__(self, loc, msg=None):
        super().__init__()
        self.loc = loc
        self.msg = msg

    def __str__(self):
        if self.msg is None:
            return '%r does not exist' % self.loc
        else:
            return self.msg


class AuthorizationError(Exception):
    '''Raised if the credentials don't give access to the requested backend'''

    def __init__(self, msg):
        super().__init__()
        self.msg = msg

    def __str__(self):
        return 'Access denied. Server said: %s' % self.msg


class AuthenticationError(Exception):
    '''Raised if the credentials are invalid'''

    def __init__(self, msg):
        super().__init__()
        self.msg = msg

    def __str__(self):
        return 'Access denied. Server said: %s' % self.msg


class CorruptedObjectError(Exception):
    """
    Raised if a storage object is corrupted.

    Note that this is different from BadDigest error, which is raised
    if a transmission error has been detected.
    """

    def __init__(self, str_):
        super().__init__()
        self.str = str_

    def __str__(self):
        return self.str


def get_ssl_context(path):
    '''Construct SSLContext object'''

    # Best practice according to http://docs.python.org/3/library/ssl.html#protocol-versions
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED

    if path is None:
        log.debug('Reading default CA certificates.')
        context.set_default_verify_paths()
    elif os.path.isfile(path):
        log.debug('Reading CA certificates from file %s', path)
        context.load_verify_locations(cafile=path)
    else:
        log.debug('Reading CA certificates from directory %s', path)
        context.load_verify_locations(capath=path)

    return context


def get_proxy(ssl):
    '''Read system proxy settings

    Returns either `None`, or a tuple ``(host, port)``.

    This function may raise `QuietError`.
    '''

    if ssl:
        proxy_env = 'https_proxy'
    else:
        proxy_env = 'http_proxy'

    if proxy_env in os.environ:
        proxy = os.environ[proxy_env]
        hit = re.match(r'^(https?://)?([a-zA-Z0-9.-]+)(:[0-9]+)?/?$', proxy)
        if not hit:
            raise QuietError(
                'Unable to parse proxy setting %s=%r' % (proxy_env, proxy), exitcode=13
            )

        if hit.group(1) == 'https://':
            log.warning(
                'HTTPS connection to proxy is probably pointless and not supported, '
                'will use standard HTTP',
                extra=LOG_ONCE,
            )

        if hit.group(3):
            proxy_port = int(hit.group(3)[1:])
        else:
            proxy_port = 80

        proxy_host = hit.group(2)
        log.info('Using proxy %s:%d', proxy_host, proxy_port, extra=LOG_ONCE)
        proxy = (proxy_host, proxy_port)
    else:
        proxy = None

    return proxy


def checksum_basic_mapping(metadata: Dict[str, Elementary], key=None):
    '''Compute checksum for mapping of elementary types

    Keys of *d* must be strings. Values of *d* must be of elementary
    type (i.e., `str`, `bytes`, `int`, `float`, `complex`, `bool` or
    None). If there is a key named ``signature``, then it is excluded
    from the checksum computation.

    If *key* is None, compute MD5. Otherwise compute HMAC using *key*.
    '''

    # In order to compute a safe checksum, we need to convert each object to a
    # unique representation (i.e, we can't use repr(), as it's output may change
    # in the future). Furthermore, we have to make sure that the representation
    # is theoretically reversible, or there is a potential for collision
    # attacks.

    if key is None:
        chk = hashlib.md5()
    else:
        chk = hmac.new(key, digestmod=hashlib.sha256)

    for mkey in sorted(metadata.keys()):
        assert isinstance(mkey, str)
        if mkey == 'signature':
            continue
        chk.update(mkey.encode('utf-8'))
        val = metadata[mkey]
        if isinstance(val, str):
            val = b'\0s' + val.encode('utf-8') + b'\0'
        elif val is None:
            val = b'\0n'
        elif isinstance(val, int):
            val = b'\0i' + ('%d' % val).encode() + b'\0'
        elif isinstance(val, bool):
            val = b'\0t' if val else b'\0f'
        elif isinstance(val, float):
            val = b'\0d' + struct.pack('<d', val)
        elif isinstance(val, (bytes, bytearray)):
            assert len(val).bit_length() <= 32
            chk.update(b'\0b' + struct.pack('<I', len(val)))
        else:
            raise ValueError("Don't know how to checksum %s instances" % type(val))
        chk.update(val)

    return chk.digest()


def co_retry(fn, _tracker=RateTracker(60)):
    '''Wrap coroutine function *fn* for retrying on temporary errors'''

    if not inspect.iscoroutinefunction(fn):
        raise TypeError('Expected coroutine function, not %r' % type(fn))

    @wraps(fn)
    async def wrapped(*a, **kw):
        self = a[0]
        interval = 1 / 50
        waited = 0
        for retries in itertools.count():
            try:
                return await fn(*a, **kw)
            except* Exception as exc:
                if await self._handle_retry_exc(exc):
                    pass
                else:
                    raise

            _tracker.register()
            rate = _tracker.get_rate()
            if rate > 5:
                log.warning(
                    'Had to retry %d times over the last %d seconds, ' 'server or network problem?',
                    rate * _tracker.window_length,
                    _tracker.window_length,
                )
            else:
                log.debug('Average retry rate: %.2f Hz', rate)

            if waited > RETRY_TIMEOUT:
                log.error(
                    '%s.%s(*): Timeout exceeded, re-raising %r exception',
                    self.__class__.__name__,
                    fn.__name__,
                    exc,
                )
                raise

            if retries <= 2:
                log_fn = log.debug
            elif retries <= 4:
                log_fn = log.info
            else:
                log_fn = log.warning

            log_fn(
                'Encountered %s (%s), retrying %s.%s (attempt %d)...',
                type(exc).__name__,
                exc,
                self.__class__.__name__,
                fn.__name__,
                retries,
            )

            if hasattr(exc, 'retry_after') and exc.retry_after:
                log.debug('retry_after is %.2f seconds', exc.retry_after)
                interval = exc.retry_after

            # Add some random variation to prevent flooding the
            # server with too many concurrent requests.
            await trio.sleep(interval * random.uniform(1, 1.5))
            waited += interval
            interval = min(5 * 60, 2 * interval)

    return wrapped


class UploadWorker(object, metaclass=ABCMeta):
    def __init__(self, backend: 'HttpBackend', queue: trio.MemoryReceiveChannel):
        super().__init__()
        self._backend = backend
        self._queue = queue
        self._conn = backend._get_conn()
        self._pending_reqs = collections.deque()
        self._max_pipelined_uploads = 50

    async def init(self, nursery: trio.Nursery) -> None:
        nursery.start_soon(self._supervisor_loop, name='upload_supervisor')

    async def close(self):
        pass

    @abstractmethod
    async def _upload_send(self, req: UploadRequest) -> None:
        '''Send HTTP request for object upload request *req*'''
        pass

    @abstractmethod
    async def _upload_recv(self, req: UploadRequest) -> int:
        '''Read HTTP response for upload request *req*

        Return the size of the resulting object.
        '''
        pass

    async def _supervisor_loop(self):
        while True:
            try:
                async with trio.open_nursery() as nursery:
                    (send_ch, recv_ch) = trio.open_memory_channel(self._max_pipelined_uploads)
                    nursery.start_soon(self._upload_send_loop, send_ch, name='upload_writer')
                    nursery.start_soon(self._upload_recv_loop, recv_ch, name='upload_reader')
            except* Exception as exc:
                if not await self._backend._handle_retry_exc(exc):
                    raise
            else:
                return  # Tasks terminated normally

            # Retry after waiting

    async def _upload_send_loop(self, queue: trio.MemorySendChannel) -> None:
        '''Send data to upload objects

        Upload requestes are received from `self._upload_ch`. Once the data has been
        sent over the connection, the request is forwarded to *queue* for consumption
        by `upload_reader`.
        '''

        async with queue:
            req: UploadRequest

            if self._pending_reqs:
                # If this is not empty on start, then upload was interrupted by an exception and we need
                # to reprocess what we were working on at the time.
                for req in self._pending_reqs:
                    await self._upload_send(req)
                    await queue.send(req)

            while True:
                log.debug('waiting for upload request')
                try:
                    req = await self._queue.receive()
                except trio.EndOfChannel:
                    break
                self._pending_reqs.append(req)
                await self._upload_send(req)
                await queue.send(req)

    async def _upload_recv_loop(self, queue: trio.MemoryReceiveChannel) -> None:
        '''Receive responses for object uploads'''

        async with queue:
            req: UploadRequest

            while True:
                log.debug('waiting for upload request')
                try:
                    req = await queue.receive()
                except trio.EndOfChannel:
                    break
                size = await self._upload_recv(req)
                exp_req = self._pending_reqs.popleft()
                assert exp_req is req
                if req.callback is not None:
                    req.callback(size)


class AsyncBackend(object, metaclass=ABCMeta):
    @property
    def has_delete_multi(self):
        raise NotImplementedError()

    async def init(self, nursery: trio.Nursery) -> None:
        pass

    async def close(self) -> None:
        '''Close any opened resources

        This method closes any resources allocated by the backend (e.g. network sockets, worker
        tasks). This method should be called explicitly before a backend object is garbage
        collected.
        '''
        pass

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
        return False

    @abstractmethod
    async def readinto_fh(self, key: str, fh: BinaryIO):
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset.
        '''
        pass

    async def write_fh(
        self,
        key: str,
        fh: BinaryIO,
        metadata: Dict[str, Elementary],
        len_: Optional[int] = None,
        off: Optional[int] = None,
    ):
        '''Upload data in *fh* into object *key*

        Return the size of the resulting storage object.

        If *off* and *len_* are not specified, they are set to read from the current position to the
        end of the file. Concurrent access to *fh* that modifies the file position will result
        in data corruption.
        '''

        if off is None:
            off = fh.tell()
        if len_ is None:
            len_ = fh.seek(0, os.SEEK_END)
        event = trio.Event
        r_size = [None]

        def cb(size: int):
            r_size[0] = size
            event.set()

        await self.write_fh_async(key, fh, off=off, len_=len_, metadata=metadata, callback=cb)
        await event.wait()
        return r_size[0]

    @abstractmethod
    async def write_fh_async(
        self,
        key: str,
        fh: BinaryIO,
        metadata: Dict[str, Elementary],
        len_: Optional[int] = None,
        off: Optional[int] = None,
        callback: Optional[Callable[[int], None]] = None,
    ):
        '''Upload data in *fh* into object *key* asychronously

        *cb(size)* is called when the upload is complete, where *size* is the size of the resulting
        storage object.

        If *off* and *len_* are not specified, they are set to read from the current position to the
        end of the file. Concurrent access to *fh* that modifies the file position will result in
        data corruption.

        Objects are guaranteed to be created in the order in which this function is called.
        '''
        pass

    async def fetch(self, key):
        """Return data stored under `key`.

        Returns a tuple with the data and metadata.
        """

        fh = BytesIO()
        metadata = await self.readinto_fh(key, fh)
        return (fh.getvalue(), metadata)

    async def store(self, key, val, metadata=None):
        """Store data under `key`.

        `metadata` can be mapping with additional attributes to store with the object. Keys have to
        be of type `str`, values have to be of elementary type (`str`, `bytes`, `int`, `float` or
        `bool`).

        If no metadata is required, one can simply assign to the subscripted backend instead of
        using this function: ``backend[key] = val`` is equivalent to ``backend.store(key, val)``.
        """

        return await self.write_fh(key, BytesIO(val), metadata)

    @abstractmethod
    async def lookup(self, key):
        """Return metadata for given key.

        If the key does not exist, `NoSuchObject` is raised.
        """

        pass

    @abstractmethod
    async def get_size(self, key):
        '''Return size of object stored under *key*'''
        pass

    async def contains(self, key):
        '''Check if `key` is in backend'''

        try:
            await self.lookup(key)
        except NoSuchObject:
            return False
        else:
            return True

    @abstractmethod
    async def delete(self, key):
        """Delete object stored under `key`

        Attempts to delete non-existing objects will silently succeed.
        """
        pass

    async def delete_multi(self, keys):
        """Delete objects stored under `keys`

        Deleted objects are removed from the *keys* list, so that the caller can determine which
        objects have not yet been processed if an exception is occurs.

        Attempts to delete non-existing objects will silently succeed.
        """

        while keys:
            await self.delete(keys[-1])
            keys.pop()

    @abstractmethod
    async def list(self, prefix=''):
        '''List keys in backend

        Returns an iterator over all keys in the backend.
        '''
        pass


class HttpBackend(AsyncBackend):
    """Base class for Backends using HTTP connections"""

    known_options = {'ssl-ca-path', 'tcp-timeout', 'no-ssl'}
    upload_worker_class = UploadWorker

    def __init__(self, options):
        super().__init__()

        self.options = options.backend_options  # type: Dict[str, str]
        self.ssl_context = get_ssl_context(
            self.options.get('ssl-ca-path', None)
        )  # type: Optional[ssl.Context]
        self.proxy = get_proxy(ssl=not self.options.get('no-ssl', False))  # type: str
        self._conn: HTTPConnection = None

        pair = trio.open_memory_channel(0)
        self._upload_queue: trio.MemorySendChannel = pair[0]
        self._upload_worker = self.upload_worker_class(self, pair[1])

    async def init(self, nursery: trio.Nursery) -> None:
        '''Validate storage URL and credentials and establish connection.'''
        await super().init(nursery)
        self._conn = self._get_conn()

        @co_retry
        async def connect(self):
            await trio.to_thread.run_sync(self._conn.connect)

        await connect(self)
        await self._upload_worker.init(nursery)

    def _get_conn(self):
        conn = HTTPConnection(
            self.hostname, self.port, proxy=self.proxy, ssl_context=self.ssl_context
        )
        conn.timeout = int(self.options.get('tcp-timeout', 20))
        return conn

    async def _handle_retry_exc(self, exc_g: ExceptionGroup, conn: Optional[HTTPConnection] = None):
        '''Handle exception(s) caught by co_retry()

        Return *True* if the function should be retried and *False* if the exception should be
        propagated. Ensure that the HTTP connection is in a consistent state for retrying.
        '''

        if conn is None:
            conn = self._conn

        conn_reset = False
        for exc in exc_g.exceptions:
            if not is_temp_network_error(exc) and not isinstance(exc, ssl.SSLError):
                return False

            if conn and not conn_reset:
                await trio.to_thread.run_sync(conn.reset)
                conn_reset = True

        return True

    async def write_fh_async(
        self,
        key: str,
        fh: BinaryIO,
        metadata: Dict[str, Elementary],
        len_: Optional[int] = None,
        off: Optional[int] = None,
        callback: Optional[Callable[[int], None]] = None,
    ):
        '''Upload data in *fh* into object *key* asychronously

        *cb(size)* is called when the upload is complete, where *size* is the size of the resulting
        storage object.

        If *off* and *len_* are not specified, they are set to read from the current position to the
        end of the file. Concurrent access to *fh* that modifies the file position will result in
        data corruption.

        Objects are guaranteed to be created in the order in which this function is called.
        '''

        if off is None:
            off = fh.tell()
        if len_ is None:
            len_ = fh.seek(0, os.SEEK_END)

        req = UploadRequest(key=key, fh=fh, off=off, len=len_, callback=callback, metadata=metadata)
        await self._upload_queue.send(req)

    async def close(self):
        '''Close any opened resources

        This method closes any resources allocated by the backend (e.g. network sockets,
        worker tasks). This
        method should be called explicitly before a backend object is garbage collected.
        '''
        log.debug('closing')
        await super().close()
        self._upload_queue.close()
        await self._upload_worker.close()
