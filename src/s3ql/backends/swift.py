'''
swift.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import hashlib
import json
import logging
import os
import re
import ssl
import urllib.parse
from typing import Any, BinaryIO, Dict, Optional
from urllib.parse import urlsplit

from s3ql.http import (
    BodyFollowing,
    CaseInsensitiveDict,
    ConnectionClosed,
    HTTPConnection,
    is_temp_network_error,
)
from packaging.version import Version

from s3ql.common import copyfh

from ..logging import LOG_ONCE, QuietError
from . import s3c
from .common import (
    AbstractBackend,
    AuthorizationError,
    DanglingStorageURLError,
    NoSuchObject,
    get_proxy,
    get_ssl_context,
    retry,
)
from .s3c import BadDigestError, HTTPError, md5sum_b64

log = logging.getLogger(__name__)

#: Suffix to use when creating temporary objects
TEMP_SUFFIX = '_tmp$oentuhuo23986konteuh1062$'


class Backend(AbstractBackend):
    """A backend to store data in OpenStack Swift"""

    hdr_prefix = 'X-Object-'
    known_options = {
        'no-ssl',
        'ssl-ca-path',
        'tcp-timeout',
        'disable-expect100',
        'no-feature-detection',
    }

    _add_meta_headers = s3c.Backend._add_meta_headers
    _extractmeta = s3c.Backend._extractmeta
    _assert_empty_response = s3c.Backend._assert_empty_response
    _dump_response = s3c.Backend._dump_response
    reset = s3c.Backend.reset

    def __init__(self, options):
        super().__init__()
        self.options = options.backend_options
        self.hostname = None
        self.port = None
        self.container_name = None
        self.prefix = None
        self.auth_token = None
        self.auth_prefix = None
        self.conn = None
        self.password = options.backend_password
        self.login = options.backend_login
        self.features = Features()

        # We may need the context even if no-ssl has been specified,
        # because no-ssl applies only to the authentication URL.
        self.ssl_context = get_ssl_context(self.options.get('ssl-ca-path', None))

        self._parse_storage_url(options.storage_url, self.ssl_context)
        self.proxy = get_proxy(self.ssl_context is not None)
        self._container_exists()

    def __str__(self):
        return 'swift container %s, prefix %s' % (self.container_name, self.prefix)

    @retry
    def _container_exists(self):
        '''Make sure that the container exists'''

        try:
            self._do_request('GET', '/', query_string={'limit': 1})
            self.conn.discard()
        except HTTPError as exc:
            if exc.status == 404:
                raise DanglingStorageURLError(self.container_name)
            raise

    def _parse_storage_url(self, storage_url, ssl_context):
        '''Init instance variables from storage url'''

        hit = re.match(
            r'^[a-zA-Z0-9]+://'  # Backend
            r'([^/:]+)'  # Hostname
            r'(?::([0-9]+))?'  # Port
            r'/([^/]+)'  # Bucketname
            r'(?:/(.*))?$',  # Prefix
            storage_url,
        )
        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        hostname = hit.group(1)
        if hit.group(2):
            port = int(hit.group(2))
        elif ssl_context:
            port = 443
        else:
            port = 80
        containername = hit.group(3)
        prefix = hit.group(4) or ''

        self.hostname = hostname
        self.port = port
        self.container_name = containername
        self.prefix = prefix

    def is_temp_failure(self, exc):  # IGNORE:W0613
        if isinstance(exc, AuthenticationExpired):
            return True

        # In doubt, we retry on 5xx (Server error). However, there are some
        # codes where retry is definitely not desired. For 4xx (client error) we
        # do not retry in general, but for 408 (Request Timeout) RFC 2616
        # specifies that the client may repeat the request without
        # modifications. We also retry on 429 (Too Many Requests).
        elif isinstance(exc, HTTPError) and (
            (500 <= exc.status <= 599 and exc.status not in (501, 505, 508, 510, 511, 523))
            or exc.status in (408, 429)
            or 'client disconnected' in exc.msg.lower()
        ):
            return True

        elif is_temp_network_error(exc):
            return True

        # Temporary workaround for
        # https://bitbucket.org/nikratio/s3ql/issues/87 and
        # https://bitbucket.org/nikratio/s3ql/issues/252
        elif isinstance(exc, ssl.SSLError) and (
            str(exc).startswith('[SSL: BAD_WRITE_RETRY]')
            or str(exc).startswith('[SSL: BAD_LENGTH]')
        ):
            return True

        return False

    def _get_conn(self):
        '''Obtain connection to server and authentication token'''

        log.debug('started')

        if 'no-ssl' in self.options:
            ssl_context = None
        else:
            ssl_context = self.ssl_context

        headers = CaseInsensitiveDict()
        headers['X-Auth-User'] = self.login
        headers['X-Auth-Key'] = self.password

        with HTTPConnection(
            self.hostname, self.port, proxy=self.proxy, ssl_context=ssl_context
        ) as conn:
            conn.timeout = int(self.options.get('tcp-timeout', 20))

            for auth_path in ('/v1.0', '/auth/v1.0'):
                log.debug('GET %s', auth_path)
                conn.send_request('GET', auth_path, headers=headers)
                resp = conn.read_response()

                if resp.status in (404, 412):
                    log.debug('auth to %s failed, trying next path', auth_path)
                    conn.discard()
                    continue

                elif resp.status == 401:
                    raise AuthorizationError(resp.reason)

                elif resp.status > 299 or resp.status < 200:
                    raise HTTPError(resp.status, resp.reason, resp.headers)

                # Pylint can't infer SplitResult Types
                # pylint: disable=E1103
                self.auth_token = resp.headers['X-Auth-Token']
                o = urlsplit(resp.headers['X-Storage-Url'])
                self.auth_prefix = urllib.parse.unquote(o.path)
                if o.scheme == 'https':
                    ssl_context = self.ssl_context
                elif o.scheme == 'http':
                    ssl_context = None
                else:
                    # fall through to scheme used for authentication
                    pass

                # mock server can only handle one connection at a time
                # so we explicitly disconnect this connection before
                # opening the feature detection connection
                # (mock server handles both - storage and authentication)
                conn.disconnect()

                self._detect_features(o.hostname, o.port, ssl_context)

                conn = HTTPConnection(o.hostname, o.port, proxy=self.proxy, ssl_context=ssl_context)
                conn.timeout = int(self.options.get('tcp-timeout', 20))
                return conn

            raise RuntimeError('No valid authentication path found')

    def _do_request(
        self,
        method,
        path,
        subres=None,
        query_string=None,
        headers=None,
        body=None,
        body_len: Optional[int] = None,
    ):
        '''Send request, read and return response object

        This method modifies the *headers* dictionary.
        '''

        log.debug(
            'started with %r, %r, %r, %r, %r, %r', method, path, subres, query_string, headers, body
        )

        if headers is None:
            headers = CaseInsensitiveDict()

        if isinstance(body, (bytes, bytearray, memoryview)):
            headers['Content-MD5'] = md5sum_b64(body)

        if self.conn is None:
            log.debug('no active connection, calling _get_conn()')
            self.conn = self._get_conn()

        # Construct full path
        path = urllib.parse.quote('%s/%s%s' % (self.auth_prefix, self.container_name, path))
        if query_string:
            s = urllib.parse.urlencode(query_string, doseq=True)
            if subres:
                path += '?%s&%s' % (subres, s)
            else:
                path += '?%s' % s
        elif subres:
            path += '?%s' % subres

        headers['X-Auth-Token'] = self.auth_token
        try:
            resp = self._do_request_inner(
                method, path, body=body, headers=headers, body_len=body_len
            )
        except Exception as exc:
            if is_temp_network_error(exc) or isinstance(exc, ssl.SSLError):
                # We probably can't use the connection anymore
                self.conn.disconnect()
            raise

        # Success
        if resp.status >= 200 and resp.status <= 299:
            return resp

        # Expired auth token
        if resp.status == 401:
            self._do_authentication_expired(resp.reason)
            # raises AuthenticationExpired

        # If method == HEAD, server must not return response body
        # even in case of errors
        self.conn.discard()
        if method.upper() == 'HEAD':
            raise HTTPError(resp.status, resp.reason, resp.headers)
        else:
            raise HTTPError(resp.status, resp.reason, resp.headers)

    # Including this code directly in _do_request would be very messy since
    # we can't `return` the response early, thus the separate method
    def _do_request_inner(self, method, path, body, headers, body_len: Optional[int] = None):
        '''The guts of the _do_request method'''

        log.debug('started with %s %s', method, path)
        use_expect_100c = not self.options.get('disable-expect100', False)

        if body is None or isinstance(body, (bytes, bytearray, memoryview)):
            self.conn.send_request(method, path, body=body, headers=headers)
            return self.conn.read_response()

        self.conn.send_request(
            method, path, expect100=use_expect_100c, headers=headers, body=BodyFollowing(body_len)
        )

        if use_expect_100c:
            log.debug('waiting for 100-continue')
            resp = self.conn.read_response()
            if resp.status != 100:
                return resp

        log.debug('writing body data')
        md5 = hashlib.md5()
        try:
            copyfh(body, self.conn, len_=body_len, update=md5.update)
        except ConnectionClosed:
            log.debug('interrupted write, server closed connection')
            # Server closed connection while we were writing body data -
            # but we may still be able to read an error response
            try:
                resp = self.conn.read_response()
            except ConnectionClosed:  # No server response available
                log.debug('no response available in  buffer')
                pass
            else:
                if resp.status >= 400:  # error response
                    return resp
                log.warning(
                    'Server broke connection during upload, but signaled %d %s',
                    resp.status,
                    resp.reason,
                )

            # Re-raise original error
            raise

        resp = self.conn.read_response()

        # On success, check MD5. Not sure if this is returned every time we send a request body, but
        # it seems to work. If not, we have to somehow pass in the information when this is expected
        # (i.e, when storing an object)
        if resp.status >= 200 and resp.status <= 299:
            etag = resp.headers['ETag'].strip('"')

            if etag != md5.hexdigest():
                raise BadDigestError(
                    'BadDigest',
                    f'MD5 mismatch when sending {method} {path} (received: {etag}, sent: {md5.hexdigest()})',
                )

        return resp

    @retry
    def lookup(self, key):
        log.debug('started with %s', key)
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)

        try:
            resp = self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            self._assert_empty_response(resp)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        return self._extractmeta(resp, key)

    @retry
    def get_size(self, key):
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)
        log.debug('started with %s', key)

        try:
            resp = self._do_request('HEAD', '/%s%s' % (self.prefix, key))
            self._assert_empty_response(resp)
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            else:
                raise

        try:
            return int(resp.headers['Content-Length'])
        except KeyError:
            raise RuntimeError('HEAD request did not return Content-Length')

    def readinto_fh(self, key: str, fh: BinaryIO):
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset. If a temporary error (as defined by
        `is_temp_failure`) occurs, the operation is retried.
        '''

        return self._readinto_fh(key, fh, fh.tell())

    @retry
    def _readinto_fh(self, key: str, fh: BinaryIO, off: int):
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)
        try:
            resp = self._do_request('GET', '/%s%s' % (self.prefix, key))
        except HTTPError as exc:
            if exc.status == 404:
                raise NoSuchObject(key)
            raise
        etag = resp.headers['ETag'].strip('"')

        try:
            meta = self._extractmeta(resp, key)
        except BadDigestError:
            # If there's less than 64 kb of data, read and throw
            # away. Otherwise re-establish connection.
            if resp.length is not None and resp.length < 64 * 1024:
                self.conn.discard()
            else:
                self.conn.disconnect()
            raise

        md5 = hashlib.md5()
        fh.seek(off)
        copyfh(self.conn, fh, update=md5.update)

        if etag != md5.hexdigest():
            log.warning('MD5 mismatch for %s: %s vs %s', key, etag, md5.hexdigest())
            raise BadDigestError('BadDigest', 'ETag header does not agree with calculated MD5')

        return meta

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
        retried. Returns the size of the resulting storage object.
        '''

        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)

        off = fh.tell()
        if len_ is None:
            fh.seek(0, os.SEEK_END)
            len_ = fh.tell()
        return self._write_fh(key, fh, off, len_, metadata or {})

    @retry
    def _write_fh(self, key: str, fh: BinaryIO, off: int, len_: int, metadata: Dict[str, Any]):
        headers = CaseInsensitiveDict()
        self._add_meta_headers(headers, metadata, chunksize=self.features.max_meta_len)

        headers['Content-Type'] = 'application/octet-stream'
        fh.seek(off)
        try:
            resp = self._do_request(
                'PUT',
                '/%s%s' % (self.prefix, key),
                headers=headers,
                body=fh,
                body_len=len_,
            )
        except BadDigestError:
            # Object was corrupted in transit, make sure to delete it
            self.delete(key)
            raise

        self._assert_empty_response(resp)
        return len_

    @retry
    def delete(self, key):
        if key.endswith(TEMP_SUFFIX):
            raise ValueError('Keys must not end with %s' % TEMP_SUFFIX)
        log.debug('started with %s', key)
        try:
            resp = self._do_request('DELETE', '/%s%s' % (self.prefix, key))
            self._assert_empty_response(resp)
        except HTTPError as exc:
            if exc.status == 404:
                pass
            else:
                raise

    @retry
    def _delete_multi(self, keys):
        """Doing bulk delete of multiple objects at a time.

        This is a feature of the configurable middleware "Bulk" so it can only
        be used after the middleware was detected. (Introduced in Swift 1.8.0.rc1)

        See https://docs.openstack.org/swift/latest/middleware.html#bulk-delete
        and https://github.com/openstack/swift/blob/master/swift/common/middleware/bulk.py

        A request example:
        'POST /?bulk-delete
        Content-type: text/plain; charset=utf-8
        Accept: application/json

        /container/prefix_key1
        /container/prefix_key2'

        A successful response:
        'HTTP/1.1 200 OK
        Content-Type: application/json

        {"Number Not Found": 0,
         "Response Status": "200 OK",
         "Response Body": "",
         "Errors": [],
         "Number Deleted": 2}'

        An error response:
        'HTTP/1.1 200 OK
        Content-Type: application/json

        {"Number Not Found": 0,
         "Response Status": "500 Internal Server Error",
         "Response Body": "An error description",
         "Errors": [
            ['/container/prefix_key2', 'An error description']
         ],
         "Number Deleted": 1}'

        Response when some objects where not found:
        'HTTP/1.1 200 OK
        Content-Type: application/json

        {"Number Not Found": 1,
         "Response Status": "400 Bad Request",
         "Response Body": "Invalid bulk delete.",
         "Errors": [],
         "Number Deleted": 1}'
        """

        body = []
        esc_prefix = "/%s/%s" % (
            urllib.parse.quote(self.container_name),
            urllib.parse.quote(self.prefix),
        )
        for key in keys:
            body.append('%s%s' % (esc_prefix, urllib.parse.quote(key)))
        body = '\n'.join(body).encode('utf-8')
        headers = {'content-type': 'text/plain; charset=utf-8', 'accept': 'application/json'}

        resp = self._do_request('POST', '/', subres='bulk-delete', body=body, headers=headers)

        # bulk deletes should always return 200
        if resp.status != 200:
            raise HTTPError(resp.status, resp.reason, resp.headers)

        hit = re.match(r'^application/json(;\s*charset="?(.+?)"?)?$', resp.headers['content-type'])
        if not hit:
            log.error(
                'Unexpected server response. Expected json, got:\n%s', self._dump_response(resp)
            )
            raise RuntimeError('Unexpected server reply')

        # there might be an arbitrary amount of whitespace before the
        # JSON response (to keep the connection from timing out)
        # but json.loads discards these whitespace characters automatically
        resp_dict = json.loads(self.conn.readall().decode(hit.group(2) or 'utf-8'))

        log.debug('Response %s', resp_dict)

        try:
            resp_status_code, resp_status_text = _split_response_status(
                resp_dict['Response Status']
            )
        except ValueError:
            raise RuntimeError('Unexpected server reply')

        if resp_status_code == 200:
            # No errors occurred, everything has been deleted
            del keys[:]
            return

        # Some errors occurred, so we need to determine what has
        # been deleted and what hasn't
        failed_keys = []
        offset = len(esc_prefix)
        for error in resp_dict['Errors']:
            fullkey = error[0]
            # strangely the name is url encoded in JSON
            assert fullkey.startswith(esc_prefix)
            key = urllib.parse.unquote(fullkey[offset:])
            failed_keys.append(key)
            log.debug('Delete %s failed with %s', key, error[1])
        for key in keys[:]:
            if key not in failed_keys:
                keys.remove(key)

        if resp_status_code in (400, 404) and len(resp_dict['Errors']) == 0:
            # Swift returns 400 instead of 404 when files were not found.
            # (but we also accept the correct status code 404 if Swift
            # decides to correct this in the future)

            # ensure that we actually have objects that were not found
            # (otherwise there is a logic error that we need to know about)
            assert resp_dict['Number Not Found'] > 0

            # Since AbstractBackend.delete_multi allows this, we just
            # swallow this error even when *force* is False.
            # N.B.: We removed even the keys from *keys* that are not found.
            # This is because Swift only returns a counter of deleted
            # objects, not the list of deleted objects (as S3 does).
            return

        # At this point it is clear that the server has sent some kind of error response.
        # Swift is not very consistent in returning errors.
        # We need to jump through these hoops to get something meaningful.

        error_msg = resp_dict['Response Body']
        error_code = resp_status_code
        if not error_msg:
            if len(resp_dict['Errors']) > 0:
                error_code, error_msg = _split_response_status(resp_dict['Errors'][0][1])
            else:
                error_msg = resp_status_text

        if error_code == 401:
            # Expired auth token
            self._do_authentication_expired(error_msg)
            # raises AuthenticationExpired
        if 'Invalid bulk delete.' in error_msg:
            error_code = 400
            # change error message to something more meaningful
            error_msg = 'Sent a bulk delete with an empty list of keys to delete'
        elif 'Max delete failures exceeded' in error_msg:
            error_code = 502
        elif 'Maximum Bulk Deletes: ' in error_msg:
            # Sent more keys in one bulk delete than allowed
            error_code = 413
        elif 'Invalid File Name' in error_msg:
            # get returned when file name is too long
            error_code = 422

        raise HTTPError(error_code, error_msg, {})

    @property
    def has_delete_multi(self):
        return self.features.has_bulk_delete

    def delete_multi(self, keys):
        log.debug('started with %s', keys)

        if self.features.has_bulk_delete:
            while len(keys) > 0:
                tmp = keys[: self.features.max_deletes]
                try:
                    self._delete_multi(tmp)
                finally:
                    keys[: self.features.max_deletes] = tmp
        else:
            super().delete_multi(keys)

    def list(self, prefix=''):
        prefix = self.prefix + prefix
        strip = len(self.prefix)
        page_token = None
        while True:
            (els, page_token) = self._list_page(prefix, page_token)
            for el in els:
                yield el[strip:]
            if page_token is None:
                break

    @retry
    def _list_page(self, prefix, page_token=None, batch_size=1000):
        # Limit maximum number of results since we read everything
        # into memory (because Python JSON doesn't have a streaming API)
        query_string = {'prefix': prefix, 'limit': str(batch_size), 'format': 'json'}
        if page_token:
            query_string['marker'] = page_token

        try:
            resp = self._do_request('GET', '/', query_string=query_string)
        except HTTPError as exc:
            if exc.status == 404:
                raise DanglingStorageURLError(self.container_name)
            raise

        if resp.status == 204:
            return

        hit = re.match('application/json; charset="?(.+?)"?$', resp.headers['content-type'])
        if not hit:
            log.error(
                'Unexpected server response. Expected json, got:\n%s', self._dump_response(resp)
            )
            raise RuntimeError('Unexpected server reply')

        body = self.conn.readall()
        names = []
        count = 0
        for dataset in json.loads(body.decode(hit.group(1))):
            count += 1
            name = dataset['name']
            if name.endswith(TEMP_SUFFIX):
                continue
            names.append(name)

        if count == batch_size:
            page_token = names[-1]
        else:
            page_token = None

        return (names, page_token)

    def close(self):
        self.conn.disconnect()

    def _detect_features(self, hostname, port, ssl_context):
        '''Try to figure out the Swift version and supported features by
        examining the /info endpoint of the storage server.

        See https://docs.openstack.org/swift/latest/middleware.html#discoverability
        '''

        if 'no-feature-detection' in self.options:
            log.debug('Skip feature detection')
            return

        if not port:
            port = 443 if ssl_context else 80

        detected_features = Features()

        with HTTPConnection(hostname, port, proxy=self.proxy, ssl_context=ssl_context) as conn:
            conn.timeout = int(self.options.get('tcp-timeout', 20))

            log.debug('GET /info')
            conn.send_request('GET', '/info')
            resp = conn.read_response()

            # 200, 401, 403 and 404 are all OK since the /info endpoint
            # may not be accessible (misconfiguration) or may not
            # exist (old Swift version).
            if resp.status not in (200, 401, 403, 404):
                log.error(
                    "Wrong server response.\n%s", self._dump_response(resp, body=conn.read(2048))
                )
                raise HTTPError(resp.status, resp.reason, resp.headers)

            if resp.status == 200:
                hit = re.match(
                    r'^application/json(;\s*charset="?(.+?)"?)?$', resp.headers['content-type']
                )
                if not hit:
                    log.error(
                        "Wrong server response. Expected json. Got: \n%s",
                        self._dump_response(resp, body=conn.read(2048)),
                    )
                    raise RuntimeError('Unexpected server reply')

                info = json.loads(conn.readall().decode(hit.group(2) or 'utf-8'))
                swift_info = info.get('swift', {})

                log.debug('%s:%s/info returns %s', hostname, port, info)

                swift_version_string = swift_info.get('version', None)
                if swift_version_string and Version(swift_version_string) >= Version('2.8'):
                    detected_features.has_copy = True

                # Default metadata value length constrain is 256 bytes
                # but the provider could configure another value.
                # We only decrease the chunk size since 255 is a big enough chunk size.
                max_meta_len = swift_info.get('max_meta_value_length', None)
                if isinstance(max_meta_len, int) and max_meta_len < 256:
                    detected_features.max_meta_len = max_meta_len

                if info.get('bulk_delete', False):
                    detected_features.has_bulk_delete = True
                    bulk_delete = info['bulk_delete']
                    assert bulk_delete.get('max_failed_deletes', 1000) <= bulk_delete.get(
                        'max_deletes_per_request', 10000
                    )
                    assert bulk_delete.get('max_failed_deletes', 1000) > 0
                    # The block cache removal queue has a capacity of 1000.
                    # We do not need bigger values than that.
                    # We use max_failed_deletes instead of max_deletes_per_request
                    # because then we can be sure even when all our delete requests
                    # get rejected we get a complete error list back from the server.
                    # If we would set the value higher, _delete_multi() would maybe
                    # delete some entries from the *keys* list that did not get
                    # deleted and would miss them in a retry.
                    detected_features.max_deletes = min(
                        1000, int(bulk_delete.get('max_failed_deletes', 1000))
                    )

                log.info(
                    'Detected Swift features for %s:%s: %s',
                    hostname,
                    port,
                    detected_features,
                    extra=LOG_ONCE,
                )
            else:
                log.debug(
                    '%s:%s/info not found or not accessible. Skip feature detection.',
                    hostname,
                    port,
                )

        self.features = detected_features

    def _do_authentication_expired(self, reason):
        '''Closes the current connection and raises AuthenticationExpired'''
        log.info('OpenStack auth token seems to have expired, requesting new one.')
        self.conn.disconnect()
        # Force constructing a new connection with a new token, otherwise
        # the connection will be reestablished with the same token.
        self.conn = None
        raise AuthenticationExpired(reason)


def _split_response_status(line):
    '''Splits a HTTP response line into status code (integer)
    and status text.

    Returns 2-tuple (int, string)

    Raises ValueError when line is not parsable'''
    hit = re.match(r'^([0-9]{3})\s+(.*)$', line)
    if not hit:
        log.error('Expected valid Response Status, got: %s', line)
        raise ValueError('Expected valid Response Status, got: %s' % line)
    return (int(hit.group(1)), hit.group(2))


class AuthenticationExpired(Exception):
    '''Raised if the provided Authentication Token has expired'''

    def __init__(self, msg):
        super().__init__()
        self.msg = msg

    def __str__(self):
        return 'Auth token expired. Server said: %s' % self.msg


class Features:
    """Set of configurable features for Swift servers.

    Swift is deployed in many different versions and configurations.
    To be able to use advanced features like bulk delete we need to
    make sure that the Swift server we are using can handle them.

    This is a value object."""

    __slots__ = ['has_copy', 'has_bulk_delete', 'max_deletes', 'max_meta_len']

    def __init__(self, has_copy=False, has_bulk_delete=False, max_deletes=1000, max_meta_len=255):
        self.has_copy = has_copy
        self.has_bulk_delete = has_bulk_delete
        self.max_deletes = max_deletes
        self.max_meta_len = max_meta_len

    def __str__(self):
        features = []
        if self.has_copy:
            features.append('copy via COPY')
        if self.has_bulk_delete:
            features.append('Bulk delete %d keys at a time' % self.max_deletes)
        features.append('maximum meta value length is %d bytes' % self.max_meta_len)
        return ', '.join(features)

    def __repr__(self):
        init_kwargs = [p + '=' + repr(getattr(self, p)) for p in self.__slots__]
        return 'Features(%s)' % ', '.join(init_kwargs)

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return repr(self) == repr(other)

    def __ne__(self, other):
        return repr(self) != repr(other)
