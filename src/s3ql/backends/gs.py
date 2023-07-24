'''
backends/gs.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import hashlib
import json
import logging
import os
import re
import ssl
import threading
import urllib.parse
from ast import literal_eval
from base64 import b64decode, b64encode
from itertools import count
from typing import Any, BinaryIO, Dict, Optional

from s3ql.http import (
    BodyFollowing,
    CaseInsensitiveDict,
    ConnectionClosed,
    HTTPConnection,
    HTTPResponse,
    UnsupportedResponse,
    is_temp_network_error,
)

from ..common import OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, copyfh
from ..logging import QuietError
from .common import (
    AbstractBackend,
    AuthenticationError,
    AuthorizationError,
    CorruptedObjectError,
    DanglingStorageURLError,
    NoSuchObject,
    get_proxy,
    get_ssl_context,
    retry,
)

try:
    import google.auth as g_auth
except ModuleNotFoundError:
    g_auth = None


log = logging.getLogger(__name__)


class ServerResponseError(Exception):
    '''Raised if the server response cannot be parsed.

    For HTTP errors (i.e., non 2xx response codes), RequestError should
    always be used instead (since in that case the response body can
    not be expected to have any specific format).
    '''

    def __init__(self, resp: HTTPResponse, error: str):
        self.resp = resp
        self.error = error

    def __str__(self):
        return '<ServerResponseError: %s>' % self.error


class RequestError(Exception):
    '''
    An error returned by the server.
    '''

    def __init__(
        self, code: str, reason: str, message: Optional[str] = None, body: Optional[str] = None
    ):
        super().__init__()
        self.code = code
        self.reason = reason
        self.message = message
        self.body = body

    def __str__(self) -> str:
        if self.message:
            return '<RequestError, code=%d, reason=%r, message=%r>' % (
                self.code,
                self.reason,
                self.message,
            )
        elif self.body:
            return '<RequestError, code=%d, reason=%r, with body data>' % (self.code, self.reason)
        else:
            return '<RequestError, code=%d, reason=%r>' % (self.code, self.reason)


class AccessTokenExpired(Exception):
    '''
    Raised if the access token has expired.
    '''


def _parse_error_response(resp, body: bytes):
    '''Return exception corresponding to server response.'''

    try:
        json_resp = _parse_json_response(resp, body)
    except ServerResponseError:
        # Error messages may come from intermediate proxies and thus may not be in JSON.
        log.debug('Server response not JSON - intermediate proxy failure?')
        return RequestError(code=resp.status, reason=resp.reason)

    try:
        message = json_resp['error']['message']
        body = None
    except KeyError:
        log.warning('Did not find error.message element in JSON error response. This is odd.')
        message = None
        body = str(json_resp)

    return RequestError(code=resp.status, reason=resp.reason, message=message)


def _parse_json_response(resp, body: bytes):
    # Note that even though the final server backend may guarantee to always deliver a JSON document
    # body with a detailed error message, we may also get errors from intermediate proxies.
    content_type = resp.headers.get('Content-Type', None)
    if content_type:
        hit = re.match(
            r'application/json(?:; charset="(.+)")?$',
            resp.headers['Content-Type'],
            re.IGNORECASE,
        )
    if not content_type or not hit:
        raise ServerResponseError(resp, error='expected json, got %s' % content_type)
    charset = hit.group(1)

    try:
        body_text = body.decode(charset)
    except UnicodeDecodeError as exc:
        log.warning('Unable to decode JSON response as Unicode (%s) - this is odd.', str(exc))
        raise ServerResponseError(resp, error=str(exc))

    try:
        resp_json = json.loads(body_text)
    except json.JSONDecodeError as exc:
        log.warning('Unable to decode JSON response (%s) - this is odd.', str(exc))
        raise ServerResponseError(resp, error=str(exc))

    return resp_json


class Backend(AbstractBackend):
    """A backend to store data in Google Storage"""

    known_options = {'ssl-ca-path', 'tcp-timeout'}

    # We don't want to request an access token for each instance,
    # because there is a limit on the total number of valid tokens.
    # This class variable holds the mapping from refresh tokens to
    # access tokens.
    access_token = dict()
    _refresh_lock = threading.Lock()
    adc = None

    def __init__(self, options):
        super().__init__()

        self.ssl_context = get_ssl_context(
            options.backend_options.get('ssl-ca-path', None)
        )  # type: Optional[ssl.Context]
        self.options = options.backend_options  # type: Dict[str, str]
        self.proxy = get_proxy(ssl=True)  # type: str
        self.login = options.backend_login  # type: str
        self.refresh_token = options.backend_password  # type: str

        if self.login == 'adc':
            if g_auth is None:
                raise QuietError('ADC authentication requires the google.auth module')
            elif self.adc is None:
                import google.auth.transport.urllib3
                import urllib3

                # Deliberately ignore proxy and SSL context when attempting
                # to connect to Compute Engine Metadata server.
                requester = google.auth.transport.urllib3.Request(urllib3.PoolManager())
                try:
                    credentials, _ = g_auth.default(
                        request=requester,
                        scopes=['https://www.googleapis.com/auth/devstorage.full_control'],
                    )
                except g_auth.exceptions.DefaultCredentialsError as exc:
                    raise QuietError('ADC found no valid credential sources: ' + str(exc))
                type(self).adc = (credentials, requester)
        elif self.login != 'oauth2':
            raise QuietError("Google Storage backend requires OAuth2 or ADC authentication")

        # Special case for unit testing against local mock server
        hit = re.match(
            r'^gs://!unittest!'
            r'([^/:]+)'  # Hostname
            r':([0-9]+)'  # Port
            r'/([^/]+)'  # Bucketname
            r'(?:/(.*))?$',  # Prefix
            options.storage_url,
        )
        if hit:
            self.hostname = hit.group(1)
            self.port = int(hit.group(2))
            self.bucket_name = hit.group(3)
            self.prefix = hit.group(4) or ''
        else:
            hit = re.match(r'^gs://([^/]+)(?:/(.*))?$', options.storage_url)
            if not hit:
                raise QuietError('Invalid storage URL', exitcode=2)

            self.bucket_name = hit.group(1)
            self.hostname = 'www.googleapis.com'
            self.prefix = hit.group(2) or ''
            self.port = 443

        self.conn = self._get_conn()
        self._check_bucket()

    @retry
    def _check_bucket(self):
        '''Check if bucket exists and/or credentials are correct.'''

        path = '/storage/v1/b/' + urllib.parse.quote(self.bucket_name, safe='')
        try:
            resp = self._do_request('GET', path)
        except RequestError as exc:
            if exc.code == 404:
                raise DanglingStorageURLError("Bucket '%s' does not exist" % self.bucket_name)
            exc = _map_request_error(exc, None)
            if exc:
                raise exc
            raise
        _parse_json_response(resp, self.conn.readall())

    def reset(self):
        if self.conn is not None and (self.conn.response_pending() or self.conn._out_remaining):
            log.debug('Resetting state of http connection %d', id(self.conn))
            self.conn.disconnect()

    def _get_conn(self):
        '''Return connection to server'''

        conn = HTTPConnection(
            self.hostname, self.port, proxy=self.proxy, ssl_context=self.ssl_context
        )
        conn.timeout = int(self.options.get('tcp-timeout', 20))
        return conn

    def is_temp_failure(self, exc):  # IGNORE:W0613
        if is_temp_network_error(exc) or isinstance(exc, ssl.SSLError):
            # We probably can't use the connection anymore, so use this
            # opportunity to reset it
            self.conn.reset()
            return True

        elif isinstance(exc, RequestError) and (500 <= exc.code <= 599 or exc.code == 408):
            return True

        elif isinstance(exc, AccessTokenExpired):
            # Delete may fail if multiple threads encounter the same error
            self.access_token.pop(self.refresh_token, None)
            return True

        # Not clear at all what is happening here, but in doubt we retry
        elif isinstance(exc, ServerResponseError):
            return True

        if g_auth and isinstance(exc, g_auth.exceptions.TransportError):
            return True

        return False

    def _assert_empty_response(self, resp):
        '''Assert that current response body is empty'''

        buf = self.conn.read(2048)
        if not buf:
            return  # expected

        body = '\n'.join('%s: %s' % x for x in resp.headers.items())

        hit = re.search(r'; charset="(.+)"$', resp.headers.get('Content-Type', ''), re.IGNORECASE)
        if hit:
            charset = hit.group(1)
            body += '\n' + buf.decode(charset, errors='backslashreplace')

        log.warning('Expected empty response body, but got data - this is odd.')
        raise ServerResponseError(resp, error='expected empty response')

    @retry
    def delete(self, key):
        log.debug('started with %s', key)
        path = '/storage/v1/b/%s/o/%s' % (
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + key, safe=''),
        )
        try:
            resp = self._do_request('DELETE', path)
            self._assert_empty_response(resp)
        except RequestError as exc:
            exc = _map_request_error(exc, key)
            if isinstance(exc, NoSuchObject):
                pass
            elif exc:
                raise exc
            else:
                raise

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
        query_string = {'prefix': prefix, 'maxResults': str(batch_size)}
        if page_token:
            query_string['pageToken'] = page_token

        path = '/storage/v1/b/%s/o' % (urllib.parse.quote(self.bucket_name, safe=''),)

        try:
            resp = self._do_request('GET', path, query_string=query_string)
        except RequestError as exc:
            exc = _map_request_error(exc, None)
            if exc:
                raise exc
            raise
        json_resp = _parse_json_response(resp, self.conn.readall())
        page_token = json_resp.get('nextPageToken', None)

        if 'items' not in json_resp:
            assert page_token is None
            return ((), None)

        return ([x['name'] for x in json_resp['items']], page_token)

    @retry
    def lookup(self, key):
        log.debug('started with %s', key)
        return _unwrap_user_meta(self._get_gs_meta(key))

    def _get_gs_meta(self, key):

        path = '/storage/v1/b/%s/o/%s' % (
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + key, safe=''),
        )
        try:
            resp = self._do_request('GET', path)
        except RequestError as exc:
            exc = _map_request_error(exc, key)
            if exc:
                raise exc
            raise
        return _parse_json_response(resp, self.conn.readall())

    @retry
    def get_size(self, key):
        json_resp = self._get_gs_meta(key)
        return json_resp['size']

    def readinto_fh(self, key: str, fh: BinaryIO):
        '''Transfer data stored under *key* into *fh*, return metadata.

        The data will be inserted at the current offset. If a temporary error (as defined by
        `is_temp_failure`) occurs, the operation is retried.
        '''

        return self._readinto_fh(key, fh, fh.tell())

    @retry
    def _readinto_fh(self, key: str, fh: BinaryIO, off: int):

        gs_meta = self._get_gs_meta(key)
        metadata = _unwrap_user_meta(gs_meta)

        path = '/storage/v1/b/%s/o/%s' % (
            urllib.parse.quote(self.bucket_name, safe=''),
            urllib.parse.quote(self.prefix + key, safe=''),
        )
        try:
            self._do_request('GET', path, query_string={'alt': 'media'})
        except RequestError as exc:
            exc = _map_request_error(exc, key)
            if exc:
                raise exc
            raise

        copyfh(self.conn, fh)

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
        retried. Returns the size of the resulting storage object.
        '''

        off = fh.tell()
        if len_ is None:
            fh.seek(0, os.SEEK_END)
            len_ = fh.tell()
        return self._write_fh(key, fh, off, len_, metadata or {})

    @retry
    def _write_fh(self, key: str, fh: BinaryIO, off: int, len_: int, metadata: Dict[str, Any]):

        metadata = json.dumps(
            {
                'metadata': _wrap_user_meta(metadata),
                'name': self.prefix + key,
            }
        )

        # Google Storage uses Content-Length to read the object data, so we
        # don't have to worry about the boundary occurring in the object data.
        boundary = 'foo_bar_baz'
        headers = CaseInsensitiveDict()
        headers['Content-Type'] = 'multipart/related; boundary=%s' % boundary

        body_prefix = '\n'.join(
            (
                '--' + boundary,
                'Content-Type: application/json; charset=UTF-8',
                '',
                metadata,
                '--' + boundary,
                'Content-Type: application/octet-stream',
                '',
                '',
            )
        ).encode()
        body_suffix = ('\n--%s--\n' % boundary).encode()

        body_size = len(body_prefix) + len(body_suffix) + len_

        path = '/upload/storage/v1/b/%s/o' % (urllib.parse.quote(self.bucket_name, safe=''),)
        query_string = {'uploadType': 'multipart'}
        try:
            resp = self._do_request(
                'POST',
                path,
                query_string=query_string,
                headers=headers,
                body=BodyFollowing(body_size),
            )
        except RequestError as exc:
            exc = _map_request_error(exc, key)
            if exc:
                raise exc
            raise

        assert resp.status == 100
        fh.seek(off)

        try:
            self.conn.write(body_prefix)
            copyfh(fh, self.conn, len_)
            self.conn.write(body_suffix)
        except ConnectionClosed:
            # Server closed connection while we were writing body data -
            # but we may still be able to read an error response
            try:
                resp = self.conn.read_response()
            except ConnectionClosed:  # No server response available
                pass
            else:
                log.warning(
                    'Server broke connection during upload, signaled %d %s',
                    resp.status,
                    resp.reason,
                )
            # Re-raise first ConnectionClosed exception
            raise

        resp = self.conn.read_response()
        # If we're really unlucky, then the token has expired while we were uploading data.
        if resp.status == 401:
            self.conn.discard()
            raise AccessTokenExpired()
        elif resp.status != 200:
            exc = _parse_error_response(resp, self.conn.co_readall())
            raise _map_request_error(exc, key) or exc
        _parse_json_response(resp, self.conn.readall())

        return body_size

    def close(self):
        self.conn.disconnect()

    def __str__(self):
        return '<gs.Backend, name=%s, prefix=%s>' % (self.bucket_name, self.prefix)

    # This method uses a different HTTP connection than its callers, but shares
    # the same retry logic. It is therefore possible that errors with this
    # connection cause the other connection to be reset - but this should not
    # be a problem, because there can't be a pending request if we don't have
    # a valid access token.
    def _get_access_token(self):
        log.info('Requesting new access token')

        if self.adc:
            try:
                self.adc[0].refresh(self.adc[1])
            except g_auth.exceptions.RefreshError as exc:
                raise AuthenticationError('Failed to refresh credentials: ' + str(exc))
            self.access_token[self.refresh_token] = self.adc[0].token
            return

        headers = CaseInsensitiveDict()
        headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=utf-8'

        body = urllib.parse.urlencode(
            {
                'client_id': OAUTH_CLIENT_ID,
                'client_secret': OAUTH_CLIENT_SECRET,
                'refresh_token': self.refresh_token,
                'grant_type': 'refresh_token',
            }
        )

        conn = HTTPConnection(
            'accounts.google.com', 443, proxy=self.proxy, ssl_context=self.ssl_context
        )
        try:

            conn.send_request('POST', '/o/oauth2/token', headers=headers, body=body.encode('utf-8'))
            resp = conn.read_response()
            json_resp = _parse_json_response(resp, conn.readall())

            if resp.status > 299 or resp.status < 200:
                assert 'error' in json_resp
            if 'error' in json_resp:
                raise AuthenticationError(json_resp['error'])
            else:
                self.access_token[self.refresh_token] = json_resp['access_token']
        finally:
            conn.disconnect()

    def _dump_body(self, resp):
        '''Return truncated string representation of response body.'''

        is_truncated = False
        try:
            body = self.conn.read(2048)
            if self.conn.read(1):
                is_truncated = True
                self.conn.discard()
        except UnsupportedResponse:
            log.warning('Unsupported response, trying to retrieve data from raw socket!')
            body = self.conn.read_raw(2048)
            self.conn.close()

        hit = re.search(r'; charset="(.+)"$', resp.headers.get('Content-Type', ''), re.IGNORECASE)
        if hit:
            charset = hit.group(1)
        else:
            charset = 'utf-8'

        body = body.decode(charset, errors='backslashreplace')

        if is_truncated:
            body += '... [truncated]'

        return body

    def _do_request(self, method, path, query_string=None, headers=None, body=None):
        '''Send request, read and return response object'''

        log.debug('started with %s %s, qs=%s', method, path, query_string)

        if headers is None:
            headers = CaseInsensitiveDict()

        expect100 = isinstance(body, BodyFollowing)
        headers['host'] = self.hostname
        if query_string:
            s = urllib.parse.urlencode(query_string, doseq=True)
            path += '?%s' % s

        # If we have an access token, try to use it.
        token = self.access_token.get(self.refresh_token, None)
        if token is not None:
            headers['Authorization'] = 'Bearer ' + token
            self.conn.send_request(method, path, body=body, headers=headers, expect100=expect100)
            resp = self.conn.read_response()
            if (expect100 and resp.status == 100) or (not expect100 and 200 <= resp.status <= 299):
                return resp
            elif resp.status != 401:
                raise _parse_error_response(resp, self.conn.readall())
            self.conn.discard()

        # If we reach this point, then the access token must have
        # expired, so we try to get a new one. We use a lock to prevent
        # multiple threads from refreshing the token simultaneously.
        with self._refresh_lock:
            # Don't refresh if another thread has already done so while
            # we waited for the lock.
            if token is None or self.access_token.get(self.refresh_token, None) == token:
                self._get_access_token()

        # Try request again. If this still fails, propagate the error
        # (because we have just refreshed the access token).
        # FIXME: We can't rely on this if e.g. the system hibernated
        # after refreshing the token, but before reaching this line.
        headers['Authorization'] = 'Bearer ' + self.access_token[self.refresh_token]
        self.conn.send_request(method, path, body=body, headers=headers, expect100=expect100)
        resp = self.conn.read_response()
        if (expect100 and resp.status == 100) or (not expect100 and 200 <= resp.status <= 299):
            return resp
        else:
            raise _parse_error_response(resp, self.conn.readall())


def _map_request_error(exc: RequestError, key: str):
    '''Map RequestError to more general exception if possible'''

    if exc.code == 404 and key:
        return NoSuchObject(key)
    elif exc.message == 'Forbidden':
        return AuthorizationError(exc.message)
    elif exc.message in ('Login Required', 'Invalid Credentials'):
        return AuthenticationError(exc.message)

    return None


def _wrap_user_meta(user_meta):

    obj_meta = dict()
    for (key, val) in user_meta.items():
        if not isinstance(key, str):
            raise TypeError('metadata keys must be str, not %s' % type(key))
        if not isinstance(val, (str, bytes, int, float, complex, bool)) and val is not None:
            raise TypeError('value for key %s (%s) is not elementary' % (key, val))
        if isinstance(val, (bytes, bytearray)):
            val = b64encode(val)

        obj_meta[key] = repr(val)

    return obj_meta


def _unwrap_user_meta(json_resp):
    '''Extract user metadata from JSON object metadata'''

    meta_raw = json_resp.get('metadata', None)
    if meta_raw is None:
        return {}

    # Detect Legacy format.
    if (
        meta_raw.get('format', None) == 'raw2'
        and 'md5' in meta_raw
        and all(key in ('format', 'md5') or re.match(r'^\d\d\d$', key) for key in meta_raw.keys())
    ):
        parts = []
        for i in count():
            part = meta_raw.get('%03d' % i, None)
            if part is None:
                break
            parts.append(part)
        buf = ''.join(parts)
        meta = literal_eval('{ %s }' % buf)
        for (k, v) in meta.items():
            if isinstance(v, bytes):
                meta[k] = b64decode(v)

        return meta

    meta = {}
    for (k, v) in meta_raw.items():
        try:
            v2 = literal_eval(v)
        except ValueError as exc:
            raise CorruptedObjectError('Invalid metadata value: ' + str(exc))
        if isinstance(v2, bytes):
            meta[k] = b64decode(v2)
        else:
            meta[k] = v2

    return meta


def md5sum_b64(buf):
    '''Return base64 encoded MD5 sum'''

    return b64encode(hashlib.md5(buf).digest()).decode('ascii')
