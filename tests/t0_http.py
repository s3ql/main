#!/usr/bin/env python3
'''
t1_http.py - this file is part of S3QL.

Copyright © 2014 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))


import hashlib
import html
import http.client
import itertools
import os
import re
import socketserver
import ssl
import threading
import time
from base64 import b64encode
from http.server import BaseHTTPRequestHandler
from io import TextIOWrapper

import pytest
from pytest import raises as assert_raises

import s3ql.http
from s3ql.http import (
    BodyFollowing,
    CaseInsensitiveDict,
    ConnectionClosed,
    ConnectionTimedOut,
    DNSUnavailable,
    ExcessBodyData,
    HostnameNotResolvable,
    HTTPConnection,
    StateError,
    UnsupportedResponse,
    _Buffer,
)

# We want to test with a real certificate
SSL_TEST_HOST = 'www.google.com'

TEST_DIR = os.path.dirname(__file__)


def check_for_internet_access():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.set_default_verify_paths()
    try:
        conn = http.client.HTTPSConnection(SSL_TEST_HOST, context=ssl_context)
        conn.request('GET', '/')
        resp = conn.getresponse()
        assert resp.status in (200, 301, 302)
        return True
    except:
        return False
    finally:
        conn.close()


no_internet_access = not check_for_internet_access()


class HTTPServer(socketserver.TCPServer):
    # Extended to add SSL support
    def get_request(self):
        (sock, addr) = super().get_request()
        if self.ssl_context:
            sock = self.ssl_context.wrap_socket(sock, server_side=True)
        return (sock, addr)


class HTTPServerThread(threading.Thread):
    def __init__(self, use_ssl=False):
        super().__init__()
        self.host = 'localhost'
        self.httpd = HTTPServer((self.host, 0), MockRequestHandler)
        self.port = self.httpd.socket.getsockname()[1]
        self.use_ssl = use_ssl
        self.timeout = 1

        if use_ssl:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
            ssl_context.verify_mode = ssl.CERT_NONE
            ssl_context.load_cert_chain(
                os.path.join(TEST_DIR, 'server.crt'), os.path.join(TEST_DIR, 'server.key')
            )
            self.httpd.ssl_context = ssl_context
        else:
            self.httpd.ssl_context = None

    def run(self):
        self.httpd.serve_forever()

    def shutdown(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def pytest_generate_tests(metafunc):
    if not 'http_server' in metafunc.fixturenames:
        return

    if hasattr(metafunc, 'definition'):
        if metafunc.definition.get_closest_marker('no_ssl'):
            params = ('plain',)
        else:
            params = ('plain', 'ssl')
    else:
        # pytest < 3.6
        if getattr(metafunc.function, 'no_ssl', False):
            params = ('plain',)
        else:
            params = ('plain', 'ssl')

    metafunc.parametrize("http_server", params, indirect=True, scope='module')


@pytest.fixture()
def http_server(request):
    httpd = HTTPServerThread(use_ssl=(request.param == 'ssl'))
    httpd.start()
    request.addfinalizer(httpd.shutdown)
    return httpd


@pytest.fixture()
def conn(request, http_server):
    if http_server.use_ssl:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_verify_locations(cafile=os.path.join(TEST_DIR, 'ca.crt'))
    else:
        ssl_context = None
    conn = HTTPConnection(http_server.host, port=http_server.port, ssl_context=ssl_context)
    conn.timeout = 0.5
    request.addfinalizer(conn.disconnect)
    return conn


@pytest.fixture(scope='module')
def random_fh(request):
    fh = open('/dev/urandom', 'rb')
    request.addfinalizer(fh.close)
    return fh


@pytest.mark.skipif(no_internet_access, reason='no internet access available')
def test_connect_ssl():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.set_default_verify_paths()

    conn = HTTPConnection(SSL_TEST_HOST, ssl_context=ssl_context)
    conn.send_request('GET', '/')
    resp = conn.read_response()
    assert resp.status in (200, 301, 302)
    assert resp.path == '/'
    conn.discard()
    conn.disconnect()


@pytest.mark.skipif(no_internet_access, reason='no internet access available')
def test_invalid_ssl():
    # Don't load certificates
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED

    conn = HTTPConnection(SSL_TEST_HOST, ssl_context=context)
    with pytest.raises(ssl.SSLError):
        conn.send_request('GET', '/')
    conn.disconnect()


@pytest.mark.skipif(no_internet_access, reason='no internet access available')
def test_dns_one(monkeypatch):
    monkeypatch.setattr(s3ql.http, 'DNS_TEST_HOSTNAMES', ((SSL_TEST_HOST, 443),))
    with pytest.raises(HostnameNotResolvable):
        conn = HTTPConnection('foobar.invalid')
        conn.connect()


def test_dns_two(monkeypatch):
    monkeypatch.setattr(s3ql.http, 'DNS_TEST_HOSTNAMES', (('grumpf.invalid', 80),))
    with pytest.raises(DNSUnavailable):
        conn = HTTPConnection('foobar.invalid')
        conn.connect()


@pytest.mark.parametrize('test_port', (None, 8080))
@pytest.mark.no_ssl
def test_http_proxy(http_server, monkeypatch, test_port):
    test_host = 'www.foobarz.invalid'
    test_path = '/someurl?barf'

    get_path = None

    def do_GET(self):
        nonlocal get_path
        get_path = self.path
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", '0')
        self.end_headers()

    monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)

    conn = HTTPConnection(test_host, test_port, proxy=(http_server.host, http_server.port))
    try:
        conn.send_request('GET', test_path)
        resp = conn.read_response()
        assert resp.status == 200
        conn.discard()
    finally:
        conn.disconnect()

    if test_port is None:
        exp_path = 'http://%s%s' % (test_host, test_path)
    else:
        exp_path = 'http://%s:%d%s' % (test_host, test_port, test_path)

    assert get_path == exp_path


class FakeSSLSocket:
    def __init__(self, socket):
        self.socket = socket

    def getpeercert(self):
        return None

    def __getattr__(self, name):
        return getattr(self.socket, name)


class FakeSSLContext:
    def wrap_socket(self, socket, server_hostname):
        return FakeSSLSocket(socket)

    def __bool__(self):
        return True


@pytest.mark.parametrize('test_port', (None, 8080))
@pytest.mark.no_ssl
def test_connect_proxy(http_server, monkeypatch, test_port):
    test_host = 'www.foobarz.invalid'
    test_path = '/someurl?barf'

    connect_path = None

    def do_CONNECT(self):
        # Pretend we're the remote server too
        nonlocal connect_path
        connect_path = self.path
        self.send_response(200)
        self.end_headers()
        self.close_connection = 0

    monkeypatch.setattr(MockRequestHandler, 'do_CONNECT', do_CONNECT, raising=False)

    get_path = None

    def do_GET(self):
        nonlocal get_path
        get_path = self.path
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", '0')
        self.end_headers()

    monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)

    # We don't *actually* want to establish SSL, that'd be
    # to complex for our mock server
    monkeypatch.setattr('ssl.match_hostname', lambda x, y: True)
    conn = HTTPConnection(
        test_host,
        test_port,
        proxy=(http_server.host, http_server.port),
        ssl_context=FakeSSLContext(),
    )
    try:
        conn.send_request('GET', test_path)
        resp = conn.read_response()
        assert resp.status == 200
        conn.discard()
    finally:
        conn.disconnect()

    if test_port is None:
        test_port = 443
    exp_path = '%s:%d' % (test_host, test_port)
    assert connect_path == exp_path
    assert get_path == test_path


def get_chunked_GET_handler(path, chunks, delay=None):
    '''Return GET handler for *path* sending *chunks* of data'''

    def do_GET(self):
        if self.path != path:
            self.send_error(500, 'Assertion failure: %s != %s' % (self.path, path))
            return
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Transfer-Encoding", 'chunked')
        self.end_headers()
        for i, chunk_size in enumerate(chunks):
            if i % 3 == 0 and delay:
                time.sleep(delay * 1e-3)
            self.wfile.write(('%x\r\n' % chunk_size).encode('us-ascii'))
            if i % 3 == 1 and delay:
                self.wfile.write(DUMMY_DATA[: chunk_size // 2])
                time.sleep(delay * 1e-3)
                self.wfile.write(DUMMY_DATA[chunk_size // 2 : chunk_size])
            else:
                self.wfile.write(DUMMY_DATA[:chunk_size])
            if i % 3 == 2 and delay:
                time.sleep(delay * 1e-3)
            self.wfile.write(b'\r\n')
            self.wfile.flush()
        self.wfile.write(b'0\r\n\r\n')

    return do_GET


def test_get_pipeline(conn):
    # We assume that internal buffers are big enough to hold
    # a few requests

    paths = ['/send_120_bytes' for _ in range(3)]

    # Send requests
    for path in paths:
        conn.send_request('GET', path)

    # Read responses
    for path in paths:
        resp = conn.read_response()
        assert resp.status == 200
        assert resp.path == path
        assert conn.readall() == DUMMY_DATA[:120]


async def test_blocking_send(conn, random_fh, monkeypatch):
    # Send requests until we block because all TCP buffers are full

    out_len = 102400
    in_len = 8192
    path = '/buo?com'

    def do_GET(self):
        self.rfile.read(in_len)
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", str(out_len))
        self.end_headers()
        self.wfile.write(random_fh.read(out_len))

    monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)

    with pytest.raises(ConnectionTimedOut):
        for count in itertools.count():
            await conn.co_send_request('GET', path, body=random_fh.read(in_len))
            if count > 1000000:
                pytest.fail("no blocking even after %d requests!?" % count)

    # Read responses
    for _ in range(count):
        resp = await conn.co_read_response()
        assert resp.status == 200
        await conn.co_discard()

    # Now we should be able to complete the request
    await conn.co_send_request('GET', path, body=random_fh.read(in_len))

    resp = await conn.co_read_response()
    assert resp.status == 200
    await conn.co_discard()


class CountSuspensions:
    '''Count how often a coroutine is suspended'''

    def __init__(self):
        self.n = 0

    def __call__(self, /, fn, *a, **k):
        self.fn = fn
        self.a = a
        self.k = k
        return self

    def __await__(self):
        it = self.fn(*self.a, **self.k).__await__()
        try:
            r = None
            while True:
                r = it.send(r)
                r = yield r
                self.n += 1
        except StopIteration as r:
            return r.value


async def test_blocking_read(conn, monkeypatch):
    path = '/foo/wurfl'
    chunks = [120] * 10
    delay = 10

    while True:
        monkeypatch.setattr(
            MockRequestHandler, 'do_GET', get_chunked_GET_handler(path, chunks, delay)
        )
        await conn.co_send_request('GET', path)

        resp = await conn.co_read_response()
        assert resp.status == 200

        t = CountSuspensions()
        parts = []
        while True:
            buf = await t(conn.co_read, 100)
            if not buf:
                break
            parts.append(buf)
        assert not conn.response_pending()
        assert b''.join(parts) == b''.join(DUMMY_DATA[:x] for x in chunks)

        if t.n >= 8:
            break
        elif delay > 5000:
            pytest.fail('no blocking read even with %f sec sleep' % delay)
        delay *= 2


def test_discard(conn, monkeypatch):
    data_len = 512
    path = '/send_%d_bytes' % data_len
    conn.send_request('GET', path)
    resp = conn.read_response()
    assert resp.status == 200
    assert resp.path == path
    assert resp.length == data_len
    conn.discard()
    assert not conn.response_pending()


def test_discard_chunked(conn, monkeypatch):
    path = '/foo/wurfl'
    chunks = [512, 312, 837, 361]
    monkeypatch.setattr(MockRequestHandler, 'do_GET', get_chunked_GET_handler(path, chunks))

    conn.send_request('GET', path)
    resp = conn.read_response()
    assert resp.status == 200
    assert resp.path == path
    assert resp.length is None
    conn.discard()
    assert not conn.response_pending()


def test_read_text(conn):
    conn.send_request('GET', '/send_%d_bytes' % len(DUMMY_DATA))
    conn.read_response()
    fh = TextIOWrapper(conn)
    assert fh.read() == DUMMY_DATA.decode('utf8')
    assert not conn.response_pending()


def test_read_text2(conn):
    conn.send_request('GET', '/send_%d_bytes' % len(DUMMY_DATA))
    conn.read_response()
    fh = TextIOWrapper(conn)

    # This used to fail because TextIOWrapper can't deal with bytearrays
    fh.read(42)


def test_read_text3(conn):
    conn.send_request('GET', '/send_%d_bytes' % len(DUMMY_DATA))
    conn.read_response()
    fh = TextIOWrapper(conn)

    # This used to fail because TextIOWrapper tries to read from
    # the underlying fh even after getting ''
    while True:
        if not fh.read(77):
            break

    assert not conn.response_pending()


def test_read_identity(conn):
    conn.send_request('GET', '/send_512_bytes')
    resp = conn.read_response()
    assert resp.status == 200
    assert resp.path == '/send_512_bytes'
    assert resp.length == 512
    assert conn.readall() == DUMMY_DATA[:512]
    assert not conn.response_pending()


def test_conn_close_1(conn, monkeypatch):
    data_size = 500
    conn._rbuf = _Buffer(int(4 / 5 * data_size))

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Connection", 'close')
        self.end_headers()
        self.wfile.write(DUMMY_DATA[:data_size])
        self.rfile.close()
        self.wfile.close()

    monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)

    conn.send_request('GET', '/whatever')
    resp = conn.read_response()
    assert resp.status == 200
    assert conn.readall() == DUMMY_DATA[:data_size]

    with pytest.raises(ConnectionClosed):
        conn.send_request('GET', '/whatever')
        conn.read_response()


def test_conn_close_3(conn, monkeypatch):
    # Server keeps reading
    data_size = 500
    conn._rbuf = _Buffer(int(4 / 5 * data_size))

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Connection", 'close')
        self.end_headers()
        self.wfile.write(DUMMY_DATA[:data_size])
        self.wfile.close()

    monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)

    conn.send_request('GET', '/whatever')
    resp = conn.read_response()
    assert resp.status == 200
    assert conn.readall() == DUMMY_DATA[:data_size]

    conn.send_request('GET', '/whatever')
    assert_raises(ConnectionClosed, conn.read_response)


def test_conn_close_4(conn, monkeypatch):
    # Content-Length should take precedence
    data_size = 500
    conn._rbuf = _Buffer(int(4 / 5 * data_size))

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", str(data_size))
        self.send_header("Connection", 'close')
        self.end_headers()
        self.wfile.write(DUMMY_DATA[: data_size + 10])
        self.wfile.close()

    monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)

    conn.send_request('GET', '/whatever')
    resp = conn.read_response()
    assert resp.status == 200
    assert conn.readall() == DUMMY_DATA[:data_size]


def test_conn_close_5(conn, monkeypatch):
    # Pipelining
    data_size = 500
    conn._rbuf = _Buffer(int(4 / 5 * data_size))

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", str(data_size))
        self.send_header("Connection", 'close')
        self.end_headers()
        self.wfile.write(DUMMY_DATA[:data_size])
        self.wfile.close()
        self.rfile.close()

    monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)

    conn.send_request('GET', '/whatever_one')
    conn.send_request('GET', '/whatever_two')
    resp = conn.read_response()
    assert resp.status == 200
    assert conn.readall() == DUMMY_DATA[:data_size]
    assert_raises(ConnectionClosed, conn.read_response)


@pytest.mark.no_ssl
async def test_exhaust_buffer(conn):
    conn._rbuf = _Buffer(600)
    conn.send_request('GET', '/send_512_bytes')
    conn.read_response()

    # Test the case where the read buffer is truncated and
    # returned, instead of copied
    conn._rbuf.compact()
    await conn._co_fill_buffer(1)
    assert conn._rbuf.b == 0
    assert conn._rbuf.e > 0
    buf = await conn.co_read(600)
    assert len(conn._rbuf.d) == 600
    assert buf == DUMMY_DATA[: len(buf)]
    assert conn.readall() == DUMMY_DATA[len(buf) : 512]


@pytest.mark.no_ssl
def test_full_buffer(conn):
    conn._rbuf = _Buffer(100)
    conn.send_request('GET', '/send_512_bytes')
    conn.read_response()

    buf = conn.read(101)
    pos = len(buf)
    assert buf == DUMMY_DATA[:pos]

    # Make buffer empty, but without capacity for more
    assert conn._rbuf.e == 0
    conn._rbuf.e = len(conn._rbuf.d)
    conn._rbuf.b = conn._rbuf.e

    assert conn.readall() == DUMMY_DATA[pos:512]


def test_read_chunked(conn, monkeypatch):
    path = '/foo/wurfl'
    chunks = [300, 283, 377]
    monkeypatch.setattr(MockRequestHandler, 'do_GET', get_chunked_GET_handler(path, chunks))
    conn.send_request('GET', path)
    resp = conn.read_response()
    assert resp.status == 200
    assert resp.path == path
    assert resp.length is None
    assert conn.readall() == b''.join(DUMMY_DATA[:x] for x in chunks)
    assert not conn.response_pending()


def test_read_chunked2(conn, monkeypatch):
    path = '/foo/wurfl'
    chunks = [5] * 10
    monkeypatch.setattr(MockRequestHandler, 'do_GET', get_chunked_GET_handler(path, chunks))
    conn.send_request('GET', path)
    resp = conn.read_response()
    assert resp.status == 200
    assert resp.length is None
    assert resp.path == path
    assert conn.readall() == b''.join(DUMMY_DATA[:x] for x in chunks)
    assert not conn.response_pending()


def test_double_read(conn):
    conn.send_request('GET', '/send_10_bytes')
    resp = conn.read_response()
    assert resp.status == 200
    assert resp.length == 10
    assert resp.path == '/send_10_bytes'
    with pytest.raises(StateError):
        resp = conn.read_response()


def test_read_raw(conn, monkeypatch):
    path = '/ooops'

    def do_GET(self):
        assert self.path == path
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.end_headers()
        self.wfile.write(b'body data')
        self.wfile.close()

    monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)
    conn.send_request('GET', path)
    resp = conn.read_response()
    assert resp.status == 200
    with pytest.raises(UnsupportedResponse):
        conn.readall()
    assert conn.read_raw(512) == b'body data'
    assert conn.read_raw(512) == b''


def test_abort_read(conn, monkeypatch):
    path = '/foo/wurfl'
    chunks = [300, 317, 283]
    monkeypatch.setattr(MockRequestHandler, 'do_GET', get_chunked_GET_handler(path, chunks))
    conn.send_request('GET', path)
    resp = conn.read_response()
    assert resp.status == 200
    conn.read(200)
    conn.disconnect()
    assert_raises(ConnectionClosed, conn.read, 200)


def test_abort_write(conn):
    conn.send_request('PUT', '/allgood', body=BodyFollowing(42))
    conn.write(b'fooo')
    conn.disconnect()
    assert_raises(ConnectionClosed, conn.write, b'baar')


def test_write_toomuch(conn):
    conn.send_request('PUT', '/allgood', body=BodyFollowing(42))
    with pytest.raises(ExcessBodyData):
        conn.write(DUMMY_DATA[:43])


def test_write_toolittle(conn):
    conn.send_request('PUT', '/allgood', body=BodyFollowing(42))
    conn.write(DUMMY_DATA[:24])
    with pytest.raises(StateError):
        conn.send_request('GET', '/send_5_bytes')


def test_write_toolittle2(conn):
    conn.send_request('PUT', '/allgood', body=BodyFollowing(42))
    conn.write(DUMMY_DATA[:24])
    with pytest.raises(StateError):
        conn.read_response()


def test_write_toolittle3(conn):
    conn.send_request('GET', '/send_10_bytes')
    conn.send_request('PUT', '/allgood', body=BodyFollowing(42))
    conn.write(DUMMY_DATA[:24])
    resp = conn.read_response()
    assert resp.status == 200
    assert resp.path == '/send_10_bytes'
    assert len(conn.readall()) == 10
    with pytest.raises(StateError):
        conn.read_response()


def test_put(conn):
    data = DUMMY_DATA
    conn.send_request('PUT', '/allgood', body=data)
    resp = conn.read_response()
    conn.discard()
    assert resp.status == 204
    assert resp.length == 0
    assert resp.reason == 'MD5 matched'

    headers = CaseInsensitiveDict()
    headers['Content-MD5'] = 'nUzaJEag3tOdobQVU/39GA=='
    conn.send_request('PUT', '/allgood', body=data, headers=headers)
    resp = conn.read_response()
    conn.discard()
    assert resp.status == 400
    assert resp.reason.startswith('MD5 mismatch')


def test_put_separate(conn):
    data = DUMMY_DATA
    conn.send_request('PUT', '/allgood', body=BodyFollowing(len(data)))
    conn.write(data)
    resp = conn.read_response()
    conn.discard()
    assert resp.status == 204
    assert resp.length == 0
    assert resp.reason == 'Ok, but no MD5'

    headers = CaseInsensitiveDict()
    headers['Content-MD5'] = b64encode(hashlib.md5(data).digest()).decode('ascii')
    conn.send_request('PUT', '/allgood', body=BodyFollowing(len(data)), headers=headers)
    conn.write(data)
    resp = conn.read_response()
    conn.discard()
    assert resp.status == 204
    assert resp.length == 0
    assert resp.reason == 'MD5 matched'

    headers['Content-MD5'] = 'nUzaJEag3tOdobQVU/39GA=='
    conn.send_request('PUT', '/allgood', body=BodyFollowing(len(data)), headers=headers)
    conn.write(data)
    resp = conn.read_response()
    conn.discard()
    assert resp.status == 400
    assert resp.reason.startswith('MD5 mismatch')


def test_100cont(conn, monkeypatch):
    path = '/check_this_out'

    def handle_expect_100(self):
        if self.path != path:
            self.send_error(500, 'Assertion error, %s != %s' % (self.path, path))
        else:
            self.send_error(403)

    monkeypatch.setattr(MockRequestHandler, 'handle_expect_100', handle_expect_100)

    conn.send_request('PUT', path, body=BodyFollowing(256), expect100=True)
    resp = conn.read_response()
    assert resp.status == 403
    conn.discard()

    def handle_expect_100(self):
        if self.path != path:
            self.send_error(500, 'Assertion error, %s != %s' % (self.path, path))
            return

        self.send_response_only(100)
        self.end_headers()
        return True

    monkeypatch.setattr(MockRequestHandler, 'handle_expect_100', handle_expect_100)
    conn.send_request('PUT', path, body=BodyFollowing(256), expect100=True)
    resp = conn.read_response()
    assert resp.status == 100
    assert resp.length == 0
    conn.write(DUMMY_DATA[:256])
    resp = conn.read_response()
    assert resp.status == 204
    assert resp.length == 0


def test_100cont_2(conn, monkeypatch):
    def handle_expect_100(self):
        self.send_error(403)

    monkeypatch.setattr(MockRequestHandler, 'handle_expect_100', handle_expect_100)
    conn.send_request('PUT', '/fail_with_403', body=BodyFollowing(256), expect100=True)

    with pytest.raises(StateError):
        conn.send_request('PUT', '/fail_with_403', body=BodyFollowing(256), expect100=True)

    conn.read_response()
    conn.readall()


def test_100cont_3(conn, monkeypatch):
    def handle_expect_100(self):
        self.send_error(403)

    monkeypatch.setattr(MockRequestHandler, 'handle_expect_100', handle_expect_100)
    conn.send_request('PUT', '/fail_with_403', body=BodyFollowing(256), expect100=True)

    with pytest.raises(StateError):
        conn.write(b'barf!')

    conn.read_response()
    conn.readall()


def test_aborted_write1(conn, monkeypatch, random_fh):
    BUFSIZE = 64 * 1024

    # monkeypatch request handler
    def do_PUT(self):
        # Read half the data, then generate error and
        # close connection
        self.rfile.read(BUFSIZE)
        self.send_error(code=401, message='Please stop!')
        self.close_connection = True

    monkeypatch.setattr(MockRequestHandler, 'do_PUT', do_PUT)

    # Send request
    conn.send_request('PUT', '/big_object', body=BodyFollowing(BUFSIZE * 50), expect100=True)
    resp = conn.read_response()
    assert resp.status == 100
    assert resp.length == 0

    # Try to write data
    with pytest.raises(ConnectionClosed):
        for _ in range(50):
            conn.write(random_fh.read(BUFSIZE))

    # Nevertheless, try to read response
    resp = conn.read_response()
    assert resp.status == 401
    assert resp.reason == 'Please stop!'


def test_aborted_write2(conn, monkeypatch, random_fh):
    BUFSIZE = 64 * 1024

    # monkeypatch request handler
    def do_PUT(self):
        # Read half the data, then silently close connection
        self.rfile.read(BUFSIZE)
        self.close_connection = True

    monkeypatch.setattr(MockRequestHandler, 'do_PUT', do_PUT)

    # Send request
    conn.send_request('PUT', '/big_object', body=BodyFollowing(BUFSIZE * 50), expect100=True)
    resp = conn.read_response()
    assert resp.status == 100
    assert resp.length == 0

    # Try to write data
    with pytest.raises(ConnectionClosed):
        for _ in range(50):
            conn.write(random_fh.read(BUFSIZE))

    # Nevertheless, try to read response
    assert_raises(ConnectionClosed, conn.read_response)


def test_read_toomuch(conn):
    conn.send_request('GET', '/send_10_bytes')
    conn.send_request('GET', '/send_8_bytes')
    resp = conn.read_response()
    assert resp.status == 200
    assert resp.path == '/send_10_bytes'
    assert conn.readall() == DUMMY_DATA[:10]
    assert conn.read(8) == b''


def test_read_toolittle(conn):
    conn.send_request('GET', '/send_10_bytes')
    resp = conn.read_response()
    assert resp.status == 200
    assert resp.path == '/send_10_bytes'
    buf = conn.read(8)
    assert buf == DUMMY_DATA[: len(buf)]
    with pytest.raises(StateError):
        resp = conn.read_response()


def test_empty_response(conn):
    conn.send_request('HEAD', '/send_512_bytes')
    resp = conn.read_response()
    assert resp.status == 200
    assert resp.path == '/send_512_bytes'
    assert resp.length == 0

    # Check that we can go to the next response without
    # reading anything
    assert not conn.response_pending()
    conn.send_request('GET', '/send_512_bytes')
    resp = conn.read_response()
    assert resp.status == 200
    assert resp.path == '/send_512_bytes'
    assert resp.length == 512
    assert conn.readall() == DUMMY_DATA[:512]
    assert not conn.response_pending()


def test_head(conn, monkeypatch):
    conn.send_request('HEAD', '/send_10_bytes')
    resp = conn.read_response()
    assert resp.status == 200
    assert len(conn.readall()) == 0

    def do_HEAD(self):
        self.send_error(317)

    monkeypatch.setattr(MockRequestHandler, 'do_HEAD', do_HEAD)
    conn.send_request('HEAD', '/fail_with_317')
    resp = conn.read_response()
    assert resp.status == 317
    assert len(conn.readall()) == 0


@pytest.fixture(params=(63, 64, 65, 100, 99, 103, 500, 511, 512, 513))
def buffer_size(request):
    return request.param


def test_smallbuffer(conn, buffer_size):
    conn._rbuf = _Buffer(buffer_size)
    conn.send_request('GET', '/send_512_bytes')
    resp = conn.read_response()
    assert resp.status == 200
    assert resp.path == '/send_512_bytes'
    assert resp.length == 512
    assert conn.readall() == DUMMY_DATA[:512]
    assert not conn.response_pending()


def test_mutable_read(conn):
    # Read data and modify it, to make sure that this doesn't
    # affect the buffer

    conn._rbuf = _Buffer(129)
    conn.send_request('GET', '/send_512_bytes')
    conn.read_response()

    # Assert that buffer is full, but does not start at beginning
    assert conn._rbuf.b > 0

    buf = conn.read(150)
    pos = len(buf)
    assert buf == DUMMY_DATA[:pos]
    memoryview(buf)[:10] = b'\0' * 10

    # Assert that buffer is empty
    assert conn._rbuf.b == 0
    assert conn._rbuf.e == 0
    buf = conn.read(150)
    assert buf == DUMMY_DATA[pos : pos + len(buf)]
    memoryview(buf)[:10] = b'\0' * 10
    pos += len(buf)

    assert conn.readall() == DUMMY_DATA[pos:512]
    assert not conn.response_pending()


def test_recv_timeout(conn, monkeypatch):
    conn.timeout = 0.5

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", '50')
        self.end_headers()
        self.wfile.write(b'x' * 25)
        self.wfile.flush()

    monkeypatch.setattr(MockRequestHandler, 'do_GET', do_GET)

    conn.send_request('GET', '/send_something')
    resp = conn.read_response()
    assert resp.status == 200
    assert conn.read(50) == b'x' * 25
    assert_raises(ConnectionTimedOut, conn.read, 50)


def test_send_timeout(conn, monkeypatch, random_fh):
    conn.timeout = 0.5

    def do_PUT(self):
        # Read just a tiny bit
        self.rfile.read(256)

        # We need to sleep, or the rest of the incoming data will
        # be parsed as the next request.
        time.sleep(2 * conn.timeout)

    monkeypatch.setattr(MockRequestHandler, 'do_PUT', do_PUT)

    # We don't know how much data can be buffered, so we
    # claim to send a lot and do so in a loop.
    len_ = 1024**3
    conn.send_request('PUT', '/recv_something', body=BodyFollowing(len_))
    with pytest.raises(ConnectionTimedOut):
        while len_ > 0:
            conn.write(random_fh.read(min(len_, 16 * 1024)))


DUMMY_DATA = ','.join(str(x) for x in range(10000)).encode()


class MockRequestHandler(BaseHTTPRequestHandler):
    server_version = "MockHTTP"
    protocol_version = 'HTTP/1.1'

    def log_message(self, format, *args):
        pass

    def handle(self):
        # Ignore exceptions resulting from the client closing
        # the connection.
        try:
            return super().handle()
        except ValueError as exc:
            if exc.args == ('I/O operation on closed file.',):
                pass
            else:
                raise
        except (ssl.SSLEOFError, ssl.SSLZeroReturnError):
            pass
        # Linux generates BrokenPipeError, FreeBSD uses ConnectionResetError
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        len_ = int(self.headers['Content-Length'])
        if len_:
            self.rfile.read(len_)

        hit = re.match(r'^/send_([0-9]+)_bytes', self.path)
        if hit:
            len_ = int(hit.group(1))
            self.do_HEAD()
            self.wfile.write(DUMMY_DATA[:len_])
            return

        self.send_error(500)

    def do_PUT(self):
        encoding = self.headers['Content-Encoding']
        if encoding and encoding != 'identity':
            self.send_error(415)
            return

        len_ = int(self.headers['Content-Length'])
        data = self.rfile.read(len_)
        if 'Content-MD5' in self.headers:
            md5 = b64encode(hashlib.md5(data).digest()).decode('ascii')
            if md5 != self.headers['Content-MD5']:
                self.send_error(400, 'MD5 mismatch: %s vs %s' % (md5, self.headers['Content-MD5']))
                return

            self.send_response(204, 'MD5 matched')
        else:
            self.send_response(204, 'Ok, but no MD5')

        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_HEAD(self):
        hit = re.match(r'^/send_([0-9]+)_bytes', self.path)
        if hit:
            len_ = int(hit.group(1))
            self.send_response(200)
            self.send_header("Content-Type", 'application/octet-stream')
            self.send_header("Content-Length", str(len_))
            self.end_headers()
            return

        # No idea
        self.send_error(500)

    def send_error(self, code, message=None):
        # Overwritten to not close connection on errors and provide
        # content-length
        try:
            shortmsg, longmsg = self.responses[code]
        except KeyError:
            shortmsg, longmsg = '???', '???'
        if message is None:
            message = shortmsg
        explain = longmsg
        self.log_error("code %d, message %s", code, message)
        # HTML encode to prevent Cross Site Scripting attacks (see bug #1100201)
        content = (
            self.error_message_format
            % {'code': code, 'message': html.escape(message, quote=False), 'explain': explain}
        ).encode('utf-8', 'replace')
        self.send_response(code, message)
        self.send_header("Content-Type", self.error_content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        if self.command != 'HEAD' and code >= 200 and code not in (204, 304):
            self.wfile.write(content)
