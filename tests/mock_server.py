#!/usr/bin/env python3
'''
mock_server.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import hashlib
import json
import logging
import re
import socketserver
import urllib.parse
from http.server import BaseHTTPRequestHandler
from xml.sax.saxutils import escape as xml_escape

log = logging.getLogger(__name__)

ERROR_RESPONSE_TEMPLATE = '''\
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>%(code)s</Code>
  <Message>%(message)s</Message>
  <Resource>%(resource)s</Resource>
  <RequestId>%(request_id)s</RequestId>
</Error>
'''

COPY_RESPONSE_TEMPLATE = '''\
<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult xmlns="%(ns)s">
   <LastModified>2008-02-20T22:13:01</LastModified>
   <ETag>&quot;%(etag)s&quot;</ETag>
</CopyObjectResult>
'''


class StorageServer(socketserver.TCPServer):
    def __init__(self, request_handler, server_address):
        super().__init__(server_address, request_handler)
        self.data = dict()
        self.metadata = dict()
        self.hostname = self.server_address[0]
        self.port = self.server_address[1]


class ParsedURL:
    __slots__ = ['bucket', 'key', 'params', 'fragment']


class S3CRequestHandler(BaseHTTPRequestHandler):
    '''A request handler implementing a subset of the AWS S3 Interface

    Bucket names are ignored, all keys share the same global
    namespace.
    '''

    server_version = "MockHTTP"
    protocol_version = 'HTTP/1.1'
    meta_header_re = re.compile(r'X-AMZ-Meta-([a-z0-9_.-]+)$', re.IGNORECASE)
    hdr_prefix = 'X-AMZ-'
    xml_ns = 'http://s3.amazonaws.com/doc/2006-03-01/'

    def parse_url(self, path):
        p = ParsedURL()
        q = urllib.parse.urlsplit(path)

        path = urllib.parse.unquote(q.path)

        assert path[0] == '/'
        (p.bucket, p.key) = path[1:].split('/', maxsplit=1)

        p.params = urllib.parse.parse_qs(q.query)
        p.fragment = q.fragment

        return p

    def log_message(self, format, *args):
        log.debug(format, *args)

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
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_DELETE(self):
        q = self.parse_url(self.path)
        try:
            del self.server.data[q.key]
            del self.server.metadata[q.key]
        except KeyError:
            self.send_error(404, code='NoSuchKey', resource=q.key)
            return
        else:
            self.send_response(204)
            self.end_headers()

    def _check_encoding(self):
        encoding = self.headers['Content-Encoding']
        if 'Content-Length' not in self.headers:
            self.send_error(400, message='Missing Content-Length', code='MissingContentLength')
            return
        elif encoding and encoding != 'identity':
            self.send_error(501, message='Unsupported encoding', code='NotImplemented')
            return

        return int(self.headers['Content-Length'])

    def _get_meta(self):
        meta = dict()
        for name, value in self.headers.items():
            hit = self.meta_header_re.search(name)
            if hit:
                meta[hit.group(1)] = value
        return meta

    def do_PUT(self):
        len_ = self._check_encoding()
        q = self.parse_url(self.path)
        meta = self._get_meta()

        src = self.headers.get(self.hdr_prefix + 'copy-source')
        if src and len_:
            self.send_error(
                400, message='Upload and copy are mutually exclusive', code='UnexpectedContent'
            )
            return
        elif src:
            src = urllib.parse.unquote(src)
            hit = re.match('^/([a-z0-9._-]+)/(.+)$', src)
            if not hit:
                self.send_error(400, message='Cannot parse copy-source', code='InvalidArgument')
                return

            metadata_directive = self.headers.get(self.hdr_prefix + 'metadata-directive', 'COPY')
            if metadata_directive not in ('COPY', 'REPLACE'):
                self.send_error(400, message='Invalid metadata directive', code='InvalidArgument')
                return
            src = hit.group(2)
            try:
                data = self.server.data[src]
                self.server.data[q.key] = data
                if metadata_directive == 'COPY':
                    self.server.metadata[q.key] = self.server.metadata[src]
                else:
                    self.server.metadata[q.key] = meta
            except KeyError:
                self.send_error(404, code='NoSuchKey', resource=src)
                return
        else:
            data = self.rfile.read(len_)
            self.server.metadata[q.key] = meta
            self.server.data[q.key] = data

        md5 = hashlib.md5()
        md5.update(data)

        if src:
            content = (
                COPY_RESPONSE_TEMPLATE % {'etag': md5.hexdigest(), 'ns': self.xml_ns}
            ).encode('utf-8')
            self.send_response(200)
            self.send_header('ETag', '"%s"' % md5.hexdigest())
            self.send_header('Content-Length', str(len(content)))
            self.send_header("Content-Type", 'text/xml')
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_response(201)
            self.send_header('ETag', '"%s"' % md5.hexdigest())
            self.send_header('Content-Length', '0')
            self.end_headers()

    def handle_expect_100(self):
        if self.command == 'PUT':
            self._check_encoding()

        self.send_response_only(100)
        self.end_headers()
        return True

    def do_GET(self):
        q = self.parse_url(self.path)
        if not q.key:
            return self.do_list(q)

        try:
            data = self.server.data[q.key]
            meta = self.server.metadata[q.key]
        except KeyError:
            self.send_error(404, code='NoSuchKey', resource=q.key)
            return

        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", str(len(data)))
        for name, value in meta.items():
            self.send_header(self.hdr_prefix + 'Meta-%s' % name, value)
        md5 = hashlib.md5()
        md5.update(data)
        self.send_header('ETag', '"%s"' % md5.hexdigest())
        self.end_headers()
        self.send_data(data)

    def send_data(self, data):
        self.wfile.write(data)

    def do_list(self, q):
        marker = q.params['marker'][0] if 'marker' in q.params else None
        max_keys = int(q.params['max_keys'][0]) if 'max_keys' in q.params else 1000
        prefix = q.params['prefix'][0] if 'prefix' in q.params else ''

        resp = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<ListBucketResult xmlns="%s">' % self.xml_ns,
            '<MaxKeys>%d</MaxKeys>' % max_keys,
            '<IsTruncated>false</IsTruncated>',
        ]

        count = 0
        for key in sorted(self.server.data):
            if not key.startswith(prefix):
                continue
            if marker and key <= marker:
                continue
            resp.append('<Contents><Key>%s</Key></Contents>' % xml_escape(key))
            count += 1
            if count == max_keys:
                resp[3] = '<IsTruncated>true</IsTruncated>'
                break

        resp.append('</ListBucketResult>')
        body = '\n'.join(resp).encode()

        self.send_response(200)
        self.send_header("Content-Type", 'text/xml')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        q = self.parse_url(self.path)
        try:
            meta = self.server.metadata[q.key]
            data = self.server.data[q.key]
        except KeyError:
            self.send_error(404, code='NoSuchKey', resource=q.key)
            return

        self.send_response(200)
        self.send_header("Content-Type", 'application/octet-stream')
        self.send_header("Content-Length", str(len(data)))
        for name, value in meta.items():
            self.send_header(self.hdr_prefix + 'Meta-%s' % name, value)
        self.end_headers()

    def send_error(self, status, message=None, code='', resource='', extra_headers=None):
        if not message:
            try:
                (_, message) = self.responses[status]
            except KeyError:
                message = 'Unknown'

        self.log_error("code %d, message %s", status, message)
        content = (
            ERROR_RESPONSE_TEMPLATE
            % {
                'code': code,
                'message': xml_escape(message),
                'request_id': 42,
                'resource': xml_escape(resource),
            }
        ).encode('utf-8', 'replace')
        self.send_response(status, message)
        self.send_header("Content-Type", 'text/xml; charset="utf-8"')
        self.send_header("Content-Length", str(len(content)))
        if extra_headers:
            for name, value in extra_headers.items():
                self.send_header(name, value)
        self.end_headers()
        if self.command != 'HEAD' and status >= 200 and status not in (204, 304):
            self.wfile.write(content)


class BasicSwiftRequestHandler(S3CRequestHandler):
    '''A request handler implementing a subset of the OpenStack Swift Interface

    Container and AUTH_* prefix are ignored, all keys share the same global
    namespace.

    To keep it simple, this handler is both storage server and authentication
    server in one.
    '''

    meta_header_re = re.compile(r'X-Object-Meta-([a-z0-9_.-]+)$', re.IGNORECASE)
    hdr_prefix = 'X-Object-'

    SWIFT_INFO = {
        "swift": {
            "max_meta_count": 90,
            "max_meta_value_length": 256,
            "container_listing_limit": 10000,
            "extra_header_count": 0,
            "max_meta_overall_size": 4096,
            "version": "2.0.0",  # < 2.8
            "max_meta_name_length": 128,
            "max_header_size": 16384,
        }
    }

    def parse_url(self, path):
        p = ParsedURL()
        q = urllib.parse.urlsplit(path)

        path = urllib.parse.unquote(q.path)

        assert path[0:4] == '/v1/'
        (_, p.bucket, p.key) = path[4:].split('/', maxsplit=2)

        p.params = urllib.parse.parse_qs(q.query, True)
        p.fragment = q.fragment

        return p

    def do_PUT(self):
        len_ = self._check_encoding()
        q = self.parse_url(self.path)
        meta = self._get_meta()

        src = self.headers.get('x-copy-from')
        if src and len_:
            self.send_error(
                400, message='Upload and copy are mutually exclusive', code='UnexpectedContent'
            )
            return
        elif src:
            src = urllib.parse.unquote(src)
            hit = re.match('^/([a-z0-9._-]+)/(.+)$', src)
            if not hit:
                self.send_error(400, message='Cannot parse x-copy-from', code='InvalidArgument')
                return

            src = hit.group(2)
            try:
                data = self.server.data[src]
                self.server.data[q.key] = data
                if 'x-fresh-metadata' in self.headers:
                    self.server.metadata[q.key] = meta
                else:
                    self.server.metadata[q.key] = self.server.metadata[src].copy()
                    self.server.metadata[q.key].update(meta)
            except KeyError:
                self.send_error(404, code='NoSuchKey', resource=src)
                return
        else:
            data = self.rfile.read(len_)
            self.server.metadata[q.key] = meta
            self.server.data[q.key] = data

        md5 = hashlib.md5()
        md5.update(data)

        if src:
            self.send_response(202)
            self.send_header('X-Copied-From', self.headers['x-copy-from'])
            self.send_header('Content-Length', '0')
            self.end_headers()
        else:
            self.send_response(201)
            self.send_header('ETag', '"%s"' % md5.hexdigest())
            self.send_header('Content-Length', '0')
            self.end_headers()

    def do_POST(self):
        q = self.parse_url(self.path)
        meta = self._get_meta()

        if q.key not in self.server.metadata:
            self.send_error(404, code='NoSuchKey', resource=q.key)
            return

        self.server.metadata[q.key] = meta

        self.send_response(204)
        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_GET(self):
        if self.path in ('/v1.0', '/auth/v1.0'):
            self.send_response(200)
            self.send_header(
                'X-Storage-Url',
                'http://%s:%d/v1/AUTH_xyz' % (self.server.hostname, self.server.port),
            )
            self.send_header('X-Auth-Token', 'static')
            self.send_header('Content-Length', '0')
            self.end_headers()
        elif self.path == '/info':
            content = json.dumps(self.SWIFT_INFO).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Length', str(len(content)))
            self.send_header("Content-Type", 'application/json; charset="utf-8"')
            self.end_headers()
            self.wfile.write(content)
        else:
            return super().do_GET()

    def do_list(self, q):
        marker = q.params['marker'][0] if 'marker' in q.params else None
        max_keys = int(q.params['limit'][0]) if 'limit' in q.params else 10000
        prefix = q.params['prefix'][0] if 'prefix' in q.params else ''

        resp = []

        count = 0
        for key in sorted(self.server.data):
            if not key.startswith(prefix):
                continue
            if marker and key <= marker:
                continue
            resp.append({'name': key})
            count += 1
            if count == max_keys:
                break

        body = json.dumps(resp).encode('utf-8')

        self.send_response(200)
        self.send_header("Content-Type", 'application/json; charset="utf-8"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class CopySwiftRequestHandler(BasicSwiftRequestHandler):
    '''OpenStack Swift handler that emulates Copy middleware.'''

    SWIFT_INFO = {
        "swift": {
            "max_meta_count": 90,
            "max_meta_value_length": 256,
            "container_listing_limit": 10000,
            "extra_header_count": 0,
            "max_meta_overall_size": 4096,
            "version": "2.9.0",  # >= 2.8
            "max_meta_name_length": 128,
            "max_header_size": 16384,
        }
    }

    def do_COPY(self):
        src = self.parse_url(self.path)
        meta = self._get_meta()

        try:
            dst = self.headers['destination']
            assert dst[0] == '/'
            (_, dst) = dst[1:].split('/', maxsplit=1)
        except KeyError:
            self.send_error(400, message='No Destination provided', code='InvalidArgument')
            return

        if src.key not in self.server.metadata:
            self.send_error(404, code='NoSuchKey', resource=src)
            return

        if 'x-fresh-metadata' in self.headers:
            self.server.metadata[dst] = meta
        else:
            self.server.metadata[dst] = self.server.metadata[src.key].copy()
            self.server.metadata[dst].update(meta)

        self.server.data[dst] = self.server.data[src.key]

        self.send_response(202)
        self.send_header('X-Copied-From', '%s/%s' % (src.bucket, src.key))
        self.send_header('Content-Length', '0')
        self.end_headers()


class BulkDeleteSwiftRequestHandler(BasicSwiftRequestHandler):
    '''OpenStack Swift handler that emulates bulk middleware (the delete part).'''

    MAX_DELETES = 8  # test deletes 16 objects, so needs two requests
    SWIFT_INFO = {
        "bulk_delete": {"max_failed_deletes": MAX_DELETES, "max_deletes_per_request": MAX_DELETES},
        "swift": {
            "max_meta_count": 90,
            "max_meta_value_length": 256,
            "container_listing_limit": 10000,
            "extra_header_count": 0,
            "max_meta_overall_size": 4096,
            "version": "2.0.0",  # < 2.8
            "max_meta_name_length": 128,
            "max_header_size": 16384,
        },
    }

    def do_POST(self):
        q = self.parse_url(self.path)
        if not 'bulk-delete' in q.params:
            return super().do_POST()

        response = {
            'Response Status': '200 OK',
            'Response Body': '',
            'Number Deleted': 0,
            'Number Not Found': 0,
            'Errors': [],
        }

        def send_response(status_int):
            content = json.dumps(response).encode('utf-8')
            self.send_response(status_int)
            self.send_header('Content-Length', str(len(content)))
            self.send_header("Content-Type", 'application/json; charset="utf-8"')
            self.end_headers()
            self.wfile.write(content)

        def error(reason):
            response['Response Status'] = '502 Internal Server Error'
            response['Response Body'] = reason
            send_response(502)

        def inline_error(http_status, body):
            '''bail out when processing begun. Always HTTP 200 Ok.'''
            response['Response Status'] = http_status
            response['Response Body'] = body
            send_response(200)

        len_ = self._check_encoding()
        lines = self.rfile.read(len_).decode('utf-8').split("\n")
        for index, to_delete in enumerate(lines):
            if index >= self.MAX_DELETES:
                return inline_error(
                    '413 Request entity too large',
                    'Maximum Bulk Deletes: %d per request' % self.MAX_DELETES,
                )
            to_delete = urllib.parse.unquote(to_delete.strip())
            assert to_delete[0] == '/'
            to_delete = to_delete[1:].split('/', maxsplit=1)
            if len(to_delete) < 2:
                return error("deleting containers is not supported")
            to_delete = to_delete[1]
            try:
                del self.server.data[to_delete]
                del self.server.metadata[to_delete]
            except KeyError:
                response['Number Not Found'] += 1
            else:
                response['Number Deleted'] += 1

        if not (response['Number Deleted'] or response['Number Not Found']):
            return inline_error('400 Bad Request', 'Invalid bulk delete.')
        send_response(200)


#: A list of the available mock request handlers with
#: corresponding storage urls
handler_list = [
    (S3CRequestHandler, 's3c://%(host)s:%(port)d/s3ql_test'),
    # Special syntax only for testing against mock server
    (BasicSwiftRequestHandler, 'swift://%(host)s:%(port)d/s3ql_test'),
    (CopySwiftRequestHandler, 'swift://%(host)s:%(port)d/s3ql_test'),
    (BulkDeleteSwiftRequestHandler, 'swift://%(host)s:%(port)d/s3ql_test'),
]
