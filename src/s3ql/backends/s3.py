'''
s3.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import hashlib
import hmac
import logging
import re
import time
import urllib.parse
from xml.sax.saxutils import escape as xml_escape

from ..logging import QuietError
from . import s3c
from .common import retry
from .s3c import get_S3Error

log = logging.getLogger(__name__)

# Maximum number of keys that can be deleted at once
MAX_KEYS = 1000

# Pylint goes berserk with false positives
# pylint: disable=E1002,E1101


class Backend(s3c.Backend):
    """A backend to store data in Amazon S3

    This class uses standard HTTP connections to connect to S3.
    """

    known_options = (s3c.Backend.known_options | {'sse', 'rrs', 'ia', 'oia', 'it'}) - {
        'dumb-copy',
        'disable-expect100',
    }

    def __init__(self, options):
        self.region = None
        self.signing_key = None
        super().__init__(options)
        self._set_storage_options(self._extra_put_headers)

    def _parse_storage_url(self, storage_url, ssl_context):
        hit = re.match(r'^s3s?://([^/]+)/([^/]+)(?:/(.*))?$', storage_url)
        if not hit:
            raise QuietError('Invalid storage URL', exitcode=2)

        self.region = hit.group(1)
        bucket_name = hit.group(2)

        # http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/BucketRestrictions.html
        if not re.match('^[a-z0-9][a-z0-9.-]{1,60}[a-z0-9]$', bucket_name):
            raise QuietError('Invalid bucket name.', exitcode=2)

        if self.region.startswith('cn-'):
            hostname = 's3.%s.amazonaws.com.cn' % self.region
        else:
            hostname = 's3.%s.amazonaws.com' % self.region

        prefix = hit.group(3) or ''
        port = 443 if ssl_context else 80
        return (hostname, port, bucket_name, prefix)

    def __str__(self):
        return 'Amazon S3 bucket %s, prefix %s' % (self.bucket_name, self.prefix)

    @property
    def has_delete_multi(self):
        return True

    def delete_multi(self, keys):
        log.debug('started with %s', keys)

        while len(keys) > 0:
            tmp = keys[:MAX_KEYS]
            try:
                self._delete_multi(tmp)
            finally:
                keys[:MAX_KEYS] = tmp

    def _set_storage_options(self, headers):
        if 'sse' in self.options:
            headers['x-amz-server-side-encryption'] = 'AES256'

        if 'ia' in self.options:
            sc = 'STANDARD_IA'
        elif 'oia' in self.options:
            sc = 'ONEZONE_IA'
        elif 'rrs' in self.options:
            sc = 'REDUCED_REDUNDANCY'
        elif 'it' in self.options:
            sc = 'INTELLIGENT_TIERING'
        else:
            sc = 'STANDARD'
        headers['x-amz-storage-class'] = sc

    @retry
    def _delete_multi(self, keys):
        body = ['<Delete>']
        esc_prefix = xml_escape(self.prefix)
        for key in keys:
            body.append('<Object><Key>%s%s</Key></Object>' % (esc_prefix, xml_escape(key)))
        body.append('</Delete>')
        body = '\n'.join(body).encode('utf-8')
        headers = {'content-type': 'text/xml; charset=utf-8'}

        resp = self._do_request('POST', '/', subres='delete', body=body, headers=headers)
        try:
            root = self._parse_xml_response(resp)
            ns_p = self.xml_ns_prefix

            error_tags = root.findall(ns_p + 'Error')
            if not error_tags:
                # No errors occurred, everything has been deleted
                del keys[:]
                return

            # Some errors occurred, so we need to determine what has
            # been deleted and what hasn't
            offset = len(self.prefix)
            for tag in root.findall(ns_p + 'Deleted'):
                fullkey = tag.find(ns_p + 'Key').text
                assert fullkey.startswith(self.prefix)
                keys.remove(fullkey[offset:])

            if log.isEnabledFor(logging.DEBUG):
                for errtag in error_tags:
                    log.debug(
                        'Delete %s failed with %s',
                        errtag.findtext(ns_p + 'Key')[offset:],
                        errtag.findtext(ns_p + 'Code'),
                    )

            errcode = error_tags[0].findtext(ns_p + 'Code')
            errmsg = error_tags[0].findtext(ns_p + 'Message')
            errkey = error_tags[0].findtext(ns_p + 'Key')[offset:]

            if errcode == 'NoSuchKeyError':
                pass
            else:
                raise get_S3Error(errcode, 'Error deleting %s: %s' % (errkey, errmsg))

        except:
            self.conn.discard()

    def _authorize_request(self, method, path, headers, subres, query_string):
        '''Add authorization information to *headers*'''

        # See http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html

        now = time.gmtime()
        # now = time.strptime('Fri, 24 May 2013 00:00:00 GMT',
        #                    '%a, %d %b %Y %H:%M:%S GMT')

        ymd = time.strftime('%Y%m%d', now)
        ymdhms = time.strftime('%Y%m%dT%H%M%SZ', now)

        headers['x-amz-date'] = ymdhms
        headers['x-amz-content-sha256'] = 'UNSIGNED-PAYLOAD'
        # headers['x-amz-content-sha256'] = hashlib.sha256(body).hexdigest()
        headers.pop('Authorization', None)

        auth_strs = [method]
        auth_strs.append(urllib.parse.quote(path))

        if query_string:
            s = urllib.parse.urlencode(
                query_string, doseq=True, quote_via=urllib.parse.quote
            ).split('&')
        else:
            s = []
        if subres:
            s.append(urllib.parse.quote(subres) + '=')
        if s:
            s = '&'.join(sorted(s))
        else:
            s = ''
        auth_strs.append(s)

        # Headers
        sig_hdrs = sorted(x.lower() for x in headers.keys())
        for hdr in sig_hdrs:
            auth_strs.append('%s:%s' % (hdr, headers[hdr].strip()))
        auth_strs.append('')
        auth_strs.append(';'.join(sig_hdrs))
        auth_strs.append(headers['x-amz-content-sha256'])
        can_req = '\n'.join(auth_strs)
        # log.debug('canonical request: %s', can_req)

        can_req_hash = hashlib.sha256(can_req.encode()).hexdigest()
        str_to_sign = (
            "AWS4-HMAC-SHA256\n"
            + ymdhms
            + '\n'
            + '%s/%s/s3/aws4_request\n' % (ymd, self.region)
            + can_req_hash
        )
        # log.debug('string to sign: %s', str_to_sign)

        if self.signing_key is None or self.signing_key[1] != ymd:
            self.update_signing_key(ymd)
        signing_key = self.signing_key[0]

        sig = hmac_sha256(signing_key, str_to_sign.encode(), hex=True)

        cred = '%s/%04d%02d%02d/%s/s3/aws4_request' % (
            self.login,
            now.tm_year,
            now.tm_mon,
            now.tm_mday,
            self.region,
        )

        headers['Authorization'] = (
            'AWS4-HMAC-SHA256 '
            'Credential=%s,'
            'SignedHeaders=%s,'
            'Signature=%s' % (cred, ';'.join(sig_hdrs), sig)
        )

    def update_signing_key(self, ymd):
        date_key = hmac_sha256(("AWS4" + self.password).encode(), ymd.encode())
        region_key = hmac_sha256(date_key, self.region.encode())
        service_key = hmac_sha256(region_key, b's3')
        signing_key = hmac_sha256(service_key, b'aws4_request')

        self.signing_key = (signing_key, ymd)


def hmac_sha256(key, msg, hex=False):
    d = hmac.new(key, msg, hashlib.sha256)
    if hex:
        return d.hexdigest()
    else:
        return d.digest()
