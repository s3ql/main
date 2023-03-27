#!/usr/bin/env python3
'''
t5_full.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import pytest
    import sys

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

from common import populate_dir, skip_without_rsync, get_remote_test_info, NoTestSection
from s3ql import backends
from s3ql.database import Connection
import shutil
import subprocess
from subprocess import check_output, CalledProcessError
import t4_fuse
import tempfile
import pytest
import os
from s3ql.common import _escape


class TestFull(t4_fuse.TestFuse):
    def populate_dir(self, path):
        populate_dir(path)

    def test(self):
        skip_without_rsync()

        ref_dir = tempfile.mkdtemp(prefix='s3ql-ref-')
        try:
            self.populate_dir(ref_dir)

            # Copy source data
            self.mkfs()

            # Force 64bit inodes. This brings the local database out of sync with what's stored in
            # the backend, but the next mount will most likely result in modifications to the same
            # block and thus bring things back in sync.
            cachepath = os.path.join(self.cache_dir, _escape(self.storage_url))
            db = Connection(cachepath + '.db')
            db.execute('UPDATE sqlite_sequence SET seq=? WHERE name=?', (2**36 + 10, 'inodes'))
            db.close()

            self.mount()
            subprocess.check_call(['rsync', '-aHAX', ref_dir + '/', self.mnt_dir + '/'])
            self.umount()
            self.fsck()

            # Delete cache, run fsck and compare
            shutil.rmtree(self.cache_dir)
            self.cache_dir = tempfile.mkdtemp('s3ql-cache-')
            self.fsck()
            self.mount()
            try:
                out = check_output(
                    [
                        'rsync',
                        '-anciHAX',
                        '--delete',
                        '--exclude',
                        '/lost+found',
                        ref_dir + '/',
                        self.mnt_dir + '/',
                    ],
                    universal_newlines=True,
                    stderr=subprocess.STDOUT,
                )
            except CalledProcessError as exc:
                pytest.fail('rsync failed with ' + exc.output)
            if out:
                pytest.fail('Copy not equal to original, rsync says:\n' + out)

            self.umount()

            # Delete cache and mount
            shutil.rmtree(self.cache_dir)
            self.cache_dir = tempfile.mkdtemp(prefix='s3ql-cache-')
            self.mount()
            self.umount()

        finally:
            shutil.rmtree(ref_dir)


class RemoteTest:
    def setup_method(self, method, name):
        super().setup_method(method)
        try:
            (backend_login, backend_pw, self.storage_url) = get_remote_test_info(name)
        except NoTestSection as exc:
            super().teardown_method(method)
            pytest.skip(exc.reason)
        self.backend_login = backend_login
        self.backend_passphrase = backend_pw

    def populate_dir(self, path):
        populate_dir(path, entries=50, size=5 * 1024 * 1024)

    def teardown_method(self, method):
        super().teardown_method(method)

        proc = subprocess.Popen(
            self.s3ql_cmd_argv('s3qladm')
            + ['--quiet', '--authfile', '/dev/null', 'clear', self.storage_url],
            stdin=subprocess.PIPE,
            universal_newlines=True,
        )
        if self.backend_login is not None:
            print(self.backend_login, file=proc.stdin)
            print(self.backend_passphrase, file=proc.stdin)
        print('yes', file=proc.stdin)
        proc.stdin.close()

        assert proc.wait() == 0


# Dynamically generate tests for other backends
for backend_name in backends.prefix_map:
    if backend_name == 'local':
        continue

    def setup_method(self, method, backend_name=backend_name):
        RemoteTest.setup_method(self, method, backend_name + '-test')

    test_class_name = 'TestFull' + backend_name
    globals()[test_class_name] = type(
        test_class_name, (RemoteTest, TestFull), {'setup_method': setup_method}
    )
