#!/usr/bin/env python3
'''
t5_cache.py - this file is part of S3QL.

Copyright © 2017 IOFabric, Inc.

All Rights Reserved.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import os
import shutil
import subprocess
import tempfile
import time
from os.path import join as pjoin

import pytest
import t4_fuse

from s3ql.common import escape

with open(__file__, 'rb') as fh:
    TEST_DATA = fh.read()


class TestPerstCache(t4_fuse.TestFuse):
    # Need to overwrite parent class' method with something, thus the
    # inexpressive name.
    def test(self):
        # Write test data
        self.mkfs()
        self.mount(extra_args=['--keep-cache'])
        with open(pjoin(self.mnt_dir, 'testfile'), 'wb') as fh:
            fh.write(TEST_DATA)
        self.umount()

        # Poison backend storage object
        with open(pjoin(self.backend_dir, 's3ql_data_', 's3ql_data_1'), 'wb') as fh:
            fh.write(b'poison!')

        # fsck.s3ql shouldn't report errors
        self.fsck()

        # Ensure that we read from cache
        self.mount()
        with open(pjoin(self.mnt_dir, 'testfile'), 'rb') as fh:
            assert fh.read() == TEST_DATA
        self.umount()

    def test_cache_upload(self):
        self.reg_output(r'^WARNING: Writing dirty block', count=1)
        self.reg_output(r'^WARNING: Remote metadata is outdated', count=1)

        # Write test data
        self.mkfs()
        self.mount(extra_args=['--keep-cache'])
        with open(pjoin(self.mnt_dir, 'testfile'), 'wb') as fh:
            fh.write(TEST_DATA)

        # Kill mount
        self.flush_cache()
        self.mount_process.kill()
        self.mount_process.wait()
        self.umount_fuse()

        # Update cache files
        cache_file = pjoin(self.cache_dir, escape(self.storage_url) + '-cache', '4-0')
        with open(cache_file, 'rb+') as fh:
            fh.write(TEST_DATA[::-1])
        self.fsck(expect_retcode=128)
        assert not os.path.exists(cache_file)

        # Ensure that we get updated data
        self.reg_output(r'^WARNING: sqlite3: recovered [0-9]+ frames from WAL file', count=1)
        self.mount()
        with open(pjoin(self.mnt_dir, 'testfile'), 'rb') as fh:
            assert fh.read() == TEST_DATA[::-1]
        self.umount()

    def upload_meta(self):
        subprocess.check_call(
            self.s3ql_cmd_argv('s3qlctrl') + ['--quiet', 'backup-metadata', self.mnt_dir]
        )

    # Check that cache is ignored if fs was mounted elsewhere
    @pytest.mark.parametrize("with_fsck", (True, False))
    def test_cache_flush(self, with_fsck):
        # Write test data
        self.mkfs()
        self.mount(extra_args=['--keep-cache'])
        with open(pjoin(self.mnt_dir, 'testfile'), 'wb') as fh:
            fh.write(TEST_DATA)
            ino = os.fstat(fh.fileno()).st_ino
        self.umount()

        # Taint cache file, so we'll get an error if it's re-used
        cache_file = pjoin(self.cache_dir, escape(self.storage_url) + '-cache', '%d-0' % (ino,))
        with open(cache_file, 'rb+') as fh:
            fh.write(TEST_DATA[::-1])

        # Mount elsewhere
        bak = self.cache_dir
        self.cache_dir = tempfile.mkdtemp(prefix='s3ql-cache-')
        try:
            self.mount()
            with open(pjoin(self.mnt_dir, 'testfile2'), 'wb') as fh:
                fh.write(b'hello')
            self.umount()
        finally:
            shutil.rmtree(self.cache_dir)
            self.cache_dir = bak

        # Make sure that cache is ignored
        if with_fsck:
            self.fsck(args=['--keep-cache'])
        self.mount()
        with open(pjoin(self.mnt_dir, 'testfile'), 'rb') as fh:
            assert fh.read() == TEST_DATA
        self.umount()

    # Check that cache is ignored if fs was mounted elsewhere
    # and was not cleanly unmounted on either system
    def test_cache_flush_unclean(self):
        self.reg_output(r'^WARNING: Renaming outdated cache directory', count=1)
        self.reg_output(r'^WARNING: You should delete this directory', count=1)

        # Write test data
        self.mkfs()
        self.mount(extra_args=['--keep-cache'])
        with open(pjoin(self.mnt_dir, 'testfile'), 'wb') as fh:
            fh.write(TEST_DATA)

        # Kill mount
        self.flush_cache()
        self.upload_meta()

        # Metadata upload is not blocking, so give it some time to
        # complete
        time.sleep(1)

        self.mount_process.kill()
        self.mount_process.wait()
        self.umount_fuse()

        # Update cache files
        cache_file = pjoin(self.cache_dir, escape(self.storage_url) + '-cache', '4-0')
        with open(cache_file, 'rb+') as fh:
            fh.write(TEST_DATA[::-1])

        # Mount elsewhere
        self.reg_output(r'^WARNING: sqlite3: recovered [0-9]+ frames from WAL file', count=1)
        bak = self.cache_dir
        self.cache_dir = tempfile.mkdtemp(prefix='s3ql-cache-')
        try:
            self.fsck(expect_retcode=0, args=['--force-remote'])
            self.mount()
            with open(pjoin(self.mnt_dir, 'testfile2'), 'wb') as fh:
                fh.write(b'hello')
            self.mount_process.kill()
            self.mount_process.wait()
            self.umount_fuse()
        finally:
            shutil.rmtree(self.cache_dir)
            self.cache_dir = bak

        # Make sure that cache is ignored
        self.fsck(expect_retcode=0, args=['--force-remote', '--keep-cache'])
        self.mount()
        with open(pjoin(self.mnt_dir, 'testfile'), 'rb') as fh:
            assert fh.read() == TEST_DATA
        self.umount()
