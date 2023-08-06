#!/usr/bin/env python3
'''
t5_lock_rm.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import os.path
import sys

import pyfuse3
import pytest
import t4_fuse
from pytest import raises as assert_raises

import s3ql.lock
import s3ql.remove


class TestLockRemove(t4_fuse.TestFuse):
    def test(self):
        self.mkfs()
        self.mount()
        self.tst_lock_rm()
        self.umount()
        self.fsck()

    def tst_lock_rm(self):
        # Extract tar
        tempdir = os.path.join(self.mnt_dir, 'lock_dir')
        filename = os.path.join(tempdir, 'myfile')
        os.mkdir(tempdir)
        with open(filename, 'w') as fh:
            fh.write('Hello, world')

        # copy
        try:
            s3ql.lock.main([tempdir])
        except:
            sys.excepthook(*sys.exc_info())
            pytest.fail("s3qllock raised exception")

        # Try to delete
        assert_raises(PermissionError, os.unlink, filename)

        # Try to write
        with pytest.raises(PermissionError):
            open(filename, 'w+').write('Hello')

        # delete properly
        try:
            s3ql.remove.main([tempdir])
        except:
            sys.excepthook(*sys.exc_info())
            pytest.fail("s3qlrm raised exception")

        assert 'lock_dir' not in pyfuse3.listdir(self.mnt_dir)
