#!/usr/bin/env python3
'''
t3_fs_api.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

if __name__ == '__main__':
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))

import errno
import logging
import os
import shutil
import stat
import tempfile
from argparse import Namespace
from random import randint

import pytest
import trio
from common import CLOCK_GRANULARITY, safe_sleep
from pyfuse3 import FUSEError
from pytest import raises as assert_raises
from pytest_checklogs import assert_logs
from t2_block_cache import DummyQueue

from s3ql import ROOT_INODE, fs
from s3ql.backends import local
from s3ql.backends.comprenc import ComprencBackend
from s3ql.backends.pool import BackendPool
from s3ql.block_cache import BlockCache
from s3ql.database import Connection, FsAttributes, create_tables
from s3ql.fsck import Fsck
from s3ql.inode_cache import CACHE_SIZE as INODE_CACHE_SIZE
from s3ql.inode_cache import InodeCache
from s3ql.mkfs import init_tables

# We need to access to protected members
# pylint: disable=W0212


# The classes provided by pyfuse3 have read-only attributes,
# so we duck-type our own.
class Ctx:
    def __init__(self):
        self.uid = randint(0, 2**32)
        self.gid = randint(0, 2**32)
        self.pid = randint(0, 2**32)
        self.umask = 0


class SetattrFields:
    def __init__(self, **kw):
        self.update_atime = False
        self.update_mtime = False
        self.update_mode = False
        self.update_uid = False
        self.update_gid = False
        self.update_size = False
        self.__dict__.update(kw)


some_ctx = Ctx()


@pytest.fixture
async def ctx():
    ctx = Namespace()
    ctx.backend_dir = tempfile.mkdtemp(prefix='s3ql-backend-')
    plain_backend = local.Backend(Namespace(storage_url='local://' + ctx.backend_dir))
    ctx.backend_pool = BackendPool(lambda: ComprencBackend(b'schwubl', ('zlib', 6), plain_backend))
    ctx.backend = ctx.backend_pool.pop_conn()
    ctx.cachedir = tempfile.mkdtemp(prefix='s3ql-cache-')
    ctx.max_obj_size = 1024

    # Destructors are not guaranteed to run, and we can't unlink
    # the file immediately because apsw refers to it by name.
    # Therefore, we unlink the file manually in tearDown()
    ctx.dbfile = tempfile.NamedTemporaryFile(delete=False)

    ctx.db = Connection(ctx.dbfile.name)
    create_tables(ctx.db)
    init_tables(ctx.db)

    cache = BlockCache(ctx.backend_pool, ctx.db, ctx.cachedir + "/cache", ctx.max_obj_size * 5)
    cache.trio_token = trio.lowlevel.current_trio_token()
    ctx.cache = cache
    ctx.server = fs.Operations(cache, ctx.db, ctx.max_obj_size, InodeCache(ctx.db, 0))
    ctx.server.init()

    # Monkeypatch around the need for removal and upload threads
    cache.to_remove = DummyQueue(cache)

    class DummyChannel:
        async def send(self, arg):
            await trio.to_thread.run_sync(cache._do_upload, *arg)

    cache.to_upload = (DummyChannel(), None)

    # Keep track of unused filenames
    ctx.name_cnt = 0

    yield ctx

    ctx.server.inodes.destroy()
    await ctx.cache.destroy()
    ctx.db.close()
    os.unlink(ctx.dbfile.name)
    shutil.rmtree(ctx.cachedir)
    shutil.rmtree(ctx.backend_dir)


def random_data(len_):
    with open("/dev/urandom", "rb") as fd:
        return fd.read(len_)


async def fsck(ctx):
    await ctx.cache.drop()
    ctx.server.inodes.flush()
    fsck = Fsck(
        ctx.cachedir + '/cache',
        ctx.backend,
        FsAttributes(
            data_block_size=ctx.max_obj_size,
            metadata_block_size=ctx.max_obj_size,
        ),
        ctx.db,
    )
    fsck.check()
    assert not fsck.found_errors


def newname(ctx):
    ctx.name_cnt += 1
    return ("s3ql_%d" % ctx.name_cnt).encode()


async def test_getattr_root(ctx):
    assert stat.S_ISDIR((await ctx.server.getattr(ROOT_INODE, some_ctx)).st_mode)
    await fsck(ctx)


async def test_create(ctx):
    fctx = Ctx()
    mode = dir_mode()
    name = newname(ctx)

    inode_p_old = await ctx.server.getattr(ROOT_INODE, some_ctx)
    safe_sleep(CLOCK_GRANULARITY)
    ctx.server._create(ROOT_INODE, name, mode, fctx)

    id_ = ctx.db.get_val(
        'SELECT inode FROM contents JOIN names ON name_id = names.id '
        'WHERE name=? AND parent_inode = ?',
        (name, ROOT_INODE),
    )

    inode = await ctx.server.getattr(id_, some_ctx)

    assert inode.st_mode == mode
    assert inode.st_uid == fctx.uid
    assert inode.st_gid == fctx.gid
    assert inode.st_nlink == 1
    assert inode.st_size == 0

    inode_p_new = await ctx.server.getattr(ROOT_INODE, some_ctx)

    assert inode_p_new.st_mtime_ns > inode_p_old.st_mtime_ns
    assert inode_p_new.st_ctime_ns > inode_p_old.st_ctime_ns

    await ctx.server.forget([(id_, 1)])
    await fsck(ctx)


async def test_extstat(ctx):
    # Test with zero contents
    ctx.server.extstat()

    # Test with empty file
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.release(fh)
    ctx.server.extstat()

    # Test with data in file
    fi = await ctx.server.open(inode.st_ino, os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.write(fh, 0, b'foobar')
    await ctx.server.release(fh)

    ctx.server.extstat()
    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


def dir_mode():
    return (randint(0, 0o7777) & ~stat.S_IFDIR) | stat.S_IFDIR


def file_mode():
    return (randint(0, 0o7777) & ~stat.S_IFREG) | stat.S_IFREG


async def test_getxattr(ctx):
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.release(fh)

    with assert_raises(FUSEError):
        await ctx.server.getxattr(inode.st_ino, b'nonexistent-attr', some_ctx)

    await ctx.server.setxattr(inode.st_ino, b'my-attr', b'strabumm!', some_ctx)
    assert await ctx.server.getxattr(inode.st_ino, b'my-attr', some_ctx) == b'strabumm!'

    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_link(ctx):
    name = newname(ctx)

    inode_p_new = await ctx.server.mkdir(ROOT_INODE, newname(ctx), dir_mode(), some_ctx)
    inode_p_new_before = await ctx.server.getattr(inode_p_new.st_ino, some_ctx)

    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.release(fh)
    safe_sleep(CLOCK_GRANULARITY)

    inode_before = await ctx.server.getattr(inode.st_ino, some_ctx)
    await ctx.server.link(inode.st_ino, inode_p_new.st_ino, name, some_ctx)

    inode_after = await ctx.server.lookup(inode_p_new.st_ino, name, some_ctx)
    inode_p_new_after = await ctx.server.getattr(inode_p_new.st_ino, some_ctx)

    id_ = ctx.db.get_val(
        'SELECT inode FROM contents JOIN names ON names.id = name_id '
        'WHERE name=? AND parent_inode = ?',
        (name, inode_p_new.st_ino),
    )

    assert inode_before.st_ino == id_
    assert inode_after.st_nlink == 2
    assert inode_after.st_ctime_ns > inode_before.st_ctime_ns
    assert inode_p_new_before.st_mtime_ns < inode_p_new_after.st_mtime_ns
    assert inode_p_new_before.st_ctime_ns < inode_p_new_after.st_ctime_ns
    await ctx.server.forget([(inode.st_ino, 1), (inode_p_new.st_ino, 1), (inode_after.st_ino, 1)])
    await fsck(ctx)


async def test_listxattr(ctx):
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.release(fh)

    assert await ctx.server.listxattr(inode.st_ino, some_ctx) == []

    await ctx.server.setxattr(inode.st_ino, b'key1', b'blub', some_ctx)
    assert [b'key1'] == await ctx.server.listxattr(inode.st_ino, some_ctx)

    await ctx.server.setxattr(inode.st_ino, b'key2', b'blub', some_ctx)
    assert sorted([b'key1', b'key2']) == sorted(await ctx.server.listxattr(inode.st_ino, some_ctx))
    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_read(ctx):
    len_ = ctx.max_obj_size
    data = random_data(len_)
    off = ctx.max_obj_size // 2
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh

    await ctx.server.write(fh, off, data)
    inode_before = await ctx.server.getattr(inode.st_ino, some_ctx)
    safe_sleep(CLOCK_GRANULARITY)
    assert await ctx.server.read(fh, off, len_) == data
    inode_after = await ctx.server.getattr(inode.st_ino, some_ctx)
    assert inode_after.st_atime_ns > inode_before.st_atime_ns
    assert await ctx.server.read(fh, 0, len_) == b"\0" * off + data[:off]
    assert await ctx.server.read(fh, ctx.max_obj_size, len_) == data[off:]
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_readdir(ctx, monkeypatch):
    # Create a few entries
    names = [('entry_%2d' % i).encode() for i in range(20)]
    for name in names:
        (fi, inode) = await ctx.server.create(ROOT_INODE, name, file_mode(), os.O_RDWR, some_ctx)
        fh = fi.fh
        await ctx.server.release(fh)
        await ctx.server.forget([(inode.st_ino, 1)])

    # Delete some to make sure that we don't have continuous rowids
    remove_no = [0, 2, 3, 5, 9]
    for i in remove_no:
        await ctx.server.unlink(ROOT_INODE, names[i], some_ctx)
        del names[i]

    # Read all
    fh = await ctx.server.opendir(ROOT_INODE, some_ctx)
    names_ret = []
    next_ids = []

    def readdir_reply(token, name, attr, next_id):
        names_ret.append(name)
        next_ids.append(next_id)
        return True

    monkeypatch.setattr('pyfuse3.readdir_reply', readdir_reply)
    await ctx.server.readdir(fh, 0, 'my-token')
    await ctx.server.releasedir(fh)
    assert set(names + [b'lost+found']) == set(names_ret)

    # Read in parts
    fh = await ctx.server.opendir(ROOT_INODE, some_ctx)
    names_ret.clear()
    next_ids.clear()
    while True:
        old_len = len(next_ids)
        await ctx.server.readdir(fh, next_ids[-1] if next_ids else 0, 'my-token')
        if old_len == len(next_ids):
            break

    assert set(names + [b'lost+found']) == set(names_ret)
    await ctx.server.releasedir(fh)

    await fsck(ctx)


async def test_forget(ctx):
    name = newname(ctx)

    # Test that entries are deleted when they're no longer referenced
    (fi, inode) = await ctx.server.create(ROOT_INODE, name, file_mode(), os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.write(fh, 0, b'foobar')
    await ctx.server.unlink(ROOT_INODE, name, some_ctx)
    assert not ctx.db.has_val(
        'SELECT 1 FROM contents JOIN names ON names.id = name_id '
        'WHERE name=? AND parent_inode = ?',
        (name, ROOT_INODE),
    )
    assert (await ctx.server.getattr(inode.st_ino, some_ctx)).st_ino
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])

    assert not ctx.db.has_val('SELECT 1 FROM inodes WHERE id=?', (inode.st_ino,))

    await fsck(ctx)


async def test_removexattr(ctx):
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.release(fh)

    with assert_raises(FUSEError):
        await ctx.server.removexattr(inode.st_ino, b'some name', some_ctx)

    await ctx.server.setxattr(inode.st_ino, b'key1', b'blub', some_ctx)
    await ctx.server.removexattr(inode.st_ino, b'key1', some_ctx)
    assert await ctx.server.listxattr(inode.st_ino, some_ctx) == []
    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_rename(ctx):
    oldname = newname(ctx)
    newname_ = newname(ctx)

    inode = await ctx.server.mkdir(ROOT_INODE, oldname, dir_mode(), some_ctx)

    inode_p_new = await ctx.server.mkdir(ROOT_INODE, newname(ctx), dir_mode(), some_ctx)
    inode_p_new_before = await ctx.server.getattr(inode_p_new.st_ino, some_ctx)
    inode_p_old_before = await ctx.server.getattr(ROOT_INODE, some_ctx)
    safe_sleep(CLOCK_GRANULARITY)

    await ctx.server.rename(ROOT_INODE, oldname, inode_p_new.st_ino, newname_, 0, some_ctx)

    inode_p_old_after = await ctx.server.getattr(ROOT_INODE, some_ctx)
    inode_p_new_after = await ctx.server.getattr(inode_p_new.st_ino, some_ctx)

    assert not ctx.db.has_val(
        'SELECT inode FROM contents JOIN names ON names.id = name_id '
        'WHERE name=? AND parent_inode = ?',
        (oldname, ROOT_INODE),
    )
    id_ = ctx.db.get_val(
        'SELECT inode FROM contents JOIN names ON names.id == name_id '
        'WHERE name=? AND parent_inode = ?',
        (newname_, inode_p_new.st_ino),
    )
    assert inode.st_ino == id_

    assert inode_p_new_before.st_mtime_ns < inode_p_new_after.st_mtime_ns
    assert inode_p_new_before.st_ctime_ns < inode_p_new_after.st_ctime_ns
    assert inode_p_old_before.st_mtime_ns < inode_p_old_after.st_mtime_ns
    assert inode_p_old_before.st_ctime_ns < inode_p_old_after.st_ctime_ns

    await ctx.server.forget([(inode.st_ino, 1), (inode_p_new.st_ino, 1)])
    await fsck(ctx)


async def test_rename2(ctx):
    src_file_name = newname(ctx)
    src_folder_name = newname(ctx)
    dst_file_name = newname(ctx)
    dst_folder_name = newname(ctx)

    inode_dst_folder = await ctx.server.mkdir(ROOT_INODE, dst_folder_name, dir_mode(), some_ctx)
    inode_src_folder = await ctx.server.mkdir(ROOT_INODE, src_folder_name, dir_mode(), some_ctx)
    await ctx.server.create(
        inode_src_folder.st_ino, src_file_name, file_mode(), os.O_RDWR, some_ctx
    )

    safe_sleep(CLOCK_GRANULARITY)

    # Create files in order to flush the inode cache of the last 3 inodes
    for _ in range(0, INODE_CACHE_SIZE):
        await ctx.server.create(ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx)

    # Load the inodes in a specific order
    fi = await ctx.server.open(ROOT_INODE, os.O_RDWR, some_ctx)
    await ctx.server.release(fi.fh)
    fi = await ctx.server.open(inode_dst_folder.st_ino, os.O_RDWR, some_ctx)
    await ctx.server.release(fi.fh)
    fi = await ctx.server.open(inode_src_folder.st_ino, os.O_RDWR, some_ctx)
    await ctx.server.release(fi.fh)

    # Put these inodes on the brink of being flushed in the inode cache
    for _ in range(0, INODE_CACHE_SIZE - 3):
        await ctx.server.create(ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx)

    # Move the file
    await ctx.server.rename(
        inode_src_folder.st_ino, src_file_name, inode_dst_folder.st_ino, dst_file_name, 0, some_ctx
    )

    await fsck(ctx)


async def test_replace_file(ctx):
    oldname = newname(ctx)
    newname_ = newname(ctx)

    (fi, inode) = await ctx.server.create(ROOT_INODE, oldname, file_mode(), os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.write(fh, 0, b'some data to deal with')
    await ctx.server.release(fh)
    await ctx.server.setxattr(inode.st_ino, b'test_xattr', b'42*8', some_ctx)

    inode_p_new = await ctx.server.mkdir(ROOT_INODE, newname(ctx), dir_mode(), some_ctx)
    inode_p_new_before = await ctx.server.getattr(inode_p_new.st_ino, some_ctx)
    inode_p_old_before = await ctx.server.getattr(ROOT_INODE, some_ctx)

    (fi, inode2) = await ctx.server.create(
        inode_p_new.st_ino, newname_, file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, b'even more data to deal with')
    await ctx.server.release(fh)
    await ctx.server.setxattr(inode2.st_ino, b'test_xattr', b'42*8', some_ctx)
    await ctx.server.forget([(inode2.st_ino, 1)])

    safe_sleep(CLOCK_GRANULARITY)
    await ctx.server.rename(ROOT_INODE, oldname, inode_p_new.st_ino, newname_, 0, some_ctx)

    inode_p_old_after = await ctx.server.getattr(ROOT_INODE, some_ctx)
    inode_p_new_after = await ctx.server.getattr(inode_p_new.st_ino, some_ctx)

    assert not ctx.db.has_val(
        'SELECT inode FROM contents JOIN names ON names.id = name_id '
        'WHERE name=? AND parent_inode = ?',
        (oldname, ROOT_INODE),
    )
    id_ = ctx.db.get_val(
        'SELECT inode FROM contents JOIN names ON names.id = name_id '
        'WHERE name=? AND parent_inode = ?',
        (newname_, inode_p_new.st_ino),
    )
    assert inode.st_ino == id_

    assert inode_p_new_before.st_mtime_ns < inode_p_new_after.st_mtime_ns
    assert inode_p_new_before.st_ctime_ns < inode_p_new_after.st_ctime_ns
    assert inode_p_old_before.st_mtime_ns < inode_p_old_after.st_mtime_ns
    assert inode_p_old_before.st_ctime_ns < inode_p_old_after.st_ctime_ns

    assert not ctx.db.has_val('SELECT id FROM inodes WHERE id=?', (inode2.st_ino,))
    await ctx.server.forget([(inode.st_ino, 1), (inode_p_new.st_ino, 1)])
    await fsck(ctx)


async def test_replace_dir(ctx):
    oldname = newname(ctx)
    newname_ = newname(ctx)

    inode = await ctx.server.mkdir(ROOT_INODE, oldname, dir_mode(), some_ctx)

    inode_p_new = await ctx.server.mkdir(ROOT_INODE, newname(ctx), dir_mode(), some_ctx)
    inode_p_new_before = await ctx.server.getattr(inode_p_new.st_ino, some_ctx)
    inode_p_old_before = await ctx.server.getattr(ROOT_INODE, some_ctx)

    inode2 = await ctx.server.mkdir(inode_p_new.st_ino, newname_, dir_mode(), some_ctx)
    await ctx.server.forget([(inode2.st_ino, 1)])

    safe_sleep(CLOCK_GRANULARITY)
    await ctx.server.rename(ROOT_INODE, oldname, inode_p_new.st_ino, newname_, 0, some_ctx)

    inode_p_old_after = await ctx.server.getattr(ROOT_INODE, some_ctx)
    inode_p_new_after = await ctx.server.getattr(inode_p_new.st_ino, some_ctx)

    assert not ctx.db.has_val(
        'SELECT inode FROM contents JOIN names ON names.id = name_id '
        'WHERE name=? AND parent_inode = ?',
        (oldname, ROOT_INODE),
    )
    id_ = ctx.db.get_val(
        'SELECT inode FROM contents JOIN names ON names.id = name_id '
        'WHERE name=? AND parent_inode = ?',
        (newname_, inode_p_new.st_ino),
    )
    assert inode.st_ino == id_

    assert inode_p_new_before.st_mtime_ns < inode_p_new_after.st_mtime_ns
    assert inode_p_new_before.st_ctime_ns < inode_p_new_after.st_ctime_ns
    assert inode_p_old_before.st_mtime_ns < inode_p_old_after.st_mtime_ns
    assert inode_p_old_before.st_ctime_ns < inode_p_old_after.st_ctime_ns

    await ctx.server.forget([(inode.st_ino, 1), (inode_p_new.st_ino, 1)])
    assert not ctx.db.has_val('SELECT id FROM inodes WHERE id=?', (inode2.st_ino,))
    await fsck(ctx)


async def test_setattr_one(ctx):
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.release(fh)
    inode_old = await ctx.server.getattr(inode.st_ino, some_ctx)

    attr = await ctx.server.getattr(inode.st_ino, some_ctx)  # this is a fresh instance
    attr.st_mode = file_mode()
    attr.st_uid = randint(0, 2**32)
    attr.st_gid = randint(0, 2**32)  # should be ignored
    attr.st_atime_ns = randint(0, 2**50)
    attr.st_mtime_ns = randint(0, 2**50)

    safe_sleep(CLOCK_GRANULARITY)
    sf = SetattrFields(update_mode=True, update_uid=True, update_atime=True, update_mtime=True)
    await ctx.server.setattr(inode.st_ino, attr, sf, None, some_ctx)
    inode_new = await ctx.server.getattr(inode.st_ino, some_ctx)

    for name in ('st_mode', 'st_uid', 'st_atime_ns', 'st_mtime_ns'):
        assert getattr(attr, name) == getattr(inode_new, name)
    for name in ('st_gid', 'st_size', 'st_nlink', 'st_rdev', 'st_blocks', 'st_blksize'):
        assert getattr(inode_old, name) == getattr(inode_new, name)
    assert inode_old.st_ctime_ns < inode_new.st_ctime_ns

    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_setattr_two(ctx):
    (fi, inode_old) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh

    attr = await ctx.server.getattr(inode_old.st_ino, some_ctx)
    attr.st_mode = file_mode()
    attr.st_uid = randint(0, 2**32)
    attr.st_gid = randint(0, 2**32)
    attr.st_mtime_ns = randint(0, 2**50)
    attr.st_ctime_ns = 5e9

    safe_sleep(CLOCK_GRANULARITY)
    sf = SetattrFields(update_gid=True, update_mtime=True)

    await ctx.server.setattr(inode_old.st_ino, attr, sf, None, some_ctx)
    inode_new = await ctx.server.getattr(inode_old.st_ino, some_ctx)

    for name in ('st_gid', 'st_mtime_ns'):
        assert getattr(attr, name) == getattr(inode_new, name)
    for name in (
        'st_uid',
        'st_size',
        'st_nlink',
        'st_rdev',
        'st_blocks',
        'st_blksize',
        'st_mode',
        'st_atime_ns',
    ):
        assert getattr(inode_old, name) == getattr(inode_new, name)
    assert inode_old.st_ctime_ns < inode_new.st_ctime_ns

    await ctx.server.release(fh)
    await ctx.server.forget([(inode_old.st_ino, 1)])
    await fsck(ctx)


async def test_setattr_large(ctx):
    (fi, inode_old) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh

    attr = await ctx.server.getattr(inode_old.st_ino, some_ctx)
    attr.st_mtime_ns = int(2**63 + 1)
    sf = SetattrFields(update_mtime=True)

    with pytest.raises(FUSEError) as exc:
        await ctx.server.setattr(inode_old.st_ino, attr, sf, None, some_ctx)
    assert exc.value.errno == errno.EINVAL

    await ctx.server.release(fh)
    await ctx.server.forget([(inode_old.st_ino, 1)])
    await fsck(ctx)


async def test_truncate(ctx):
    len_ = int(2.7 * ctx.max_obj_size)
    data = random_data(len_)

    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, data)

    attr = await ctx.server.getattr(inode.st_ino, some_ctx)
    attr.st_size = len_ // 2
    sf = SetattrFields(update_size=True)
    await ctx.server.setattr(inode.st_ino, attr, sf, None, some_ctx)
    assert await ctx.server.read(fh, 0, len_) == data[: len_ // 2]
    attr.st_size = len_
    await ctx.server.setattr(inode.st_ino, attr, sf, None, some_ctx)
    assert await ctx.server.read(fh, 0, len_) == data[: len_ // 2] + b'\0' * (len_ // 2)
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_truncate_0(ctx):
    len1 = 158
    len2 = 133

    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, random_data(len1))
    await ctx.server.release(fh)
    ctx.server.inodes.flush()

    attr = await ctx.server.getattr(inode.st_ino, some_ctx)
    fi = await ctx.server.open(inode.st_ino, os.O_RDWR, some_ctx)
    fh = fi.fh
    attr.st_size = 0
    await ctx.server.setattr(inode.st_ino, attr, SetattrFields(update_size=True), fh, some_ctx)
    data = random_data(len2)
    await ctx.server.write(fh, 0, data)
    await ctx.server.release(fh)
    ctx.server.inodes.flush()
    fi = await ctx.server.open(inode.st_ino, os.O_RDONLY, some_ctx)
    fh = fi.fh
    assert await ctx.server.read(fh, 0, len2 + 1) == data
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_setxattr(ctx):
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.release(fh)

    await ctx.server.setxattr(inode.st_ino, b'my-attr', b'strabumm!', some_ctx)
    assert await ctx.server.getxattr(inode.st_ino, b'my-attr', some_ctx) == b'strabumm!'
    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_names(ctx):
    name1 = newname(ctx)
    name2 = newname(ctx)

    (fi, inode) = await ctx.server.create(ROOT_INODE, name1, file_mode(), os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])

    (fi, inode) = await ctx.server.create(ROOT_INODE, name2, file_mode(), os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.release(fh)

    await ctx.server.setxattr(inode.st_ino, name1, b'strabumm!', some_ctx)
    await fsck(ctx)

    await ctx.server.removexattr(inode.st_ino, name1, some_ctx)
    await fsck(ctx)

    await ctx.server.setxattr(inode.st_ino, name1, b'strabumm karacho!!', some_ctx)
    await ctx.server.unlink(ROOT_INODE, name1, some_ctx)
    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_statfs(ctx):
    # Test with zero contents
    await ctx.server.statfs(some_ctx)

    # Test with empty file
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.release(fh)
    await ctx.server.statfs(some_ctx)

    # Test with data in file
    fi = await ctx.server.open(inode.st_ino, os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.write(fh, 0, b'foobar')
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])
    await ctx.server.statfs(some_ctx)


async def test_symlink(ctx):
    target = newname(ctx)
    name = newname(ctx)

    inode_p_before = await ctx.server.getattr(ROOT_INODE, some_ctx)
    safe_sleep(CLOCK_GRANULARITY)
    inode = await ctx.server.symlink(ROOT_INODE, name, target, some_ctx)
    inode_p_after = await ctx.server.getattr(ROOT_INODE, some_ctx)

    assert target, await ctx.server.readlink(inode.st_ino == some_ctx)

    id_ = ctx.db.get_val(
        'SELECT inode FROM contents JOIN names ON names.id = name_id '
        'WHERE name=? AND parent_inode = ?',
        (name, ROOT_INODE),
    )

    assert inode.st_ino == id_
    assert inode_p_before.st_mtime_ns < inode_p_after.st_mtime_ns
    assert inode_p_before.st_ctime_ns < inode_p_after.st_ctime_ns

    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_unlink(ctx):
    name = newname(ctx)

    (fi, inode) = await ctx.server.create(ROOT_INODE, name, file_mode(), os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.write(fh, 0, b'some data to deal with')
    await ctx.server.release(fh)

    # Add extended attributes
    await ctx.server.setxattr(inode.st_ino, b'test_xattr', b'42*8', some_ctx)
    await ctx.server.forget([(inode.st_ino, 1)])

    inode_p_before = await ctx.server.getattr(ROOT_INODE, some_ctx)
    safe_sleep(CLOCK_GRANULARITY)
    await ctx.server.unlink(ROOT_INODE, name, some_ctx)
    inode_p_after = await ctx.server.getattr(ROOT_INODE, some_ctx)

    assert inode_p_before.st_mtime_ns < inode_p_after.st_mtime_ns
    assert inode_p_before.st_ctime_ns < inode_p_after.st_ctime_ns

    assert not ctx.db.has_val(
        'SELECT inode FROM contents JOIN names ON names.id = name_id '
        'WHERE name=? AND parent_inode = ?',
        (name, ROOT_INODE),
    )
    assert not ctx.db.has_val('SELECT id FROM inodes WHERE id=?', (inode.st_ino,))

    await fsck(ctx)


async def test_rmdir(ctx):
    name = newname(ctx)
    inode = await ctx.server.mkdir(ROOT_INODE, name, dir_mode(), some_ctx)
    await ctx.server.forget([(inode.st_ino, 1)])
    inode_p_before = await ctx.server.getattr(ROOT_INODE, some_ctx)
    safe_sleep(CLOCK_GRANULARITY)
    await ctx.server.rmdir(ROOT_INODE, name, some_ctx)
    inode_p_after = await ctx.server.getattr(ROOT_INODE, some_ctx)

    assert inode_p_before.st_mtime_ns < inode_p_after.st_mtime_ns
    assert inode_p_before.st_ctime_ns < inode_p_after.st_ctime_ns
    assert not ctx.db.has_val(
        'SELECT inode FROM contents JOIN names ON names.id = name_id '
        'WHERE name=? AND parent_inode = ?',
        (name, ROOT_INODE),
    )
    assert not ctx.db.has_val('SELECT id FROM inodes WHERE id=?', (inode.st_ino,))

    await fsck(ctx)


async def test_relink(ctx):
    name = newname(ctx)
    name2 = newname(ctx)
    data = b'some data to deal with'

    (fi, inode) = await ctx.server.create(ROOT_INODE, name, file_mode(), os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.write(fh, 0, data)
    await ctx.server.release(fh)
    await ctx.server.unlink(ROOT_INODE, name, some_ctx)
    ctx.server.inodes.flush()
    assert not ctx.db.has_val(
        'SELECT inode FROM contents JOIN names ON names.id = name_id '
        'WHERE name=? AND parent_inode = ?',
        (name, ROOT_INODE),
    )
    assert ctx.db.has_val('SELECT id FROM inodes WHERE id=?', (inode.st_ino,))
    await ctx.server.link(inode.st_ino, ROOT_INODE, name2, some_ctx)
    await ctx.server.forget([(inode.st_ino, 2)])

    inode = await ctx.server.lookup(ROOT_INODE, name2, some_ctx)
    fi = await ctx.server.open(inode.st_ino, os.O_RDONLY, some_ctx)
    fh = fi.fh
    assert await ctx.server.read(fh, 0, len(data)) == data
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_write(ctx):
    len_ = ctx.max_obj_size
    data = random_data(len_)
    off = ctx.max_obj_size // 2
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    inode_before = await ctx.server.getattr(inode.st_ino, some_ctx)
    safe_sleep(CLOCK_GRANULARITY)
    await ctx.server.write(fh, off, data)
    inode_after = await ctx.server.getattr(inode.st_ino, some_ctx)

    assert inode_after.st_mtime_ns > inode_before.st_mtime_ns
    assert inode_after.st_ctime_ns > inode_before.st_ctime_ns
    assert inode_after.st_size == off + len_

    await ctx.server.write(fh, 0, data)
    inode_after = await ctx.server.getattr(inode.st_ino, some_ctx)
    assert inode_after.st_size == off + len_

    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_fsync(ctx):
    len_ = ctx.max_obj_size
    data = random_data(len_)
    off = ctx.max_obj_size // 2
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, off, data)
    await ctx.server.fsync(fh, datasync=False)
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])


async def test_failsafe(ctx):
    len_ = ctx.max_obj_size
    data = random_data(len_)
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, data)
    await ctx.cache.drop()
    assert not ctx.server.failsafe

    datafile = os.path.join(ctx.backend_dir, 's3ql_data_', 's3ql_data_1')
    shutil.copy(datafile, datafile + '.bak')

    # Modify contents
    with open(datafile, 'rb+') as rfh:
        rfh.seek(560)
        rfh.write(b'blrub!')
    with assert_raises(FUSEError) as cm:
        with assert_logs('^Backend returned malformed data for', count=1, level=logging.ERROR):
            await ctx.server.read(fh, 0, len_)
    assert cm.value.errno == errno.EIO
    assert ctx.server.failsafe

    # Restore contents, but should be marked as damaged now
    os.rename(datafile + '.bak', datafile)
    with assert_raises(FUSEError) as cm:
        await ctx.server.read(fh, 0, len_)
    assert cm.value.errno == errno.EIO

    # Release and re-open, now we should be able to access again
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])

    # ..but not write access since we are in failsafe mode
    with assert_raises(FUSEError) as cm:
        await ctx.server.open(inode.st_ino, os.O_RDWR, some_ctx)
    assert cm.value.errno == errno.EPERM

    # ..ready only is fine.
    fi = await ctx.server.open(inode.st_ino, os.O_RDONLY, some_ctx)
    fh = fi.fh
    await ctx.server.read(fh, 0, len_)

    # Remove completely, should give error after cache flush
    os.unlink(datafile)
    await ctx.server.read(fh, 3, len_ // 2)
    await ctx.cache.drop()
    with assert_raises(FUSEError) as cm:
        with assert_logs('^Backend lost block', count=1, level=logging.ERROR):
            await ctx.server.read(fh, 5, len_ // 2)
    assert cm.value.errno == errno.EIO

    # Don't call fsck, we're missing a block


async def test_create_open(ctx):
    name = newname(ctx)
    # Create a new file
    (fi, inode) = await ctx.server.create(ROOT_INODE, name, file_mode(), os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])

    # Open it atomically
    (fi, inode) = await ctx.server.create(ROOT_INODE, name, file_mode(), os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])

    await fsck(ctx)


async def test_open_truncate(ctx):
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, random_data(int(1.5 * ctx.max_obj_size)))
    await ctx.server.release(fh)
    ctx.server.inodes.flush()

    data = b'this data overwrites the previous data'
    fi = await ctx.server.open(inode.st_ino, os.O_RDWR | os.O_TRUNC, some_ctx)
    fh = fi.fh
    await ctx.server.write(fh, 0, data)
    await ctx.server.release(fh)
    ctx.server.inodes.flush()

    fi = await ctx.server.open(inode.st_ino, os.O_RDONLY, some_ctx)
    fh = fi.fh
    assert await ctx.server.read(fh, 0, len(data) + 1) == data
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_edit(ctx):
    len_ = ctx.max_obj_size
    data = random_data(len_)
    (fi, inode) = await ctx.server.create(
        ROOT_INODE, newname(ctx), file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, data)
    await ctx.server.release(fh)

    await ctx.cache.drop()

    fi = await ctx.server.open(inode.st_ino, os.O_RDWR, some_ctx)
    fh = fi.fh
    attr = await ctx.server.getattr(inode.st_ino, some_ctx)
    attr.st_size = 0
    await ctx.server.setattr(inode.st_ino, attr, SetattrFields(update_size=True), fh, some_ctx)
    await ctx.server.write(fh, 0, data[50:])
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])
    await fsck(ctx)


async def test_copy_tree(ctx, monkeypatch):
    ext_attr_name = b'system.foo.brazl'
    ext_attr_val = b'schulla dku woumm bramp'

    src_inode = await ctx.server.mkdir(ROOT_INODE, b'source', dir_mode(), some_ctx)
    dst_inode = await ctx.server.mkdir(ROOT_INODE, b'dest', dir_mode(), some_ctx)

    # Create file
    (fi, f1_inode) = await ctx.server.create(
        src_inode.st_ino, b'file1', file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, b'file1 contents')
    await ctx.server.release(fh)
    await ctx.server.setxattr(f1_inode.st_ino, ext_attr_name, ext_attr_val, some_ctx)

    # Create hardlink
    (fi, f2_inode) = await ctx.server.create(
        src_inode.st_ino, b'file2', file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, b'file2 contents')
    await ctx.server.release(fh)
    f2_inode = await ctx.server.link(f2_inode.st_ino, src_inode.st_ino, b'file2_hardlink', some_ctx)

    # Create subdirectory
    d1_inode = await ctx.server.mkdir(src_inode.st_ino, b'dir1', dir_mode(), some_ctx)
    d2_inode = await ctx.server.mkdir(d1_inode.st_ino, b'dir2', dir_mode(), some_ctx)

    # ..with a 3rd hardlink
    f2_inode = await ctx.server.link(f2_inode.st_ino, d1_inode.st_ino, b'file2_hardlink', some_ctx)

    # Replicate
    monkeypatch.setattr('pyfuse3.invalidate_inode', lambda *args: None)
    await ctx.server.copy_tree(src_inode.st_ino, dst_inode.st_ino)

    # Change files
    fi = await ctx.server.open(f1_inode.st_ino, os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.write(fh, 0, b'new file1 contents')
    await ctx.server.release(fh)

    fi = await ctx.server.open(f2_inode.st_ino, os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.write(fh, 0, b'new file2 contents')
    await ctx.server.release(fh)

    # Get copy properties
    f1_inode_c = await ctx.server.lookup(dst_inode.st_ino, b'file1', some_ctx)
    f2_inode_c = await ctx.server.lookup(dst_inode.st_ino, b'file2', some_ctx)
    f2h_inode_c = await ctx.server.lookup(dst_inode.st_ino, b'file2_hardlink', some_ctx)
    d1_inode_c = await ctx.server.lookup(dst_inode.st_ino, b'dir1', some_ctx)
    d2_inode_c = await ctx.server.lookup(d1_inode_c.st_ino, b'dir2', some_ctx)
    f2_h_inode_c = await ctx.server.lookup(d1_inode_c.st_ino, b'file2_hardlink', some_ctx)

    # Check file1
    fi = await ctx.server.open(f1_inode_c.st_ino, os.O_RDWR, some_ctx)
    fh = fi.fh
    assert await ctx.server.read(fh, 0, 42) == b'file1 contents'
    await ctx.server.release(fh)
    assert f1_inode.st_ino != f1_inode_c.st_ino
    assert await ctx.server.getxattr(f1_inode_c.st_ino, ext_attr_name, some_ctx) == ext_attr_val

    # Check file2
    fi = await ctx.server.open(f2_inode_c.st_ino, os.O_RDWR, some_ctx)
    fh = fi.fh
    assert await ctx.server.read(fh, 0, 42) == b'file2 contents'
    await ctx.server.release(fh)
    assert f2_inode_c.st_ino == f2h_inode_c.st_ino
    assert f2_inode_c.st_nlink == 3
    assert f2_inode.st_ino != f2_inode_c.st_ino
    assert f2_h_inode_c.st_ino == f2_inode_c.st_ino

    # Check subdir1
    assert d1_inode.st_ino != d1_inode_c.st_ino
    assert d2_inode.st_ino != d2_inode_c.st_ino
    await ctx.server.forget(list(ctx.server.open_inodes.items()))
    await fsck(ctx)


async def test_copy_tree_2(ctx, monkeypatch):
    src_inode = await ctx.server.mkdir(ROOT_INODE, b'source', dir_mode(), some_ctx)
    dst_inode = await ctx.server.mkdir(ROOT_INODE, b'dest', dir_mode(), some_ctx)

    # Create file
    (fi, inode) = await ctx.server.create(
        src_inode.st_ino, b'file1', file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, b'block 1 contents')
    await ctx.server.write(fh, ctx.max_obj_size, b'block 1 contents')
    await ctx.server.release(fh)
    await ctx.server.forget([(inode.st_ino, 1)])

    monkeypatch.setattr('pyfuse3.invalidate_inode', lambda *args: None)
    await ctx.server.copy_tree(src_inode.st_ino, dst_inode.st_ino)

    await ctx.server.forget([(src_inode.st_ino, 1), (dst_inode.st_ino, 1)])
    await fsck(ctx)


async def test_lock_tree(ctx):
    inode1 = await ctx.server.mkdir(ROOT_INODE, b'source', dir_mode(), some_ctx)

    # Create file
    (fi, inode1a) = await ctx.server.create(
        inode1.st_ino, b'file1', file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, b'file1 contents')
    await ctx.server.release(fh)

    # Create subdirectory
    inode2 = await ctx.server.mkdir(inode1.st_ino, b'dir1', dir_mode(), some_ctx)
    (fi, inode2a) = await ctx.server.create(
        inode2.st_ino, b'file2', file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, b'file2 contents')
    await ctx.server.release(fh)

    # Another file
    (fi, inode3) = await ctx.server.create(ROOT_INODE, b'file1', file_mode(), os.O_RDWR, some_ctx)
    fh = fi.fh
    await ctx.server.release(fh)

    # Lock
    await ctx.server.lock_tree(inode1.st_ino)

    for i in (inode1.st_ino, inode1a.st_ino, inode2.st_ino, inode2a.st_ino):
        assert ctx.server.inodes[i].locked

    # Remove
    with assert_raises(FUSEError) as cm:
        await ctx.server._remove(inode1.st_ino, b'file1', inode1a.st_ino)
    assert cm.value.errno == errno.EPERM

    # Rename / Replace
    with assert_raises(FUSEError) as cm:
        await ctx.server.rename(ROOT_INODE, b'file1', inode1.st_ino, b'file2', 0, some_ctx)
    assert cm.value.errno == errno.EPERM
    with assert_raises(FUSEError) as cm:
        await ctx.server.rename(inode1.st_ino, b'file1', ROOT_INODE, b'file2', 0, some_ctx)
    assert cm.value.errno == errno.EPERM

    # Open
    with assert_raises(FUSEError) as cm:
        await ctx.server.open(inode2a.st_ino, os.O_RDWR, some_ctx)
    assert cm.value.errno == errno.EPERM
    with assert_raises(FUSEError) as cm:
        await ctx.server.open(inode2a.st_ino, os.O_WRONLY, some_ctx)
    assert cm.value.errno == errno.EPERM
    await ctx.server.release(await ctx.server.open(inode3.st_ino, os.O_WRONLY, some_ctx))

    # Write
    fi = await ctx.server.open(inode2a.st_ino, os.O_RDONLY, some_ctx)
    fh = fi.fh
    with assert_raises(FUSEError) as cm:
        await ctx.server.write(fh, 0, b'foo')
    assert cm.value.errno == errno.EPERM
    await ctx.server.release(fh)

    # Create
    with assert_raises(FUSEError) as cm:
        ctx.server._create(inode2.st_ino, b'dir1', dir_mode(), os.O_RDWR, some_ctx)
    assert cm.value.errno == errno.EPERM

    # Setattr
    attr = await ctx.server.getattr(inode2a.st_ino, some_ctx)
    with assert_raises(FUSEError) as cm:
        await ctx.server.setattr(
            inode2a.st_ino, attr, SetattrFields(update_mtime=True), None, some_ctx
        )
    assert cm.value.errno == errno.EPERM

    # xattr
    with assert_raises(FUSEError) as cm:
        await ctx.server.setxattr(inode2.st_ino, b'name', b'value', some_ctx)
    assert cm.value.errno == errno.EPERM
    with assert_raises(FUSEError) as cm:
        await ctx.server.removexattr(inode2.st_ino, b'name', some_ctx)
    assert cm.value.errno == errno.EPERM
    await ctx.server.forget(list(ctx.server.open_inodes.items()))
    await fsck(ctx)


async def test_remove_tree(ctx, monkeypatch):
    inode1 = await ctx.server.mkdir(ROOT_INODE, b'source', dir_mode(), some_ctx)

    # Create file
    (fi, inode1a) = await ctx.server.create(
        inode1.st_ino, b'file1', file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, b'file1 contents')
    await ctx.server.release(fh)

    # Create subdirectory
    inode2 = await ctx.server.mkdir(inode1.st_ino, b'dir1', dir_mode(), some_ctx)
    (fi, inode2a) = await ctx.server.create(
        inode2.st_ino, b'file2', file_mode(), os.O_RDWR, some_ctx
    )
    fh = fi.fh
    await ctx.server.write(fh, 0, b'file2 contents')
    await ctx.server.release(fh)

    # Remove
    await ctx.server.forget(list(ctx.server.open_inodes.items()))
    monkeypatch.setattr('pyfuse3.invalidate_entry', lambda *args: None)
    await ctx.server.remove_tree(ROOT_INODE, b'source')

    for id_p, name in (
        (ROOT_INODE, b'source'),
        (inode1.st_ino, b'file1'),
        (inode1.st_ino, b'dir1'),
        (inode2.st_ino, b'file2'),
    ):
        assert not ctx.db.has_val(
            'SELECT inode FROM contents JOIN names ON names.id = name_id '
            'WHERE name=? AND parent_inode = ?',
            (name, id_p),
        )

    for id_ in (inode1.st_ino, inode1a.st_ino, inode2.st_ino, inode2a.st_ino):
        assert not ctx.db.has_val('SELECT id FROM inodes WHERE id=?', (id_,))

    await fsck(ctx)
