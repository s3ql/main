#!/usr/bin/env python3
"""
t1_database.py - this file is part of S3QL.

Copyright © 2023 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
"""

if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main([__file__] + sys.argv[1:]))


import logging
import math
import os
import tempfile
from argparse import Namespace
from typing import List

import pytest
from pytest_checklogs import assert_logs
from t2_block_cache import random_data

from s3ql import sqlite3ext
from s3ql.backends import local
from s3ql.backends.common import AbstractBackend
from s3ql.database import (
    METADATA_OBJ_NAME,
    Connection,
    FsAttributes,
    download_metadata,
    expire_objects,
    upload_metadata,
    upload_params,
)

# Page size is 4k, so block sizes smaller than that don't make sense
BLOCKSIZE = 4096

# Make sure every row requires at least one block of storage
DUMMY_DATA = random_data(BLOCKSIZE)


def test_track_dirty():
    rows = 11
    sqlite3ext.reset()
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, BLOCKSIZE)
        db.execute("CREATE TABLE foo (id INT, data BLOB);")
        for i in range(rows):
            db.execute("INSERT INTO FOO VALUES(?, ?)", (i, DUMMY_DATA))

        db.checkpoint()
        assert db.dirty_blocks.get_count() >= rows

        db.dirty_blocks.clear()
        assert db.dirty_blocks.get_count() == 0

        db.execute("UPDATE FOO SET data=? WHERE id=?", (random_data(len(DUMMY_DATA)), 0))
        db.checkpoint()
        assert rows > db.dirty_blocks.get_count() > 0

        db.close()


def test_track_dirty_symlink():
    rows = 11
    sqlite3ext.reset()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.mkdir(os.path.join(tmpdir, 'target'))
        os.symlink('target', os.path.join(tmpdir, 'link'))
        fname = os.path.join(tmpdir, 'link', 'testdb')

        db = Connection(fname, BLOCKSIZE)
        db.execute("CREATE TABLE foo (id INT, data BLOB);")
        for i in range(rows):
            db.execute("INSERT INTO FOO VALUES(?, ?)", (i, DUMMY_DATA))

        db.checkpoint()
        assert db.dirty_blocks.get_count() >= rows

        db.dirty_blocks.clear()
        assert db.dirty_blocks.get_count() == 0

        db.execute("UPDATE FOO SET data=? WHERE id=?", (random_data(len(DUMMY_DATA)), 0))
        db.checkpoint()
        assert rows > db.dirty_blocks.get_count() > 0

        db.close()


def test_track_dirty_count():
    sqlite3ext.reset()
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, BLOCKSIZE)
        db.execute("CREATE TABLE foo (id INT);")
        db.execute("INSERT INTO FOO VALUES(42)")

        db.checkpoint()
        db_size = tmpfh.seek(0, os.SEEK_END)
        assert db.dirty_blocks.get_count() == math.ceil(db_size / BLOCKSIZE)

        db.close()


@pytest.fixture
def backend():
    with tempfile.TemporaryDirectory(prefix="s3ql-backend-") as backend_dir:
        yield local.Backend(Namespace(storage_url="local://" + backend_dir))


@pytest.mark.parametrize("incremental", (True, False))
def test_upload_download(backend, incremental):
    sqlite3ext.reset()
    rows = 11
    params = FsAttributes(
        metadata_block_size=BLOCKSIZE,
        data_block_size=BLOCKSIZE,
        seq_no=1,
    )
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, BLOCKSIZE)
        db.execute("CREATE TABLE foo (id INT, data BLOB);")
        for i in range(rows):
            db.execute("INSERT INTO foo VALUES(?, ?)", (i, DUMMY_DATA))

        db.checkpoint()
        upload_metadata(backend, db, params, incremental=incremental)

        # Shrink database
        db.execute('DELETE FROM foo WHERE id >= ?', (rows // 2,))
        db.execute('VACUUM')
        db.checkpoint()
        upload_metadata(backend, db, params, incremental=incremental)

        db.close()

        with tempfile.NamedTemporaryFile() as tmpfh2:
            download_metadata(backend, tmpfh2.name, params)

            tmpfh.seek(0)
            tmpfh2.seek(0)
            assert tmpfh.read() == tmpfh2.read()


def test_checkpoint():
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name)
        db.execute("CREATE TABLE foo (id INT);")
        db.execute("INSERT INTO FOO VALUES(?)", (23,))
        db.execute("INSERT INTO FOO VALUES(?)", (25,))
        db.execute("INSERT INTO FOO VALUES(?)", (30,))

        db.checkpoint()

        q = db.query('SELECT * FROM foo')
        db.execute("INSERT INTO FOO VALUES(?)", (35,))
        with assert_logs('^Unable to checkpoint WAL', count=1, level=logging.WARNING):
            with assert_logs(
                '^sqlite3: statement aborts at.+SQLITE_LOCKED', count=1, level=logging.WARNING
            ):
                db.checkpoint()

        q.close()
        db.checkpoint()
        db.close()


def get_metadata_obj_count(backend: AbstractBackend):
    i = 0
    for _ in backend.list('s3ql_metadata_'):
        i += 1
    return i


def test_versioning(backend):
    sqlite3ext.reset()
    rows = 11
    params = FsAttributes(
        metadata_block_size=BLOCKSIZE,
        data_block_size=BLOCKSIZE,
        seq_no=1,
    )

    versions = []
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, BLOCKSIZE)
        db.execute("CREATE TABLE foo (id INT, data BLOB);")
        for i in range(rows):
            db.execute("INSERT INTO foo VALUES(?, ?)", (i, DUMMY_DATA))

        def upload():
            db.checkpoint()
            upload_metadata(backend, db, params)
            tmpfh.seek(0)
            versions.append((params.copy(), tmpfh.read()))
            params.seq_no += 1

        upload()
        base_count = len(list(backend.list('s3ql_metadata_')))

        # Make some modifications
        db.execute('INSERT INTO foo(id, data) VALUES(?, ?)', (rows + 1, b'short data'))
        upload()

        db.execute('INSERT INTO foo(id, data) VALUES(?, ?)', (2 * rows + 1, DUMMY_DATA))
        upload()

        db.execute("UPDATE FOO SET data=? WHERE id=?", (random_data(len(DUMMY_DATA)), 0))
        db.execute("UPDATE FOO SET data=? WHERE id=?", (random_data(len(DUMMY_DATA)), rows // 2))
        upload()

        db.execute('DELETE FROM foo WHERE id >= ?', (rows // 2,))
        db.execute('VACUUM')
        upload()

        db.close()

    for params, ref_db in versions:
        with tempfile.NamedTemporaryFile() as tmpfh2:
            download_metadata(backend, tmpfh2.name, params)
            assert tmpfh2.read() == ref_db

    # Make sure that we did not store a full copy of every version
    obj_count = len(list(backend.list('s3ql_metadata_')))
    versions = len(versions) - 1
    assert obj_count - base_count <= versions * base_count * 0.5


def _test_expiration(
    backend: AbstractBackend,
    contents_pre: List[List[int]],
    contents_post: List[List[int]],
    db_sizes: List[int],
    versions_to_keep: int,
):
    id_seq_map = {}
    for blockno, versions in enumerate(contents_pre):
        assert len(db_sizes) == len(versions)
        last_id = None
        for seq_no, block_id in enumerate(versions):
            if last_id != block_id:
                last_id = block_id
                if block_id is None:
                    continue
                block_seq_no = seq_no
                id_seq_map[(blockno, block_id)] = block_seq_no
                backend[METADATA_OBJ_NAME % (blockno, block_seq_no)] = str(block_id).encode()

    for seq_no, size in enumerate(db_sizes):
        params = FsAttributes(
            metadata_block_size=BLOCKSIZE,
            seq_no=seq_no,
            db_size=BLOCKSIZE * size,
            data_block_size=BLOCKSIZE,
        )
        upload_params(backend, params)

    expire_objects(backend, versions_to_keep=versions_to_keep)

    expected_objects = set()
    for blockno, versions in enumerate(contents_post):
        for block_id in versions:
            if block_id is None:
                continue
            block_seq_no = id_seq_map[(blockno, block_id)]
            expected_objects.add(METADATA_OBJ_NAME % (blockno, block_seq_no))

    all_objs = set(backend.list('s3ql_metadata_'))
    assert expected_objects == all_objs


def test_expiration_nodup(backend):
    # Trivial case, every block is stored in every version
    contents_pre = [
        [10, 11, 12, 13, 14, 15],  # block 0
        [20, 21, 22, 23, 24, 25],  # block 1
        [30, 31, 32, 33, 34, 35],  # block 2
        [40, 41, 42, 43, 44, 45],  # block 3
    ]
    db_sizes = [4, 4, 4, 4, 4, 4]
    contents_post = [
        [13, 14, 15],  # block 0
        [23, 24, 25],  # block 1
        [33, 34, 35],  # block 2
        [43, 44, 45],  # block 3
    ]
    _test_expiration(backend, contents_pre, contents_post, db_sizes=db_sizes, versions_to_keep=3)


def test_expiration_dup1(backend):
    # Some old blocks are used repeatedly, but all expired
    contents_pre = [
        [10, 11, 12, 13, 14, 15],  # block 0
        [20, 20, 20, 21, 22, 23],  # block 1
        [30, 31, 32, 33, 34, 35],  # block 2
        [40, 40, 41, 42, 43, 44],  # block 3
    ]
    db_sizes = [4, 4, 4, 4, 4, 4]
    contents_post = [
        [13, 14, 15],  # block 0
        [21, 22, 23],  # block 1
        [33, 34, 35],  # block 2
        [42, 43, 44],  # block 3
    ]
    _test_expiration(backend, contents_pre, contents_post, db_sizes=db_sizes, versions_to_keep=3)


def test_expiration_dup2(backend):
    # Some old blocks are used repeatedly and thus not removed
    contents_pre = [
        [10, 11, 12, 13, 14, 15],  # block 0
        [20, 20, 20, 21, 22, 23],  # block 1
        [30, 31, 32, 33, 34, 35],  # block 2
        [40, 40, 41, 42, 43, 44],  # block 3
    ]
    db_sizes = [4, 4, 4, 4, 4, 4]

    contents_post = [
        [12, 13, 14, 15],  # block 0
        [20, 21, 22, 23],  # block 1
        [32, 33, 34, 35],  # block 2
        [41, 42, 43, 44],  # block 3
    ]
    _test_expiration(backend, contents_pre, contents_post, db_sizes=db_sizes, versions_to_keep=4)


def test_expiration_dup3(backend):
    # Some new blocks are used repeatedly
    contents_pre = [
        [10, 11, 12, 13, 13, 13],  # block 0
        [20, 20, 20, 21, 22, 22],  # block 1
        [30, 31, 32, 33, 34, 35],  # block 2
        [40, 40, 41, 42, 43, 44],  # block 3
    ]
    db_sizes = [4, 4, 4, 4, 4, 4]

    contents_post = [
        [13, 13, 13],  # block 0
        [21, 22, 22],  # block 1
        [33, 34, 35],  # block 2
        [42, 43, 44],  # block 3
    ]
    _test_expiration(backend, contents_pre, contents_post, db_sizes=db_sizes, versions_to_keep=3)


def test_expiration_smaller(backend):
    # Database size reduced
    contents_pre = [
        [10, 11, 12, 13, 14, 15],  # block 0
        [20, 21, 22, 23, 24, 25],  # block 1
        [30, 31, 32, 33, None, None],  # block 2
        [40, 41, 42, None, None, None],  # block 3
    ]
    db_sizes = [4, 4, 4, 3, 2, 2]

    contents_post = [
        [13, 14, 15],  # block 0
        [23, 24, 25],  # block 1
        [33, None, None],  # block 2
    ]
    _test_expiration(backend, contents_pre, contents_post, db_sizes=db_sizes, versions_to_keep=3)


def test_expiration_downup(backend):
    # Database size reduced and then increased again
    contents_pre = [
        [10, 11, 12, 13, 13],  # block 0
        [20, 21, 23, 23, 24],  # block 1
        [30, 31, None, 33, 34],  # block 2
        [40, None, None, 40, None],  # block 3
    ]
    db_sizes = [4, 3, 2, 4, 3]

    contents_post = [
        [12, 13, 13],  # block 0
        [23, 23, 24],  # block 1
        [None, 33, 34],  # block 2
        [None, 40, None],  # block 3
    ]
    _test_expiration(backend, contents_pre, contents_post, db_sizes=db_sizes, versions_to_keep=3)


@pytest.mark.parametrize("incremental", (True, False))
def test_download_shorter(backend, incremental):
    # Test downloading older snapshot when the filesize has grown
    sqlite3ext.reset()
    params = FsAttributes(
        metadata_block_size=BLOCKSIZE,
        data_block_size=BLOCKSIZE,
        seq_no=1,
    )
    with tempfile.NamedTemporaryFile() as tmpfh:
        db = Connection(tmpfh.name, BLOCKSIZE)
        db.execute("CREATE TABLE foo (id INT, data BLOB);")
        for i in range(2):
            db.execute("INSERT INTO foo VALUES(?, ?)", (i, DUMMY_DATA))

        db.checkpoint()
        upload_metadata(backend, db, params, incremental=incremental)
        old_params = params.copy()
        params.seq_no += 1

        # Grow database
        for i in range(2, 6):
            db.execute("INSERT INTO foo VALUES(?, ?)", (i, DUMMY_DATA))
        db.checkpoint()
        upload_metadata(backend, db, params)
        db.close()

    with tempfile.NamedTemporaryFile() as tmpfh:
        download_metadata(backend, tmpfh.name, old_params)
