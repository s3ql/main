'''
block_cache.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from . import BUFSIZE
from .database import NoSuchRowError
from .backends.common import NoSuchObject
from .multi_lock import MultiLock
from .logging import logging # Ensure use of custom logger class
from collections import OrderedDict
from contextlib import contextmanager
from llfuse import lock, lock_released
from queue import Queue, Empty as QueueEmpty, Full as QueueFull
import os
import hashlib
import shutil
import threading
import time
import re
import sys

# standard logger for this module
log = logging.getLogger(__name__)

# Special queue entry that signals threads to terminate
QuitSentinel = object()

# Special queue entry that signals that removal queue should
# be flushed
FlushSentinel = object()

class NoWorkerThreads(Exception):
    '''
    Raised when trying to enqueue an object, but there
    are no active consumer threads.
    '''

    pass

class Distributor(object):
    '''
    Distributes objects to consumers.
    '''

    def __init__(self):
        super().__init__()

        self.slot = None
        self.cv = threading.Condition()

        #: Number of threads waiting to consume an object
        self.readers = 0

    def put(self, obj, timeout=None):
        '''Offer *obj* for consumption

        The method blocks until another thread calls `get()` to consume the
        object.

        Return `True` if the object was consumed, and `False` if *timeout* was
        exceeded without any activity in the queue (this means an individual
        invocation may wait for longer than *timeout* if objects from other
        threads are being consumed).
        '''

        if obj is None:
            raise ValueError("Can't put None into Queue")

        with self.cv:
            # Wait until a thread is ready to read
            while self.readers == 0 or self.slot is not None:
                log.debug('waiting for reader..')
                if not self.cv.wait(timeout):
                    log.debug('timeout, returning')
                    return False

            log.debug('got reader, enqueueing %s', obj)
            self.readers -= 1
            assert self.slot is None
            self.slot = obj
            self.cv.notify_all() # notify readers

        return True

    def get(self):
        '''Consume and return an object

        The method blocks until another thread offers an object by calling the
        `put` method.
        '''
        with self.cv:
            self.readers += 1
            self.cv.notify_all()
            while self.slot is None:
                log.debug('waiting for writer..')
                self.cv.wait()
            tmp = self.slot
            self.slot = None
            self.cv.notify_all()

        return tmp


class SimpleEvent(object):
    '''
    Like threading.Event, but without any internal flag. Calls
    to `wait` always block until some other thread calls
    `notify` or `notify_all`.
    '''

    def __init__(self):
        super().__init__()
        self.__cond = threading.Condition(threading.Lock())

    def notify_all(self):
        self.__cond.acquire()
        try:
            self.__cond.notify_all()
        finally:
            self.__cond.release()

    def notify(self):
        self.__cond.acquire()
        try:
            self.__cond.notify()
        finally:
            self.__cond.release()

    def wait(self, timeout=None):
        self.__cond.acquire()
        try:
            return self.__cond.wait(timeout)
        finally:
            self.__cond.release()


class CacheEntry(object):
    """An element in the block cache

    Attributes:
    -----------

    :dirty:    entry has been changed since it was last uploaded.
    :size:     current file size
    :pos: current position in file
    """

    __slots__ = [ 'dirty', 'inode', 'blockno', 'last_write',
                  'size', 'pos', 'fh', 'removed' ]

    def __init__(self, inode, blockno, filename, mode='w+b'):
        super().__init__()
        # Writing 100MB in 128k chunks takes 90ms unbuffered and
        # 116ms with 1 MB buffer. Reading time does not depend on
        # buffer size.
        self.fh = open(filename, mode, 0)
        self.dirty = False
        self.inode = inode
        self.blockno = blockno
        self.last_write = 0
        self.pos = self.fh.tell()
        self.size = os.fstat(self.fh.fileno()).st_size

    def read(self, size=None):
        buf = self.fh.read(size)
        self.pos += len(buf)
        return buf

    def flush(self):
        self.fh.flush()

    def seek(self, off):
        if self.pos != off:
            self.fh.seek(off)
            self.pos = off

    def tell(self):
        return self.pos

    def truncate(self, size=None):
        self.dirty = True
        self.fh.truncate(size)
        if size is None:
            if self.pos < self.size:
                self.size = self.pos
        elif size < self.size:
            self.size = size

    def write(self, buf):
        self.dirty = True
        self.fh.write(buf)
        self.pos += len(buf)
        self.size = max(self.pos, self.size)
        self.last_write = time.time()

    def close(self):
        self.fh.close()

    def unlink(self):
        os.unlink(self.fh.name)

    def __str__(self):
        return ('<%sCacheEntry, inode=%d, blockno=%d>'
                % ('Dirty ' if self.dirty else '', self.inode, self.blockno))


class CacheDict(OrderedDict):
    '''
    An ordered dictionary designed to store CacheEntries.

    Attributes:

    :max_size: maximum size to which cache can grow
    :max_entries: maximum number of entries in cache
    :size: current size of all entries together
    '''

    def __init__(self, max_size, max_entries):
        super().__init__()
        self.max_size = max_size
        self.max_entries = max_entries
        self.size = 0

    def remove(self, key, unlink=True):
        '''Remove *key* from disk and cache, update size'''

        el = self.pop(key)
        el.close()
        self.size -= el.size
        if unlink:
            el.unlink()

    def is_full(self):
        return (self.size > self.max_size
                or len(self) > self.max_entries)

class BlockCache(object):
    """Provides access to file blocks

    This class manages access to file blocks. It takes care of creation,
    uploading, downloading and deduplication.

    This class uses the llfuse global lock. Methods which release the lock have
    are marked as such in their docstring.

    Attributes
    ----------

    :path: where cached data is stored
    :cache: ordered dictionary of cache entries
    :mlock: MultiLock to synchronize access to objects and cache entries
    :in_transit: set of cache entries that are currently being uploaded
    :to_upload: distributes objects to upload to worker threads
    :to_remove: distributes objects to remove to worker threads
    :transfer_complete: signals completion of an object upload
    :upload_threads: list of threads processing upload queue
    :removal_threads: list of threads processing removal queue
    :db: Handle to SQL DB
    :backend_pool: BackendPool instance
    """

    def __init__(self, backend_pool, db, cachedir, max_size, max_entries=768):
        log.debug('Initializing')

        self.path = cachedir
        self.db = db
        self.backend_pool = backend_pool
        self.cache = CacheDict(max_size, max_entries)
        self.mlock = MultiLock()
        self.in_transit = set()
        self.upload_threads = []
        self.removal_threads = []
        self.transfer_completed = SimpleEvent()

        # Will be initialized once threads are available
        self.to_upload = None
        self.to_remove = None

        if os.path.exists(self.path):
            self.load_cache()
            log.info('Loaded %d entries from cache', len(self.cache))
        else:
            os.mkdir(self.path)

        # Initialized fromt the outside to prevent cyclic dependency
        self.fs = None

    def load_cache(self):
        '''Initialize cache from disk'''

        for filename in os.listdir(self.path):
            match = re.match('^(\\d+)-(\\d+)$', filename)
            if not match:
                continue
            inode = int(match.group(1))
            blockno = int(match.group(2))

            el = CacheEntry(inode, blockno,
                            os.path.join(self.path, filename), mode='r+b')
            self.cache[(inode, blockno)] = el
            self.cache.size += el.size

    def __len__(self):
        '''Get number of objects in cache'''
        return len(self.cache)

    def init(self, threads=1):
        '''Start worker threads'''

        self.to_upload = Distributor()
        for _ in range(threads):
            t = threading.Thread(target=self._upload_loop)
            t.start()
            self.upload_threads.append(t)

        self.to_remove = Queue(1000)
        with self.backend_pool() as backend:
            has_delete_multi = backend.has_delete_multi

        if has_delete_multi:
            t = threading.Thread(target=self._removal_loop_multi)
            t.daemon = True # interruption will do no permanent harm
            t.start()
            self.removal_threads.append(t)
        else:
            for _ in range(20):
                t = threading.Thread(target=self._removal_loop_simple)
                t.daemon = True # interruption will do no permanent harm
                t.start()
                self.removal_threads.append(t)

    def _lock_obj(self, obj_id, release_global=False):
        '''Acquire lock on *obj*id*'''

        if release_global:
            with lock_released:
                self.mlock.acquire(obj_id)
        else:
            self.mlock.acquire(obj_id)

    def _unlock_obj(self, obj_id, release_global=False, noerror=False):
        '''Release lock on *obj*id*'''

        if release_global:
            with lock_released:
                self.mlock.release(obj_id, noerror=noerror)
        else:
            self.mlock.release(obj_id, noerror=noerror)

    def _lock_entry(self, inode, blockno, release_global=False, timeout=None):
        '''Acquire lock on cache entry'''

        if release_global:
            with lock_released:
                return self.mlock.acquire((inode, blockno), timeout=timeout)
        else:
            return self.mlock.acquire((inode, blockno), timeout=timeout)

    def _unlock_entry(self, inode, blockno, release_global=False,
                      noerror=False):
        '''Release lock on cache entry'''

        if release_global:
            with lock_released:
                self.mlock.release((inode, blockno), noerror=noerror)
        else:
            self.mlock.release((inode, blockno), noerror=noerror)

    def destroy(self, keep_cache=False):
        '''Clean up and stop worker threads

        This method should be called without the global lock held.
        '''

        log.debug('Flushing cache...')
        try:
            with lock:
                if keep_cache:
                    self.flush() # releases global lock
                    for el in self.cache.values():
                        assert not el.dirty
                        el.close()
                else:
                    self.drop()
        except NoWorkerThreads:
            log.error('Unable to flush cache, no upload threads left alive')

        # Signal termination to worker threads. If some of them
        # terminated prematurely, continue gracefully.
        log.debug('Signaling upload threads...')
        try:
            for t in self.upload_threads:
                self._queue_upload(QuitSentinel)
        except NoWorkerThreads:
            pass

        log.debug('Signaling removal threads...')
        try:
            for t in self.removal_threads:
                self._queue_removal(QuitSentinel)
        except NoWorkerThreads:
            pass

        log.debug('waiting for upload threads...')
        for t in self.upload_threads:
            t.join()

        log.debug('waiting for removal threads...')
        for t in self.removal_threads:
            t.join()

        assert len(self.in_transit) == 0
        try:
            while self.to_remove.get_nowait() is QuitSentinel:
                pass
        except QueueEmpty:
            pass
        else:
            log.error('Could not complete object removals, '
                      'no removal threads left alive')

        self.to_upload = None
        self.to_remove = None
        self.upload_threads = None
        self.removal_threads = None

        if not keep_cache:
            os.rmdir(self.path)

        log.debug('cleanup done.')


    def _upload_loop(self):
        '''Process upload queue'''

        while True:
            tmp = self.to_upload.get()

            if tmp is QuitSentinel:
                break

            self._do_upload(*tmp)


    def _do_upload(self, el, obj_id):
        '''Upload object'''

        def do_write(fh):
            el.seek(0)
            while True:
                buf = el.read(BUFSIZE)
                if not buf:
                    break
                fh.write(buf)
            return fh

        try:
            with self.backend_pool() as backend:
                if log.isEnabledFor(logging.DEBUG):
                    time_ = time.time()
                    obj_size = backend.perform_write(do_write, 's3ql_data_%d'
                                                     % obj_id).get_obj_size()
                    time_ = time.time() - time_
                    rate = el.size / (1024 ** 2 * time_) if time_ != 0 else 0
                    log.debug('uploaded %d bytes in %.3f seconds, %.2f MiB/s',
                              el.size, time_, rate)
                else:
                    obj_size = backend.perform_write(do_write, 's3ql_data_%d'
                                                     % obj_id).get_obj_size()

            with lock:
                self.db.execute('UPDATE objects SET size=? WHERE id=?', (obj_size, obj_id))
                el.dirty = False

        except Exception as exc:
            log.debug('upload of %d failed: %s', obj_id, exc)
            # At this point we have to remove references to this storage object
            # from the objects and blocks table to prevent future cache elements
            # to be de-duplicated against this (missing) one. However, this may
            # already have happened during the attempted upload. The only way to
            # avoid this problem is to insert the hash into the blocks table
            # *after* successfull upload. But this would open a window without
            # de-duplication just to handle the special case of an upload
            # failing.
            #
            # On the other hand, we also want to prevent future deduplication
            # against this block: otherwise the next attempt to upload the same
            # cache element (by a different upload thread that has not
            # encountered problems yet) is guaranteed to link against the
            # non-existing block, and the data will be lost.
            #
            # Therefore, we just set the hash of the missing block to NULL,
            # and rely on fsck to pick up the pieces. Note that we cannot
            # delete the row from the blocks table, because the id will get
            # assigned to a new block, so the inode_blocks entries will
            # refer to incorrect data.
            #

            with lock:
                self.db.execute('UPDATE blocks SET hash=NULL WHERE obj_id=?',
                                (obj_id,))
            raise

        finally:
            self.in_transit.remove(el)
            self._unlock_obj(obj_id)
            self._unlock_entry(el.inode, el.blockno)
            self.transfer_completed.notify_all()


    def wait(self):
        '''Wait until an object has been uploaded

        If there are no objects in transit, return immediately. This method
        releases the global lock.
        '''

        # Loop to avoid the race condition of a transfer terminating
        # between the call to transfer_in_progress() and wait().
        while True:
            if not self.transfer_in_progress():
                return

            with lock_released:
                if self.transfer_completed.wait(timeout=5):
                    return

    def upload_if_dirty(self, el):
        '''Upload cache entry asynchronously

        This method releases the global lock. Return True if the object
        is actually scheduled for upload.
        '''

        log.debug('started with %s', el)

        if el in self.in_transit:
            return True
        elif not el.dirty:
            return False

        # Calculate checksum
        with lock_released:
            self._lock_entry(el.inode, el.blockno)
            added_to_transit = False
            try:
                if el is not self.cache.get((el.inode, el.blockno), None):
                    log.debug('%s got removed while waiting for lock', el)
                    self._unlock_entry(el.inode, el.blockno)
                    return False
                if el in self.in_transit:
                    log.debug('%s already in transit', el)
                    self._unlock_entry(el.inode, el.blockno)
                    return True
                if not el.dirty:
                    log.debug('no longer dirty, returning')
                    self._unlock_entry(el.inode, el.blockno)
                    return False

                log.debug('uploading %s..', el)
                self.in_transit.add(el)
                added_to_transit = True
                sha = hashlib.sha256()
                el.seek(0)
                while True:
                    buf = el.read(BUFSIZE)
                    if not buf:
                        break
                    sha.update(buf)
                hash_ = sha.digest()
            except:
                if added_to_transit:
                    self.in_transit.discard(el)
                self._unlock_entry(el.inode, el.blockno)
                raise

        obj_lock_taken = False
        try:
            try:
                old_block_id = self.db.get_val('SELECT block_id FROM inode_blocks '
                                               'WHERE inode=? AND blockno=?',
                                               (el.inode, el.blockno))
            except NoSuchRowError:
                old_block_id = None

            try:
                block_id = self.db.get_val('SELECT id FROM blocks WHERE hash=?', (hash_,))

            # No block with same hash
            except NoSuchRowError:
                obj_id = self.db.rowid('INSERT INTO objects (refcount, size) VALUES(1, -1)')
                log.debug('created new object %d', obj_id)
                block_id = self.db.rowid('INSERT INTO blocks (refcount, obj_id, hash, size) '
                                         'VALUES(?,?,?,?)', (1, obj_id, hash_, el.size))
                log.debug('created new block %d', block_id)
                log.debug('adding to upload queue')

                # Note: we must finish all db transactions before adding to
                # in_transit, otherwise commit() may return before all blocks
                # are available in db.
                self.db.execute('INSERT OR REPLACE INTO inode_blocks (block_id, inode, blockno) '
                                'VALUES(?,?,?)', (block_id, el.inode, el.blockno))

                with lock_released:
                    self._lock_obj(obj_id)
                    obj_lock_taken = True
                    self._queue_upload((el, obj_id))

            # There is a block with the same hash
            else:
                if old_block_id != block_id:
                    log.debug('(re)linking to %d', block_id)
                    self.db.execute('UPDATE blocks SET refcount=refcount+1 WHERE id=?',
                                    (block_id,))
                    self.db.execute('INSERT OR REPLACE INTO inode_blocks (block_id, inode, blockno) '
                                    'VALUES(?,?,?)', (block_id, el.inode, el.blockno))

                el.dirty = False
                self.in_transit.remove(el)
                self._unlock_entry(el.inode, el.blockno, release_global=True)

                if old_block_id == block_id:
                    log.debug('unchanged, block_id=%d', block_id)
                    return False

        except:
            self.in_transit.discard(el)
            with lock_released:
                self._unlock_entry(el.inode, el.blockno, noerror=True)
                if obj_lock_taken:
                    self._unlock_obj(obj_id)
            raise

        if old_block_id:
            self._deref_block(old_block_id)
        else:
            log.debug('no old block')

        return obj_lock_taken


    def _queue_upload(self, obj):
        '''Put *obj* into upload queue'''

        while True:
            if self.to_upload.put(obj, timeout=5):
                return
            for t in self.upload_threads:
                if t.is_alive():
                    break
            else:
                raise NoWorkerThreads('no upload threads')

    def _queue_removal(self, obj):
        '''Put *obj* into removal queue'''

        while True:
            try:
                self.to_remove.put(obj, timeout=5)
            except QueueFull:
                pass
            else:
                return

            for t in self.removal_threads:
                if t.is_alive():
                    break
            else:
                raise NoWorkerThreads('no removal threads')

    def _deref_block(self, block_id):
        '''Decrease reference count for *block_id*

        If reference counter drops to zero, remove block and propagate
        to objects table (possibly removing the referenced object
        as well).

        This method releases the global lock.
        '''

        refcount = self.db.get_val('SELECT refcount FROM blocks WHERE id=?', (block_id,))
        if refcount > 1:
            log.debug('decreased refcount for block: %d', block_id)
            self.db.execute('UPDATE blocks SET refcount=refcount-1 WHERE id=?', (block_id,))
            return

        log.debug('removing block %d', block_id)
        obj_id = self.db.get_val('SELECT obj_id FROM blocks WHERE id=?', (block_id,))
        self.db.execute('DELETE FROM blocks WHERE id=?', (block_id,))
        (refcount, size) = self.db.get_row('SELECT refcount, size FROM objects WHERE id=?',
                                           (obj_id,))
        if refcount > 1:
            log.debug('decreased refcount for obj: %d', obj_id)
            self.db.execute('UPDATE objects SET refcount=refcount-1 WHERE id=?',
                            (obj_id,))
            return

        log.debug('removing object %d', obj_id)
        self.db.execute('DELETE FROM objects WHERE id=?', (obj_id,))

        # Taking the lock ensures that the object is no longer in
        # transit itself. We can release it immediately after, because
        # the object is no longer in the database.
        log.debug('adding %d to removal queue', obj_id)

        with lock_released:
            self._lock_obj(obj_id)
            self._unlock_obj(obj_id)

            if size == -1:
                # size == -1 indicates that object has not yet been uploaded.
                # However, since we just acquired a lock on the object, we know
                # that the upload must have failed. Therefore, trying to remove
                # this object would just give us another error.
                return

            self._queue_removal(obj_id)


    def transfer_in_progress(self):
        '''Return True if there are any cache entries being uploaded'''

        return len(self.in_transit) > 0

    def _removal_loop_multi(self):
        '''Process removal queue'''

        # This method may look more complicated than necessary, but it ensures
        # that we read as many objects from the queue as we can without
        # blocking, and then hand them over to the backend all at once.

        ids = []
        while True:
            try:
                log.debug('reading from queue (blocking=%s)', len(ids)==0)
                tmp = self.to_remove.get(block=len(ids)==0)
            except QueueEmpty:
                tmp = FlushSentinel

            if tmp in (FlushSentinel, QuitSentinel) and ids:
                log.debug('removing: %s', ids)
                try:
                    with self.backend_pool() as backend:
                        backend.delete_multi(['s3ql_data_%d' % i for i in ids])
                except NoSuchObject:
                    log.warning('Backend lost object s3ql_data_%d' % ids.pop(0))
                    self.fs.failsafe = True
                ids = []
            else:
                ids.append(tmp)

            if tmp is QuitSentinel:
                break

    def _removal_loop_simple(self):
        '''Process removal queue'''

        while True:
            log.debug('reading from queue..')
            id_ = self.to_remove.get()
            if id_ is QuitSentinel:
                break
            with self.backend_pool() as backend:
                try:
                    backend.delete('s3ql_data_%d' % id_)
                except NoSuchObject:
                    log.warning('Backend lost object s3ql_data_%d' % id_)
                    self.fs.failsafe = True

    @contextmanager
    def get(self, inode, blockno):
        """Get file handle for block `blockno` of `inode`

        This method releases the global lock.
        """

        #log.debug('started with %d, %d', inode, blockno)

        if self.cache.is_full():
            self.expire()

        self._lock_entry(inode, blockno, release_global=True)
        try:
            el = self._get_entry(inode, blockno)
            oldsize = el.size
            try:
                yield el
            finally:
                # Update cachesize. NOTE: this requires that at most one
                # thread has access to a cache entry at any time.
                self.cache.size += el.size - oldsize
        finally:
            self._unlock_entry(inode, blockno, release_global=True)

        #log.debug('finished')

    def _get_entry(self, inode, blockno):
        '''Get cache entry for `blockno` of `inode`

        Assume that cache entry lock has been acquired.
        '''

        log.debug('started with %d, %d', inode, blockno)
        try:
            el = self.cache[(inode, blockno)]

        # Not in cache
        except KeyError:
            filename = os.path.join(self.path, '%d-%d' % (inode, blockno))
            try:
                block_id = self.db.get_val('SELECT block_id FROM inode_blocks '
                                           'WHERE inode=? AND blockno=?', (inode, blockno))

            # No corresponding object
            except NoSuchRowError:
                log.debug('creating new block')
                el = CacheEntry(inode, blockno, filename)
                self.cache[(inode, blockno)] = el
                return el

            # Need to download corresponding object
            obj_id = self.db.get_val('SELECT obj_id FROM blocks WHERE id=?', (block_id,))
            log.debug('downloading object %d..', obj_id)
            tmpfh = open(filename + '.tmp', 'wb')
            try:
                def do_read(fh):
                    tmpfh.seek(0)
                    tmpfh.truncate()
                    shutil.copyfileobj(fh, tmpfh, BUFSIZE)

                with lock_released:
                    # Lock object. This ensures that we wait until the object
                    # is uploaded. We don't have to worry about deletion, because
                    # as long as the current cache entry exists, there will always be
                    # a reference to the object (and we already have a lock on the
                    # cache entry).
                    self._lock_obj(obj_id)
                    self._unlock_obj(obj_id)
                    with self.backend_pool() as backend:
                        backend.perform_read(do_read, 's3ql_data_%d' % obj_id)

                tmpfh.flush()
                os.fsync(tmpfh.fileno())
                os.rename(tmpfh.name, filename)
            except:
                os.unlink(tmpfh.name)
                raise
            finally:
                tmpfh.close()

            el = CacheEntry(inode, blockno, filename, mode='r+b')
            self.cache[(inode, blockno)] = el
            self.cache.size += el.size

        # In Cache
        else:
            #log.debug('in cache')
            self.cache.move_to_end((inode, blockno), last=True) # move to head

        return el

    def expire(self):
        """Perform cache expiry

        This method releases the global lock.
        """

        # Note that we have to make sure that the cache entry is written into
        # the database before we remove it from the cache!

        log.debug('started')

        while True:
            need_size = self.cache.size - self.cache.max_size
            need_entries = len(self.cache) - self.cache.max_entries

            if need_size <= 0 and need_entries <= 0:
                break

            # Need to make copy, since we aren't allowed to change dict while
            # iterating through it. Look at the comments in CommitThread.run()
            # (mount.py) for an estimate of the resulting performance hit.
            sth_in_transit = False
            for el in list(self.cache.values()):
                if need_size <= 0 and need_entries <= 0:
                    break

                need_entries -= 1
                need_size -= el.size

                if self.upload_if_dirty(el): # Releases global lock
                    sth_in_transit = True
                    continue

                self._lock_entry(el.inode, el.blockno, release_global=True)
                try:
                    # May have changed while we were waiting for lock
                    if el is not self.cache.get((el.inode, el.blockno), None):
                        log.debug('%s removed while waiting for lock', el)
                        continue
                    if el.dirty:
                        log.debug('%s got dirty while waiting for lock', el)
                        continue
                    log.debug('removing %s from cache', el)
                    self.cache.remove((el.inode, el.blockno))
                finally:
                    self._unlock_entry(el.inode, el.blockno, release_global=True)

            if sth_in_transit:
                log.debug('waiting for transfer threads..')
                self.wait() # Releases global lock

        log.debug('finished')


    def remove(self, inode, start_no, end_no=None):
        """Remove blocks for `inode`

        If `end_no` is not specified, remove just the `start_no` block.
        Otherwise removes all blocks from `start_no` to, but not including,
         `end_no`.

        This method releases the global lock.
        """

        log.debug('started with %d, %d, %s', inode, start_no, end_no)

        if end_no is None:
            end_no = start_no + 1
        blocknos = set(range(start_no, end_no))

        # First do an opportunistic pass and remove everything where we can
        # immediately get a lock. This is important when removing a file right
        # after it has been created. If the upload of the first block has
        # already started , removal would be stuck behind the upload procedure,
        # waiting for every block to be uploaded only to remove it afterwards.
        for timeout in (0, None):
            for blockno in list(blocknos):
                if not self._lock_entry(inode, blockno, release_global=True, timeout=timeout):
                    continue
                blocknos.remove(blockno)
                try:
                    if (inode, blockno) in self.cache:
                        log.debug('removing from cache')
                        self.cache.remove((inode, blockno))

                    try:
                        block_id = self.db.get_val('SELECT block_id FROM inode_blocks '
                                                   'WHERE inode=? AND blockno=?', (inode, blockno))
                    except NoSuchRowError:
                        log.debug('block not in db')
                        continue

                    # Detach inode from block
                    self.db.execute('DELETE FROM inode_blocks WHERE inode=? AND blockno=?',
                                    (inode, blockno))

                finally:
                    self._unlock_entry(inode, blockno, release_global=True)

                # Decrease block refcount
                self._deref_block(block_id)

        log.debug('finished')

    def flush_local(self, inode, blockno):
        """Flush buffers for given block"""

        try:
            el = self.cache[(inode, blockno)]
        except KeyError:
            return

        el.flush()

    def start_flush(self):
        """Initiate upload of all dirty blocks

        When the method returns, all blocks have been registered
        in the database (but the actual uploads may still be
        in progress).

        This method releases the global lock.
        """

        # Need to make copy, since dict() may change while global lock is
        # released. Look at the comments in CommitThread.run() (mount.py) for an
        # estimate of the performance impact.
        for el in list(self.cache.values()):
            self.upload_if_dirty(el) # Releases global lock

    def flush(self):
        """Upload all dirty blocks

        This method releases the global lock.
        """

        log.debug('started')

        while True:
            sth_in_transit = False

            # Need to make copy, since dict() may change while global lock is
            # released. Look at the comments in CommitThread.run() (mount.py)
            # for an estimate of the performance impact.
            for el in list(self.cache.values()):
                if self.upload_if_dirty(el): # Releases global lock
                    sth_in_transit = True

            if not sth_in_transit:
                break

            log.debug('waiting for transfer threads..')
            self.wait() # Releases global lock

        log.debug('finished')

    def drop(self):
        """Drop cache

        This method releases the global lock.
        """

        log.debug('started')
        bak = self.cache.max_entries
        self.cache.max_entries = 0
        self.expire() # Releases global lock
        self.cache.max_entries = bak
        log.debug('finished')

    def get_usage(self):
        '''Get cache usage information.

        Return a tuple of

        * cache entries
        * cache size
        * dirty cache entries
        * dirty cache size
        * pending removals

        This method is O(n) in the number of cache entries.
        '''

        used = self.cache.size
        dirty_size = 0
        dirty_cnt = 0
        for el in self.cache.values():
            if el.dirty:
                dirty_size += el.size
                dirty_cnt += 1

        if self.to_remove is None:
            remove_cnt = 0
        else:
            # This is an estimate which may be negative
            remove_cnt = max(0, self.to_remove.qsize())

        return (len(self.cache), used, dirty_cnt, dirty_size, remove_cnt)

    def __del__(self):
        # break reference loop
        self.fs = None

        for el in self.cache.values():
            if el.dirty:
                break
        else:
            return

        # Force execution of sys.excepthook (exceptions raised
        # by __del__ are ignored)
        try:
            raise RuntimeError("BlockManager instance was destroyed without "
                               "calling destroy()!")
        except RuntimeError:
            exc_info = sys.exc_info()

        sys.excepthook(*exc_info)
