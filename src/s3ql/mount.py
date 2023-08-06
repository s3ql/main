'''
mount.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import _thread
import argparse
import atexit
import faulthandler
import functools
import logging
import os
import platform
import re
import resource
import shutil
import signal
import subprocess
import sys
import threading
import time
from contextlib import AsyncExitStack
from typing import Tuple

import pyfuse3
import trio

from . import fs
from .backends.pool import BackendPool
from .block_cache import BlockCache
from .common import get_backend_factory, is_mounted
from .daemonize import daemonize
from .database import (
    Connection,
    FsAttributes,
    download_metadata,
    expire_objects,
    read_cached_params,
    read_remote_params,
    upload_metadata,
    upload_params,
    write_params,
)
from .inode_cache import InodeCache
from .logging import QuietError, setup_logging, setup_warnings
from .parse_args import ArgumentParser

log = logging.getLogger(__name__)


def install_thread_excepthook():
    """work around sys.excepthook thread bug

    See http://bugs.python.org/issue1230540.

    Call once from __main__ before creating any threads. If using
    psyco, call psyco.cannotcompile(threading.Thread.run) since this
    replaces a new-style class method.
    """

    init_old = threading.Thread.__init__

    def init(self, *args, **kwargs):
        init_old(self, *args, **kwargs)
        run_old = self.run

        def run_with_except_hook(*args, **kw):
            try:
                run_old(*args, **kw)
            except SystemExit:
                raise
            except:
                sys.excepthook(*sys.exc_info())

        self.run = run_with_except_hook

    threading.Thread.__init__ = init


def main(args=None):
    '''Mount S3QL file system'''

    if args is None:
        args = sys.argv[1:]

    install_thread_excepthook()
    setup_warnings()
    options = parse_args(args)

    # Save handler so that we can remove it when daemonizing
    stdout_log_handler = setup_logging(options)

    if not os.path.exists(options.mountpoint):
        raise QuietError('Mountpoint does not exist.', exitcode=36)

    # Check if fs is mounted on this computer
    # This is not foolproof but should prevent common mistakes
    if is_mounted(options.storage_url):
        raise QuietError('File system already mounted elsewhere on this machine.', exitcode=40)

    if options.threads is None:
        options.threads = determine_threads(options)

    avail_fd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if avail_fd == resource.RLIM_INFINITY:
        avail_fd = 4096
    resource.setrlimit(resource.RLIMIT_NOFILE, (avail_fd, avail_fd))

    # Subtract some fd's for random things we forgot, and a fixed number for
    # each upload thread (because each thread is using at least one socket and
    # at least one temporary file)
    avail_fd -= 32 + 3 * options.threads

    if options.max_cache_entries is None:
        if avail_fd <= 64:
            raise QuietError("Not enough available file descriptors.", exitcode=37)
        log.info('Autodetected %d file descriptors available for cache entries', avail_fd)
        options.max_cache_entries = avail_fd
    else:
        if options.max_cache_entries > avail_fd:
            log.warning(
                "Up to %d cache entries requested, but detected only %d "
                "available file descriptors.",
                options.max_cache_entries,
                avail_fd,
            )
            options.max_cache_entries = avail_fd

    if options.profile:
        import cProfile
        import pstats

        prof = cProfile.Profile()
        prof.runcall(trio.run, main_async, options, stdout_log_handler)
        with open('s3ql_profile.txt', 'w') as fh:
            prof.dump_stats('s3ql_profile.dat')
            p = pstats.Stats('s3ql_profile.dat', stream=fh)
            p.strip_dirs()
            p.sort_stats('cumulative')
            p.print_stats(50)
            p.sort_stats('time')
            p.print_stats(50)
    else:
        # trio.run(main_async, options, stdout_log_handler,
        #         instruments=[Tracer()])
        trio.run(main_async, options, stdout_log_handler)


class Tracer(trio.abc.Instrument):
    def _print_with_task(self, msg, task):
        print("{}: {}".format(msg, task.name))

    def task_spawned(self, task):
        self._print_with_task("### new task spawned", task)

    def task_scheduled(self, task):
        self._print_with_task("### task scheduled", task)

    def before_task_step(self, task):
        self._print_with_task(">>> about to run one step of task", task)

    def after_task_step(self, task):
        self._print_with_task("<<< task step finished", task)

    def task_exited(self, task):
        self._print_with_task("### task exited", task)

    def before_io_wait(self, timeout):
        if timeout:
            print("### waiting for I/O for up to {} seconds".format(timeout))
        else:
            print("### doing a quick check for I/O")
        self._sleep_time = trio.current_time()

    def after_io_wait(self, timeout):
        duration = trio.current_time() - self._sleep_time
        print("### finished I/O check (took {} seconds)".format(duration))


async def main_async(options, stdout_log_handler):
    # Get paths
    cachepath = options.cachepath

    backend_factory = get_backend_factory(options)
    backend_pool = BackendPool(backend_factory)
    atexit.register(backend_pool.flush)

    with backend_pool() as backend:
        (param, db) = get_metadata(backend, cachepath)

    # Handle --cachesize
    rec_cachesize = options.max_cache_entries * param.data_block_size / 2
    avail_cache = shutil.disk_usage(os.path.dirname(cachepath))[2] / 1024
    if options.cachesize is None:
        options.cachesize = min(rec_cachesize, 0.8 * avail_cache)
        log.info('Setting cache size to %d MB', options.cachesize / 1024)
    elif options.cachesize > avail_cache:
        log.warning(
            'Requested cache size %d MB, but only %d MB available',
            options.cachesize / 1024,
            avail_cache / 1024,
        )

    if options.nfs:
        # NFS may try to look up '..', so we have to speed up this kind of query
        log.info('Creating NFS indices...')
        db.execute('CREATE INDEX IF NOT EXISTS ix_contents_inode ON contents(inode)')

    else:
        db.execute('DROP INDEX IF EXISTS ix_contents_inode')

    metadata_upload_task = MetadataUploadTask(param, backend_pool, db, options)

    async with trio.open_nursery() as nursery:
        async with AsyncExitStack() as cm:
            block_cache = BlockCache(
                backend_pool,
                db,
                cachepath + '-cache',
                options.cachesize * 1024,
                options.max_cache_entries,
            )
            cm.push_async_callback(block_cache.destroy, options.keep_cache)

            operations = fs.Operations(
                block_cache,
                db,
                max_obj_size=param.data_block_size,
                inode_cache=InodeCache(db, param.inode_gen),
                upload_task=metadata_upload_task,
            )
            cm.push_async_callback(operations.destroy)
            block_cache.fs = operations
            metadata_upload_task.fs = operations

            log.info('Mounting %s at %s...', options.storage_url, options.mountpoint)
            try:
                pyfuse3.init(operations, options.mountpoint, get_fuse_opts(options))
            except RuntimeError as exc:
                raise QuietError(str(exc), exitcode=39)

            unmount_clean = False

            def unmount():
                log.info("Unmounting file system...")
                pyfuse3.close(unmount=unmount_clean)

            cm.callback(unmount)

            if options.fg or options.systemd:
                faulthandler.enable()
                faulthandler.register(signal.SIGUSR1)
            else:
                if stdout_log_handler:
                    logging.getLogger().removeHandler(stdout_log_handler)
                crit_log_fd = os.open(
                    os.path.join(options.cachedir, 'mount.s3ql_crit.log'),
                    flags=os.O_APPEND | os.O_CREAT | os.O_WRONLY,
                    mode=0o644,
                )
                faulthandler.enable(crit_log_fd)
                faulthandler.register(signal.SIGUSR1, file=crit_log_fd)
                daemonize(options.cachedir)

            param.seq_no += 1
            param.is_mounted = True
            write_params(cachepath, param)
            upload_params(backend, param)

            block_cache.init(options.threads)

            nursery.start_soon(metadata_upload_task.run, name='metadata-upload-task')
            cm.callback(metadata_upload_task.stop)

            commit_task = CommitTask(block_cache, options.dirty_block_upload_delay)
            nursery.start_soon(commit_task.run, name='commit-task')
            cm.callback(commit_task.stop)

            exc_info = setup_exchook()

            received_signals = []
            old_handler = None

            def signal_handler(signum, frame, _main_thread=_thread.get_ident()):
                log.debug('Signal %d received', signum)
                if pyfuse3.trio_token is None:
                    log.debug('FUSE loop not running, calling parent handler...')
                    return old_handler(signum, frame)
                log.debug('Calling pyfuse3.terminate()...')
                if _thread.get_ident() == _main_thread:
                    pyfuse3.terminate()
                else:
                    trio.from_thread.run_sync(pyfuse3.terminate, trio_token=pyfuse3.trio_token)
                received_signals.append(signum)

            signal.signal(signal.SIGTERM, signal_handler)
            old_handler = signal.signal(signal.SIGINT, signal_handler)

            if options.systemd:
                import systemd.daemon

                systemd.daemon.notify('READY=1')

            await pyfuse3.main()

            if received_signals == [signal.SIGINT]:
                log.info('SIGINT received, exiting normally.')
            elif received_signals:
                log.info('Received termination signal, exiting without data upload.')
                raise RuntimeError('Received signal(s) %s, terminating' % (received_signals,))

            # Re-raise if main loop terminated due to exception in other thread
            if exc_info:
                (exc_inst, exc_tb) = exc_info
                raise exc_inst.with_traceback(exc_tb)

            log.info("FUSE main loop terminated.")

            unmount_clean = True

    # At this point, there should be no other threads left
    param.is_mounted = False
    db.close()

    # Do not update .params yet, dump_metadata() may fail if the database is
    # corrupted, in which case we want to force an fsck.
    if operations.failsafe:
        log.warning('File system errors encountered, marking for fsck.')
        param.needs_fsck = True

    with backend_pool() as backend:
        param.last_modified = time.time()
        upload_metadata(backend, db, param)
        write_params(cachepath, param)
        upload_params(backend, param)
        expire_objects(backend)

    log.info('All done.')


def get_system_memory():
    '''Attempt to determine total system memory

    If amount cannot be determined, emits warning and
    returns -1.
    '''

    # MacOS X doesn't support sysconf('SC_PHYS_PAGES')
    if platform.system() == 'Darwin':
        try:
            out = subprocess.check_output(['sysctl', 'hw.memsize'], universal_newlines=True)
        except subprocess.CalledProcessError as exc:
            log.warning('Cannot determine system memory, sysctl failed with %s', exc.output)
            return -1

        # output of sysctl is 'hw.memsize: #'. Strip the prefix.
        hit = re.match(r'^hw.memsize: ([0-9]+)$', out)
        if not hit:
            log.warning('Cannot determine system memory, unable to parse sysctl output.')
            return -1

        return int(hit.group(1))

    else:
        try:
            return os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGESIZE')
        except ValueError:
            log.warning('Unable to determine number of CPU cores (sysconf failed).')
            return -1


# Memory required for LZMA compression in MB (from xz(1))
LZMA_MEMORY = {0: 3, 1: 9, 2: 17, 3: 32, 4: 48, 5: 94, 6: 94, 7: 186, 8: 370, 9: 674}


def determine_threads(options):
    '''Return optimum number of upload threads'''

    try:
        cores = os.sysconf('SC_NPROCESSORS_ONLN')
    except ValueError:
        log.warning('Unable to determine number of CPU cores (sysconf failed).')
        cores = -1

    memory = get_system_memory()

    if options.compress[0] == 'lzma':
        # Keep this in sync with compression level in backends/common.py
        # Memory usage according to man xz(1)
        mem_per_thread = LZMA_MEMORY[options.compress[1]] * 1024**2
    else:
        # Only check LZMA memory usage
        mem_per_thread = 0

    if cores == -1:
        log.warning("Can't determine number of cores, using 2 upload threads.")
        return 2
    elif memory == -1 and mem_per_thread != 0:
        log.warning("Can't determine available memory, using 2 upload threads.")
        return 2
    elif 2 * cores * mem_per_thread > (memory / 2):
        threads = min(int((memory / 2) // mem_per_thread), 10)
        if threads > 0:
            log.info('Using %d upload threads (memory limited).', threads)
        else:
            log.warning(
                'Compression will require %d MiB memory (%d%% of total system memory',
                mem_per_thread / 1024**2,
                mem_per_thread * 100 / memory,
            )
            threads = 1
        return threads
    else:
        threads = min(2 * cores, 10)
        log.info("Using %d upload threads.", threads)
        return threads


def get_metadata(backend, cachepath) -> Tuple[FsAttributes, Connection]:
    '''Retrieve metadata'''

    db = None
    param = read_remote_params(backend)
    local_param = read_cached_params(cachepath, min_seq=param.seq_no)
    if local_param is not None:
        if local_param.seq_no > param.seq_no:
            raise QuietError("File system not unmounted cleanly, run fsck!", exitcode=30)
        log.info('Using cached metadata.')
        assert local_param == param
        db = Connection(cachepath + '.db', param.metadata_block_size)

    if param.is_mounted:
        raise QuietError(
            'Backend reports that fs is still mounted elsewhere, aborting.', exitcode=31
        )

    if param.needs_fsck:
        raise QuietError("File system damaged or not unmounted cleanly, run fsck!", exitcode=30)
    if time.time() - param.last_fsck > 60 * 60 * 24 * 31:
        log.warning(
            'Last file system check was more than 1 month ago, running fsck.s3ql is recommended.'
        )

    # Download metadata
    if not db:
        log.info('Downloading metadata...')
        db = download_metadata(backend, cachepath + '.db', param)

        # Drop cache
        if os.path.exists(cachepath + '-cache'):
            shutil.rmtree(cachepath + '-cache')

    return (param, db)


def get_fuse_opts(options):
    '''Return fuse options for given command line options'''

    fsname = options.fs_name
    if not fsname:
        fsname = options.storage_url
    fuse_opts = ['fsname=%s' % fsname, 'subtype=s3ql']

    if options.allow_other:
        fuse_opts.append('allow_other')
    if options.allow_root:
        fuse_opts.append('allow_root')
    if options.allow_other or options.allow_root:
        fuse_opts.append('default_permissions')

    return fuse_opts


def parse_args(args):
    '''Parse command line'''

    # Parse fstab-style -o options
    if '--' in args:
        max_idx = args.index('--')
    else:
        max_idx = len(args)
    if '-o' in args[:max_idx]:
        pos = args.index('-o')
        val = args[pos + 1]
        del args[pos]
        del args[pos]
        for opt in reversed(val.split(',')):
            if '=' in opt:
                (key, val) = opt.split('=')
                args.insert(pos, val)
                args.insert(pos, '--' + key)
            else:
                if opt in (
                    'rw',
                    'defaults',
                    'auto',
                    'noauto',
                    'user',
                    'nouser',
                    'dev',
                    'nodev',
                    'suid',
                    'nosuid',
                    'atime',
                    'diratime',
                    'exec',
                    'noexec',
                    'group',
                    'mand',
                    'nomand',
                    '_netdev',
                    'nofail',
                    'norelatime',
                    'strictatime',
                    'owner',
                    'users',
                    'nobootwait',
                ):
                    continue
                elif opt == 'ro':
                    raise QuietError('Read-only mounting not supported.', exitcode=35)
                args.insert(pos, '--' + opt)

    parser = ArgumentParser(description="Mount an S3QL file system.")

    parser.add_log('~/.s3ql/mount.log')
    parser.add_cachedir()
    parser.add_debug()
    parser.add_quiet()
    parser.add_backend_options()
    parser.add_version()
    parser.add_storage_url()
    parser.add_compress()

    parser.add_argument(
        "mountpoint",
        metavar='<mountpoint>',
        type=os.path.abspath,
        help='Where to mount the file system',
    )
    parser.add_argument(
        "--cachesize",
        type=int,
        default=None,
        metavar='<size>',
        help="Cache size in KiB (default: autodetect).",
    )
    parser.add_argument(
        "--max-cache-entries",
        type=int,
        default=None,
        metavar='<num>',
        help="Maximum number of entries in cache (default: autodetect). "
        'Each cache entry requires one file descriptor, so if you increase '
        'this number you have to make sure that your process file descriptor '
        'limit (as set with `ulimit -n`) is high enough (at least the number '
        'of cache entries + 100).',
    )
    parser.add_argument(
        "--keep-cache",
        action="store_true",
        default=False,
        help="Do not purge locally cached files on exit.",
    )
    parser.add_argument(
        "--allow-other",
        action="store_true",
        default=False,
        help='Normally, only the user who called `mount.s3ql` can access the mount '
        'point. This user then also has full access to it, independent of '
        'individual file permissions. If the `--allow-other` option is '
        'specified, other users can access the mount point as well and '
        'individual file permissions are taken into account for all users.',
    )
    parser.add_argument(
        "--allow-root",
        action="store_true",
        default=False,
        help='Like `--allow-other`, but restrict access to the mounting user and the root user.',
    )
    parser.add_argument(
        "--dirty-block-upload-delay",
        action="store",
        type=int,
        default=10,
        metavar='<seconds>',
        help="Upload delay for dirty blocks in seconds (default: 10 seconds).",
    )
    parser.add_argument(
        "--fg", action="store_true", default=False, help="Do not daemonize, stay in foreground"
    )
    parser.add_argument(
        "--fs-name",
        default=None,
        help="Mount name passed to fuse, the name will be shown in the first "
        "column of the system mount command output. If not specified your "
        "storage url is used.",
    )
    parser.add_argument(
        "--systemd",
        action="store_true",
        default=False,
        help="Run as systemd unit. Consider specifying --log none as well "
        "to make use of journald.",
    )
    parser.add_argument(
        "--metadata-backup-interval",
        action="store",
        type=int,
        default=6 * 60 * 60,
        metavar='<seconds>',
        help='Interval between metadata backups. Should the filesystem crash while mounted, '
        'modifications made after the most recent metadata backup may be lost. During '
        'backups, the filesystem will be unresponsile. Default: 6h',
    )
    parser.add_argument(
        "--threads",
        action="store",
        type=int,
        default=None,
        metavar='<no>',
        help='Number of parallel upload threads to use (default: auto).',
    )
    parser.add_argument(
        "--nfs",
        action="store_true",
        default=False,
        help='Enable some optimizations for exporting the file system '
        'over NFS. (default: %(default)s)',
    )
    parser.add_argument("--profile", action="store_true", default=False, help=argparse.SUPPRESS)

    options = parser.parse_args(args)

    if options.allow_other and options.allow_root:
        parser.error("--allow-other and --allow-root are mutually exclusive.")

    if not options.log and not (options.fg or options.systemd):
        parser.error("Please activate logging to a file or syslog, or use the --fg option.")

    if options.metadata_backup_interval == 0:
        options.metadata_backup_interval = None

    return options


class MetadataUploadTask:
    '''
    Periodically upload metadata. Upload is done every `interval`
    seconds, and whenever `event` is set. To terminate thread,
    set `quit` attribute as well as `event` event.
    '''

    def __init__(self, params: FsAttributes, backend_pool, db, options):
        super().__init__()
        self.backend_pool = backend_pool
        self.db = db
        self.event = trio.Event()
        self.quit = False
        self.params = params
        self.options = options

    async def run(self):
        log.debug('started')

        while not self.quit:
            with trio.move_on_after(self.options.metadata_backup_interval):
                await self.event.wait()

            if self.quit:
                break

            self.event = trio.Event()  # reset

            with self.backend_pool() as backend:
                # Upload asynchronously twice to reduce the amount of data left for
                # synchronous upload.
                self.db.checkpoint()
                for _ in range(2):
                    await trio.to_thread.run_sync(
                        functools.partial(
                            upload_metadata,
                            backend,
                            self.db,
                            self.params,
                            update_params=False,
                            incremental=True,
                        )
                    )

                # Now upload synchronously to get consistent snapshot (at the cost of stopping file
                # system operation). As a future optimization, we could first copy all modified
                # blocks locally, and then upload async...
                self.db.checkpoint()
                self.params.last_modified = time.time()
                upload_metadata(
                    backend,
                    self.db,
                    self.params,
                    update_params=True,
                    incremental=True,
                )
                write_params(self.options.cachepath, self.params)
                upload_params(backend, self.params)

                # Write a new params file immediately, so that we're in the same state as right
                # after mounting and there is no window where we could have metadata_* objects with
                # a sequence number for which there is no corresponding s3ql_params_* object.
                self.params.seq_no += 1
                write_params(self.options.cachepath, self.params)
                upload_params(backend, self.params)

                await trio.to_thread.run_sync(expire_objects, backend)

        log.debug('finished')

    def stop(self):
        '''Signal thread to terminate'''

        log.debug('started')
        self.quit = True
        self.event.set()


def setup_exchook():
    '''Terminate FUSE main loop if any thread terminates with an exception

    The exc_info will be saved in the list object returned by this function.
    '''

    main_thread = _thread.get_ident()
    old_exchook = sys.excepthook
    exc_info = []

    def exchook(exc_type, exc_inst, tb):
        reporting_thread = _thread.get_ident()
        if reporting_thread != main_thread:
            if exc_info:
                log.warning("Unhandled top-level exception during shutdown (will not be re-raised)")
            else:
                log.error("Unhandled exception in thread, terminating", exc_info=True)
                exc_info.append(exc_inst)
                exc_info.append(tb)
                trio.from_thread.run_sync(pyfuse3.terminate, trio_token=pyfuse3.trio_token)

            old_exchook(exc_type, exc_inst, tb)

        # If the main thread re-raised exception, there is no need to call
        # excepthook again
        elif exc_info and exc_info[0] is exc_inst:
            log.debug('Suppressing exception hook for re-raised %s', exc_inst)
        else:
            old_exchook(exc_type, exc_inst, tb)

    sys.excepthook = exchook

    return exc_info


class CommitTask:
    '''
    Periodically upload dirty blocks.
    '''

    def __init__(self, block_cache, dirty_block_upload_delay):
        super().__init__()
        self.block_cache = block_cache
        self.stop_event = trio.Event()
        self.dirty_block_upload_delay = dirty_block_upload_delay

    async def run(self):
        log.debug('started')

        while not self.stop_event.is_set():
            did_sth = False

            stamp = time.time()
            # Need to make copy, since we aren't allowed to change
            # dict while iterating through it. The performance hit doesn't seem
            # to be that bad:
            # >>> from timeit import timeit
            # >>> timeit("k=0\nfor el in list(d.values()):\n k += el",
            # ... setup='\nfrom collections import OrderedDict\nd = OrderedDict()\nfor i in range(5000):\n d[i]=i\n',
            # ... number=500)/500 * 1e3
            # 1.3769531380003173
            # >>> timeit("k=0\nfor el in d.values(n:\n k += el",
            # ... setup='\nfrom collections import OrderedDict\nd = OrderedDict()\nfor i in range(5000):\n d[i]=i\n',
            # ... number=500)/500 * 1e3
            # 1.456586996000624
            for el in list(self.block_cache.cache.values()):
                if (
                    self.stop_event.is_set()
                    or stamp - el.last_write < self.dirty_block_upload_delay
                ):
                    break
                if el.dirty and el not in self.block_cache.in_transit:
                    await self.block_cache.upload_if_dirty(el)
                    did_sth = True

            if not did_sth:
                with trio.move_on_after(5):
                    await self.stop_event.wait()

        log.debug('finished')

    def stop(self):
        '''Signal thread to terminate'''

        log.debug('started')
        self.stop_event.set()


if __name__ == '__main__':
    main(sys.argv[1:])
