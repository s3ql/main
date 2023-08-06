'''
adm.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import logging
import os
import re
import shutil
import sys
import tempfile
import textwrap
import time
from base64 import b64decode
from datetime import datetime
from getpass import getpass
from queue import Full as QueueFull
from queue import Queue

from s3ql.database import (
    Connection,
    FsAttributes,
    download_metadata,
    get_available_seq_nos,
    read_remote_params,
    upload_metadata,
    upload_params,
    write_params,
)

from . import CURRENT_FS_REV, REV_VER_MAP
from .backends.comprenc import ComprencBackend
from .common import AsyncFn, get_backend, handle_on_return, is_mounted, thaw_basic_mapping
from .logging import QuietError, setup_logging, setup_warnings
from .parse_args import ArgumentParser

log = logging.getLogger(__name__)


def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description="Manage S3QL File Systems.",
        epilog=textwrap.dedent(
            '''\
               Hint: run `%(prog)s <action> --help` to get help on the additional
               arguments that the different actions take.'''
        ),
    )

    pparser = ArgumentParser(
        add_help=False,
        epilog=textwrap.dedent(
            '''\
               Hint: run `%(prog)s --help` to get help on other available actions and
               optional arguments that can be used with all actions.'''
        ),
    )

    subparsers = parser.add_subparsers(metavar='<action>', dest='action', help='may be either of')
    subparsers.add_parser("passphrase", help="change file system passphrase", parents=[pparser])
    subparsers.add_parser(
        "restore-metadata",
        help="Interactively restore metadata backups.",
        parents=[pparser],
    )

    sparser = subparsers.add_parser(
        "clear", help="delete file system and all data", parents=[pparser]
    )
    sparser.add_argument("--threads", type=int, default=20, help='Number of threads to use')
    subparsers.add_parser(
        "recover-key", help="Recover master key from offline copy.", parents=[pparser]
    )
    sparser = subparsers.add_parser(
        "upgrade", help="upgrade file system to newest revision", parents=[pparser]
    )
    sparser.add_argument(
        "--metadata-block-size",
        type=int,
        default=64,
        metavar='<size>',
        help="Block size (in KiB) to use for storing filesystem metadata. "
        "Backend object size may be smaller than this due to compression. "
        "Default: %(default)d KiB.",
    )

    parser.add_storage_url()
    parser.add_debug()
    parser.add_quiet()
    parser.add_log()
    parser.add_backend_options()
    parser.add_cachedir()
    parser.add_version()

    options = parser.parse_args(args)

    return options


def main(args=None):
    '''Change or show S3QL file system parameters'''

    if args is None:
        args = sys.argv[1:]

    setup_warnings()
    options = parse_args(args)
    setup_logging(options)

    # Check if fs is mounted on this computer
    # This is not foolproof but should prevent common mistakes
    if is_mounted(options.storage_url):
        raise QuietError('Can not work on mounted file system.')

    if options.action == 'clear':
        return clear(options)

    elif options.action == 'recover-key':
        with get_backend(options, raw=True) as backend:
            return recover(backend, options)

    with get_backend(options) as backend:
        if options.action == 'passphrase':
            return change_passphrase(backend)

        elif options.action == 'restore-metadata':
            return restore_metadata_cmd(backend, options)

        elif options.action == 'upgrade':
            return upgrade(backend, options)


def change_passphrase(backend):
    '''Change file system passphrase'''

    if not isinstance(backend, ComprencBackend) and backend.passphrase:
        raise QuietError('File system is not encrypted.')

    data_pw = backend.passphrase

    print(
        textwrap.dedent(
            '''\
       NOTE: If your password has been compromised already, then changing
       it WILL NOT PROTECT YOUR DATA, because an attacker may have already
       retrieved the master key.
       '''
        )
    )
    if sys.stdin.isatty():
        wrap_pw = getpass("Enter new encryption password: ")
        if not wrap_pw == getpass("Confirm new encryption password: "):
            raise QuietError("Passwords don't match")
    else:
        wrap_pw = sys.stdin.readline().rstrip()
    wrap_pw = wrap_pw.encode('utf-8')

    backend.passphrase = wrap_pw
    backend['s3ql_passphrase'] = data_pw
    backend['s3ql_passphrase_bak1'] = data_pw
    backend['s3ql_passphrase_bak2'] = data_pw
    backend['s3ql_passphrase_bak3'] = data_pw
    backend.passphrase = data_pw


def recover(backend, options):
    print("Enter master key (should be 11 blocks of 4 characters each): ")
    data_pw = sys.stdin.readline()
    data_pw = re.sub(r'\s+', '', data_pw)
    try:
        data_pw = b64decode(data_pw)
    except ValueError:
        raise QuietError("Malformed master key. Expected valid base64.")

    if len(data_pw) != 32:
        raise QuietError("Malformed master key. Expected length 32, got %d." % len(data_pw))

    if sys.stdin.isatty():
        wrap_pw = getpass("Enter new encryption password: ")
        if not wrap_pw == getpass("Confirm new encryption password: "):
            raise QuietError("Passwords don't match")
    else:
        wrap_pw = sys.stdin.readline().rstrip()
    wrap_pw = wrap_pw.encode('utf-8')

    backend = ComprencBackend(wrap_pw, ('lzma', 2), backend)
    backend['s3ql_passphrase'] = data_pw
    backend['s3ql_passphrase_bak1'] = data_pw
    backend['s3ql_passphrase_bak2'] = data_pw
    backend['s3ql_passphrase_bak3'] = data_pw


@handle_on_return
def clear(options, on_return):
    backend_factory = lambda: options.backend_class(options)
    backend = on_return.enter_context(backend_factory())

    print(
        'I am about to DELETE ALL DATA in %s.' % backend,
        'This includes not just S3QL file systems but *all* stored objects.',
        'Depending on the storage service, it may be necessary to run this command',
        'several times to delete all data, and it may take a while until the ',
        'removal becomes effective.',
        'Please enter "yes" to continue.',
        '> ',
        sep='\n',
        end='',
    )
    sys.stdout.flush()

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError()

    log.info('Deleting...')
    for suffix in ('.db', '.params'):
        name = options.cachepath + suffix
        if os.path.exists(name):
            os.unlink(name)

    name = options.cachepath + '-cache'
    if os.path.exists(name):
        shutil.rmtree(name)

    queue = Queue(maxsize=options.threads)

    def removal_loop():
        with backend_factory() as backend:
            while True:
                key = queue.get()
                if key is None:
                    return
                backend.delete(key)

    threads = []
    for _ in range(options.threads):
        t = AsyncFn(removal_loop)
        # Don't wait for worker threads, gives deadlock if main thread
        # terminates with exception
        t.daemon = True
        t.start()
        threads.append(t)

    for i, obj_id in enumerate(backend.list()):
        log.info(
            'Deleting objects (%d so far)...', i, extra={'rate_limit': 1, 'update_console': True}
        )

        # Avoid blocking if all threads terminated
        while True:
            try:
                queue.put(obj_id, timeout=1)
            except QueueFull:
                pass
            else:
                break
            for t in threads:
                if not t.is_alive():
                    t.join_and_raise()

    queue.maxsize += len(threads)
    for t in threads:
        queue.put(None)

    for t in threads:
        t.join_and_raise()

    log.info('All visible objects deleted.')


def get_old_rev_msg(rev, prog):
    return textwrap.dedent(
        '''\
        The last S3QL version that supported this file system revision
        was %(version)s. To run this version's %(prog)s, proceed along
        the following steps:

          $ # retrieve and unpack required release
          $ (cd s3ql-%(version)s; ./setup.py build_ext --inplace)
          $ s3ql-%(version)s/bin/%(prog)s <options>
        '''
        % {'version': REV_VER_MAP[rev], 'prog': prog}
    )


def upgrade(backend, options):
    '''Upgrade file system to newest revision'''

    params_path = options.cachepath + '.params'
    if not os.path.exists(options.cachepath + '.db') or not os.path.exists(params_path):
        print(
            'To upgrade the filesystem, first download file system metadata using the previous',
            'version of S3QL (eg. by running fsck.s3ql)',
            sep='\n',
        )
        sys.exit(1)

    with open(params_path, 'rb') as fh:
        local_params = thaw_basic_mapping(fh.read())
    remote_params = backend.lookup('s3ql_metadata')

    if local_params['seq_no'] < remote_params['seq_no']:
        print(
            'Local metadata copy is not up-to-date. To upgrade the filesystem, first download',
            'file system metadata using the previous  version of S3QL (eg. by running fsck.s3ql)',
            sep='\n',
        )
        sys.exit(1)

    elif local_params['seq_no'] > remote_params['seq_no'] or local_params['needs_fsck']:
        print(
            'Filesystem was not cleanly unmounted. Check file system using fsck.s3ql',
            'from the previous version of S3QL.',
            get_old_rev_msg(local_params['revision'], 'fsck.s3ql'),
            sep='\n',
        )
        sys.exit(30)

    if local_params['revision'] < CURRENT_FS_REV - 1:
        print(
            textwrap.dedent(
                '''\
            File system revision too old to upgrade!

            You need to use an older S3QL version to upgrade to a more recent revision before you
            can use this version to upgrade to the newest revision.
            '''
            )
        )
        print(get_old_rev_msg(local_params['revision'] + 1, 's3qladm'))
        raise QuietError()

    elif local_params['revision'] >= CURRENT_FS_REV:
        print('File system already at most-recent revision')
        return

    print(
        textwrap.dedent(
            f'''
        I am about to update the file system to the newest revision. You will not be able to access
        the file system with any older version of S3QL after this operation.

        You should make very sure that this command is not interrupted and that no one else tries to
        mount, fsck or upgrade the file system at the same time. You may interrupt the update with
        Ctrl+C and resume at a later time, but the filesystem will not be in a usable state until
        the upgrade is complete.

        Filesystem metadata will be split into {options.metadata_block_size} kB blocks for remote
        storage. This value CAN NOT BE CHANGED after upgrade. To use a different value, restart the
        update with a different --metadata-block-size parameter.
        '''
        )
    )

    print('Please enter "yes" to continue.', '> ', sep='\n', end='')
    sys.stdout.flush()

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError()

    log.info('Upgrading from revision %d to %d...', local_params['revision'], CURRENT_FS_REV)

    new_params = {k.replace('-', '_'): v for k, v in local_params.items()}
    new_params['data_block_size'] = new_params['max_obj_size']
    new_params['metadata_block_size'] = options.metadata_block_size * 1024
    new_params['revision'] = CURRENT_FS_REV
    # remove all deprecated attributes
    valid_keys = FsAttributes.__init__.__code__.co_varnames
    for k in list(new_params.keys()):
        if k not in valid_keys:
            del new_params[k]
    params = FsAttributes(**new_params)

    db = Connection(options.cachepath + '.db', options.metadata_block_size * 1024)

    params.last_modified = time.time()
    params.seq_no += 1

    upload_metadata(backend, db, params, incremental=False)
    write_params(options.cachepath, params)
    upload_params(backend, params)

    # Re-upload the old metadata object to make sure that we don't accidentally
    # re-mount the filesystem with an older S3QL version.
    log.info('Backing up old metadata...')
    local_params['revision'] = CURRENT_FS_REV
    local_params['seq_no'] += 1
    with tempfile.TemporaryFile() as tmpfh:
        backend.readinto_fh('s3ql_metadata', tmpfh)
        tmpfh.seek(0)
        backend.write_fh("s3ql_metadata", tmpfh, metadata=local_params, dont_compress=True)

    print('File system upgrade complete.')


def restore_metadata_cmd(backend, options):
    backups = sorted(get_available_seq_nos(backend))

    if not backups:
        raise QuietError('No metadata backups found.')

    print('The following backups are available:')
    print('Idx    Seq No: Last Modified:')
    for i, seq_no in enumerate(backups):
        # De-serialize directly instead of using read_remote_params() to avoid exceptions on old
        # filesystem revisions.
        d = thaw_basic_mapping(backend['s3ql_params_%010x' % seq_no])
        if d['revision'] != CURRENT_FS_REV:
            print(f'{i:3d} {seq_no:010x} (unsupported revision)')
            continue
        params = FsAttributes(**d)
        assert params.seq_no == seq_no
        date = datetime.fromtimestamp(params.last_modified).strftime('%Y-%m-%d %H:%M:%S')
        print(f'{i:3d} {seq_no:010x} {date}')

    print(
        'Restoring a metadata backup will almost always result in partial data loss and',
        'should only be done if the filesystem metadata has been corruped beyond repair.',
        sep='\n',
    )

    seq_no = None
    while seq_no is None:
        buf = input('Enter index to revert to: ')
        try:
            seq_no = backups[int(buf.strip())]
        except:
            print('Invalid selection.')

    params = read_remote_params(backend, seq_no=seq_no)
    log.info('Downloading metadata...')
    conn = download_metadata(backend, options.cachepath + ".db", params)
    conn.close()

    params.is_mounted = False
    params.needs_fsck = True
    new_seq = max(int(time.time()), backups[-1] + 1)
    params.seq_no = new_seq

    write_params(options.cachepath, params)

    print(
        'Backup restored into local metadata. Run fsck.s3ql to commit changes to',
        'backend and ensure file system consistency',
        sep='\n',
    )


if __name__ == '__main__':
    main(sys.argv[1:])
