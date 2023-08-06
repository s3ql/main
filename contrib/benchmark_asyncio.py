#!/usr/bin/env python3
'''
benchmark_asyncio.py - this file is part of S3QL.

Compare performance of multi-threaded, synchronous backends with async-I/O, pipelined backend. This
script is for temporary use only and will be removed again once we have made the decision which kind
of backend to use in the future.

Copyright © 2023 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import argparse
import os
import sys
import threading
import time
from io import BytesIO
from queue import Queue

import trio

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if os.path.exists(os.path.join(basedir, 'setup.py')) and os.path.exists(
    os.path.join(basedir, 'src', 's3ql', '__init__.py')
):
    sys.path = [os.path.join(basedir, 'src')] + sys.path
    exec_prefix = os.path.join(basedir, 'bin', '')
else:
    exec_prefix = ''

import logging

from s3ql.common import get_backend, get_backend_factory
from s3ql.logging import setup_logging, setup_warnings
from s3ql.parse_args import ArgumentParser

log = logging.getLogger(__name__)


def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_quiet()
    parser.add_log()
    parser.add_debug()
    parser.add_backend_options()
    parser.add_version()
    parser.add_storage_url()
    parser.add_argument(
        'file', metavar='<file>', type=argparse.FileType(mode='rb'), help='File to transfer'
    )
    parser.add_argument(
        '--threads',
        metavar='<n>',
        type=int,
        default=8,
    )
    parser.add_argument(
        '--copies',
        metavar='<n>',
        type=int,
        default=64,
        help='How many copies of the file to upload.',
    )

    options = parser.parse_args(args)

    options.testdata = options.file.read()
    options.file.close()
    del options.file

    return options


def main(args=None):
    setup_warnings()
    options = parse_args(args)
    setup_logging(options)

    log.info('Test data size: %.2f KB', len(options.testdata) / 1024)

    log.info('Measuring raw backend throughput..')

    def backend_factory():
        return get_backend(options, raw=True)

    upload_threaded(options, backend_factory)
    trio.run(upload_async, options, backend_factory)

    log.info('Measuring compressed backend throughput..')
    backend_factory = get_backend_factory(options)
    upload_threaded(options, backend_factory)
    trio.run(upload_async, options, backend_factory)


async def upload_async(options, backend_factory):
    backend = backend_factory().get_async_backend()
    event = trio.Event()
    size = len(options.testdata)

    async with trio.open_nursery() as nursery:
        await backend.init(nursery)
        async with backend:
            t1 = time.time()
            for i in range(options.copies):
                if i == options.copies - 1:
                    callback = lambda size: event.set()
                else:
                    callback = None
                await backend.write_fh_async(
                    key=f'test_{i}',
                    fh=BytesIO(options.testdata),
                    callback=callback,
                    metadata={'foo': 42},
                )
            await event.wait()
            dt = time.time() - t1
        log.info(
            'Async upload speed: %d KiB/sec (%d objects)',
            options.copies * size / dt / 1024,
            options.copies,
        )


def upload_loop(queue: Queue, backend):
    while True:
        fh = queue.get()
        if fh is None:
            return

        backend.write_fh(key=f'test_{id(fh)}', fh=fh)
        fh.close()


def upload_threaded(options, backend_factory):
    size = len(options.testdata)
    threads = []
    q = Queue()
    for _ in range(options.threads):
        t = threading.Thread(target=upload_loop, args=(q, backend_factory()))
        t.start()
        threads.append(t)

    t1 = time.time()
    for _ in range(options.copies):
        q.put(BytesIO(options.testdata))

    for _ in threads:
        q.put(None)

    for t in threads:
        t.join()

    dt = time.time() - t1
    log.info(
        'Threaded upload speed: %d KiB/sec (%d threads, %d objects)',
        options.copies * size / dt / 1024,
        options.threads,
        options.copies,
    )


if __name__ == '__main__':
    main(sys.argv[1:])
