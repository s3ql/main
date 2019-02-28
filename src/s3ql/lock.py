'''
lock.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging, QuietError
from .common import assert_fs_owner
from .parse_args import ArgumentParser
import llfuse
import os
import sys
import textwrap

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent('''\
        Makes the given directory tree(s) immutable. No changes of any sort can
        be performed on the tree after that. Immutable entries can only be
        deleted with s3qlrm.
        '''))

    parser.add_log()
    parser.add_debug()
    parser.add_quiet()
    parser.add_version()

    parser.add_argument('path', metavar='<path>', nargs='+',
                        help='Directories to make immutable.',
                         type=(lambda x: x.rstrip('/')))

    return parser.parse_args(args)


def main(args=None):
    '''Make directory tree immutable'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    for name in options.path:
        if os.path.ismount(name):
            raise QuietError('%s is a mount point.' % name)
        ctrlfile = assert_fs_owner(name)
        fstat = os.stat(name)
        llfuse.setxattr(ctrlfile, 'lock', ('%d' % fstat.st_ino).encode())

if __name__ == '__main__':
    main(sys.argv[1:])
