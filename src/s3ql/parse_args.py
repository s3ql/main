'''
argparse.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.

This module provides a customized ArgumentParser class. Differences
are:

  * a --version argument is added by default

  * convenience functions are available for adding --quiet,
    --debug, --cachedir, --log and --authfile options.

  * instead of the usage string one can pass a usage list. The first
    element will be prefixed with ``usage: `` as usual. Additional
    elements will be printed on separate lines and prefixed with
    ``  or:  ``.

  * When element of an usage list, the ``DEFAULT_USAGE`` object
    will be replaced by the automatically generated usage message,
    excluding any --help arguments.

  * When specified on its own, the replacement will be done including
    any --help arguments.

  * The ``usage`` and ``add_help`` settings are inherited from the
    parent parser to the subparsers.

'''

# Pylint really gets confused by this module
# pylint: disable-all


import argparse
import configparser
import logging
import os
import re
import stat
import sys
from argparse import ArgumentError, ArgumentTypeError
from getpass import getpass

from . import RELEASE
from .backends import prefix_map
from .common import escape

DEFAULT_USAGE = object()
log = logging.getLogger(__name__)


class HelpFormatter(argparse.HelpFormatter):
    def _format_usage(self, usage, actions, groups, prefix):
        '''Special handling for usage lists

        If usage is a list object, its elements will be printed on
        separate lines. DEFAULT_USAGE will be replaced by the
        default usage string of the parser (but, if `usage`` is a list,
        excluding any --help arguments)).
        '''

        if isinstance(usage, list):
            # Omit help argument
            actions = [x for x in actions if not isinstance(x, argparse._HelpAction)]
            res = []
            for s in usage:
                if not res:
                    res.append('usage: ')
                else:
                    res.append('  or:  ')
                if s is DEFAULT_USAGE:
                    res.append(super()._format_usage(None, actions, groups, '')[:-1])
                else:
                    res.append(s % dict(prog=self._prog))
                    res.append('\n')

            return '%s\n\n' % ''.join(res)

        elif usage is DEFAULT_USAGE:
            return super()._format_usage(None, actions, groups, prefix)
        else:
            return super()._format_usage(usage, actions, groups, prefix)

    def format_help(self):
        help_ = super().format_help()
        if help_.count('\n') > 2:
            return help_ + '\n'
        else:
            return help_


class SubParsersAction(argparse._SubParsersAction):
    '''A replacement for _SubParsersAction that keeps
    track of the parent parser'''

    def __init__(self, **kw):
        self.parent = kw.pop('parent')
        super().__init__(**kw)

    def add_parser(self, *a, **kwargs):
        '''Pass parent usage and add_help attributes to new parser'''

        if 'usage' not in kwargs:
            # Inherit, but preserve old progs attribute
            usage = self.parent.usage
            repl = dict(prog=self.parent.prog)
            if isinstance(usage, list):
                usage = [(x % repl if isinstance(x, str) else x) for x in usage]
            elif usage:
                usage = usage % repl
            kwargs['usage'] = usage

        if 'help' in kwargs:
            kwargs.setdefault('description', kwargs['help'].capitalize() + '.')

        kwargs.setdefault('add_help', self.parent.add_help)
        kwargs.setdefault('formatter_class', self.parent.formatter_class)

        if 'parents' in kwargs:
            for p in kwargs['parents']:
                if p.epilog:
                    kwargs.setdefault('epilog', p.epilog % dict(prog=self.parent.prog))

        return super().add_parser(*a, **kwargs)


class ArgumentParser(argparse.ArgumentParser):
    def __init__(self, *a, **kw):
        if 'formatter_class' not in kw:
            kw['formatter_class'] = HelpFormatter

        super().__init__(*a, **kw)
        self.register('action', 'parsers', SubParsersAction)

    def add_version(self):
        self.add_argument(
            '--version',
            action='version',
            help="just print program version and exit",
            version='S3QL %s' % RELEASE,
        )

    def add_quiet(self):
        self.add_argument("--quiet", action="store_true", default=False, help="be really quiet")

    def add_backend_options(self):
        self.add_argument(
            "--backend-options",
            default={},
            type=suboptions_type,
            metavar='<options>',
            help="Backend specific options (separate by commas). See "
            "backend documentation for available options.",
        )

    def add_debug(self):
        destnote = 'Debug messages will be written to the target specified by the ``--log`` option.'
        self.add_argument(
            "--debug-modules",
            metavar='<modules>',
            type=lambda s: s.split(','),
            dest='debug',
            help="Activate debugging output from specified modules "
            "(use commas to separate multiple modules, 'all' for everything). " + destnote,
        )
        self.add_argument(
            "--debug",
            action='append_const',
            const='s3ql',
            help="Activate debugging output from all S3QL modules. " + destnote,
        )

    def add_cachedir(self):
        self.add_argument(
            "--cachedir",
            type=str,
            metavar='<path>',
            default=os.path.expanduser("~/.s3ql"),
            help='Store cached data in this directory (default: `~/.s3ql)`',
        )

    def add_log(self, default=None):
        self.add_argument(
            "--log",
            type=str_or_None_type,
            metavar='<target>',
            default=default,
            help='Destination for log messages. Specify ``none`` for standard '
            'output or ``syslog`` for the system logging daemon. '
            'Anything else will be interpreted as a file name. Log files '
            'will be rotated when they reach 1 MiB, and at most 5 old log '
            'files will be kept. Default: ``%(default)s``',
        )

    def add_storage_url(self):
        self.add_argument(
            "storage_url",
            metavar='<storage-url>',
            type=storage_url_type,
            help='Storage URL of the backend that contains the file system',
        )
        self.add_argument(
            "--authfile",
            type=str,
            metavar='<path>',
            default=os.path.expanduser("~/.s3ql/authinfo2"),
            help='Read authentication credentials from this file (default: `~/.s3ql/authinfo2)`',
        )

    def add_compress(self):
        def compression_type(s):
            hit = re.match(r'^([a-z0-9]+)(?:-([0-9]))?$', s)
            if not hit:
                raise argparse.ArgumentTypeError('%s is not a valid --compress value' % s)
            alg = hit.group(1)
            lvl = hit.group(2)
            if alg not in ('none', 'zlib', 'bzip2', 'lzma'):
                raise argparse.ArgumentTypeError('Invalid compression algorithm: %s' % alg)
            if lvl is None:
                lvl = 6
            else:
                lvl = int(lvl)
            if alg == 'none':
                alg = None
            return (alg, lvl)

        self.add_argument(
            "--compress",
            action="store",
            default='lzma-6',
            metavar='<algorithm-lvl>',
            type=compression_type,
            help="Compression algorithm and compression level to use when "
            "storing new data. *algorithm* may be any of `lzma`, `bzip2`, "
            "`zlib`, or none. *lvl* may be any integer from 0 (fastest) "
            "to 9 (slowest). Default: `%(default)s`",
        )

    def add_subparsers(self, **kw):
        '''Pass parent and set prog to default usage message'''
        kw.setdefault('parser_class', argparse.ArgumentParser)

        kw['parent'] = self

        # prog defaults to the usage message of this parser, skipping
        # optional arguments and with no "usage:" prefix
        if kw.get('prog') is None:
            formatter = self._get_formatter()
            positionals = self._get_positional_actions()
            groups = self._mutually_exclusive_groups
            formatter.add_usage(None, positionals, groups, '')
            kw['prog'] = formatter.format_help().strip()

        return super().add_subparsers(**kw)

    def _read_authinfo(self, path, storage_url):
        ini_config = configparser.ConfigParser()
        if os.path.isfile(path):
            mode = os.stat(path).st_mode
            if mode & (stat.S_IRGRP | stat.S_IROTH):
                self.exit(12, "%s has insecure permissions, aborting." % path)
            ini_config.read(path)

        merged = dict()
        for section in ini_config.sections():
            pattern = ini_config[section].get('storage-url', None)
            if not pattern or not storage_url.startswith(pattern):
                continue

            for key, val in ini_config[section].items():
                if key != 'storage-url':
                    merged[key] = val
        return merged

    def parse_args(self, *args, **kwargs):
        try:
            options = super().parse_args(*args, **kwargs)
        except ArgumentError as exc:
            self.error(str(exc))

        if hasattr(options, 'authfile'):
            storage_url = getattr(options, 'storage_url', '')
            ini_config = self._read_authinfo(options.authfile, storage_url)

            # Validate configuration file
            fixed_keys = {'backend-login', 'backend-password', 'fs-passphrase', 'storage-url'}
            unknown_keys = (
                set(ini_config.keys())
                - {x.replace('_', '-') for x in options.__dict__.keys()}
                - fixed_keys
            )
            if unknown_keys:
                self.exit(2, 'Unknown keys(s) in configuration file: ' + ', '.join(unknown_keys))

            # Update defaults and re-parse arguments
            defaults = {
                k.replace('-', '_'): v for (k, v) in ini_config.items() if k != 'storage_url'
            }
            self.set_defaults(**defaults)
            options = super().parse_args(*args, **kwargs)

        if hasattr(options, 'storage_url'):
            self._init_backend_factory(options)

        if hasattr(options, 'cachedir'):
            assert options.storage_url
            if not os.path.exists(options.cachedir):
                try:
                    os.mkdir(options.cachedir, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
                except PermissionError:
                    self.exit(45, 'No permission to create cache directory ' + options.cachedir)

            if not os.access(options.cachedir, os.R_OK | os.W_OK | os.X_OK):
                self.exit(45, 'No permission to access cache directory ' + options.cachedir)

            cachedir = os.path.abspath(options.cachedir)
            os.environ['SQLITE_TMPDIR'] = cachedir
            options.cachepath = os.path.join(cachedir, escape(options.storage_url))

        return options

    def _init_backend_factory(self, options):
        storage_url = options.storage_url
        hit = re.match(r'^([a-zA-Z0-9]+)://', storage_url)
        if not hit:
            self.exit(2, 'Unable to parse storage url ' + storage_url)

        backend = hit.group(1)
        try:
            backend_class = prefix_map[backend]
        except KeyError:
            self.exit(11, 'No such backend: ' + backend)

        backend_options = options.backend_options
        for opt in backend_options.keys():
            if opt not in backend_class.known_options:
                self.exit(3, 'Unknown backend option: ' + opt)

        if not hasattr(options, 'backend_login') and backend_class.needs_login:
            if sys.stdin.isatty():
                options.backend_login = getpass("Enter backend login: ")
            else:
                options.backend_login = sys.stdin.readline().rstrip()

        if not hasattr(options, 'backend_password') and backend_class.needs_login:
            if sys.stdin.isatty():
                options.backend_password = getpass("Enter backend password: ")
            else:
                options.backend_password = sys.stdin.readline().rstrip()

        options.backend_class = backend_class


def storage_url_type(s):
    '''Validate and canonicalize storage url'''

    if not re.match(r'^([a-zA-Z0-9]+)://(.+)$', s):
        raise ArgumentTypeError('%s is not a valid storage url.' % s)

    if s.startswith('local://'):
        # Append trailing slash so that we can match patterns with
        # trailing slash in authinfo2 file.
        return 'local://%s/' % os.path.abspath(s[len('local://') :])

    # If there is no prefix specified, then e.g. s3://foo and s3://foo/ point to
    # the same location (even though s3://foo/bar and s3://foo/bar/ are pointing
    # to *different* locations). However, the first two storage urls would
    # nevertheless get different cache directories, which is undesired.
    # Therefore, we handle these special cases right when parsing the command
    # line. In retrospect, it would have been better to always add an implicit
    # slash (even when using a prefix), but we can't do that now because it
    # would make file systems created without trailing slash inaccessible.
    if re.match(r'^(s3|gs)://[^/]+$', s) or re.match(
        r'^(s3c|swift(ks)?|rackspace)://[^/]+/[^/]+$', s
    ):
        s += '/'

    return s


def str_or_None_type(s):
    if s.lower() == 'none':
        return None
    return s


def suboptions_type(s):
    '''An argument converter for suboptions

    A suboption takes a comma separated list of additional
    options, e.g. --backend-options ssl,timeout=42,sse
    '''

    assert isinstance(s, str)

    opts = dict()
    for opt in s.split(','):
        if '=' in opt:
            (key, val) = opt.split('=', 1)
        else:
            key = opt
            val = True
        opts[key] = val

    return opts
