#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
setup.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

try:
    import setuptools
except ModuleNotFoundError:
    raise SystemExit(
        'Setuptools package not found. Please install from '
        'https://pypi.python.org/pypi/setuptools'
    )
import faulthandler
import os
import subprocess
import sys
from glob import glob

from setuptools import Extension
from setuptools.command.test import test as TestCommand

faulthandler.enable()

basedir = os.path.abspath(os.path.dirname(sys.argv[0]))
DEVELOPER_MODE = os.path.exists(os.path.join(basedir, 'MANIFEST.in'))
if DEVELOPER_MODE:
    print('MANIFEST.in exists, running in developer mode')

# Add S3QL sources
sys.path.insert(0, os.path.join(basedir, 'src'))
sys.path.insert(0, os.path.join(basedir, 'util'))
import s3ql


class pytest(TestCommand):
    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest

        errno = pytest.main(['tests'])
        sys.exit(errno)


def main():
    with open(os.path.join(basedir, 'README.rst'), 'r') as fh:
        long_desc = fh.read()

    compile_args = ['-Wall', '-Wextra', '-Wconversion', '-Wsign-compare']

    # Enable all fatal warnings only when compiling from Mercurial tip.
    # (otherwise we break forward compatibility because compilation with newer
    # compiler may fail if additional warnings are added)
    if DEVELOPER_MODE:
        if os.environ.get('CI') != 'true':
            compile_args.append('-Werror')

        # Value-changing conversions should always be explicit.
        compile_args.append('-Werror=conversion')

        # Note that (i > -1) is false if i is unsigned (-1 will be converted to
        # a large positive value). We certainly don't want to do this by
        # accident.
        compile_args.append('-Werror=sign-compare')

        # These warnings have always been harmless, and have always been due to
        # issues in Cython code rather than S3QL. Cython itself warns if there
        # are unused variables in .pyx code.
        compile_args.append('-Wno-unused-parameter')
        compile_args.append('-Wno-unused-function')

    required_pkgs = [
        'apsw >= 3.42.0',  # https://github.com/rogerbinns/apsw/issues/459
        'cryptography',
        'requests',
        'defusedxml',
        'google-auth',
        'google-auth-oauthlib',
        # Need trio.lowlevel
        'trio >= 0.15',
        'pyfuse3 >= 3.2.0, < 4.0',
    ]
    if sys.version_info < (3, 7, 0):
        required_pkgs.append('async_generator')

    setuptools.setup(
        name='s3ql',
        zip_safe=False,
        version=s3ql.VERSION,
        description='a full-featured file system for online data storage',
        long_description=long_desc,
        author='Nikolaus Rath',
        author_email='Nikolaus@rath.org',
        license='GPLv3',
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Environment :: No Input/Output (Daemon)',
            'Environment :: Console',
            'License :: OSI Approved :: GNU Library or Lesser General Public License (GPLv3)',
            'Topic :: Internet',
            'Operating System :: POSIX',
            'Topic :: System :: Archiving',
        ],
        platforms=['POSIX', 'UNIX', 'Linux'],
        package_dir={'': 'src'},
        packages=setuptools.find_packages('src'),
        provides=['s3ql'],
        ext_modules=[
            Extension(
                's3ql.sqlite3ext',
                ['src/s3ql/sqlite3ext.cpp'],
                extra_compile_args=compile_args,
                language='c++',
                depends=['src/s3ql/_sqlite3ext.cpp'],
            ),
        ],
        data_files=[
            (
                'share/man/man1',
                [
                    os.path.join('doc/manpages/', x)
                    for x in glob(os.path.join(basedir, 'doc', 'manpages', '*.1'))
                ],
            )
        ],
        entry_points={
            'console_scripts': [
                'mkfs.s3ql = s3ql.mkfs:main',
                'fsck.s3ql = s3ql.fsck:main',
                'mount.s3ql = s3ql.mount:main',
                'umount.s3ql = s3ql.umount:main',
                's3qlcp = s3ql.cp:main',
                's3qlstat = s3ql.statfs:main',
                's3qladm = s3ql.adm:main',
                's3qlctrl = s3ql.ctrl:main',
                's3qllock = s3ql.lock:main',
                's3qlrm = s3ql.remove:main',
                's3ql_oauth_client = s3ql.oauth_client:main',
                's3ql_verify = s3ql.verify:main',
            ]
        },
        install_requires=required_pkgs,
        tests_require=['pytest >= 3.7'],
        cmdclass={'upload_docs': upload_docs, 'build_cython': build_cython, 'pytest': pytest},
    )


class build_cython(setuptools.Command):
    user_options = []
    boolean_options = []
    description = "Compile .pyx to .c/.cpp"

    def initialize_options(self):
        pass

    def finalize_options(self):
        # Attribute defined outside init
        # pylint: disable=W0201
        self.extensions = self.distribution.ext_modules

    def run(self):
        try:
            from Cython.Build import cythonize
        except ImportError:
            raise SystemExit('Cython needs to be installed for this command') from None

        for extension in self.extensions:
            for fpath in extension.sources:
                (file_, ext) = os.path.splitext(fpath)
                spath = os.path.join(basedir, file_ + '.pyx')
                if ext not in ('.c', '.cpp') or not os.path.exists(spath):
                    continue
                print('compiling %s to %s' % (file_ + '.pyx', file_ + ext))
                cythonize(spath, language_level=3)


class upload_docs(setuptools.Command):
    user_options = []
    boolean_options = []
    description = "Upload documentation"

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        subprocess.check_call(
            [
                'rsync',
                '-aHv',
                '--del',
                os.path.join(basedir, 'doc', 'html') + '/',
                'ebox.rath.org:/srv/www.rath.org/s3ql-docs/',
            ]
        )
        subprocess.check_call(
            [
                'rsync',
                '-aHv',
                '--del',
                os.path.join(basedir, 'doc', 'manual.pdf'),
                'ebox.rath.org:/srv/www.rath.org/s3ql-docs/',
            ]
        )


if __name__ == '__main__':
    main()
