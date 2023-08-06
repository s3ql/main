#!/usr/bin/env python3
'''
pytest_checklogs.py - this file is part of S3QL.

Copyright (C) 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.

py.test plugin to look for suspicious phrases in messages emitted on
stdout/stderr or via the logging module.

False positives can be registered via a new `reg_output` fixture (for messages
to stdout/stderr), and a `assert_logs` function (for logging messages).
'''

import functools
import logging
import re
from contextlib import contextmanager

import pytest


class CountMessagesHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
        self.count = 0

    def emit(self, record):
        self.count += 1


@contextmanager
def assert_logs(pattern, level=logging.WARNING, count=None):
    '''Assert that suite emits specified log message

    *pattern* is matched against the *unformatted* log message, i.e. before any
    arguments are merged.

    If *count* is not None, raise an exception unless exactly *count* matching
    messages are caught.

    Matched log records will also be flagged so that the caplog fixture
    does not generate exceptions for them (no matter their severity).
    '''

    def filter(record):
        if record.levelno == level and re.search(pattern, record.msg):
            record.checklogs_ignore = True
            return True
        return False

    handler = CountMessagesHandler()
    handler.setLevel(level)
    handler.addFilter(filter)
    logger = logging.getLogger()
    logger.addHandler(handler)
    try:
        yield

    finally:
        logger.removeHandler(handler)

        if count is not None and handler.count != count:
            pytest.fail(
                'Expected to catch %d %r messages, but got only %d'
                % (count, pattern, handler.count)
            )


def check_test_output(capfd, item):
    (stdout, stderr) = capfd.readouterr()

    # Strip out false positives
    try:
        false_pos = item.checklogs_fp
    except AttributeError:
        false_pos = ()
    for pattern, flags, count in false_pos:
        cp = re.compile(pattern, flags)
        (stdout, cnt) = cp.subn('', stdout, count=count)
        if count == 0 or count - cnt > 0:
            stderr = cp.sub('', stderr, count=count - cnt)

    for pattern in (
        'exception',
        'error',
        'warning',
        'fatal',
        'traceback',
        'fault',
        'crash(?:ed)?',
        'abort(?:ed)',
        'fishy',
    ):
        cp = re.compile(r'\b{}\b'.format(pattern), re.IGNORECASE | re.MULTILINE)
        hit = cp.search(stderr)
        if hit:
            pytest.fail('Suspicious output to stderr (matched "%s")' % hit.group(0))
        hit = cp.search(stdout)
        if hit:
            pytest.fail('Suspicious output to stdout (matched "%s")' % hit.group(0))


def register_output(item, pattern, count=1, flags=re.MULTILINE):
    '''Register *pattern* as false positive for output checking

    This prevents the test from failing because the output otherwise
    appears suspicious.
    '''

    item.checklogs_fp.append((pattern, flags, count))


@pytest.fixture()
def reg_output(request):
    assert not hasattr(request.node, 'checklogs_fp')
    request.node.checklogs_fp = []
    return functools.partial(register_output, request.node)


# Autouse fixtures are instantiated before explicitly used fixtures, this should also
# catch log messages emitted when e.g. initializing resources in other fixtures.
@pytest.fixture(autouse=True)
def check_output(caplog, capfd, request):
    yield
    for when in ("setup", "call", "teardown"):
        for record in caplog.get_records(when):
            if record.levelno >= logging.WARNING and not getattr(record, 'checklogs_ignore', False):
                pytest.fail('Logger received warning messages.')
    check_test_output(capfd, request.node)
