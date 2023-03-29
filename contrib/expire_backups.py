#!/usr/bin/env python3
'''
expire_backups.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import sys
import os
import re
import textwrap
import shutil
from datetime import datetime, timedelta
from collections import defaultdict

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if os.path.exists(os.path.join(basedir, 'setup.py')) and os.path.exists(
    os.path.join(basedir, 'src', 's3ql', '__init__.py')
):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.logging import setup_logging, QuietError, logging, setup_warnings
from s3ql.common import thaw_basic_mapping, freeze_basic_mapping
from s3ql.parse_args import ArgumentParser
from s3ql.remove import main as s3qlrm

log = logging.getLogger(__name__)


def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent(
            '''\
        ``expire_backups.py`` is a program to intelligently remove old backups
        that are no longer needed.

        To define what backups you want to keep for how long, you define a
        number of *age ranges*. ``expire_backups`` ensures that you will
        have at least one backup in each age range at all times. It will keep
        exactly as many backups as are required for that and delete any
        backups that become redundant.

        Age ranges are specified by giving a list of range boundaries in terms
        of backup cycles. Every time you create a new backup, the existing
        backups age by one cycle.

        Please refer to the S3QL documentation for details.
        '''
        )
    )

    parser.add_quiet()
    parser.add_log()
    parser.add_debug()
    parser.add_version()

    parser.add_argument(
        'cycles',
        nargs='+',
        type=int,
        metavar='<age>',
        help='Age range boundaries in terms of backup cycles',
    )
    parser.add_argument(
        '--state',
        metavar='<file>',
        type=str,
        default='.expire_backups.dat',
        # Add quotes around default to prevent groff
        # from choking on leading . generated by buggy
        # docutils man page generator.
        help='File to save state information in (default: "%(default)s")',
    )
    parser.add_argument(
        "-n",
        action="store_true",
        default=False,
        help="Dry run. Just show which backups would be deleted.",
    )
    parser.add_argument(
        "-p",
        "--proportion-delete",
        type=float,
        default=0.5,
        metavar="<N>",
        help='Maximum proportion of backups to delete (between 0 and 1, default: %(default)s)',
    )
    parser.add_argument(
        '--reconstruct-state',
        action='store_true',
        default=False,
        help='Try to reconstruct a missing state file from backup dates.',
    )

    parser.add_argument(
        "--use-s3qlrm", action="store_true", help="Use `s3qlrm` command to delete backups."
    )

    options = parser.parse_args(args)

    if sorted(options.cycles) != options.cycles:
        parser.error('Age range boundaries must be in increasing order')

    if not (0 < options.proportion_delete <= 1):
        parser.error('Proportion of backups to delete must be between 0 and 1')

    return options


def main(args=None):

    if args is None:
        args = sys.argv[1:]

    setup_warnings()
    options = parse_args(args)
    setup_logging(options)

    # Determine available backups
    backup_list = set(
        x for x in os.listdir('.') if re.match(r'^\d{4}-\d\d-\d\d_\d\d:\d\d:\d\d$', x)
    )

    if not os.path.exists(options.state) and len(backup_list) > 1:
        if not options.reconstruct_state:
            raise QuietError('Found more than one backup but no state file! Aborting.')

        log.warning('Trying to reconstruct state file..')
        state = upgrade_to_state(backup_list)
        if not options.n:
            log.info('Saving reconstructed state..')
            with open(options.state, 'wb') as fh:
                fh.write(freeze_basic_mapping(state))
    elif not os.path.exists(options.state):
        log.warning('Creating state file..')
        state = dict()
    else:
        log.info('Reading state...')
        with open(options.state, 'rb') as fh:
            state = thaw_basic_mapping(fh.read())

    to_delete = process_backups(backup_list, state, options.cycles)

    if len(backup_list) and (len(to_delete) / len(backup_list) > options.proportion_delete):
        raise QuietError(
            'Would remove more than %d%% of backups, aborting' % (options.proportion_delete * 100)
        )

    for x in to_delete:
        log.info('Backup %s is no longer needed, removing...', x)
        if not options.n:
            if options.use_s3qlrm:
                s3qlrm([x])
            else:
                shutil.rmtree(x)

    if options.n:
        log.info('Dry run, not saving state.')
    else:
        log.info('Saving state..')
        with open(options.state + '.new', 'wb') as fh:
            fh.write(freeze_basic_mapping(state))
        if os.path.exists(options.state):
            os.rename(options.state, options.state + '.bak')
        os.rename(options.state + '.new', options.state)


def upgrade_to_state(backup_list):
    log.info('Several existing backups detected, trying to convert absolute ages to cycles')

    now = datetime.now()
    age = dict()
    for x in sorted(backup_list):
        age[x] = now - datetime.strptime(x, '%Y-%m-%d_%H:%M:%S')
        log.info('Backup %s is %s hours old', x, age[x])

    deltas = [abs(x - y) for x in age.values() for y in age.values() if x != y]
    step = min(deltas)
    log.info('Assuming backup interval of %s hours', step)

    state = dict()
    for x in sorted(age):
        state[x] = 0
        while age[x] > timedelta(0):
            state[x] += 1
            age[x] -= step
        log.info('Backup %s is %d cycles old', x, state[x])

    log.info('State construction complete.')
    return state


def process_backups(backup_list, state, cycles):

    # New backups
    new_backups = backup_list - set(state)
    for x in sorted(new_backups):
        log.info('Found new backup %s', x)
        for y in state:
            state[y] += 1
        state[x] = 0

    for x in state:
        log.debug('Backup %s has age %d', x, state[x])

    # Missing backups
    missing_backups = set(state) - backup_list
    for x in missing_backups:
        log.warning('backup %s is missing. Did you delete it manually?', x)
        del state[x]

    # Ranges
    ranges = [(0, cycles[0])]
    for i in range(1, len(cycles)):
        ranges.append((cycles[i - 1], cycles[i]))

    # Go forward in time to see what backups need to be kept
    simstate = dict()
    keep = set()
    missing = defaultdict(list)
    for step in range(max(cycles)):

        log.debug('Considering situation after %d more backups', step)
        for x in simstate:
            simstate[x] += 1
            log.debug('Backup x now has simulated age %d', simstate[x])

        # Add the hypothetical backup that has been made "just now"
        if step != 0:
            simstate[step] = 0

        for (min_, max_) in ranges:
            log.debug('Looking for backup for age range %d to %d', min_, max_)

            # Look in simstate
            found = False
            for (backup, age) in simstate.items():
                if min_ <= age < max_:
                    found = True
                    break
            if found:
                # backup and age will be defined
                # pylint: disable=W0631
                log.debug('Using backup %s (age %d)', backup, age)
                continue

            # Look in state
            for (backup, age) in state.items():
                age += step
                if min_ <= age < max_:
                    log.info(
                        'Keeping backup %s (current age %d) for age range %d to %d%s',
                        backup,
                        state[backup],
                        min_,
                        max_,
                        (' in %d cycles' % step) if step else '',
                    )
                    simstate[backup] = age
                    keep.add(backup)
                    break

            else:
                if step == 0:
                    log.info(
                        'Note: there is currently no backup available ' 'for age range %d to %d',
                        min_,
                        max_,
                    )
                else:
                    missing['%d to %d' % (min_, max_)].append(step)

    for range_ in sorted(missing):
        log.info(
            'Note: there will be no backup for age range %s ' 'in (forthcoming) cycle(s): %s',
            range_,
            format_list(missing[range_]),
        )

    to_delete = set(state) - keep
    for x in to_delete:
        del state[x]

    return to_delete


def format_list(l):
    if not l:
        return ''
    l = l[:]

    # Append bogus end element
    l.append(l[-1] + 2)

    range_start = l.pop(0)
    cur = range_start
    res = list()
    for n in l:
        if n == cur + 1:
            pass
        elif range_start == cur:
            res.append('%d' % cur)
        elif range_start == cur - 1:
            res.append('%d' % range_start)
            res.append('%d' % cur)
        else:
            res.append('%d-%d' % (range_start, cur))

        if n != cur + 1:
            range_start = n
        cur = n

    if len(res) > 1:
        return '%s and %s' % (', '.join(res[:-1]), res[-1])
    else:
        return ', '.join(res)


if __name__ == '__main__':
    main(sys.argv[1:])
