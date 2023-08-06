'''
oauth_client.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import logging
import sys
import textwrap

from google_auth_oauthlib.flow import InstalledAppFlow

from .common import OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET
from .logging import setup_logging, setup_warnings
from .parse_args import ArgumentParser

log = logging.getLogger(__name__)


def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent(
            '''\
        Obtain OAuth2 refresh token for Google Storage
        '''
        )
    )

    parser.add_log()
    parser.add_debug()
    parser.add_quiet()
    parser.add_version()

    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    setup_warnings()
    options = parse_args(args)
    setup_logging(options)

    # We need full control in order to be able to update metadata
    # cf. https://stackoverflow.com/questions/24718787
    flow = InstalledAppFlow.from_client_config(
        client_config={
            "installed": {
                "client_id": OAUTH_CLIENT_ID,
                "client_secret": OAUTH_CLIENT_SECRET,
                "redirect_uris": ["http://localhost", "urn:ietf:wg:oauth:2.0:oob"],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://accounts.google.com/o/oauth2/token",
            }
        },
        scopes=['https://www.googleapis.com/auth/devstorage.full_control'],
    )

    credentials = flow.run_local_server(open_browser=True)

    print('Success. Your refresh token is:\n', credentials.refresh_token)
