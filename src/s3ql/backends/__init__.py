'''
backends/__init__.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from . import local, s3, gs, gdrive, s3c, swift, rackspace, swiftks

#: Mapping from storage URL prefixes to backend classes
prefix_map = { 's3': s3.Backend,
               'local': local.Backend,
               'gs': gs.Backend,
               'gdrive': gdrive.Backend,
               's3c': s3c.Backend,
               'swift': swift.Backend,
               'swiftks': swiftks.Backend,
               'rackspace': rackspace.Backend }

__all__ = [ 'common', 'pool', 'comprenc' ] + list(prefix_map.keys())

