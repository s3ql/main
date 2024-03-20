'''
backends/__init__.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from . import gs, local, rackspace, s3, s3c, s3c4, swift, swiftks
from .b2.b2_backend import B2Backend

#: Mapping from storage URL prefixes to backend classes
prefix_map = {
    's3': s3.Backend,
    'local': local.Backend,
    'gs': gs.Backend,
    's3c': s3c.Backend,
    's3c4': s3c4.Backend,
    'swift': swift.Backend,
    'swiftks': swiftks.Backend,
    'rackspace': rackspace.Backend,
    'b2': B2Backend,
}

__all__ = ['common', 'pool', 'comprenc'] + list(prefix_map.keys())
