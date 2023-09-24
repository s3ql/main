.. -*- mode: rst -*-

=====================
The |command| command
=====================


Synopsis
========

::

   pcp [options] <source> [<source> ...] <destination>


Description
===========

The |command| command is a is a wrapper that starts several
:program:`sync` processes to copy directory trees in parallel. This is
allows much better copying performance on file system that have
relatively high latency when retrieving individual files like S3QL.

**Note**: Using this program only improves performance when copying
*from* an S3QL file system. When copying *to* an S3QL file system,
using |command| is more likely to *decrease* performance.


Options
=======

The |command| command accepts the following options:

.. pipeinclude:: python3 ../../contrib/pcp.py --help
   :start-after: show this help message and exit


Exit Codes
==========

|command| may terminate with the following exit codes:

.. include:: ../include/exitcodes.rst


See Also
========

|command| is shipped as part of S3QL, https://github.com/s3ql/s3ql/.

.. |command| replace:: :program:`pcp`
