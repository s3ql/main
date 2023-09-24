============================
 Frequently Asked Questions
============================


How can I improve the file system throughput?
=============================================

There are three possible limiting factors for file system throughput:

#. Throughput from process through kernel to S3QL
#. Compression speed
#. Upload speed

To figure out what's the limiting factor in your case, you can use the
script :file:`contrib/benchmark.py` in the S3QL tarball. It will
determine the individual throughputs, and also give a recommendation
for compression algorithm and number of upload threads.

Why does `df` show 1 TB of free space?
======================================

The file system size of S3QL is unlimited. However, most Unix
utilities (including `df`) have no concept of an infinite size, so
S3QL has to provide some numerical value. Therefore, S3QL always
reports that the file system is 50% free, but it also reports at least
1 TB. This means that if you store 1.5 TB you will have another 1.5 TB
free, but if you store just 2 MB, you will still have TB free rather
than just another 2 MB.

Which operating systems are supported?
======================================

S3QL is developed on Linux, but should in principle work on all FUSE
supported operating systems that have the required Python modules. The
:program:`umount.s3ql` program does some tricks which may not work on
non-Linux systems, but you can always umount with the ``fusermount
-u`` command provided by FUSE itself (the only difference is that it
will not block until all data has been uploaded to S3 but return
immediately). Please report your success (or failure) with other
operating systems on the mailing list, so that this information can be
extended.

Is there a file size limit?
===========================

No, S3QL puts no limits on the size of your files. Any limits on
maximum object size imposed by the storage service do not matter for
S3QL, since files are automatically and transparently split into
smaller blocks.

Suppose I want to make a small change in a very large file. Will S3QL download and re-upload the entire file?
=============================================================================================================

No, S3QL splits files in blocks of a configurable size (default: 10 MB). So to make a small change in a big file, only one block needs to be transferred and not the entire file.

I don't quite understand this de-duplication feature...
=======================================================

The great thing about data de-duplication is that you don't need to know anything about it and will still get all the benefits. Towards you, S3QL will behave and look exactly like a file system without data duplication. When talking to the storage backend, however, the S3QL will try to store identical data only once and thus occupy less space than an ordinary file system would.

What does the "Transport endpoint not connected" error mean?
============================================================

It means that the file system has crashed. Please check :file:`mount.log` and
:file:`mount.s3ql_crit.log` for a more useful error message and report a bug if
appropriate.

To make the mountpoint available again (i.e., unmount the crashed file system), use the
``fusermount -u`` command.

Before reporting a bug, please make sure that you're using the most recent S3QL version.

What does "Backend reports that fs is still mounted elsewhere, aborting" mean?
==============================================================================

It means that either the file systems has not been unmounted cleanly or is still
mounted. If you are sure that the file system is not mounted anywhere, run
:command:`fsck.s3ql`, ideally on the computer where the file system has been mounted most
recently.


Can I access an S3QL file system on multiple computers simultaneously?
======================================================================

To share a file system between several computers, you need to have a
method for these computers to communicate, so that they can tell each
other when they are changing data. However, S3QL is designed to store
data in cloud storage services which can not be used for this kind of
communication, because different computers may end up talking to
different servers in the cloud (and there is no way for S3QL to force
the cloud servers to synchronize quickly and often enough).

Therefore, the only way to share an S3QL file system among different
computers is to have one "master" computer that runs *mount.s3ql*,
and then shares the mountpoint over a network file system like NFS or
CIFS.


What block size should I use?
=============================

The :cmdopt:`--data-block-size` option of the :program:`mkfs.s3ql` command determines the maximum
size of the data blocks that S3QL exchanges with the backend server.

For files smaller than the block size, this option has no effect. Files larger than the
maximum object size are split into multiple blocks. Whenever you upload or download data
from the backend, this is done in complete blocks. So if you have configured a maximum
object size of 10 MB, and want to read (or write) 5 bytes in a 100 MB file, you will still
download (or upload) roughly 10 MB of data (compression may reduce this amount). If you
decreased the maximum object size to 5 MB, you'd download/upload only about 5 MB.

On the other hand, if you want to read the whole 100 MB, and have
configured a block size of 10 MB, S3QL will have to send 10 separate
requests to the backend. With a maximum object size of 1 MB, there'd
be 100 separate requests. The larger the number of requests, the more
inefficient this becomes, because there is a fixed time associated
with the processing of each request. Also, many storage providers
charge a fixed amount for each request, so downloading 100 MB in one
request of 100 MB is cheaper than downloading it with 100 requests of
1 MB.

When choosing a block size, you have to find a balance
between these two effects. The bigger the size, the less requests will
be used, but the more data is transferred uselessly. The smaller the
size, the more requests will be used, but less traffic will be wasted.

Generally you should go with the default unless you have a good reason
to change it. When adjusting the value, keep in mind that it only
affects files larger than the block size. For example, if most of your
files are less than 1 MB, decreasing the block size from the
default 10 MB to 1 MB will have almost no effect.

Is there a way to access the file system offline?
=================================================

No, there is no way to do this. However, for most use-cases this
feature is actually not required. If you want to ensure that all data
in a directory is stored in the cloud *and* available offline, all you
need to do is store your data in a local directory that you
periodically synchronize to an S3QL mountpoint. For example, if you
would like to have an S3QL mountpoint with persistent cache at
:file:`/mnt/data`, you get can this as follows:

#. Mount a local block device at :file:`/mnt/data`.
#. Mount an S3QL file system at :file:`/mnt/data_online` using a small
   cache size and number of threads (eg. :cmdopt:`--threads
   2 --max-cache-entries 10`).
#. Keep :file:`/mnt/data_online` synchronized with :file:`/mnt/data`
   by either

   * Periodically running rsync, e.g. by calling
     ``rsync -aHA --delete-during --partial`` from cron, or

   * Continuously synchronizing using e.g. `watch.sh
     <https://github.com/drunomics/syncd/blob/master/watch.sh>`_ or
     `lsyncd <https://code.google.com/p/lsyncd/>`_ (these programs use
     *inotify* to constantly monitor the source directory and
     immediately copy over any changes)

#. Now use :file:`/mnt/data` in the same way as you would use an S3QL
   file system with a persistent cache.
