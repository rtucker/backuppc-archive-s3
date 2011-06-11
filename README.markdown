BackupPC_archiveHost_s3
=======================

This is a Python script that acts as an interface between
[BackupPC](http://backuppc.sourceforge.net/) and
[Amazon S3](http://aws.amazon.com/s3/).  It uses BackupPC's
[archive function](http://backuppc.sourceforge.net/faq/BackupPC.html#archive_functions)
to extract a tarball and split it into chunks, like the normal archive
function.  Then, the chunks are encrypted using gpg and transmitted to
S3 using [Boto](http://code.google.com/p/boto/).

Installation
------------

I wrote this script some years ago, and can't remember how to get it going.
But, here's going to be my best guess :-)

1.  Install prerequisites

    You will need Python, [Boto](http://code.google.com/p/boto/), and a
    working BackupPC installation.

2.  Download and install this script

    Something like this seems like a good idea:

        cd /usr/local/src/
        git clone git://github.com/rtucker/backuppc-archive-s3.git

    Then create a link from `/usr/share/backuppc/bin/` to here:

        ln -s /usr/local/src/backuppc-archive-s3/BackupPC_archiveHost_s3 /usr/share/backuppc/bin/

3.  Configure this script

    Create a file in this directory called `secrets.py`, based upon the
    `secrets.py.orig` file.  It should have your AWS Access and Shared keys,
    a passphrase that will be used to encrypt the tarballs, and, optionally,
    a path to a file that contains a maximum upload rate in kilobits per
    second:

        accesskey = 'ASDIASDVINASDVASsvblahblah'
        sharedkey = '889rv98rv8fmasmvasdvsdvasdv'
        gpgsymmetrickey = 'hunter2'
        speedfile = '/var/cache/speedlimit.txt'

    If you use the `speedfile` option, you can change this on the fly to
    limit upstream bandwidth usage during peak hours, etc.

4.  Configure BackupPC

    From the BackupPC configuration interface, go to `Edit Hosts` and add a
    new host, `archiveS3`, which looks like the existing `archive` host.
    Save this, select the `archives3` host, and then `Edit Config` for that
    host.

    Change the settings on each tab as follows:

    Xfer:
        XferMethod:         archive
        ArchiveDest:        /var/lib/backuppc/archives3
        ArchiveComp:        bzip2
        ArchiveSplit:       500
        ArchiveClientCmd:   $Installdir/bin/BackupPC_archiveHost_s3 $tarCreatePath $splitpath $parpath $host $backupnumber $compression $compext $splitsize $archiveloc $parfile *

    Backup Settings:
        ClientTimeout:      720000

    That should be just about it.  Note that `ArchiveDest` is where it will
    stage the tarballs before it uploads them; this must have enough disk
    space for your archive!  `ArchiveSplit` is the size of each tar file,
    in megabytes; you may want to adjust this for your needs.  Also, the
    `ArchiveClientCmd` is the default, except with the `_s3` added.

5.  Use it

    Go to the main page for the `archives3` host and click `Start Archive`.
    To start with, just tick the box next to the smallest backup you have,
    then `Archive selected hosts`.  Go with the defaults (which look
    suspiciously like what you set on the Xfer tab, do they not?  :-) and
    then `Start the Archive`.

    Watch syslog and hopefully everything will work.

backup-manager.py
-----------------

There is a companion script, `backup-manager.py`, that can be used to see
what's on S3.  Run it with no arguments to get a listing of backups and
their ages, or use the `--help` argument to see what it can do.

The "crown jewel" of this whole system is the `script` command, which
produces a script that can be used to restore a backup.  It uses S3's
[Query String Request Authentication](http://docs.amazonwebservices.com/AmazonS3/latest/dev/index.html?RESTAuthentication.html#RESTAuthenticationQueryStringAuth)
mechanism to generate temporary URLs to download each file required to
restore a backup.

Each night, from `cron`, I run a script:

    #!/bin/sh
    BACKUPMGR=/path/to/backup-manager.py

    # Delete all backups older than 30 days.
    $BACKUPMGR delete --age=30

    # Create restore scripts, valid for one week, for all of my computers
    cd /home/rtucker/Dropbox/RestoreScripts/
    $BACKUPMGR --expire=604800 --host=gandalf script > restore_gandalf.sh
    $BACKUPMGR --expire=604800 --host=witte script > restore_witte.sh
    # etc, etc

    # Output a list of what's on the server
    $BACKUPMGR

The output of this is mailed to me, so I always know what's going on!

FAQs
----
*   BackupPC is written in Perl.  Why is this thing written in Python?

    I know Python much better than I know Perl, so I wrote it in Python.
    The good news is that BackupPC doesn't care, but it does mean this
    probably won't be part of the BackupPC main distribution any time soon.

*   Is this project dead?

    You could say that.  A lot of [my projects](https://github.com/rtucker/)
    are one-off scripts that solve a very specific need I have, and I don't
    put too much thought into making them useful for other people.  This
    script works for me and (sorta) meets my needs, so that's where it is.
