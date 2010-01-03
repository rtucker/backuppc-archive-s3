#!/usr/bin/python -W ignore::DeprecationWarning

# Script to manage S3-stored backups

import base64
import glob
import md5
import os
import secrets
import sys
import time

from subprocess import *

from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto.exception

def open_s3(accesskey, sharedkey):
    return S3Connection(accesskey, sharedkey)

def iter_backup_buckets(conn, name=None):
    """Yields an iterator of buckets that probably have backups in them."""

    bucket_prefix = secrets.accesskey.lower() + '-bkup-'
    if name:
        bucket_prefix += name

    buckets = conn.get_all_buckets()

    for bucket in buckets:
        if bucket.name.startswith(bucket_prefix):
            yield bucket

def list_backups(bucket):
    """Returns a dict of backups in a bucket, with dicts of:
    {hostname (str):
        {Backup number (int):
            {'date': Timestamp of backup (int),
             'keys': A list of keys comprising the backup,
             'hostname': Hostname (str),
             'backupnum': Backup number (int)
            }
        }
    }
    """

    backups = {}

    for key in bucket.list():
        keyparts = key.key.split('.')
        encrypted = split = tarred = False

        if keyparts[-1] == 'gpg':
            encrypted = True
            keyparts.pop()

        if keyparts[-1] != 'tar' and len(keyparts[-1]) is 2:
            split = True
            keyparts.pop()

        if keyparts[-1] == 'tar':
            tarred = True
            keyparts.pop()

        backupnum = int(keyparts.pop())
        hostname = '.'.join(keyparts)

        lastmod = time.strptime(key.last_modified, '%Y-%m-%dT%H:%M:%S.000Z')

        if hostname in backups.keys():
            if backupnum in backups[hostname].keys():
                backups[hostname][backupnum]['keys'].append(key)
            else:
                backups[hostname][backupnum] = {'keys': [key], 'date': lastmod, 'hostname': hostname, 'backupnum': backupnum}
        else:
            backups[hostname] = {backupnum: {'keys': [key], 'date': lastmod, 'hostname': hostname, 'backupnum': backupnum}}

    return backups

def iter_urls(keyset, expire=86400):
    """Given a list of keys and an optional expiration time (in seconds),
       returns an iterator of URLs to fetch to reassemble the backup."""

    for key in keyset:
        yield key.generate_url(expires_in=expire)

def make_restore_script(backup, expire=86400):
    """Returns a quick and easy restoration script to restore the given system,
       requires a backup, and perhaps expire"""

    myhostname = backup['hostname']
    mybackupnum = backup['backupnum']
    myfilecount = len(backup['keys'])
    myfriendlytime = time.strftime('%Y-%m-%d at %H:%M GMT', backup['date'])
    myexpiretime = time.strftime('%Y-%m-%d at %H:%M GMT', time.gmtime(time.time()+expire))
    myexpiretimestamp = time.time()+expire

    output = []

    output.append('#!/bin/sh\n')
    output.append('# Restoration script for %s backup %s,\n' % (myhostname, mybackupnum))
    output.append('# a backup created on %s.\n' % (myfriendlytime))
    output.append('# To use: bash scriptname /path/to/put/the/files\n\n')
    output.append('# WARNING: THIS FILE EXPIRES AFTER %s\n' % (myexpiretime))
    output.append('if [ "`date +%%s`" -gt "%i" ];\n' % (myexpiretimestamp))
    output.append('    then echo "Sorry, but this restore script is too old.";\n')
    output.append('         exit 1;\n')
    output.append('fi\n\n')
    output.append('if [ -z "$1" ];\n')
    output.append('   then echo "Usage: ./scriptname /path/to/restore/to";\n')
    output.append('        exit 1;\n')
    output.append('fi\n\n')
    output.append('# Check the destination\n')
    output.append('if [ ! -d $1 ];\n')
    output.append('    then echo "Target $1 does not exist!";\n')
    output.append('         exit 1;\n')
    output.append('fi\n\n')
    output.append('if [ -n "`ls --almost-all $1`" ];\n')
    output.append('    then echo "Target $1 is not empty!";\n')
    output.append('         exit 1;\n')
    output.append('fi\n\n')
    output.append('# cd to the destination, create a temporary workspace\n')
    output.append('cd $1\n')
    output.append('mkdir .restorescript-scratch\n\n')
    output.append('# retrieve files\n')

    mysortedfilelist = []
    for key in backup['keys']:
        output.append('wget -O $1/.restorescript-scratch/%s "%s"\n' % (key.name, key.generate_url(expires_in=expire)))
        mysortedfilelist.append('.restorescript-scratch/' + key.name)
    mysortedfilelist.sort()

    output.append('\n# decrypt files\n')
    output.append('gpg --decrypt-files << EOF\n')
    output.append('\n'.join(mysortedfilelist))
    output.append('\nEOF\n')

    output.append('\n# join and untar files\n')
    output.append('\ncat .restorescript-scratch/*.tar.?? | tar -xf -\n\n')

    output.append('\necho "DONE!  Have a nice day."\n')

    return output

def main():
    conn = open_s3(secrets.accesskey, secrets.sharedkey)

    for bucket in iter_backup_buckets(conn, name='olpc'):
        backups = list_backups(bucket)

        for backup in backups['olpc'].keys():
            print make_restore_script(backups['olpc'][backup])
            sys.exit(0)

if __name__ == '__main__':
    main()

