#!/usr/bin/python -W ignore::DeprecationWarning
#
# Script to manage S3-stored backups
#
# Copyright (c) 2009-2011 Ryan S. Tucker
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import optparse
import os
import pwd
import secrets
import sys
import time

from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto.exception

from subprocess import *

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
             'backupnum': Backup number (int),
             'finalized': 0, or the timestamp the backup was finalized
            }
        }
    }
    """

    backups = {}

    for key in bucket.list():
        keyparts = key.key.split('.')
        encrypted = split = tarred = final = False

        if keyparts[-1] == 'COMPLETE':
            final = True
            keyparts.pop() # back to tar
            keyparts.pop() # back to backup number
        else:
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
            if not backupnum in backups[hostname].keys():
                backups[hostname][backupnum] = {'date': lastmod, 'hostname': hostname, 'backupnum': backupnum, 'finalized': 0, 'keys': [], 'finalkey': None}
        else:
            backups[hostname] = {backupnum: {'date': lastmod, 'hostname': hostname, 'backupnum': backupnum, 'finalized': 0, 'keys': [], 'finalkey': None}}
        if final:
            backups[hostname][backupnum]['finalized'] = lastmod
            backups[hostname][backupnum]['finalkey'] = key
        else:
            if lastmod < backups[hostname][backupnum]['date']:
                backups[hostname][backupnum]['date'] = lastmod
            backups[hostname][backupnum]['keys'].append(key)
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
    output.append('cat .restorescript-scratch/*.tar.?? | tar -xf -\n\n')

    output.append('echo "DONE!  Have a nice day."\n##\n')

    return output

def start_archive(hosts):
    "Starts an archive operation for a list of hosts."
    if 'LOGNAME' in os.environ:
        username = os.environ['LOGNAME']
    else:
        try:
            username = pwd.getpwuid(os.getuid()).pw_name
        except KeyError:
            username = 'nobody'

    scriptdir = os.path.dirname(sys.argv[0])

    cmd = [os.path.join(scriptdir, 'BackupPC_archiveStart'), 'archives3',
           username]
    cmd.extend(hosts)

    proc = Popen(cmd)
    proc.communicate()

def main():
    # check command line options
    parser = optparse.OptionParser(
        usage="usage: %prog [options] [list|delete|script]",
        description="" +
            "Companion maintenance script for BackupPC_archiveHost_s3. " +
            "By default, it assumes the 'list' command, which displays all " +
            "of the backups currently archived on S3.  The 'delete' command " +
            "is used to delete backups.  The 'script' command produces a " +
            "script that can be used to download and restore a backup."
        )
    parser.add_option("-H", "--host", dest="host",
                      help="Name of backed-up host")
    parser.add_option("-b", "--backup-number", dest="backupnum",
                      help="Backup number")
    parser.add_option("-a", "--age", dest="age",
                      help="Delete backups older than AGE days")
    parser.add_option("-k", "--keep", dest="keep",
                      help="When used with --age, keep this many recent backups (default=1)", default=1)
    parser.add_option("-f", "--filename", dest="filename",
                      help="Output filename for script")
    parser.add_option("-x", "--expire", dest="expire",
                      help="Maximum age of script, default 86400 seconds")
    parser.add_option("-t", "--test", dest="test", action="store_true",
                      help="Test mode; don't actually delete")
    parser.add_option("-u", "--unfinalized", dest="unfinalized",
                      action="store_true", help="Consider unfinalized backups")
    parser.add_option("-s", "--start-backups", dest="start",
                      action="store_true", help="When used with --age, start backups for hosts with fewer than keep+1 backups")

    (options, args) = parser.parse_args()

    conn = open_s3(secrets.accesskey, secrets.sharedkey)

    if options.backupnum and not options.host:
        parser.error('Must specify --host when specifying --backup-number')

    if options.backupnum:
        options.backupnum = int(options.backupnum)

    if len(args) == 0:
        args.append('list')

    if len(args) > 1:
        parser.error('Too many arguments.')

    if args[0] != 'delete' and options.age:
        parser.error('--age only makes sense with delete')

    if options.start and not (args[0] == 'delete' and options.age):
        parser.error('--start-backups only makes sense with delete and --age')

    if args[0] != 'script' and (options.expire or options.filename):
        parser.error('--expire and --filename only make sense with script')

    if args[0] in ['list', 'script', 'delete']:
        if options.host:
            buckets = iter_backup_buckets(conn, name=options.host)
            if not buckets:
                parser.error('No buckets found for host "%s"' % options.host)
        else:
            buckets = iter_backup_buckets(conn)
            if not buckets:
                parser.error('No buckets found!')
    else:
        parser.error('Invalid option: %s' + args[0])

    if args[0] == 'script':
        if not options.host:
            parser.error('Must specify --host to generate a script for')

        backups = list_backups(buckets.next())

        if not options.backupnum and options.unfinalized:
            # assuming highest number
            options.backupnum = max(backups[options.host].keys())
        elif not options.backupnum:
            # assuming highest finalized number
            options.backupnum = 0
            for backup in backups[options.host].keys():
                if backups[options.host][backup]['finalized'] > 0:
                    options.backupnum = max(options.backupnum, backup)
            if options.backupnum == 0:
                parser.error('No finalized backups found!  Try --unfinalized if you dare')

        backup = backups[options.host][options.backupnum]

        if not options.expire:
            options.expire = "86400"

        if options.filename:
            fd = open(options.filename, 'w')
            fd.writelines(make_restore_script(backup, expire=int(options.expire)))
        else:
            sys.stdout.writelines(make_restore_script(backup, expire=int(options.expire)))
    elif args[0] == 'list':
        sys.stdout.write('%25s | %5s | %20s | %5s\n' % ("Hostname", "Bkup#", "Age", "Files"))
        sys.stdout.write('-'*72 + '\n')
        for bucket in buckets:
            hostnames = list_backups(bucket)
            for hostname in hostnames.keys():
                backups = hostnames[hostname]
                for backupnum in sorted(backups.keys()):
                    filecount = len(backups[backupnum]['keys'])
                    datestruct = backups[backupnum]['date']
                    if backups[backupnum]['finalized'] > 0:
                        inprogress = ''
                    else:
                        inprogress = '*'
                    timestamp = time.mktime(datestruct)
                    delta = int(time.time() - timestamp + time.timezone)
                    if delta < 3600:
                        prettydelta = '%i min ago' % (delta/60)
                    elif delta < 86400:
                        prettydelta = '%i hr ago' % (delta/3600)
                    else:
                        days = int(delta/60/60/24)
                        if days == 1:
                            s = ''
                        else:
                            s = 's'
                        prettydelta = '%i day%s ago' % (days, s)

                    sys.stdout.write('%25s | %5i | %20s | %5i%s\n' % (hostname, backupnum, prettydelta, filecount, inprogress))
        sys.stdout.write('* == not yet finalized (Age == time of last activity)\n')
    elif args[0] == 'delete':
        if options.age:
            maxage = int(options.age)*86400
            needs_backup = []
            for bucket in buckets:
                hostnames = list_backups(bucket)
                for hostname in hostnames.keys():
                    backups = hostnames[hostname]
                    backuplist = sorted(backups.keys())
                    oldest_timestamp = -1
                    # remove a number of recent backups from the delete list
                    to_ignore = int(options.keep)
                    while to_ignore > 0:
                        if len(backuplist) > 0:
                            backupnum = backuplist.pop()
                            filecount = len(backups[backupnum]['keys'])
                            datestruct = backups[backupnum]['date']
                            timestamp = time.mktime(datestruct)
                            delta = int(time.time() - timestamp + time.timezone)
                            if backups[backupnum]['finalized'] == 0:
                                sys.stdout.write('Ignoring in-progress backup %s #%i\n' % (hostname, backupnum))
                            else:
                                sys.stdout.write('Keeping recent backup %s #%i (%i files, age %.2f days)\n' % (hostname, backupnum, filecount, delta/86400.0))
                                if timestamp < oldest_timestamp:
                                    oldest_timestamp = timestamp
                                to_ignore -= 1
                        else:
                            to_ignore = 0
                    deletes = 0
                    for backupnum in backuplist:
                        filecount = len(backups[backupnum]['keys'])
                        if backups[backupnum]['finalized'] > 0:
                            datestruct = backups[backupnum]['finalized']
                        else:
                            datestruct = backups[backupnum]['date']
                        timestamp = time.mktime(datestruct)
                        delta = int(time.time() - timestamp + time.timezone)
                        if delta > maxage:
                            if not options.unfinalized and backups[backupnum]['finalized'] == 0:
                                sys.stdout.write('Bypassing unfinalized backup %s #%i (%i files, age %.2f days)\n' % (hostname, backupnum, filecount, delta/86400.0))
                            else:
                                sys.stdout.write('Deleting %s #%i (%i files, age %.2f days)...' % (hostname, backupnum, filecount, delta/86400.0))
                                for key in backups[backupnum]['keys']:
                                    if options.test:
                                        sys.stdout.write('*')
                                    else:
                                        key.delete()
                                        sys.stdout.write('.')
                                if backups[backupnum]['finalkey']:
                                    if options.test:
                                        sys.stdout.write('X')
                                    else:
                                        backups[backupnum]['finalkey'].delete()
                                        sys.stdout.write('!')
                                sys.stdout.write('\n')
                                deletes += 1
                    if (len(backuplist)-deletes) < int(options.keep):
                        needs_backup.append((oldest_timestamp, hostname))
            if options.start and len(needs_backup) > 0:
                sys.stdout.write('Starting archive operations for hosts: %s\n' % ', '.join(x[1] for x in sorted(needs_backup)))
                start_archive([x[1] for x in sorted(needs_backup)])
        elif options.host and options.backupnum:
            for bucket in buckets:
                hostnames = list_backups(bucket)
                if options.host in hostnames.keys():
                    if options.backupnum not in hostnames[options.host].keys():
                        parser.error('Backup number %i not found' % options.backupnum)
                    toast = hostnames[options.host][options.backupnum]
                    filecount = len(toast['keys'])
                    if toast['finalized'] > 0:
                        datestruct = toast['finalized']
                    else:
                        datestruct = toast['date']

                    datestruct = toast['date']
                    timestamp = time.mktime(datestruct)
                    delta = int(time.time() - timestamp + time.timezone)

                    if options.unfinalized and toast['finalized'] > 0:
                        sys.stdout.write('Bypassing finalized backup %s #%i (%i files, age %.2f days)\n' % (hostname, backupnum, filecount, delta/86400.0))
                    else:
                        sys.stdout.write('Deleting %s #%i (%i files, age %.2f days)...' % (options.host, options.backupnum, filecount, delta/86400.0))
                        for key in toast['keys']:
                            if options.test:
                                sys.stdout.write('*')
                            else:
                                key.delete()
                                sys.stdout.write('.')
                        if toast['finalkey']:
                            if options.test:
                                sys.stdout.write('X')
                            else:
                                toast['finalkey'].delete()
                                sys.stdout.write('!')
                        sys.stdout.write('\n')
                else:
                    parser.error('Host %s not found' % options.host)
        else:
            parser.error('Need either an age or a host AND backup number.')

if __name__ == '__main__':
    main()

