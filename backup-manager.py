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

from collections import defaultdict
from math import log10
from subprocess import *

class BackupManager:
    def __init__(self, accesskey, sharedkey):
        self._accesskey = accesskey
        self._connection = S3Connection(accesskey, sharedkey)

        self._buckets = None
        self._bucketbackups = {}
        self._backups = None

    def _generate_backup_buckets(self):
        bucket_prefix = self._accesskey.lower() + '-bkup-'
        buckets = self._connection.get_all_buckets()
        self._buckets = []

        for bucket in buckets:
            if bucket.name.startswith(bucket_prefix):
                self._buckets.append(bucket)

    @property
    def backup_buckets(self):   # property
        if self._buckets is None:
            self._generate_backup_buckets()
        return self._buckets

    def _list_backups(self, bucket):
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

            nextpart = keyparts.pop()
            if nextpart == 'COMPLETE':
                print("Stray file: %s" % key.key)
                continue
            backupnum = int(nextpart)
            hostname = '.'.join(keyparts)

            lastmod = time.strptime(key.last_modified, '%Y-%m-%dT%H:%M:%S.000Z')

            if hostname in backups.keys():
                if not backupnum in backups[hostname].keys():
                    backups[hostname][backupnum] = {'date': lastmod, 'hostname': hostname, 'backupnum': backupnum, 'finalized': 0, 'keys': [], 'finalkey': None, 'finalized_age': -1}
            else:
                backups[hostname] = {backupnum: {'date': lastmod, 'hostname': hostname, 'backupnum': backupnum, 'finalized': 0, 'keys': [], 'finalkey': None, 'finalized_age': -1}}
            if final:
                backups[hostname][backupnum]['finalized'] = lastmod
                backups[hostname][backupnum]['finalkey'] = key
                timestamp = time.mktime(lastmod)
                delta = int(time.time() - timestamp + time.timezone)
                backups[hostname][backupnum]['finalized_age'] = delta 
            else:
                if lastmod < backups[hostname][backupnum]['date']:
                    backups[hostname][backupnum]['date'] = lastmod
                backups[hostname][backupnum]['keys'].append(key)
        return backups

    def get_backups_by_bucket(self, bucket):
        if bucket.name not in self._bucketbackups:
            self._bucketbackups[bucket.name] = self._list_backups(bucket)

        return self._bucketbackups[bucket.name]

    @property
    def all_backups(self):  # property
        if self._backups is None:
            sys.stderr.write("Enumerating backups")
            self._backups = {}
            for bucket in self.backup_buckets:
                for hostname, backups in self.get_backups_by_bucket(bucket).items():
                    sys.stderr.write('.')
                    sys.stderr.flush()
                    if hostname not in self._backups:
                        self._backups[hostname] = {}
                    self._backups[hostname].update(backups)
            sys.stderr.write("\n")
        return self._backups

    def invalidate_host_cache(self, hostname):
        nuke = []
        for bucket in self._bucketbackups:
            if hostname in self._bucketbackups[bucket]:
                nuke.append(bucket)

        for bucket in nuke:
            if bucket in self._bucketbackups:
                del self._bucketbackups[bucket]
                self._backups = None

    @property
    def backups_by_age(self):   # property
        "Returns a dict of {hostname: [(backupnum, age), ...]}"
        results = defaultdict(list)
        for hostname, backups in self.all_backups.items():
            for backupnum, statusdict in backups.items():
                results[hostname].append((backupnum, statusdict['finalized_age']))
        return results

def choose_host_to_backup(agedict, target_count=2):
    "Takes a dict from backups_by_age, returns a hostname to back up."

    host_scores = defaultdict(int)

    for hostname, backuplist in agedict.items():
        bl = sorted(backuplist, key=lambda x: x[1])
        if len(bl) > 0 and bl[0][1] == -1:
            # unfinalized backup alert
            host_scores[hostname] += 200
            bl.pop(0)
        if len(bl) >= target_count:
            host_scores[hostname] -= 100
        host_scores[hostname] -= len(bl)
        if len(bl) > 0:
            # age of oldest backup helps score
            oldest = bl[0]
            host_scores[hostname] += log10(oldest[1])
            # recency of newest backup hurts score
            newest = bl[-1]
            host_scores[hostname] -= log10(max(1, (oldest[1] - newest[1])))

    for candidate, score in sorted(host_scores.items(), key=lambda x: x[1], reverse=True):
        yield (candidate, score)

def choose_backups_to_delete(agedict, target_count=2, max_age=30):
    "Takes a dict from backups_by_age, returns a list of backups to delete"

    decimate = defaultdict(list)

    for hostname, backuplist in agedict.items():
        bl = []
        for backup in sorted(backuplist, key=lambda x: x[1]):
            if backup[1] > 0:
                bl.append(backup)

        while len(bl) > target_count:
            backup = bl.pop()
            if backup[1] > max_age*24*60*60:
                decimate[hostname].append(backup)

    return decimate

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
    parser.add_option("-l", "--list", dest="list", action="store_true", help="List stored backups after completing operations")

    (options, args) = parser.parse_args()

    bmgr = BackupManager(secrets.accesskey, secrets.sharedkey)

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
            if options.host not in bmgr.all_backups:
                parser.error('No backups found for host "%s"' % options.host)
        else:
            if len(bmgr.all_backups) == 0:
                parser.error('No buckets found!')
    else:
        parser.error('Invalid option: %s' + args[0])

    if args[0] == 'script':
        if not options.host:
            parser.error('Must specify --host to generate a script for')

        if not options.backupnum and options.unfinalized:
            # assuming highest number
            options.backupnum = max(bmgr.all_backups[options.host].keys())
        elif not options.backupnum:
            # assuming highest finalized number
            options.backupnum = 0
            for backup in bmgr.all_backups[options.host].keys():
                if bmgr.all_backups[options.host][backup]['finalized'] > 0:
                    options.backupnum = max(options.backupnum, backup)
            if options.backupnum == 0:
                parser.error('No finalized backups found!  Try --unfinalized if you dare')

        backup = bmgr.all_backups[options.host][options.backupnum]

        if not options.expire:
            options.expire = "86400"

        if options.filename:
            fd = open(options.filename, 'w')
            fd.writelines(make_restore_script(backup, expire=int(options.expire)))
        else:
            sys.stdout.writelines(make_restore_script(backup, expire=int(options.expire)))
    elif args[0] == 'delete':
        to_ignore = int(options.keep)
        to_delete = []
        if options.host and options.backupnum:
            print("Will delete backup: %s %i (forced)" % (options.host, options.backupnum))
            to_delete.append((options.host, options.backupnum))
        elif options.age:
            to_delete_dict = choose_backups_to_delete(bmgr.backups_by_age, target_count=to_ignore, max_age=int(options.age))
            for hostname, backuplist in to_delete_dict.items():
                for backupstat in backuplist:
                    print("Will delete backup: %s %i (expired, age=%g days)" % (hostname, backupstat[0], backupstat[1]/86400.0))
                    to_delete.append((hostname, backupstat[0]))

        else:
            parser.error('Need either an age or a host AND backup number.')

        if len(to_delete) > 0:
            for deletehost, deletebackupnum in to_delete:
                hostbackups = bmgr.all_backups.get(deletehost, {})
                deletebackup = hostbackups.get(deletebackupnum, {})
                deletekeys = deletebackup.get('keys', [])
                finalkey = deletebackup.get('finalkey', None)
                if len(deletekeys) > 0:
                    sys.stdout.write("Deleting backup: %s %d (%d keys)" % (deletehost, deletebackupnum, len(deletekeys)))
                    for key in deletekeys:
                        if options.test:
                            sys.stdout.write('_')
                        else:
                            key.delete()
                            sys.stdout.write('.')
                        sys.stdout.flush()
                    if finalkey is not None:
                        if options.test:
                            sys.stdout.write('+')
                        else:
                            finalkey.delete()
                            sys.stdout.write('!')
                        sys.stdout.flush()
                    sys.stdout.write('\n')

        if options.start:
            for deletehost, deletebackupnum in to_delete:
                bmgr.invalidate_host_cache(deletehost)
            score_iter = choose_host_to_backup(bmgr.backups_by_age, target_count=int(options.keep)+1)
            for candidate, score in score_iter:
                if score > 0:
                    sys.stdout.write('Starting archive operation for host: %s (score=%g)\n' % (candidate, score))
                    start_archive([candidate])
                    break
    if args[0] == 'list' or options.list:
        sys.stdout.write('%25s | %5s | %20s | %5s\n' % ("Hostname", "Bkup#", "Age", "Files"))
        sys.stdout.write('-'*72 + '\n')
        for hostname, backups in bmgr.all_backups.items():
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

if __name__ == '__main__':
    main()
