#!/usr/bin/python -W ignore::DeprecationWarning

# Script to manage S3-stored backups

import optparse
import secrets
import sys
import time

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
                backups[hostname][backupnum] = {'date': lastmod, 'hostname': hostname, 'backupnum': backupnum, 'finalized': 0, 'keys': []}
        else:
            backups[hostname] = {backupnum: {'date': lastmod, 'hostname': hostname, 'backupnum': backupnum, 'finalized': 0, 'keys': []}}
        if final:
            backups[hostname][backupnum]['finalized'] = lastmod
        else:
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

def main():
    # check command line options
    parser = optparse.OptionParser(usage="usage: %prog [options] list/delete/script")
    parser.add_option("-H", "--host", dest="host",
                      help="Name of backed-up host")
    parser.add_option("-b", "--backup-number", dest="backupnum",
                      help="Backup number")
    parser.add_option("-a", "--age", dest="age",
                      help="Delete backups older than AGE days")
    parser.add_option("-f", "--filename", dest="filename",
                      help="Output filename for script")
    parser.add_option("-x", "--expire", dest="expire",
                      help="Maximum age of script, default 86400 seconds")
    parser.add_option("-t", "--test", dest="test", action="store_true",
                      help="Test mode; don't actually delete")
    parser.add_option("-u", "--unfinalized", dest="unfinalized",
                      action="store_true", help="Consider unfinalized backups")

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
                for backupnum in backups.keys():
                    filecount = len(backups[backupnum]['keys'])
                    if backups[backupnum]['finalized'] > 0:
                        datestruct = backups[backupnum]['finalized']
                        inprogress = ''
                    else:
                        datestruct = backups[backupnum]['date']
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
            for bucket in buckets:
                hostnames = list_backups(bucket)
                for hostname in hostnames.keys():
                    backups = hostnames[hostname]
                    for backupnum in backups.keys():
                        filecount = len(backups[backupnum]['keys'])
                        if backups[backupnum]['finalized'] > 0:
                            datestruct = backups[backupnum]['finalized']
                        else:
                            datestruct = backups[backupnum]['date']
                        timestamp = time.mktime(datestruct)
                        delta = int(time.time() - timestamp + time.timezone)
                        if delta > maxage:
                            if options.unfinalized and backups[backupnum]['finalized'] > 0:
                                sys.stdout.write('Bypassing finalized backup %s #%i (%i files, age %.2f days)\n' % (hostname, backupnum, filecount, delta/86400.0))
                            else:
                                sys.stdout.write('Deleting %s #%i (%i files, age %.2f days)...' % (hostname, backupnum, filecount, delta/86400.0))
                                for key in backups[backupnum]['keys']:
                                    if options.test:
                                        sys.stdout.write('*')
                                    else:
                                        key.delete()
                                        sys.stdout.write('.')
                                sys.stdout.write('\n')
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
                        sys.stdout.write('\n')
                else:
                    parser.error('Host %s not found' % options.host)
        else:
            parser.error('Need either an age or a host AND backup number.')

if __name__ == '__main__':
    main()

