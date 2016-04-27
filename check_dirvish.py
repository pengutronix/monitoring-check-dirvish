#!/usr/bin/python3

"""Nagios plugin to check the existence and freshness of a valid backup"""

import argparse
import logging
import subprocess
import os
import datetime
import collections

try:
	import nagiosplugin
except ImportError as e:
    print("Please install python3-nagiosplugin")
    raise e

try:
    import dateutil.parser
except ImportError as e:
    print("Please install python3-dateutil")
    raise e


_log = logging.getLogger('nagiosplugin')


class E_PathNotAccessible(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return "Basepath %r is not accessible" %repr(self.value)

class E_PathNoDir(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return "Basepath %r is not a directory" %repr(self.value)

class E_HistoryFileNotFound(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return "HistoryFile %r not found. Is there at last one Backup?" %repr(self.value)

class E_BackupNotValid(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return "Backup is not valid. %s" % (self.value)

class Backup(nagiosplugin.Resource):
    """Domain model: Dirvish vaults"""

    def __init__(self, vault, base_path):
        self.vault = vault
        self.base_path = base_path

    def check_path_accessible(self, directory):
        _log.debug("Check if %r is accessible and a directory", directory)
        if not os.access(directory, os.R_OK | os.X_OK):
            raise E_PathNotAccessible(directory)
        if not os.path.isdir(directory):
            raise E_PathNoDir(directory)
        return
        
    def backups(self):
        """Returns the path to the latest backup"""
        _log.debug('Finding the latest backup for vault "%s"', self.vault)
        self.check_path_accessible(self.base_path)
        self.vault_base_path = os.path.join(self.base_path, self.vault)
        self.check_path_accessible(self.vault_base_path)
        self.history_file = os.path.join(self.vault_base_path, 'dirvish', 'default.hist')
        _log.debug('Check for %r' % self.history_file)
        if not os.access(self.history_file, os.R_OK):
            raise E_HistoryFileNotFound(self.history_file)
        with open(self.history_file) as histfile:
            lines = histfile.readlines()
        for entry in reversed(lines):
            try:
                last_entry = entry.strip()
                backup = dict(zip(['image', 'created', 'reference', 'expires'], last_entry.split('\t')))
                _log.info("Found next backup in %r", backup) 
            except Exception as e:
                _log.error("Something unexpected happened, while reading file %r", self.history_file)
                next
            yield(backup['image'])

    def parse_backup(self, backup, parameterL = ['status', 'backup-begin', 'backup-complete']):
        """ Check the last backup for validity.
            Returns a dict with found keys in parameterL.
            All parameters are treated as caseinsensitive via str.casefold
        """
        _log.debug('Parsing backup: %r', backup)
        _parameterL = [ s.casefold() for  s in parameterL ]
        _log.debug("Searching for parameters %r", _parameterL)
        _resultD = dict()
        backup_image = os.path.join(self.vault_base_path, backup)
        self.check_path_accessible(backup_image)
        self.check_path_accessible(os.path.join(backup_image, 'tree'))
        summary_file = os.path.join(backup_image, 'summary')
        if not os.access(summary_file, os.R_OK):
            raise E_BackupNotValid('could not access summary file')
        with open(summary_file) as summary:
            for line in summary.readlines():
                parts = line.strip().split(': ')
                if len(parts) >= 2:
                    # we have a definition
                    parameter = parts[0]
                    value = " ".join(parts[1:])
                    _log.debug('Found parameter %r with value %r', parameter.casefold(), value)
                    parameter_casefold = parameter.casefold()
                    if parameter_casefold in _parameterL:
                        _log.debug("Adding parameter %r to returnDict", parameter_casefold)
                        _resultD[parameter_casefold] = value
        _log.info("parsed Backup to: %r", _resultD)
        return _resultD

    def check_backups(self):
        for backup in self.backups():
            try:
                parsed_backup = self.parse_backup(backup, ['status', 'backup-begin', 'backup-complete'])
            except E_PathNotAccessible as e:
                _log.debug("Exception thrown: %s", e)
                continue
            begin = dateutil.parser.parse(parsed_backup['backup-begin']) 
            _log.debug("Backup begin %r to %r", parsed_backup['backup-begin'], begin)
            end = dateutil.parser.parse(parsed_backup['backup-complete']) 
            _log.debug("Backup end %r to %r", parsed_backup['backup-complete'], end)
            dur = end - begin
            _log.debug("Duration is: %s", dur)
            if self.duration is None:
                self.duration = round(dur.total_seconds() / 3600., 2)
                _log.info('Gathered last duration to %s hours', self.duration)
            if self.age_try is None:
                age = datetime.datetime.now() - begin
                self.age_try = round(age.total_seconds() / (24*60*60.), 2)
                _log.info('Gathered age_try to %s days', self.age_try)
            if parsed_backup['status'].casefold() == "success":
                if self.age_success is None:
                    age = datetime.datetime.now() - begin
                    self.age_success = round(age.total_seconds() / (24*60*60.), 2)
                    _log.info('Gathered age_success to %s days', self.age_success)
            if self.duration and self.age_try and self.age_success:
                _log.info('I have all required Informations. Exiting backup loop')
                break


    def probe(self):
        """Create check metric for Backups

        'age_success' is the metric for the lastsuccessful backup
        'age_try' is the metric for the last try
        'duraction' is the metric for the duration of the last backup
        """
        self.duration = None
        self.age_try = None
        self.age_success = None

        self.check_backups()

        yield nagiosplugin.Metric('age_success', self.age_success, min=0)
        yield nagiosplugin.Metric('age_try', self.age_try, min=0)
        yield nagiosplugin.Metric('duration', self.duration, min=0)


class BackupSummary(nagiosplugin.Summary):
    """Create status line and long output.  """

    def verbose(self, results):
        super(BackupSummary, self).verbose(results)
        #if 'total' in results:
        #    return 'users: ' + ', '.join(results['total'].resource.users)


@nagiosplugin.guarded
def main():
    argp = argparse.ArgumentParser()
    argp.add_argument('-w', '--warning', metavar='RANGE',
                      help='warning if backup age is outside RANGE'),
    argp.add_argument('-c', '--critical', metavar='RANGE',
                      help='critical if backup age is outside RANGE')
    argp.add_argument('-v', '--verbose', action='count', default=0,
                      help='increase output verbosity (use up to 3 times)')
    argp.add_argument('-t', '--timeout', default=10,
                      help='abort execution after TIMEOUT seconds')
    argp.add_argument('--base-path', default="/srv/backup/",
                      help="Path to the bank of the vault (/srv/backup)")
    argp.add_argument('--max-duration', default=12.0,
                      help="max time in hours to take a backup (12.0)")
    argp.add_argument('vault', help='Name of the vault to check')
    args = argp.parse_args()
    check = nagiosplugin.Check(
        Backup(args.vault, args.base_path),
        nagiosplugin.ScalarContext('age_success', args.warning, args.critical,
                                   fmt_metric='Last successful backup is {value} days old'),
        nagiosplugin.ScalarContext('age_try', args.warning, args.critical,
                                   fmt_metric='Last backup tried {value} days ago'),
        nagiosplugin.ScalarContext('duration', args.warning, args.critical,
                                   fmt_metric='Last run took {value} hours'),
        BackupSummary())
    check.main(args.verbose, args.timeout)

if __name__ == '__main__':
    main()
