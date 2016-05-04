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

class E_VaultIsNotDirvishDirectory(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return "Dirvish config in %r not found!" %repr(self.value)

class E_FileNotAccessible(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return "File %r is not accessible" %repr(self.value)

class Backup(nagiosplugin.Resource):
    """Domain model: Dirvish vaults"""

    def __init__(self, vault, base_path):
        self.vault = vault
        self.base_path = base_path
        self.vault_base_path = os.path.join(self.base_path, self.vault)
        self.valid_backup_found = 0
        self.backup_running_now = 0

    @property
    def name(self):
        """formatting the Testname (will be formatted as uppercase letters)"""
        return "%s %s" % (self.__class__.__name__, self.vault.split('.')[0])


    def check_path_accessible(self, directory):
        _log.debug("Check if %r is accessible and a directory", directory)
        if not os.access(directory, os.R_OK | os.X_OK):
            raise E_PathNotAccessible(directory)
        if not os.path.isdir(directory):
            raise E_PathNoDir(directory)
        return

    def check_file_accessible(self, filename):
        _log.debug("Check if %r is accessible", filename)
        if not os.access(filename, os.R_OK):
            raise E_FileNotAccessible(filename)
        return

    def backups(self):
        """Returns a iterable of backup-sub-directories"""
        _log.debug('Finding the latest backup for vault "%s"', self.vault)
        self.history_file = os.path.join(self.vault_base_path, 'dirvish', 'default.hist')
        _log.debug('Check for %r' % self.history_file)
        resultS = set()
        if os.access(self.history_file, os.R_OK):
            with open(self.history_file) as histfile:
                lines = histfile.readlines()[1:]
            for entry in reversed(lines):
                try:
                    last_entry = entry.strip()
                    image = last_entry.split('\t')[0]
                    _log.info("Found next backup in %r", image)
                except Exception as e:
                    _log.error("Something unexpected happened, while reading file %r", self.history_file)
                    next
                resultS.add(image)
        for dirname, dirnames, filenames in os.walk(self.vault_base_path):
            _log.info("Adding directories in %r", self.vault_base_path)
            # files that should be in every dirvish backup directory:
            mustHaveS = {'log', 'summary', 'tree'}
            for directory in dirnames:
                dirCont = set(os.listdir(os.path.join(self.vault_base_path, directory)))
                if mustHaveS.issubset(dirCont):
                    resultS.add(directory)
            dirnames.clear()
        _log.info("Found possible backups: %r", resultS)
        return resultS

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
        backups = self.backups()
        if len(backups) == 0:
            self.valid_backup_found = 0
            return
        for backup in reversed(sorted(backups)):
            try:
                parsed_backup = self.parse_backup(backup, ['status', 'backup-begin', 'backup-complete'])
            except E_PathNotAccessible as e:
                _log.debug("Exception thrown: %s", e)
                continue
            begin = dateutil.parser.parse(parsed_backup['backup-begin'])
            _log.debug("Backup begin %r to %r", parsed_backup['backup-begin'], begin)
            if parsed_backup.get('backup-complete') is None:
                # backup is probably still running or was killed hard!
                self.backup_running_now = round((datetime.datetime.now() - begin).total_seconds())
                continue
            end = dateutil.parser.parse(parsed_backup['backup-complete'])
            _log.debug("Backup end %r to %r", parsed_backup.get('backup-complete'), end)
            dur = end - begin
            _log.debug("Duration is: %s", dur)
            if self.duration is None:
                self.duration = round(dur.total_seconds())
                _log.info('Gathered last duration to %s hours', dur)
            if self.last_try is None:
                age = datetime.datetime.now() - begin
                self.last_try = round(age.total_seconds())
                _log.info('Gathered last_try to %s days', age)
            if parsed_backup['status'].casefold() == "success":
                _log.debug('Valid backup found: %r', backup)
                self.valid_backup_found = 1
                if self.last_success is None:
                    age = datetime.datetime.now() - begin
                    self.last_success = round(age.total_seconds())
                    _log.info('Gathered last_success to %s', age)
            if self.duration and self.last_try and self.last_success:
                _log.info('I have all required Informations. Exiting backup loop')
                break



    def check_valid_dirvish_vault(self):
        _log.debug("Check if %r is a dirvish vault", self.vault)
        dirvish_dir = os.path.join(self.vault_base_path, 'dirvish')
        try:
            self.check_path_accessible(dirvish_dir)
            self.check_file_accessible(os.path.join(dirvish_dir, 'default.conf'))
        except (E_PathNotAccessible, E_FileNotAccessible): 
            raise E_VaultIsNotDirvishDirectory(dirvish_dir)

    def probe(self):
        """Create check metric for Backups

        'last_success' is the metric for the lastsuccessful backup
        'last_try' is the metric for the last try
        'duraction' is the metric for the duration of the last backup
        """
        self.duration = None
        self.last_try = None
        self.last_success = None

        self.check_path_accessible(self.base_path)
        self.check_path_accessible(self.vault_base_path)
        self.check_valid_dirvish_vault()
        self.check_backups()

        # the order of metrices matters which human readable output you'll get!
        _log.debug('last_success is %r seconds ago <%r>', self.last_success, type(self.last_success))
        if isinstance(self.last_success, int):
            yield nagiosplugin.Metric('last_success', self.last_success, uom='s', min=0)
        _log.debug('last_try is %r seconds ago, <%r>', self.last_try, type(self.last_try))
        if isinstance(self.last_try, int):
            yield nagiosplugin.Metric('last_try', self.last_try, uom='s', min=0)
        _log.debug('duration is instance of: %r seconds <%r>', self.duration, type(self.duration))
        if isinstance(self.duration, int):
            yield nagiosplugin.Metric('duration', self.duration, uom='s', min=0)
        _log.debug('Running backup runs for: %r seconds <%r>', self.backup_running_now, type(self.backup_running_now))
        if self.backup_running_now:
            yield nagiosplugin.Metric('running_backup_for', self.backup_running_now, uom='s', min=0)
        _log.debug('Valid Backup found: %r <%r>', self.valid_backup_found, type(self.valid_backup_found))
        yield nagiosplugin.Metric('valid_backup_found', self.valid_backup_found, min=0, max=1)

class Duration_Fmt_Metric(object):
    """ this class only use is to format a metric containing timedeltas
        to print a human readable output like 7:30 or 6Y7d. """

    def __init__(self, fmt_string):
        self.fmt_string = fmt_string

    @staticmethod
    def seconds_human_readable(seconds):
        year   = 60*60*24*365
        month  = 60*60*24*30
        day    = 60*60*24
        hour   = 60*60
        minute = 60

        string = ""
        remaining_unitcount = 2
        years, remain = divmod(seconds, year)
        if years > 0:
            string += "%sY" % years
            seconds = remain
            remaining_unitcount -= 1
            if remaining_unitcount <=0:
                 return string
        months, remain = divmod(seconds, month)
        if months > 2:
            string += "%sM" % months
            seconds = remain
            remaining_unitcount -= 1
            if remaining_unitcount <=0:
                 return string
        days, remain = divmod(seconds, day)
        if days > 0:
            string += "%sd" % days
            seconds = remain
            remaining_unitcount -= 1
            if remaining_unitcount <=0:
                 return string
        hours, seconds = divmod(seconds, hour)
        minutes, seconds = divmod(seconds, minute)
        if remaining_unitcount > 1:
            string += "{0:0>2}h{1:0>2}".format(hours, minutes)
        else:
            string += "{0:0>2}h".format(hours)
        assert seconds < 60
        return string

    def __call__(self, metric, context):
        assert metric.uom == "s"
        valueunit = self.seconds_human_readable(int(metric.value))
        return self.fmt_string.format(
            name=metric.name, value=metric.value, uom=metric.uom,
            valueunit=valueunit, min=metric.min, max=metric.max)

class Bool_Fmt_Metric(object):
    """print a message for a bool-metric  """

    def __init__(self, msg_success, msg_fail):
        self.msg_success = msg_success
        self.msg_fail = msg_fail

    def __call__(self, metric, context):
        _log.debug('UOM: %r', metric.uom)
        if metric.value:
            return self.msg_success
        else:
            return self.msg_fail



@nagiosplugin.guarded
def main():
    argp = argparse.ArgumentParser()
    argp.add_argument('-w', '--warning', metavar='RANGE',
                      help='warning if backup age is outside RANGE in seconds'),
    argp.add_argument('-c', '--critical', metavar='RANGE',
                      help='critical if backup age is outside RANGE in seconds')
    argp.add_argument('-v', '--verbose', action='count', default=0,
                      help='increase output verbosity (use up to 3 times)')
    argp.add_argument('-t', '--timeout', default=10,
                      help='abort execution after TIMEOUT seconds')
    argp.add_argument('--base-path', default="/srv/backup/",
                      help="Path to the bank of the vault (/srv/backup)")
    argp.add_argument('--max-duration', default=3600, metavar='RANGE',
                      help="max time in hours to take a backup (3600) in seconds")
    argp.add_argument('vault', help='Name of the vault to check')
    args = argp.parse_args()
    check = nagiosplugin.Check(
        Backup(args.vault, args.base_path),
        nagiosplugin.ScalarContext( 'valid_backup_found', critical='0.5:1',
                                    fmt_metric = Bool_Fmt_Metric('Valid backup found!', 'No valid Backup found!')),
        nagiosplugin.ScalarContext( 'last_success', args.warning, args.critical,
                                    Duration_Fmt_Metric('Last successful backup is {valueunit} old')),
        nagiosplugin.ScalarContext( 'last_try', args.warning, args.critical,
                                    Duration_Fmt_Metric('Last backup tried {valueunit} ago')),
        nagiosplugin.ScalarContext( name = 'duration',
                                    warning = args.max_duration,
                                    fmt_metric = Duration_Fmt_Metric('Last backuprun took {valueunit}')),
        nagiosplugin.ScalarContext( name = 'running_backup_for',
                                    warning = args.max_duration,
                                    critical = args.max_duration*3,
                                    fmt_metric = Duration_Fmt_Metric('Running backup since {valueunit}')),)
    check.main(args.verbose, args.timeout)

if __name__ == '__main__':
    main()
