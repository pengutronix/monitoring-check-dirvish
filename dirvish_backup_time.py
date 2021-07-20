#! /usr/bin/python3

"""
This script generates a list of Timestamp - length 
if it is run in a dirvish vault.
"""
import logging
import os

import check_dirvish
from IPython import embed as ipy
import dateutil.parser

config = {
    'base_pathL' :['/srv/backup'],
}

log = logging.getLogger('dirvish_duration')

def backup_durations(bank, vault):
    log.debug('Check %r as a valid dirvish backup.', os.path.join(bank, vault))
    backups = check_dirvish.Backup(vault, bank)
    result = list()
    backups.duration=None
    backups.last_try=None
    backups.last_success=None

    try:
        backups.check_path_accessible(backups.base_path)
        backups.check_path_accessible(backups.vault_base_path)
        backups.check_valid_dirvish_vault()
    except check_dirvish.E_PathNotAccessible as e:
        return None
    except check_dirvish.E_VaultIsNotDirvishDirectory as e:
        return None
    backups.check_backups()
    for backup in backups.backups():
        try:
            d = backups.parse_backup(backup)
            
        except:
            continue
        try:
            end = dateutil.parser.parse(d['backup-complete'])
            begin = dateutil.parser.parse(d['backup-begin'])
        except KeyError:
            continue
        result.append((backup, end - begin))
    result.sort()
    return result


if __name__=='__main__':
    # current working directory
    cwd = os.getcwd()
    vault = os.path.basename(cwd)
    bank = os.path.dirname(cwd)
    result = backup_durations(bank, vault)
    for b, duration  in result:
        print(b , duration)
