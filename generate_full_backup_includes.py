#! /usr/bin/python3

"""
This script generates a include list (for rsync) for every suspected backup directory.

If it is a dirvirsh directory, it find the latest complete snapshot.
"""
import logging
import os

import check_dirvish
from IPython import embed as ipy

config = {
    'base_pathL' :['/srv/backup'],
    # all files with entries in this directory,
    # that match are ignored as possible dirvish vaults
    'blackListFileDir' : '/etc/ptx_backup/blacklist.d/',
    'blackListFileExtension' : '.list',
}

log = logging.getLogger('nagiosplugin')

def backup_dir(bank, vault):
    log.debug('Check %r as a valid dirvish backup.', os.path.join(bank, vault))
    backup = check_dirvish.Backup(vault, bank)
    print(backup.vault_base_path)
    backup.duration = None
    backup.last_try = None
    backup.last_success = None

    try:
        backup.check_path_accessible(backup.base_path)
        backup.check_path_accessible(backup.vault_base_path)
        backup.check_valid_dirvish_vault()
    except check_dirvish.E_PathNotAccessible as e:
        return None
    except check_dirvish.E_VaultIsNotDirvishDirectory as e:
        return None
    backup.check_backups()
    last_backup_subdir = getattr(backup, 'last_successful_backup', None)
    if last_backup_subdir:
        return os.path.join(bank, vault, last_backup_subdir)
    return None

def filter_list():
    """ find all files in config['blackListFileDir'] that ends in '.list', read
        them to return one list with all entries
    """
    filterL = []
    for root, dirs, files in os.walk(config['blackListFileDir']):
        for file in files:
            if file.endswith(config['blackListFileExtension']):
                with open(os.path.join(root, file), 'r') as f:
                    for l in f.readlines():
                        sl = os.path.realpath(l.strip())
                        if l.startswith('#'): next
                        if sl:
                            filterL.append(sl)
    return filterL
    
def is_blacklisted(path, filter_list):
    """ check if a path is blacklisted """
    _path = os.path.normpath(path)
    _filterL = filter(lambda e,_path=_path: os.path.commonprefix([_path, e])==e, filter_list)
    filtered = bool(list(_filterL))
    if filtered:
        print('filtered %r' % path)
    return filtered

if __name__=='__main__':
    resultL = []
    filterL = filter_list()
    print("generated Filterlist: \n%r" % filterL)

    for base_path in config['base_pathL']:
        log.debug('Find dirvish vaults in path %r', base_path)
        for dirname, dirnames, _ in os.walk(base_path):
            for possible_vault in dirnames:
                possible_vault_dir = os.path.join(base_path, possible_vault)
                if is_blacklisted(possible_vault_dir, filterL): 
                    continue
                print("Check directory in %r/%r" %(base_path, possible_vault))
                backupDir = backup_dir(base_path, possible_vault)
                if backupDir:
                    resultL.append(os.path.join(backupDir))
            dirnames.clear()
    with open('/etc/ptx_backup/dirvish.vault.list', 'w') as file:
        for backup_dir in sorted(resultL):
           print('%s/**' % backup_dir, file=file)
