#  monitoring-check-dirvish

```
# ./check_dirvish.py  -h
usage: check_dirvish.py [-h] [-w RANGE] [-c RANGE] [-v] [-t TIMEOUT]
                        [--base-path BASE_PATH] [--max-duration RANGE]
                        vault

positional arguments:
  vault                 Name of the vault to check

optional arguments:
  -h, --help            show this help message and exit
  -w RANGE, --warning RANGE
                        warning if backup age is outside RANGE in seconds
  -c RANGE, --critical RANGE
                        critical if backup age is outside RANGE in seconds
  -v, --verbose         increase output verbosity (use up to 3 times)
  -t TIMEOUT, --timeout TIMEOUT
                        abort execution after TIMEOUT seconds
  --base-path BASE_PATH
                        Path to the bank of the vault (/srv/backup)
  --max-duration RANGE  max time in hours to take a backup (3600) in seconds
```
