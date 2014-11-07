
"""
load the given var from the given config
file and print. Allows us to decouple some
processing scripts from the config file by
loading the needed params into shell vars
via:

  VAL = $(python getparam.py myfile.conf myvar)

"""

import argparse
from configobj import ConfigObj

def getparam(filename, varname, default=None):
    try:
        cfg = ConfigObj(filename, encoding='UTF8')
        value = cfg[varname]
        return value
    except KeyError as e:
        if default is not None:
            return default
        else:
            raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='extract and print config var from file')

    parser.add_argument('config_file')
    parser.add_argument('var_name')
    parser.add_argument('--default')

    args = parser.parse_args()

    value = getparam(args.config_file, args.var_name, args.default)
    print(value)
