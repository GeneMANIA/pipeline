
'''
for integration with lucene index creation prog, create
an config file in old format
'''

import argparse
from configobj import ConfigObj

def main(config, output):

    cfg = ConfigObj(config, encoding='utf8')
    short_id = cfg['short_id']

    compat_cfg = ConfigObj(encoding='utf8')

    compat_cfg['FileLocations'] = {}
    compat_cfg['FileLocations']['generic_db_dir'] = 'generic_db'

    compat_cfg['Organisms'] = {}
    compat_cfg['Organisms']['organisms'] = [short_id]

    compat_cfg[short_id] = {}
    compat_cfg[short_id]['name'] = cfg['name']
    compat_cfg[short_id]['short_name'] = cfg['short_name']
    compat_cfg[short_id]['common_name'] = cfg['common_name']
    compat_cfg[short_id]['gm_organism_id'] = cfg['gm_organism_id']

    compat_cfg.filename = output
    compat_cfg.write()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='create config for generic2lucene')
    parser.add_argument("config", type=str, help="organism config file")
    parser.add_argument("output", type=str, help="config file in old format")

    args = parser.parse_args()

    main(args.config, args.output)