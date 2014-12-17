
'''
for integration with lucene index creation prog, create
a config file in old format
'''

import argparse
from configobj import ConfigObj


def main(configs, output):

    compat_cfg = ConfigObj(encoding='utf8')

    compat_cfg['FileLocations'] = {}
    compat_cfg['FileLocations']['generic_db_dir'] = 'generic_db'

    all_short_ids = []

    for config in configs:

        cfg = ConfigObj(config, encoding='utf8')

        # if the original config has a short_id, use it, but if
        # not, synthesize one from the genemania organism id. hopefully
        # just needed for compatability with the index building program
        # until its revised, can drop after that. TODO
        if 'short_id' in cfg:
            short_id = cfg['short_id']
        else:
            short_id = 'GMORG' + str(cfg['gm_organism_id'])

        all_short_ids.append(short_id)

        compat_cfg[short_id] = {}
        compat_cfg[short_id]['name'] = cfg['name']
        compat_cfg[short_id]['short_name'] = cfg['short_name']
        compat_cfg[short_id]['common_name'] = cfg['common_name']
        compat_cfg[short_id]['gm_organism_id'] = cfg['gm_organism_id']

        compat_cfg.filename = output

    compat_cfg['Organisms'] = {}
    compat_cfg['Organisms']['organisms'] = all_short_ids

    compat_cfg.write()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='create config for generic2lucene')
    parser.add_argument("config", type=str, help="one ore more organism config files", nargs='+')
    parser.add_argument("output", type=str, help="output config file produced in old format")

    args = parser.parse_args()

    main(args.config, args.output)