

import argparse
import pandas as pd
from configobj import ConfigObj


def main(cfg, outputfile):
    outfile_header = ['ID', 'NAME', 'DESCRIPTION', 'ALIAS', 'ONTOLOGY_ID', 'TAXONOMY_ID']

    config = ConfigObj(cfg, encoding='utf8')

    df = pd.DataFrame(columns=outfile_header, index=[1]) # prealloc 1 row

    df['ID'] = config['gm_organism_id']
    df['NAME'] = config['common_name']
    df['DESCRIPTION'] = config['short_name']
    df['ALIAS'] = config['name']
    df['ONTOLOGY_ID'] = config['gm_organism_id'] # using organism id here as well, since only 1 enrichment ontology per organism
    df['TAXONOMY_ID'] = config['ncbi_taxonomy_id']

    df.to_csv(outputfile, sep='\t', header=False, index=False)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='export organisms')

    parser.add_argument('cfg', type=str,
                        help='organism config file')

    parser.add_argument('output', type=str,
                        help='output file')

    args = parser.parse_args()
    main(args.cfg, args.output)