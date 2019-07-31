

'''
compute basic network stats

file must be processed/clean:

 * 3 columns per row, node/node/weight
 * all weights > 0
 * symmetric without duplicate, that is if node1/node2/weightx in file, then
 * implied interaction node2/node1/weightx is not in the file, and no other
   weights for the pair node1/node2 are present

with these assumptions (which is what our tools produce) we don't have to do
much here.

output is in text file with

  var = value

pairs, for the following measures:

 * num_interactions
 * num_genes

'''

import argparse
import pandas as pd
from configobj import ConfigObj


def main(input_file, output_file):

    network = pd.read_csv(input_file, sep='\t', header=None, names=['gene1', 'gene2', 'weight'])

    # num interactions is just the line count
    num_interactions = len(network)

    # count genes
    all_genes = pd.concat([network[['gene1']], network[['gene2']]],sort=True)
    all_genes.drop_duplicates(inplace=True)
    num_genes = len(all_genes)


    # output
    cfg = ConfigObj(encoding='utf8')

    cfg['num_interactions'] = num_interactions
    cfg['num_genes'] = num_genes

    cfg.filename = output_file
    cfg.write()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='count interactions etc')

    parser.add_argument('input', type=str,
                    help='input network file')

    parser.add_argument('output', type=str,
                    help='output summary file')

    args = parser.parse_args()
    main(args.input, args.output)

