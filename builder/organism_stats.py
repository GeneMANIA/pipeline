
"""compute summary statistics for an organism,
for the final processed data:

 * number of genes
 * number of networks
 * number of interactions
 * number of attribute sets
 * number of attributes

"""

import argparse, datetime
import pandas as pd


def main(network_metadata_file, attribute_metadata_file, symbols_file, output_file):

    network_metadata = pd.read_csv(network_metadata_file, sep='\t', header=0, index_col=0,
                                   encoding='UTF8')

    num_networks = len(network_metadata)
    num_interactions = network_metadata['num_interactions'].sum()

    symbols = pd.read_csv(symbols_file, sep='\t', na_filter=False, encoding='UTF8',
                          dtype=str, names=['id', 'symbol', 'source'])
    num_genes = symbols['id'].nunique()

    # TODO: attributes
    num_attribute_sets = 0
    num_attributes = 0

    today = datetime.date.today()
    date = today.strftime('%Y-%m-%d')

    # write out
    stats = pd.DataFrame([[1, 1, num_networks, num_interactions, num_genes, 0, date]],
        columns=['ID', 'organisms', 'networks', 'interactions', 'genes', 'predictions', 'date'])

    stats.to_csv(output_file, sep='\t', header=False, index=False)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='compute organism data statistics')

    parser.add_argument('network_metadata', help='table of processed network metadata')
    parser.add_argument('attribute_metadata', help='table of processed attribute metadata')
    parser.add_argument('symbols', help='cleaned symbols file')

    parser.add_argument('output', help='resulting summary table')

    args = parser.parse_args()
    main(args.network_metadata, args.attribute_metadata, args.symbols, args.output)
