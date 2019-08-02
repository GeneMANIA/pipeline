"""
given a file containing gene symbols and attribute identifiers,
produce an corresponding output file containing genemania internal
gene ids and attribute ids
"""

import argparse
import pandas as pd

def main(filename, lin_attr_id, mapping, output):

    lin_attr_id = pd.read_csv(lin_attr_id, sep='\t',
        na_filter=False)
    lin_attr_id['ATTRIBUTE_SYMBOL'] = lin_attr_id['EXTERNAL_ID'].str.upper()

    symbols = pd.read_csv(mapping, sep='\t', header=None, na_filter=False, names=['NODE_ID', 'GENE_SYMBOL', 'SOURCE'])
    symbols['GENE_SYMBOL'] = symbols['GENE_SYMBOL'].str.upper()

    data = pd.read_csv(filename, sep='\t', header=None, na_filter=False, names=['GENE_SYMBOL', 'ATTRIBUTE_SYMBOL'])
    data['GENE_SYMBOL'] = data['GENE_SYMBOL'].str.upper()
    data['ATTRIBUTE_SYMBOL'] = data['ATTRIBUTE_SYMBOL'].astype(str).str.upper()

    # apply cleaning by joining on the tables of clean symbols
    # TODO: maybe can make this faster by putting the join column as the index?
    clean = pd.merge(data, symbols, on='GENE_SYMBOL', how='inner')
    clean = pd.merge(clean, lin_attr_id, on='ATTRIBUTE_SYMBOL', how='inner')

    # dedup
    clean = clean[['NODE_ID', 'LINEARIZED_ID']]
    clean.drop_duplicates(inplace=True)
    clean.sort_index(inplace=True)

    # write output
    clean.to_csv(output, sep='\t', header=False, index=False)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='attribute id remapping')

    parser.add_argument('filename', help='attribute file')
    parser.add_argument('lin_attr_id', help='linearized attribute id file')
    parser.add_argument('mapping', help='cleaned gene symbols')
    parser.add_argument('output', help='output file')

    args = parser.parse_args()
    main(args.filename, args.lin_attr_id, args.mapping, args.output)
