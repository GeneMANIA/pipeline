
import argparse
import pandas as pd

def main(filename, desc, mapping, output):

    desc = pd.read_csv(desc, sep='\t', header=False, na_filter=False, names = ['ATTRIBUTE_ID', 'ATTRIBUTE_SYMBOL', 'DESCRIPTION'])
    desc['ATTRIBUTE_SYMBOL'] = desc['ATTRIBUTE_SYMBOL'].str.upper()

    symbols = pd.read_csv(mapping, sep='\t', header=None, na_filter=False, names=['NODE_ID', 'GENE_SYMBOL', 'SOURCE'])
    symbols['GENE_SYMBOL'] = symbols['GENE_SYMBOL'].str.upper()

    data = pd.read_csv(filename, sep='\t', header=None, na_filter=False, names=['GENE_SYMBOL', 'ATTRIBUTE_SYMBOL'])
    data['GENE_SYMBOL'] = data['GENE_SYMBOL'].str.upper()
    data['ATTRIBUTE_SYMBOL'] = data['ATTRIBUTE_SYMBOL'].str.upper()

    # apply cleaning by joining on the tables of clean symbols
    # TODO: maybe can make this faster by putting the join column as the index?
    clean = pd.merge(data, symbols, on='GENE_SYMBOL', how='inner')
    clean = pd.merge(clean, desc, on='ATTRIBUTE_SYMBOL', how='inner')

    # dedup
    clean = clean[['NODE_ID', 'ATTRIBUTE_ID']]
    clean.drop_duplicates(inplace=True)
    clean.sort(inplace=True)

    # write output
    clean.to_csv(output, sep='\t', header=False, index=False)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='attribute id remapping')

    parser.add_argument('filename', help='attribute file')
    parser.add_argument('desc', help='cleaned attribute id/name/descriptions')
    parser.add_argument('mapping', help='cleaned gene symbols')
    parser.add_argument('output', help='output file')

    args = parser.parse_args()
    main(args.filename, args.desc, args.mapping, args.output)