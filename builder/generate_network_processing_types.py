
"""add a string describing how the network was generated,
based on its location in the folder hierarchy which affects
the processing rules
"""


import argparse
import pandas as pd


def main(metadata_file, proctypes_file, output_file):
    metadata = pd.read_csv(metadata_file, sep='\t', encoding='UTF8', dtype=str, header=0, index_col=False)
    proctypes = pd.read_csv(proctypes_file, sep='\t', header=0)

    # rename colums for convenience
    assert list(proctypes.columns) == ['LOCATION', 'PROCESSING_TYPE']
    proctypes.columns = ['key_prefix', 'processing_type']

    # first two parts of dataset key
    metadata['key_prefix'] = metadata['dataset_key'].str.split('/')
    metadata['key_prefix'] = metadata['key_prefix'].apply(lambda x: '/'.join(x[:2]))

    # join to get processing type, then cleanup and write
    merged_metadata = pd.merge(metadata, proctypes, on='key_prefix', how='left')
    merged_metadata['processing_type'].fillna('', inplace=True)
    merged_metadata.drop(['key_prefix'], axis=1, inplace=True)

    merged_metadata.to_csv(output_file, sep='\t', header=True, index=False, encoding='UTF8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='join a pair of tables')

    parser.add_argument('metadata')
    parser.add_argument('proctypes')
    parser.add_argument('output')

    args = parser.parse_args()

    main(args.metadata, args.proctypes, args.output)


