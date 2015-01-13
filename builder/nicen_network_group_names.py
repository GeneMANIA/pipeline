
"""add nice network group names like Co-expression
from short group codes like coexp where available.
if not defined, use the code as the name.
"""


import argparse
import pandas as pd


def main(metadata_file, group_file, output_file):
    metadata = pd.read_csv(metadata_file, sep='\t', encoding='UTF8', dtype=str, header=0, index_col=False)
    group_names = pd.read_csv(group_file, sep='\t', header=0)

    group_names.columns=['group', 'group_name']

    merged_metadata = pd.merge(metadata, group_names, on='group', how='left')
    merged_metadata['group_name'].fillna(merged_metadata['group'], inplace=True)

    merged_metadata.to_csv(output_file, sep='\t', header=True, index=False, encoding='UTF8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='join a pair of tables')

    parser.add_argument('metadata')
    parser.add_argument('groups')
    parser.add_argument('output')

    args = parser.parse_args()

    main(args.metadata, args.groups, args.output)


