
"""some simple data processing steps amount to little more
than a join on two data files by a column. This just wraps
the corresponding pandas operation.
"""


import argparse
import pandas as pd


def main(left_file, right_file, output_file, join_column):
    left_df = pd.read_csv(left_file, sep='\t', header=0,
                          encoding='UTF8', dtype=str)
    right_df = pd.read_csv(right_file, sep='\t', header=0,
                           encoding='UTF8', dtype=str)

    merged = pd.merge(left_df, right_df, how='inner', on=join_column)

    merged.to_csv(output_file, sep='\t', header=True, index=False,
                  encoding='UTF8')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='join a pair of tables')

    parser.add_argument('left_file')
    parser.add_argument('right_file')
    parser.add_argument('output_file')
    parser.add_argument('join_column')

    args = parser.parse_args()

    main(args.left_file, args.right_file, args.output_file, args.join_column)


