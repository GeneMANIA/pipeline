
"""
filter a given file by requiring the values in a specified column
exist in a specified column of a second file of acceptable values

e.g. filter a network by requiring identifiers existing in a file
of valid identifiers
"""

import argparse
import pandas as pd

SEP = '\t'


def main(inputfile, identsfile, outputfile, col, symbolcol, logfile):

    idents = pd.read_csv(identsfile, sep=SEP, header=None, na_filter=False, dtype=str)
    indata = pd.read_csv(inputfile, sep=SEP, header=None, na_filter=False, dtype=str)

    # upper case the columns we are filtering by, since we
    # want case insensitive compare
    idents['_upper'] = idents[symbolcol].str.upper()
    indata['_upper'] = indata[col].str.upper()

    # remove unknowns
    clean_data = indata[indata['_upper'].isin(idents['_upper'])]
    clean_data = clean_data.copy() # not a view so can change without causing pandas to complain

    # drop the _upper column, write output
    clean_data.drop('_upper', axis=1, inplace=True)
    clean_data.to_csv(outputfile, sep=SEP, header=False, index=False)

    # write summary log TODO


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='scrub attributes against identifiers')

    parser.add_argument('inputfile', type=str,
                        help='input attribute file')

    parser.add_argument('identsfile', type=str,
                        help='name of identifiers file')

    parser.add_argument('outputfile', type=str,
                        help='name of clean output file')

    parser.add_argument('--col', type=int, default=1,
                        help='default 1')

    parser.add_argument('--symbolcol', type=int, default=2,
                        help='default 2')

    parser.add_argument('--log', type=str,
                        help='name of report log file')

    args = parser.parse_args()

    # -1 because functions are 0-indexed, only use api is 1-indexed
    main(args.inputfile, args.identsfile, args.outputfile, args.col-1, args.symbolcol-1, args.log)
