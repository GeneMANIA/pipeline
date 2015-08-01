"""
remove duplicate rows accounting for symbols that
map to the same underlying gene
"""

import argparse
import pandas as pd

SEP = '\t'

# Q: if no matches on symbol, should we raise an exception or just allow to drop?

def main(inputfile, identsfile, outputfile, col, symbolcol, idcol, logfile):

    # index col #s from 0 instead of 1
    symbolcol -= 1
    idcol -= 1
    col -= 1

    idents = pd.read_csv(identsfile, sep=SEP, header=None, na_filter=False)
    assert len(idents.columns) == 3
    idents.columns = ['GMID', 'SYMBOL', 'SOURCE']

    # create empty output file if no input data. the read_csv fails if no
    # columns given so we can't test if the resulting dataframe is empty
    # so catch the exception and handle
    try:
        indata = pd.read_csv(inputfile, sep=SEP, header=None, na_filter=False)
    except ValueError as e:
        if str(e) == "No columns to parse from file":
            open(outputfile, 'w').close() # create empty output
            return
        else:
            raise

    indata_cols = list(indata.columns)

    # upper case the columns we are filtering by, since we
    # want case insensitive compare
    idents = idents[[idcol, symbolcol]]
    idents.columns = ['GMID', 'SYMBOL']

    idents['SYMBOL_UPPER'] = idents['SYMBOL'].str.upper()
    indata['SYMBOL_UPPER'] = indata[col].str.upper()

    # join
    merged = pd.merge(indata, idents, on='SYMBOL_UPPER', how='inner')
    merged.drop(['SYMBOL_UPPER', 'SYMBOL'], axis=1, inplace=True)

    cols = list(merged.columns)
    cols.remove(col)

    # drop duplicates ignoring the symbol column
    merged.drop_duplicates(subset=cols, inplace=True)
    merged.drop('GMID', axis=1, inplace=True)

    # write output, same columns & order as input data
    merged.to_csv(outputfile, sep=SEP, header=False, index=False, columns=indata_cols)

    # write summary log TODO


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='remove duplicate rows, with respect to given identifiers')

    parser.add_argument('inputfile', type=str,
                        help='input attribute file')

    parser.add_argument('identsfile', type=str,
                        help='name of identifiers file')

    parser.add_argument('outputfile', type=str,
                        help='name of clean output file')

    parser.add_argument('--col', type=int, default=1,
                        help='default 1')

    parser.add_argument('--symbolcol', type=int, default=2,
                        help='column containing symbol in identifiers file, default 2')

    parser.add_argument('--idcol', type=int, default=1,
                        help='column containing id in indentifiers file, default 1')

    parser.add_argument('--log', type=str,
                        help='name of report log file')

    args = parser.parse_args()
    main(args.inputfile, args.identsfile, args.outputfile, args.col, args.symbolcol, args.idcol, args.log)
