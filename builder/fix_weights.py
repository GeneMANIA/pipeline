
"""
2-column input: add a column of '1's for weight

3-column input: remove any values in 3rd column that are not numeric or have value <= 0

>3 columns: use first 3 only

<2 columns: error
"""

import argparse
import pandas as pd

SEP = '\t'


def main(inputfile, outputfile, logfile):

    try:
        data = pd.read_csv(inputfile, sep=SEP, header=None, na_filter=False)
    except ValueError as e:
        # probably the file is empty, create an empty 3 column data frame
        assert str(e) == 'No columns to parse from file'
        print('warning, file is empty: ' + inputfile)
        data = pd.DataFrame(columns=range(3))

    if len(data.columns) < 2:
        raise Exception("input file %s contains too few columns (%s)" % (inputfile, len(data.columns)))

    elif len(data.columns) == 2:
        data[2] = 1  # set weights to 1, making a third column

    if len(data.columns) > 3:
        print("Warning: input file %s contains too many columns (%s), using first 3" % (inputfile, len(data.columns)))
        data = data.ix[:, :3]

    else: 
        # drop non-numeric values in 3rd column
        data[2] = data[2].to_numeric()
        data.dropna(inplace=True)

    # write output
    data.to_csv(outputfile, sep=SEP, header=False, index=False)

    # write summary log TODO


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='scrub attributes against identifiers')

    parser.add_argument('inputfile', type=str,
                        help='input attribute file')

    parser.add_argument('outputfile', type=str,
                        help='name of clean output file')

    parser.add_argument('--log', type=str,
                        help='name of report log file')

    args = parser.parse_args()
    main(args.inputfile, args.outputfile, args.log)
