
"""
2-column input: add a column of '1's for weight

3-column input: remove any values in 3rd column that are not numeric or have value <= 0

>3 or <2 columns, error
"""

import argparse
import pandas as pd

SEP = '\t'


def main(inputfile, outputfile, logfile):

    indata = pd.read_csv(inputfile, sep=SEP, header=None, na_filter=False)

    
    if len(indata.columns) > 3:
        raise("input file %s contains too many columns (%s)" % (inputfile, len(indata.columns)))

    elif len(indata.columns) < 2:
        raise("input file %s contains too few columns (%s)" % (inputfile, len(indata.columns)))

    elif len(indata.columns) == 2:
        indata[2] = 1  # set weights to 1, making a third column

    else: 
        # drop non-numeric values in 3rd column
        indata[2] = indata[2].convert_objects(convert_numeric=True)
        indata.dropna(inplace=True)

    # write output
    indata.to_csv(outputfile, sep=SEP, header=False, index=False)

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
