

import argparse
import pandas as pd


def main(filenames, outputfile):

    df = pd.DataFrame(filenames)
    df.to_csv(outputfile, sep='\t', header=False, index=True)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='enumerate')

    parser.add_argument('filenames', metavar='files', type=str, nargs='+',
                        help='list of input files')

    parser.add_argument('output', type=str,
                        help='name of clean output file, stdout if missing')

    args = parser.parse_args()
    main(args.filenames, args.output)