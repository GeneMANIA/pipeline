
"""for ease of use, we don't require a network data file
have any metadata. here we set some default for missing
values, e.g.:

  * network group to 'other'


what else? network name defaults to file name later
in the processing in the name generation.
"""

import argparse
import pandas as pd


def main(inputfile, outputfile):

    metadata = pd.read_csv(inputfile, sep='\t', encoding='UTF8', dtype=str)

    metadata.fillna({'group': 'other'}, inplace=True)

    metadata.to_csv(outputfile, sep='\t', header=True, index=False, encoding='UTF8')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='assign required values for network metadata')

    parser.add_argument('inputfile', help='input network metadata file')
    parser.add_argument('outputfile', help='output network metadata file')

    args = parser.parse_args()
    main(args.inputfile, args.outputfile)



