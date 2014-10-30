
import argparse
import pandas as pd




def main(inputfile, outputfile):

    metadata = pd.read_csv(inputfile, sep='\t', header=0, index_col=True)

    # TODO: call connect existing metadata processing for pubmed, name dedup. and add
    # in the network group classification somehow

    processed_metadata = metadata.copy()

    # write output
    processed_metadata.to_csv(outputfile, sep='\t', header=True, index=True)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='network metadata processing')

    parser.add_argument('input', type=str,
                        help='input metadata file')

    parser.add_argument('output', type=str,
                    help='processed metadata file')

    args = parser.parse_args()
    main(args.input, args.output)




