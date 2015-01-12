
'''
create a clean description file, containing only descriptions
for those attributes present after cleaning, and with empty descriptions
for any attributes lacking descriptions at all. Add an internal ID column
enumerating the attributes.
'''

import argparse
import pandas as pd


def main(attribute_file, description_file, output_file):

    attribs = pd.read_csv(attribute_file, sep='\t', header=None, na_filter=False,
                          names=['GENE', 'ATTRIBUTE'])

    descs = pd.read_csv(description_file, sep='\t', header=None, na_filter=False,
                        names=['ATTRIBUTE', 'DESCRIPTION'])

    # remove duplicates in the descriptions
    descs.drop_duplicates(subset=['ATTRIBUTE'], inplace=True)

    # dedup attributes, since they can appear in multiple rows
    # by being associated with different genes
    attribs = attribs[['ATTRIBUTE']]
    attribs.drop_duplicates(inplace=True)

    # left merge, to grab only the needed descriptions
    output = pd.merge(attribs[['ATTRIBUTE']], descs[['ATTRIBUTE', 'DESCRIPTION']],
                      left_on='ATTRIBUTE', right_on='ATTRIBUTE', how='left')

    # empty string for missing descriptions
    output.fillna('', inplace=True)

    # add ID
    output.reset_index(drop=True, inplace=False)
    output.index += 1
    output.index.name = 'ID'

    # and that's all! write output
    output.to_csv(output_file, sep='\t', header=True, index=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="attribute description cleaning")

    parser.add_argument('attrib', type=str,
                        help='clean attribute file')

    parser.add_argument('desc', type=str,
                        help='all available attribute descriptions')

    parser.add_argument('output', type=str,
                        help='clean description file')

    args = parser.parse_args()
    main(args.attrib, args.desc, args.output)
