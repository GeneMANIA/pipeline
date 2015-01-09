

import argparse
import extract_identifiers
import pandas as pd


def main(identifiers_filename, descriptions_filename_list,
         output_filename, report_filename):

    # loading the cleaned identifiers gives the list for which we need descriptions
    idents = extract_identifiers.load_identifiers(identifiers_filename)
    idents.reset_index(inplace=True)
    idents.drop_duplicates(subset=['GMID'], inplace=True)

    # load in all the available descriptions
    descs_list = []
    for filename in descriptions_filename_list:
        df = pd.read_csv(filename, sep='\t', names=['GMID', 'Definition'],
                         na_filter=False, header=None)
        descs_list.append(df)

    descs = pd.concat(descs_list, ignore_index=True)
    descs.drop_duplicates(subset=['GMID'], inplace=True)

    # left join onto the identifiers, so any
    # descriptions we don't need get dropped, and we
    # get empty entries where we have no description
    # for the symbol
    wanted_descs = pd.merge(idents, descs, on='GMID', how='left')
    wanted_descs.fillna('', inplace=True)

    # write output
    wanted_descs.to_csv(output_filename, sep='\t', header=True, index=False,
                  columns=['GMID', 'Definition'])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='scrub identifier descriptions')

    parser.add_argument('identifiers', type=str,
                        help='clean deduped file containing identifier triplets')

    parser.add_argument('descriptions', type=str, nargs='*',
                        help='list if descriptions files')

    parser.add_argument('--output', type=str,
                        help='name of clean output file, stdout if missing')

    parser.add_argument('--log', type=str,
                        help='name of report log file')

    args = parser.parse_args()
    main(args.identifiers, args.descriptions, args.output, args.log)