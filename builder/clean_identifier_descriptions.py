

import argparse
import extract_identifiers

# TODO: actually extract all matching descriptions from filename,
# possibly using symbol names instead of ids, fill in any missing
# ones with empty strings. think about columns naming, maybe not needed here
def main(identifiers_filename, descriptions_filename_list,
         output_filename, report_filename):

    idents = extract_identifiers.load_identifiers(identifiers_filename)
    idents.reset_index(inplace=True)
    idents.drop_duplicates(subset=['GMID'], inplace=True)
    idents.rename(columns={'SYMBOL': 'Definition'}, inplace=True)
    idents.to_csv(output_filename, sep='\t', header=True, index=False,
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