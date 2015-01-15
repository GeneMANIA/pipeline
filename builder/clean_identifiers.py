

import argparse
from identifiers import identifier_merger
from buildutils import str2bool

def main(filenames, output_filename, report_filename, merge_names, filters):
    print(filenames)
    print(output_filename)

    reverse_filename = None
    temp_dir = None
    org_prefix = None

    # these inputs have no biotype, filtering should have been performed
    # upstream. TODO: doesn't make sense to have this as a param, should
    # update triplets_to_processed() instead.
    biotypes = ['True']

    identifier_merger.triplets_to_processed(filenames, reverse_filename,
                                            output_filename, report_filename,
                                            org_prefix, temp_dir, biotypes, filters,
                                            merge_names)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='scrub identifiers')

    parser.add_argument('filenames', metavar='files', type=str, nargs='+',
                        help='list if input files')

    parser.add_argument('--output', type=str,
                        help='name of clean output file, stdout if missing')

    parser.add_argument('--log', type=str,
                        help='name of report log file')

    parser.add_argument('--merge_names', help='merge identifiers with matching names',
                        type=str2bool, default=False)

    parser.add_argument('--ignore', help='ignore identifiers with given sources, comma delimted list with no space',
                        default=[])

    args = parser.parse_args()
    main(args.filenames, args.output, args.log, args.merge_names, args.ignore)