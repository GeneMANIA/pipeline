
import argparse

SEP = '\t'


def main(inputfile, outputfile, col, keep, logfile):

    data = open(inputfile).read().splitlines()

    with open(outputfile, 'w') as out:

        for line in data:
            parts = line.split(SEP)

            # skip lines with no ragged parts, including those that are too short
            if len(parts) < col:
                continue

            # write out
            for i in range(col, len(parts)):
                if keep is not None:
                    newline_parts = [parts[j] for j in keep]
                else:
                    newline_parts = parts[:col]

                newline_parts.append(parts[i])
                out.write(SEP.join(newline_parts) + '\n')

    # write summary log TODO


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='create a regular tabular data file from ragged input file')

    parser.add_argument('inputfile', type=str,
                        help='input attribute file')

    parser.add_argument('outputfile', type=str,
                        help='name of clean output file')

    parser.add_argument('--col', type=int, default=1,
                        help='position of first ragged column, default 1')

    parser.add_argument('--keep', type=int, nargs='+', default=None,
                        help='for columns before --col, list of those to preserve in output, defaults to all if not given')

    parser.add_argument('--log', type=str,
                        help='name of report log file')

    args = parser.parse_args()
    main(args.inputfile, args.outputfile, args.col, args.keep, args.log)
