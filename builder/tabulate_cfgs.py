
"""takes a list of property/config/ini style files
containing name=value pairs, and returns a single table
in a file that has name in the columns, the values in the
rows, one row for each of the input files. The input file
itself is included as an additional colummn, as is optionally
an extra integer id column.
"""

import argparse
import pandas as pd
from configobj import ConfigObj
from buildutils import str2bool

DEFAULT = ''

def fix_newlines(row):
    '''
    some fields may contain newlines, replace with spaces

    alternative would be to html-escape??
    '''

    return [' '.join(item.split('\n')) for item in row]


def main(filenames, params, enumerate, outputfile, key_lstrip=None, key_rstrip=None):
    rows = []
    for filename in filenames:
        config = ConfigObj(filename, encoding='utf8')

        if params == ['*']:
            params = config.keys()

        # find id corresponding to file
        dataset_key = filename
        if key_lstrip and dataset_key.startswith(key_lstrip):
            dataset_key = dataset_key[len(key_lstrip):]
        if key_rstrip and dataset_key.endswith(key_rstrip):
            dataset_key = dataset_key[:-len(key_rstrip)]

        row = [dataset_key] + [config.get(param, DEFAULT) for param in params]
        row = fix_newlines(row)
        rows.append(row)

    header_params = ['dataset_key'] + params
    df = pd.DataFrame(rows, columns=header_params)
    df.index += 1 # start index at 1 not 0
    df.index.name='id'
    df.to_csv(outputfile, sep='\t', header=True, index=enumerate)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='tabulate')

    parser.add_argument('filenames', metavar='files', type=str, nargs='+',
                        help='list of input files')

    parser.add_argument('output', type=str,
                        help='name of clean output file, stdout if missing')

    parser.add_argument('--cfgparams', type=str, nargs='+',
                        help='list of parameter names in the config files', default=['*'])

    parser.add_argument('--missing', type=str,
                        help='what to do when a param is missing, ... TODO')

    parser.add_argument('--enumerate', type=str2bool, default=True,
                        help='also add an id column along with file path')

    parser.add_argument('--key_lstrip', type=str,
                        help='remove given string from left of filename to compute dataset key')

    parser.add_argument('--key_rstrip', type=str,
                        help='remove given string from right of filename to compute dataset key')

    args = parser.parse_args()
    main(args.filenames, args.cfgparams, args.enumerate, args.output, args.key_lstrip, args.key_rstrip)