
import argparse
import pandas as pd
from configobj import ConfigObj

DEFAULT = ''

# bool doesn't work as naively expected in argparse, see
# http://stackoverflow.com/questions/15008758/parsing-boolean-values-with-argparse
def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")

def main(filenames, params, enumerate, outputfile, key_ext):
    rows = []
    for filename in filenames:
        config = ConfigObj(filename, encoding='utf8')

        if params == ['*']:
            params = config.keys()

        # find id corresponding to file
        if key_ext is not None:
            if filename.endswith(key_ext):
                dataset_key = filename[:-len(key_ext)]
            else:
                raise Exception('Unexpected file extension in:' + filename)
        else:
            dataset_key = filename

        row = [dataset_key] + [config.get(param, DEFAULT) for param in params]
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

    parser.add_argument('--key_ext', type=str,
                        help='to determine dataset key, from filename = key.key_ext')

    args = parser.parse_args()
    main(args.filenames, args.cfgparams, args.enumerate, args.output, args.key_ext)