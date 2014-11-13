
"""interaction and attribute data is put in files named by IDs, to match
with IDs in the various metadata tables. Copy from processed data files to
the corresponding final ID-named file.

TODO: hardlink for performance/disk usage??
"""

import argparse, os, shutil
import pandas as pd


#TODO: move to utils, use in var
def strip_key(filename, key_lstrip, key_rstrip):
    dataset_key = filename
    if key_lstrip and dataset_key.startswith(key_lstrip):
        dataset_key = dataset_key[len(key_lstrip):]
    if key_rstrip and dataset_key.endswith(key_rstrip):
        dataset_key = dataset_key[:-len(key_rstrip)]

    return dataset_key


# interactions
def copy_interactions(mapfile, newdir, org_id, filenames, key_lstrip=None, key_rstrip=None):

    if os.path.exists(newdir):
        shutil.rmtree(newdir)
    os.mkdir(newdir)

    metadata = pd.read_csv(mapfile, sep='\t')
    filenames = pd.DataFrame(filenames, columns=['filename'])
    filenames['dataset_key'] = filenames['filename'].apply(lambda x: strip_key(x, key_lstrip, key_rstrip))

    joined = pd.merge(metadata, filenames, on='dataset_key', how='inner')

    assert len(joined) == len(metadata) == len(filenames)

    for index, row in joined.iterrows():
        network_id = row['id']
        filename = row['filename']

        link_name = os.path.join(newdir, '%s.%s.txt' % (org_id, network_id))
        #os.symlink(filename, link_name)
        shutil.copy(filename, link_name)


# attributes
def copy_attributes(mapfile, newdir, filenames, key_lstrip=None, key_rstrip=None):

    if os.path.exists(newdir):
        shutil.rmtree(newdir)
    os.makedirs(newdir)

    metadata = pd.read_csv(mapfile, sep='\t')
    filenames = pd.DataFrame(filenames, columns=['filename'])
    filenames['dataset_key'] = filenames['filename'].apply(lambda x: strip_key(x, key_lstrip, key_rstrip))

    joined = pd.merge(metadata, filenames, on='dataset_key', how='inner')

    assert len(joined) == len(metadata) == len(filenames)

    for index, row in joined.iterrows():
        id = row['id']
        filename = row['filename']

        link_name = os.path.join(newdir, '%s.txt' % (id))
        #os.symlink(filename, link_name)
        shutil.copy(filename, link_name)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='copy/rename or something i dunno')
    subparsers = parser.add_subparsers(dest='subparser_name')

    # interactions
    parser_interactions = subparsers.add_parser('interactions')
    parser_interactions.add_argument('mapfile', type=str,
                        help='name of clean output file, stdout if missing')

    parser_interactions.add_argument('newdir', type=str,
                        help='location of copied/linked files')

    parser_interactions.add_argument('orgid', type=str,
                        help='organism id to use in copied/linked fiels')

    parser_interactions.add_argument('filenames', metavar='files', type=str, nargs='+',
                        help='list of input files')

    parser_interactions.add_argument('--key_lstrip', type=str,
                                help='remove given string from left of filename to compute dataset key')

    parser_interactions.add_argument('--key_rstrip', type=str,
                                help='remove given string from right of filename to compute dataset key')

    # attribs
    parser_attribs = subparsers.add_parser('attribs')
    parser_attribs.add_argument('mapfile', type=str,
                        help='name of clean output file, stdout if missing')

    parser_attribs.add_argument('newdir', type=str,
                        help='location of copied/linked files')

    parser_attribs.add_argument('filenames', metavar='files', type=str, nargs='+',
                        help='list of input files')

    parser_attribs.add_argument('--key_lstrip', type=str,
                        help='remove given string from left of filename to compute dataset key')

    parser_attribs.add_argument('--key_rstrip', type=str,
                        help='remove given string from right of filename to compute dataset key')

    args = parser.parse_args()

    if args.subparser_name == 'interactions':
        copy_interactions(args.mapfile, args.newdir, args.orgid, args.filenames, args.key_lstrip, args.key_rstrip)
    elif args.subparser_name == 'attribs':
        copy_attributes(args.mapfile, args.newdir, args.filenames, args.key_lstrip, args.key_rstrip)
    else:
        raise Exception('unexpected command')

