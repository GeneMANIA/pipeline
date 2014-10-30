
import argparse, os, shutil
import pandas as pd


# interactions
def copy_interactions(mapfile, newdir, org_id, filenames):

    if os.path.exists(newdir):
        shutil.rmtree(newdir)
    os.mkdir(newdir)

    metadata = pd.read_csv(mapfile, sep='\t')
    filenames = pd.DataFrame(filenames, columns=['filename'])

    # get rid of prefix, TODO shouldn't have to do this, fix in metadata file
    metadata['dataset_key'] = metadata['dataset_key'].str.replace('data/', '', 1)
    filenames['dataset_key'] = filenames['filename'].str.replace('work/', '', 1)
    filenames['dataset_key'] = filenames['dataset_key'].str.replace('.txt.nn', '', 1)


    joined = pd.merge(metadata, filenames, on='dataset_key', how='inner')

    #pd.set_option('display.max_colwidth', 100)
    #print(len(joined))
    #print(joined[['id', 'dataset_key', 'filename']])

    assert len(joined) == len(metadata) == len(filenames)

    for index, row in joined.iterrows():
        network_id = row['id']
        filename = row['filename']

        link_name = os.path.join(newdir, '%s.%s.txt' % (org_id, network_id))
        #os.symlink(filename, link_name)
        shutil.copy(filename, link_name)

# attributes
def copy_attributes(mapfile, newdir, filenames):

    if os.path.exists(newdir):
        shutil.rmtree(newdir)
    os.mkdir(newdir)

    metadata = pd.read_csv(mapfile, sep='\t')
    filenames = pd.DataFrame(filenames, columns=['filename'])

    pd.set_option('display.max_colwidth', 100)
    print('================')
    print(metadata[['id', 'dataset_key']])
    print(filenames[['filename']])

    # get rid of prefix, TODO shouldn't have to do this, fix in metadata file
    metadata['dataset_key'] = metadata['dataset_key'].str.replace('work/', '', 1)
    filenames['dataset_key'] = filenames['filename'].str.replace('work/', '', 1)
    filenames['dataset_key'] = filenames['dataset_key'].str.replace('.txt.mapped', '', 1)

    print('================')
    print(metadata[['id', 'dataset_key']])
    print(filenames[['dataset_key', 'filename']])

    joined = pd.merge(metadata, filenames, on='dataset_key', how='inner')

    #pd.set_option('display.max_colwidth', 100)
    #print(len(joined))
    #print(joined[['id', 'dataset_key', 'filename']])

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

    # attribs
    parser_attribs = subparsers.add_parser('attribs')
    parser_attribs.add_argument('mapfile', type=str,
                        help='name of clean output file, stdout if missing')

    parser_attribs.add_argument('newdir', type=str,
                        help='location of copied/linked files')

    parser_attribs.add_argument('filenames', metavar='files', type=str, nargs='+',
                        help='list of input files')

    args = parser.parse_args()

    if args.subparser_name == 'interactions':
        copy_interactions(args.mapfile, args.newdir, args.orgid, args.filenames)
    elif args.subparser_name == 'attribs':
        copy_attributes(args.mapfile, args.newdir, args.filenames)
    else:
        raise Exception('unexpected command')

