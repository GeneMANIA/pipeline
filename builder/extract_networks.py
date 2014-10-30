
'''

Create GENERIC_DB files:

 * NETWORKS.txt
 * NETWORK_GROUPS.txt
 * NETWORK_METADATA.txt

See GENERIC_DB.md

input:

 * tabular metadata file, containing records to transform into GENERIC_DB format

'''


import argparse
import pandas as pd


def extract_network_groups(input_file, output_file):

    output_cols =  ['ID', 'NAME', 'CODE', 'DESCRIPTION', 'ORGANISM_ID']

    metadata = pd.read_csv(input_file, sep='\t', header=0)

    # extract & tidy up the network groups column
    network_groups = metadata[['group']].copy()
    network_groups.drop_duplicates(inplace=True)
    network_groups.sort(inplace=True)

    network_groups.columns = ['NAME']
    network_groups['ORGANISM_ID'] = 1 # TODO
    network_groups['CODE'] = network_groups['NAME']
    network_groups['DESCRIPTION'] = ''

    # want ids starting at 1
    network_groups.reset_index(drop=True, inplace=True)
    network_groups.index += 1

    # write output
    network_groups.to_csv(output_file, sep='\t', header=False, index=True,
                          columns=output_cols[1:])


def extract_network_metadata(input_file, output_file):

    # many of these ae unused
    output_cols =  ['ID', 'source', 'reference',
                    'pubmedId', 'authors', 'publicationName', 'yearPublished',
                    'processingDescription', 'networkType', 'alias',
                    'interactionCount', 'dynamicRange',
                    'edgeWeightDistribution', 'accessStats', 'comment',
                    'other', 'title', 'url', 'sourceUrl']

    metadata = pd.read_csv(input_file, sep='\t', header=0)


    # metadata in generic_db format
    gdb_metadata = pd.DataFrame(columns=output_cols)
    gdb_metadata['ID'] = metadata['id']
    gdb_metadata['interactionCount'] = 0 # TODO
    gdb_metadata['accessStats'] = 0 # will fail if left blank


    # write output
    gdb_metadata.to_csv(output_file, sep='\t', header=False, index=False)


def extract_networks(input_file, groups_file, output_file):

    output_cols =  ['ID', 'NAME', 'METADATA_ID', 'DESCRIPTION', 'DEFAULT_SELECTED', 'GROUP_ID']


    metadata = pd.read_csv(input_file, sep='\t', na_filter=False, header=0)
    groups = pd.read_csv(groups_file, sep='\t', na_filter=False, header=None, names=['ID', 'NAME', 'CODE', 'DESCRIPTION', 'ORGANISM_ID'])

    # just the columns we need, rename others to avoid collisions when merging
    groups.drop(['CODE', 'DESCRIPTION', 'ORGANISM_ID'], axis=1, inplace=True)
    groups.columns = ['GROUP_ID', 'GROUP_NAME']

    #networks = pd.DataFrame(columns=output_cols)
    networks = metadata[['id', 'name', 'group', 'description', 'default_selected']].copy()
    networks.columns = ['ID', 'NAME', 'GROUP_NAME', 'DESCRIPTION', 'DEFAULT_SELECTED']

    networks = pd.merge(networks, groups, on='GROUP_NAME', how='inner')
    networks['METADATA_ID'] = networks['ID']
    networks.to_csv(output_file, sep='\t', header=False, index=False,
                    columns=output_cols)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create network files in generic_db format')
    subparsers = parser.add_subparsers(dest = 'subparser_name')

    # networks groups
    parser_network_groups = subparsers.add_parser('network_groups')
    parser_network_groups.add_argument('input', help='1 or more input files')
    parser_network_groups.add_argument('output', help='output networks groups file')

    # networks metadata
    parser_network_metadata = subparsers.add_parser('network_metadata')
    parser_network_metadata.add_argument('input', help='1 or more input files')
    parser_network_metadata.add_argument('output', help='output networks metadata file')

     # networks
    parser_networks = subparsers.add_parser('networks')
    parser_networks.add_argument('input', help='1 or more input files')
    parser_networks.add_argument('groups', help='network groups file')
    parser_networks.add_argument('output', help='output networks file')

    # parse args and dispatch
    args = parser.parse_args()

    if args.subparser_name == 'network_groups':
        extract_network_groups(args.input, args.output)
    elif args.subparser_name == 'network_metadata':
        extract_network_metadata(args.input, args.output)
    elif args.subparser_name == 'networks':
        extract_networks(args.input, args.groups, args.output)
    else:
        raise Exception('unexpected command: "%s"' % args.subparser_name)
