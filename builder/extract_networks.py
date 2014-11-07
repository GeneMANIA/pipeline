
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
import numpy as np


def extract_network_groups(input_file, output_file, organism_id):

    output_cols =  ['ID', 'NAME', 'CODE', 'DESCRIPTION', 'ORGANISM_ID']

    metadata = pd.read_csv(input_file, sep='\t', header=0)

    # extract & tidy up the network groups column
    network_groups = metadata[['group']].copy()
    network_groups.drop_duplicates(inplace=True)
    network_groups.sort(inplace=True)

    network_groups.columns = ['CODE']
    network_groups['ORGANISM_ID'] = organism_id

    # TODO: allow user-friendly network names configured
    # in an additional data file
    network_groups['NAME'] = network_groups['CODE']
    network_groups['DESCRIPTION'] = ''

    # want ids starting at 1
    network_groups.reset_index(drop=True, inplace=True)
    network_groups.index += 1

    # write output
    network_groups.to_csv(output_file, sep='\t', header=False, index=True,
                          columns=output_cols[1:])


def format_pubmed_url(row):
    pubmed_id = row['pubmed_id']

    if pubmed_id and pubmed_id != '0':
        return 'http://www.ncbi.nlm.nih.gov/pubmed/%s' % int(pubmed_id)
    else:
        return ''


def format_authors_list(row):
    # comma separated list of authors in a single field.
    # not all authors, just first and last is all we ever display
    parts = [row['first_author'], row['last_author']]
    parts = [part for part in parts if part]
    return ','.join(parts)

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

    # initialize new columns to empty
    for col in output_cols:
        if col not in metadata.columns:
            metadata[col] = np.nan

    # 0's and empty strings where needed
    metadata['pubmed_id'].replace('', 0, inplace=True)
    metadata.fillna({'pubmed_id': 0, 'accessStats': 0}, inplace=True)
    metadata.fillna('', inplace=True)

    metadata[['pubmed_id', 'accessStats']] = metadata[['pubmed_id', 'accessStats']].astype(int)

    # fill in values from other colums
    metadata['ID'] = metadata['id']
    metadata['interactionCount'] = metadata['num_interactions']
    metadata['pubmedId'] = metadata['pubmed_id']
    metadata['url'] = metadata.apply(format_pubmed_url, axis=1)
    metadata['publicationName'] = metadata['journal_short']
    metadata['authors'] = metadata.apply(format_authors_list, axis=1)
    metadata['yearPublished'] = metadata['year']

    # contained keywords like 'Small-scale studies', seems to not be used
    # set to empty
    metadata['other'] = ''

    # this is set to the nice network group name in old metadata, e.g. 'Co-expression'
    # but hopefully not used. set to empty
    metadata['networkType'] = ''

    # empty values for misc other stuff, needs explanation TODO
    metadata['source'] = 'None'
    metadata['reference'] = ''
    metadata['processingDescription'] = ''
    metadata['sourceUrl'] = ''

    # write output
    metadata.to_csv(output_file, sep='\t', header=False, index=False,
                    columns=output_cols)


def extract_networks(input_file, groups_file, output_file):

    output_cols =  ['ID', 'NAME', 'METADATA_ID', 'DESCRIPTION', 'DEFAULT_SELECTED', 'GROUP_ID']


    metadata = pd.read_csv(input_file, sep='\t', na_filter=False, header=0)
    groups = pd.read_csv(groups_file, sep='\t', na_filter=False, header=None, names=['ID', 'NAME', 'CODE', 'DESCRIPTION', 'ORGANISM_ID'])

    # just the columns we need, rename others to avoid collisions when merging
    groups.drop(['CODE', 'DESCRIPTION', 'ORGANISM_ID'], axis=1, inplace=True)
    groups.columns = ['GROUP_ID', 'GROUP_NAME']

    #networks = pd.DataFrame(columns=output_cols)
    networks = metadata[['id', 'selected_name', 'group', 'description', 'default_selected']].copy()
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
    parser_network_groups.add_argument('organism_id', help='organism id')
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
        extract_network_groups(args.input, args.output, args.organism_id)
    elif args.subparser_name == 'network_metadata':
        extract_network_metadata(args.input, args.output)
    elif args.subparser_name == 'networks':
        extract_networks(args.input, args.groups, args.output)
    else:
        raise Exception('unexpected command: "%s"' % args.subparser_name)
