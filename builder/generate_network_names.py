

import argparse, os
import pandas as pd
import numpy as np

DEDUP_DISCLAIMER = 'One of %s datasets produced from this publication.'
ENDINGS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

def format_author_info(first_author, last_author, year):
    '''
        First-Last-Year, with missing parts removed
    '''

    parts = [first_author, last_author, year]

    # nan's in here? remove by checking for numbers
    parts = [part for part in parts if not np.isreal(part)]

    # empty strings? remove
    parts = [str(part) for part in parts if part]

    # join up whats left
    return '-'.join(parts)


def name_selector(row):
    if row['name']:
        return row['name']
    elif row['auto_name']:
        return row['auto_name']
    else:
        return row['name_from_file']


def subgroup_auxer(subgroup_frame):
    if len(subgroup_frame) == 1: #or subgroup_key == "":
        return subgroup_frame

    num_pubmid_matches = len(subgroup_frame)
    aux = DEDUP_DISCLAIMER % num_pubmid_matches
    subgroup_frame['aux_description'] = aux
    return subgroup_frame


def deduper(group_frame):

        group_frame['dedup_suffix'] = ''
        group_frame['aux_description'] = ''

        if len(group_frame) < 2:
            return group_frame

        group_frame = group_frame.sort(['pubmed_id', 'dataset_key'], axis=0)

        if len(group_frame) > len(ENDINGS):
            raise Exception("too many colliding networks with name '%s' to rename (%s)" %
                            (group_frame['selected_name'].iloc[0], len(group_frame)))

        # append the dedup letters 'A', 'B', etc to the name
        group_frame['dedup_suffix'] = [' ' + ending for ending in ENDINGS[:len(group_frame)]]
        group_frame['selected_name'] = group_frame['selected_name'] \
                                           + group_frame['dedup_suffix']

        # add extra text when the pmids match
        subgroups = group_frame.groupby('pubmed_id')
        return subgroups.apply(subgroup_auxer)


def main(inputfile, outputfile):

    network_metadata = pd.read_csv(inputfile, sep='\t', header=0, index_col=0,
                                   encoding='UTF8', dtype=str)

    # new cols for network naming steps
    #network_metadata['auto_name'] = ''
    network_metadata['name_from_file'] = ''
    network_metadata['selected_name'] = ''
    network_metadata['final_name'] = ''

    # generate auto name from pubmed metadata
    network_metadata['auto_name'] = network_metadata.apply(lambda row:
                                                           format_author_info(row['first_author'],
                                                                              row['last_author'],
                                                                              row['year']),
                                                           axis=1)

    # also a name based on the file
    network_metadata['name_from_file'] = network_metadata.apply(lambda row:
                                                                os.path.basename(row['dataset_key']),
                                                                axis=1)

    # choose a name, either the name provided by user,
    # else the name from pubmed, else the filename
    network_metadata.fillna({'name': '', 'auto_name': ''}, inplace=True)
    network_metadata['selected_name'] = network_metadata.apply(lambda row: name_selector(row), axis=1)

    # take care of name collisions
    groups = network_metadata.groupby(['group', 'selected_name'])
    updated_network_metadata = groups.apply(deduper)

    # write out update
    updated_network_metadata.to_csv(outputfile, sep='\t', header=True, index=True, encoding='UTF8')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='compute network names')

    parser.add_argument('inputfile', help='input network metadata file')
    parser.add_argument('outputfile', help='output network metadata file')

    args = parser.parse_args()
    main(args.inputfile, args.outputfile)


