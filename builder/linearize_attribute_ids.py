"""
each individual attribute file has attribute ids assigned,
but we require unique ids for all attributes. So read
in all the files, and add a column of unique attribute ids,
which we'll call 'LINEARIZED_ID'
"""

import argparse
import pandas as pd

def linearize_attributes(input_files, groups_file, output_file, organism_id,
                         key_lstrip=None, key_rstrip=None):

    groups = pd.read_csv(groups_file, sep='\t', header=0, index_col=0)
    assert groups.index.name == 'id'

    all_attributes = []

    for input_file in input_files:

        # find id corresponding to file
        dataset_key = input_file
        if key_lstrip and dataset_key.startswith(key_lstrip):
            dataset_key = dataset_key[len(key_lstrip):]
        if key_rstrip and dataset_key.endswith(key_rstrip):
            dataset_key = dataset_key[:-len(key_rstrip)]

        matched = groups[groups['dataset_key'] == dataset_key].index

        if len(matched) == 0:
            raise Exception('Group file has no id: %s' % input_file)
        elif len(matched) > 2:
            raise Exception('Group file has multiple ids (expected 1): %s' % input_file)

        group_id = matched[0]
        print("group id for input file", input_file, group_id)

        attributes = pd.read_csv(input_file, sep='\t', header=True,
                            dtype='str', na_filter=False)
        
        attributes.rename(column={attributes.columns[0]: "ID", attributes.columns[1]: "EXTERNAL_ID", attributes.columns[2]: "NAME", attributes.columns[3]: "DESCRIPTION"}, inplace = True)
        
        attributes['ATTRIBUTE_GROUP_ID'] = group_id
        attributes['ORGANISM_ID'] = organism_id

        all_attributes.append(attributes)

    all_attributes = pd.concat(all_attributes)
    all_attributes.reset_index(inplace=True)
    all_attributes.index += 1 # start indexing at 1 not 0
    all_attributes.index.name = 'LINEARIZED_ID'

    all_attributes.to_csv(output_file, sep='\t', header=True, index=True,
                          columns=['ID', 'ORGANISM_ID', 'ATTRIBUTE_GROUP_ID', 'EXTERNAL_ID', 'NAME', 'DESCRIPTION'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create attribute files in generic_db format')

    parser.add_argument('organism_id', help='organism id')
    parser.add_argument('output', help='output file')
    parser.add_argument('groups', help='table of attribute group metadata')
    parser.add_argument('inputs', help='something', nargs='+')
    parser.add_argument('--key_lstrip', type=str,
                                   help='remove given string from left of filename to compute dataset key')
    parser.add_argument('--key_rstrip', type=str,
                                   help='remove given string from right of filename to compute dataset key')

    # parse args and dispatch
    args = parser.parse_args()

    print('organisms id is', args.organism_id)

    linearize_attributes(args.inputs, args.groups, args.output, args.organism_id,
                       args.key_lstrip, args.key_rstrip)
