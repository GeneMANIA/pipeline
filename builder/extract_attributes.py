
import argparse
import pandas as pd

# see GENERIC_DB.md for output file formats

def extract_attribute_groups(input_file, output_file, organism_id):
    data = pd.read_csv(input_file, sep='\t', header=0, index_col=0)

    data['ORGANISM_ID'] = organism_id
    data['CODE'] = data['name']

    data.to_csv(output_file, sep='\t',
                columns=['ORGANISM_ID', 'name', 'CODE', 'desc', 'linkout_label', 'linkout_url',
                      'default_selected', 'pub_name', 'pub_url'],
                header=False, index=True)


def extract_attributes(input_file, output_file):
    '''the linearized attribute id file is almost
    what we need, just drop the group level attribute ids
    '''

    attributes = pd.read_csv(input_file, sep='\t',
        na_filter=False)

    attributes.drop(['ID'], axis=1, inplace=True)

    attributes.to_csv(output_file, sep='\t', header=False, index=False,
                          columns=['LINEARIZED_ID', 'ORGANISM_ID', 'ATTRIBUTE_GROUP_ID', 'EXTERNAL_ID', 'NAME', 'DESCRIPTION'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create attribute files in generic_db format')
    subparsers = parser.add_subparsers(dest='subparser_name')

    # attribute groups
    parser_attribute_groups = subparsers.add_parser('attribute_groups')
    parser_attribute_groups.add_argument('organism_id', help='organism id')
    parser_attribute_groups.add_argument('output', help='output file')
    parser_attribute_groups.add_argument('input', help='table of attribute group metadata')

    # attributes
    parser_attributes = subparsers.add_parser('attributes')
    parser_attributes.add_argument('input', help='linearized attributes file')
    parser_attributes.add_argument('output', help='output file')

    # parse args and dispatch
    args = parser.parse_args()

    if args.subparser_name == 'attribute_groups':
        extract_attribute_groups(args.input, args.output, args.organism_id)
    elif args.subparser_name == 'attributes':
        extract_attributes(args.input, args.output)
    else:
        raise Exception('unexpected command')
