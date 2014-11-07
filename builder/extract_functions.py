#! /usr/bin/env python

'''

Create GENERIC_DB files:

 * ONTOLOGIES.txt columns 'ID', 'NAME'
 * ONTOLOGY_CATEGORIES columns 'ID', 'ONTOLOGY_ID', 'NAME', 'DESCRIPTION'

See GENERIC_DB.md

input:
 * list of files, each containing all

The naming is terrible ... ONTOLOGY_CATEGORIES argh. I'll use
function and function_groups.
'''


import argparse
import pandas as pd


def extract_function_groups(input_files, output_file, organism_id):
    '''
    create a dataframe from the list of input file names
    (not their contents)

    the data schema is setup to support multiple enrichment categories per organism,
    but our data pipeline code doesn't support this yet, and instead
    just assumes a single enrichment cateogry with same is as the organism id.

    TODO someday.

    :param input_files:
    :param output_file:
    :return:
    '''

    assert len(input_files) < 2 # we only allow 1

    groups = pd.DataFrame(input_files, columns=['NAME'], index=[organism_id])
    groups.index.name = 'ID'

    groups.to_csv(output_file, sep='\t', header=False, index=True)


def extract_functions(input_file, groups_file, names_file, output_file):

    groups = pd.read_csv(groups_file, sep='\t', header=None, index_col=0, names=['NAME'])
    groups.index.name = 'ID'

    # find id corresponding to file
    matched = groups[groups['NAME'] == input_file].index

    if len(matched) == 0:
        raise Exception('Group file has no id: %s' % input_file)
    elif len(matched) > 2:
        raise Exception('Group file has multiple ids (expected 1): %s' % input_file)

    group_id = matched[0]

    # load functions and names
    functions = pd.read_csv(input_file, sep='\t', header=0,
                            dtype='str', na_filter=False)
    assert list(functions.columns) == ['category', 'gene']

    function_names = pd.read_csv(names_file, sep='\t', header=0,
                                 dtype='str', na_filter=False)
    assert list(function_names.columns) == ['category', 'name']

    # extract names we need by joining on the annotations

    functions.drop('gene', axis=1, inplace=True)
    functions.drop_duplicates(['category'], inplace=True)

    needed_names = pd.merge(functions, function_names, on='category', how='inner')
    needed_names['ONTOLOGY_ID'] = group_id

    # fixup naming to generic_db conventions
    needed_names.rename(columns={'name': 'DESCRIPTION', 'category': 'NAME'}, inplace=True)

    needed_names.reset_index(drop=True, inplace=True)
    needed_names.index += 1
    needed_names.index.name = 'ID'

    needed_names.to_csv(output_file, sep='\t', header=False, index=True,
                         columns=['ONTOLOGY_ID', 'NAME', 'DESCRIPTION'])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create function (AKA ONTOLOGY) files in generic_db format')
    subparsers = parser.add_subparsers(dest = 'subparser_name')

    # function groups
    parser_function_groups = subparsers.add_parser('function_groups')
    parser_function_groups.add_argument('organism_id', help='organism id')
    parser_function_groups.add_argument('output', help='output function groups file')
    parser_function_groups.add_argument('inputs', help='1 or more input files', nargs='+')

    # functions
    parser_functions = subparsers.add_parser('functions')
    parser_functions.add_argument('annos', help='input file of category/gene annotation pairs')
    parser_functions.add_argument('groups', help='function groups file')
    parser_functions.add_argument('names', help='category names file')
    parser_functions.add_argument('output', help='output functions file')

    # parse args and dispatch
    args = parser.parse_args()

    if args.subparser_name == 'function_groups':
        extract_function_groups(args.inputs, args.output, args.organism_id)
    elif args.subparser_name == 'functions':
        extract_functions(args.annos, args.groups, args.names, args.output)
    else:
        raise Exception('unexpected command')

