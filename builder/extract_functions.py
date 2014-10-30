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


def load_functions(functions_file):
    functions = pd.read_csv(functions_file, sep='\t', header=None,
                            names=['NAME', 'DESCRIPTION'],
                            dtype='str', na_filter=False)
    return functions


def extract_function_groups(input_files, output_file):
    '''
    create a dataframe from the list of input file names
    (not their contents). the automatically assigned id's
    will are written along with the list to the given output file.

    :param input_files:
    :param output_file:
    :return:
    '''

    groups = pd.DataFrame(input_files, columns=['NAME'])
    groups.index += 1
    groups.index.name = 'ID'

    groups.to_csv(output_file, sep='\t', header=True, index=True)


def extract_functions(input_files, groups_file, output_file):

    groups = pd.read_csv(groups_file, sep='\t', header=0, index_col=0)
    assert groups.index.name == 'ID'

    all_functions = []

    for input_file in input_files:
        # find id corresponding to file
        matched = groups[groups['NAME'] == input_file].index

        if len(matched) == 0:
            raise Exception('Group file has no id: %s' % input_file)
        elif len(matched) > 2:
            raise Exception('Group file has multiple ids (expected 1): %s' % input_file)

        group_id = matched[0]

        functions = load_functions(input_file)
        functions['ONTOLOGY_ID'] = group_id

        all_functions.append(functions)

    all_functions = pd.concat(all_functions)
    all_functions.index += 1
    all_functions.index.name = 'ID'

    all_functions.to_csv(output_file, sep='\t', header=True, index=True)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Create function (AKA ONTOLOGY) files in generic_db format')
    subparsers = parser.add_subparsers(dest = 'subparser_name')

    # function groups
    parser_function_groups = subparsers.add_parser('function_groups')
    parser_function_groups.add_argument('output', help='output function groups file')
    parser_function_groups.add_argument('inputs', help='1 or more input files', nargs='+')

    # functions
    parser_functions = subparsers.add_parser('functions')
    parser_functions.add_argument('output', help='output functions file')
    parser_functions.add_argument('groups', help='function groups file')
    parser_functions.add_argument('inputs', help='1 or more input files', nargs='+')

    # parse args and dispatch
    args = parser.parse_args()

    if args.subparser_name == 'function_groups':
        extract_function_groups(args.inputs, args.output)
    elif args.subparser_name == 'functions':
        extract_functions(args.inputs, args.groups, args.output)
    else:
        raise Exception('unexpected command')

