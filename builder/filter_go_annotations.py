#
# cleaning for functional annotations required by
# enrichment analysis or combining methods
#
# assumes evidence code filtering already applied to input file



import argparse
import pandas as pd


def clean(input_file, symbols_file, output_file_annos, output_file_anno_names):

    # load table of annotations, as produced by query_go_annotations
    annos = pd.read_table(input_file, sep='\t', skiprows=2, header=None, na_filter=False,
                          names=['name', 'branch', 'category', 'd', 'e', 'f', 'gene', 'h', 'i', 'k', 'l'])

    annos['gene_orig'] = annos['gene']
    annos['gene'] = annos['gene'].str.upper()

    # drop rows that don't belong to one of 3 main go branches (universal/root level annotations)
    annos = annos[annos['branch'].isin(['cellular_component', 'molecular_function', 'biological_process'])]

    # load symbol mappings
    mappings = pd.read_table(symbols_file, sep='\t', header=None, na_filter=False,
                             names=['id', 'symbol', 'source'])

    mappings['symbol'] = mappings['symbol'].str.upper()

    # merge annotations with mappings, excluding entries that don't match up
    annos = pd.merge(annos, mappings, left_on = 'gene', right_on = 'symbol', how='inner')

    # get rid of duplicates
    annos.drop_duplicates(['category', 'id'], inplace=True)

    # write out clean two-column annotations file
    annos.to_csv(output_file_annos, sep='\t', header=False, index=False, columns=['branch', 'category', 'id', 'gene_orig'])

    # write annotation category names
    names = annos[['category', 'name']].drop_duplicates().sort(['category'])
    names.to_csv(output_file_anno_names, sep='\t', header=False, index=False)


def filter(input_file, output_file, min_size, max_size, branch_filter=None):

    annos = pd.read_csv(input_file, sep='\t', header=None, na_filter=False, names=['branch', 'category', 'id', 'gene_orig'])

    # subset annotations extracting branch of interest
    if branch_filter is not None:
        annos = annos[annos['branch'] == branch_filter]

    # group by category
    grouped = annos.groupby("category")

    # count number of unique genes in each group
    counts = grouped['id'].nunique()
    counts.name = 'size'

    # only want annotations satisfying size constraints
    wanted = counts[(counts >= min_size) & (counts <= max_size)]
    wanted = wanted.reset_index() # push category into a column

    out = pd.merge(annos, wanted, left_on = 'category', right_on = 'category', how='inner')
    out = out.ix[:, ('category', 'gene_orig')]
    out.sort(columns=['category', 'gene_orig'], inplace=True)

    out.to_csv(output_file, sep='\t', header=False, index=False, columns=['category', 'gene_orig'])


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='process go annotations')
    subparsers = parser.add_subparsers(dest = 'subparser_name')

    # clean
    parser_clean = subparsers.add_parser('clean')
    parser_clean.add_argument('input', help='annotations from query_go_annotations')
    parser_clean.add_argument('symbols', help='clean symbol mappings file')
    parser_clean.add_argument('annotations', help='output clean annotations')
    parser_clean.add_argument('names', help='output annotation category names')

    # filter
    parser_filter = subparsers.add_parser('filter')
    parser_filter.add_argument('input', help='clean annotations file')
    parser_filter.add_argument('output', help='filtered annotations file')
    parser_filter.add_argument('min_size', help='min category size', type=int)
    parser_filter.add_argument('max_size', help='max category size', type=int)
    parser_filter.add_argument('--branch', help='optional, branch required', default=None)

    # parse args and dispatch
    args = parser.parse_args()

    if args.subparser_name == 'clean':
        clean(args.input, args.symbols, args.annotations, args.names)
    elif args.subparser_name == 'filter':
        filter(args.input, args.output, args.min_size, args.max_size, args.branch)
    else:
        raise Exception('unexpected command: "%s"' % args.subparser_name)
