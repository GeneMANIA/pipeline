
'''

Create GENERIC_DB files:
 * NODES.txt columns 'ID', 'NAME', 'GENE_DATA_ID', 'ORGANISM_ID'

 * GENES.txt columns 'ID', 'SYMBOL', 'SYMBOL_TYPE', 'NAMING_SOURCE_ID',
   'NODE_ID', 'ORGANISM_ID', 'DEFAULT_SELECTED'

 * GENE_DATA.txt 'ID', 'DESCRIPTION', 'EXTERNAL_ID', 'LINKOUT_SOURCE_ID'

 * GENE_NAMING_SOURCES.txt, columns 'ID', 'NAME', 'RANK', 'SHORT_NAME'

See GENERIC_DB.md

input:
 * raw mappings file
 * processed mappings file
 * default genes
 * organism id

'''


import argparse
import pandas as pd
from configobj import ConfigObj


def load_identifiers(identifiers_file):
    identifiers = pd.read_csv(identifiers_file, sep='\t', header=None,
                             names=['GMID', 'SYMBOL', 'SOURCE'],
                             dtype='str', na_filter=False, index_col=0)

    assert identifiers.index.name == 'GMID'
    return identifiers


def load_descriptions(descriptions_file):
    # column names in first row of the file
    descriptions = pd.read_csv(descriptions_file, sep='\t', header=0,
                               dtype='str', na_filter=False, index_col=0)

    assert descriptions.index.name == 'GMID'
    return descriptions


def extract_nodes(identifiers, organism_id, output):

    identifiers = load_identifiers(identifiers)

    # nodes are the unique GMID's
    gmids = identifiers.index.unique()
    nodes = pd.DataFrame(index=gmids)
    nodes['NAME'] = nodes.index
    nodes['GENE_DATA_ID'] = nodes.index
    nodes['ORGANISM_ID'] = organism_id

    nodes.to_csv(output, sep='\t',  header=False, index=True,
                 index_label='ID', columns=['NAME', 'GENE_DATA_ID', 'ORGANISM_ID'])


def extract_naming_sources(identifiers_file, naming_sources_file):

    must_have_sources = ['Entrez Gene ID']

    identifiers = load_identifiers(identifiers_file)

    # extract naming sources. unique() returns a series,
    # use it to build a dataframe adding other columns
    naming_sources = list(identifiers['SOURCE'].unique()) + must_have_sources
    naming_sources = pd.Series(naming_sources).unique() # in case some must-have's were already there
    naming_sources = pd.DataFrame(naming_sources, columns=['NAME'])
    naming_sources.sort(columns=['NAME'], inplace=True)
    naming_sources.reset_index(inplace=True, drop=True)
    naming_sources.index += 1 # start numbering at 1

    naming_sources.index.name = 'ID'

    # SHORT_NAME and RANK may not be used anymore
    naming_sources['SHORT_NAME'] = ''
    naming_sources['RANK'] = 0

    naming_sources.to_csv(naming_sources_file, sep='\t', header=False, index=True,
                          index_label='ID', columns=['NAME', 'RANK', 'SHORT_NAME'])


def extract_genes(identifiers_file, naming_sources_file, organism_cfg, genes_file):
    identifiers = load_identifiers(identifiers_file)

    cfg = ConfigObj(organism_cfg, encoding='UTF8')
    default_genes = cfg['default_genes']
    organism_id = cfg['gm_organism_id']

    naming_sources = pd.read_csv(naming_sources_file, sep='\t', header=None,
                                 dtype='str', na_filter=False, index_col=0,
                                 names=['NAME', 'RANK', 'SHORT_NAME'])

    # load genes
    genes = identifiers.copy()
    genes.index.name = 'NODE_ID'
    genes.reset_index(inplace=True) # push into column

    genes.index.name = 'ID'
    genes['SYMBOL_TYPE'] = ''
    genes['ORGANISM_ID'] = organism_id
    genes['DEFAULT_SELECTED'] = 0

# set source ids by joining
    sources_temp = naming_sources.copy()
    sources_temp.index.name='NAMING_SOURCE_ID'
    sources_temp.reset_index(inplace=True)
    sources_temp = sources_temp[['NAMING_SOURCE_ID', 'NAME']]
    genes = pd.merge(genes, sources_temp, left_on='SOURCE', right_on='NAME', how='inner')
    genes.index += 1
    genes.drop(['SOURCE', 'NAME'], axis=1, inplace=True)

    # assign defaults genes
    genes['SYMBOL_UPPER'] = genes['SYMBOL'].str.upper()
    default_genes = [gene.upper() for gene in default_genes]
    genes.loc[genes['SYMBOL_UPPER'].isin(default_genes), 'DEFAULT_SELECTED'] = 1
    genes.drop(['SYMBOL_UPPER'], axis=1, inplace=True)

    # write out files. be explicit about column order so as not to mess things up
    genes.to_csv(genes_file, sep='\t', header=False, index=True,
                 index_label='ID', columns=['SYMBOL', 'SYMBOL_TYPE', 'NAMING_SOURCE_ID',
                 'NODE_ID', 'ORGANISM_ID', 'DEFAULT_SELECTED'])


def extract_gene_data(identifiers_file, descriptions_file, gene_data_file):
    # gene_data is just descriptions, same index as nodes
    # merge with 'Definition' column of the descriptions data
    # on ID index

    identifiers = load_identifiers(identifiers_file)
    descriptions = load_descriptions(descriptions_file)

    # nodes are the unique GMID's
    gmids = identifiers.index.unique()

    gene_data = pd.DataFrame(index=gmids)
    gene_data.index.name = 'ID'
    descriptions_temp = descriptions[['Definition']].copy()
    descriptions_temp.rename(columns={'Definition': 'DESCRIPTION'}, inplace=True)
    gene_data = pd.merge(gene_data, descriptions_temp, how='inner', left_index=True, right_index=True)

    # hopefully these next two are not used anymore
    gene_data['EXTERNAL_ID'] = ''
    gene_data['LINKOUT_SOURCE_ID'] = 0

    gene_data.to_csv(gene_data_file, sep='\t', header=False, index=True,
                     index_label='ID', columns=['DESCRIPTION', 'EXTERNAL_ID', 'LINKOUT_SOURCE_ID'])


#def process(identifiers_file, descriptions_file, default_genes_file, organism_id,
#            nodes_file, genes_file, gene_data_file, naming_sources_file):
def process(args):

    if args.type == 'nodes':
        #extract_nodes(identifiers_file, organism_id, nodes_file)
        extract_nodes(args.identifiers, args.organism_id, args.output)
    elif args.type == 'gene_data':
        #extract_gene_data(identifiers_file, descriptions_file, gene_data_file)
        extract_gene_data(args.identifiers, args.descriptions, args.outptu)
    elif args.type == 'naming_sources':
        #extract_naming_sources(identifiers_file, naming_sources_file)
        extract_naming_sources(args.identifiers, args.output)
    elif args.type == 'genes':
        #extract_genes(identifiers_file, naming_sources_file, organism_id, genes_file)
        extract_genes(args.identifiers, args.output, args.organism_id, args.output)


if __name__ == '__main__':

    # setup subcommands for each output file
    parser = argparse.ArgumentParser(description='Create identifier files in generic_db format')
    subparsers = parser.add_subparsers(dest = 'subparser_name')

    # nodes
    parser_nodes = subparsers.add_parser('nodes')
    parser_nodes.add_argument("organism_id", type=int, help="organism id")
    parser_nodes.add_argument("identifiers", help="cleaned identifiers file")
    parser_nodes.add_argument("output", help="output file")

    # genes
    parser_genes = subparsers.add_parser('genes')
    parser_genes.add_argument("organism_cfg", type=str, help="organism config file")
    parser_genes.add_argument("identifiers", help="cleaned identifiers file")
    parser_genes.add_argument("naming_sources", help="naming sources file")
    parser_genes.add_argument("output", help="output file")

   # gene_data
    parser_gene_data = subparsers.add_parser('gene_data')
    parser_gene_data.add_argument("identifiers", help="cleaned identifiers file")
    parser_gene_data.add_argument("descriptions", help="descriptions file")
    parser_gene_data.add_argument("output", help="output file")

    # naming_sources
    parser_naming_sources = subparsers.add_parser('naming_sources')
    parser_naming_sources.add_argument("identifiers", help="cleaned identifiers file")
    parser_naming_sources.add_argument("output", help="output file")

     # parse args and dispatch
    args = parser.parse_args()

    if args.subparser_name == 'nodes':
        extract_nodes(args.identifiers, args.organism_id, args.output)
    elif args.subparser_name == 'genes':
        extract_genes(args.identifiers, args.naming_sources, args.organism_cfg, args.output)
    elif args.subparser_name == 'gene_data':
        extract_gene_data(args.identifiers, args.descriptions, args.output)
    elif args.subparser_name == 'naming_sources':
        extract_naming_sources(args.identifiers, args.output)
    else:
        raise Exception('unexpected command')
