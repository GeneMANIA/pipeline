
import extract_identifiers


def test():

    identifiers = 'Hs_names.txt'
    descriptions = 'ENSEMBL_ENTREZ_Hs'
    organism_id = 4

    extract_identifiers.extract_nodes(identifiers, organism_id, 'NODES.txt')
    extract_identifiers.extract_naming_sources(identifiers, 'GENE_NAMING_SOURCES.txt')
    extract_identifiers.extract_gene_data(identifiers, descriptions, 'GENE_DATA.txt')
    extract_identifiers.extract_genes(identifiers, 'GENE_NAMING_SOURCES.txt', organism_id, 'GENES.txt')
