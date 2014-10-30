'''
functions & data useful for test cases
'''

from . import utils

def testDataToText(data, sep=utils.SEP):
    '''
    make a tab delimted text block from a sequence
    of sequences, suitable to write into a file for 
    using as test data
    '''
    
    result_parts = []
    for record in data:
        result_parts.append(sep.join(record))
    
    return '\n'.join(result_parts)

def testDataToFile(data, afile):
    '''
    convert to text block and write.
    '''
    afile.write(testDataToText(data))
    afile.flush()

# table of node/symbol/source data, equivalent
# to our 'processed_mapping' files.
#   * for symbols
#   * two nodes
#   * two sources
#   * no duplicates
NODES = [('Org:1', 'gene1', 'source1'),
         ('Org:1', 'gene1b', 'source2'),
         ('Org:3', 'gene2', 'source1'),
         ('Org:3', 'gene2b', 'source2')]

# attribute data relationships in sparse profile format
#  * two genes
#  * 4 different attributes, one repeated with different case
NODE_ATTR_ASSOC = [('gene1', 'attr1', 'attr2', 'attr3'),
                   ('gene2', 'Attr1', 'attr4')]

# attribute metadata as (id, name, description) tuples
ATTR_METADATA = [('attr1', 'attr1_name', 'description of attr1'),
                 ('attr2', 'attr2_name', 'description of attr2'),
                 ('attr3', 'attr3_name', 'description of attr3'),
                 ('attr4', 'attr4_name', 'description of attr4')]
                 
# attribute relationships along with some unknown symbols to be ignored
#  * 4 records, one with unrecognized gene, one with repeated gene
#  * 8 total associations, 2 with unrecognized idents, 1 repeated recognized association
NODE_ATTR_ASSOC_WITH_UNKNOWNS = [('gene1', 'attr1', 'attr2', 'attr3'),
                   ('gene2', 'Attr1', 'attr4'),
                   ('unknown_gene', 'attr1'),
                   ('gene1', 'attr2', 'unknown_attribute')]

# attribute-id, attribute-name, nodeid1, nodeid2, ...
# includes a couple of non-matches
NODE_ATTR_ASSOC_DETAILED_FORMAT = [('attr1', 'attr1_name', 'gene1', 'gene1b'),
                                   ('attr3', 'attr3_name', 'gene2', 'gene2b'),
                                   ('', 'attr2_name', '', 'gene2b'),
                                   ('', 'unknown_attribute', '', 'gene1'),
                                   ('attr1', '', '', 'unknown-gene')
                                   ]