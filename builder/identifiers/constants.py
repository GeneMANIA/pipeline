'''
some field names and values from the input data files
that we need to manipulate
'''

# constants for the 'raw' (ensembl-to-entrez) mapping file

# values in the biotype field we want to keep
RAW_DEFAULT_BIOTYPE_KEEPERS = ['protein_coding', 'True']        

# column names that do not contain symbols to load
RAW_DEFINITION = 'Definition'
RAW_GMID = 'GMID'
RAW_BIOTYPE = 'Protein Coding' # historical reasons

# we don't care what (most) of the other columns are,
# and just load them directly as source names. exceptions
# are gene name, for which we need to do extra processing,
# and synonym which we de-prioritize in conflict resolution
# (and also adjust for source names)
RAW_GENE_NAME = 'Gene Name'
RAW_SYNONYM = 'Synonym'
RAW_SYNONYM_ORIG = 'Synonyms'

# fields can contain multiple values, delimited 
# by a ';' char, except for the following which are allowed
# to include that char in their value
RAW_NON_DELIMITED_FIELDS = [RAW_GENE_NAME]

# symbols we want to exclude completely based on source
RAW_DEFAULT_SOURCES_TO_REMOVE = ['Ensembl Transcript ID']

# constants for the reverse (entrez to ensembl) mapping file
REV_ENTREZ_ID = 'GeneID'
REV_GENE_NAME = 'Symbol'
REV_BIOTYPE = 'type_of_gene'
REV_DEFAULT_BIOTYPE_KEEPERS = ['protein-coding']
REV_ENSEMBL = 'Ensembl'
REV_XREFS = 'dbXrefs'

# 3 col format fields
COL3_GMID = 'GMID'
COL3_SYMBOL = 'Symbol'
COL3_SOURCE = 'Source'

