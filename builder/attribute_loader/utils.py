'''
shared stuff for:
  
  * parsing text files in specific layouts
  
'''

import codecs
SEP = '\t'
ENCODING="utf8"

def sparse_attribute_profile_reader(filename, sep=SEP, encoding=ENCODING):
    '''
    given a file in format:
    
       gene_id<tab>attribute_id<tab>attribute_id...
       ...
       
    return (gene_id, attribute_id) tuples until exhaustion
    '''
        
    with codecs.open(filename, encoding=encoding, errors='replace') as f:
        for line in f:
            line = line.strip()
            parts = line.split(sep)
            
            if len(parts) < 2:
                raise Exception("gene but no attributes? >>>%s<<<" % line)
            
            gene = parts[0]
            for attribute in parts[1:]:
                yield (gene, attribute)

def enhanced_attribute_profile_reader(filename, sep=SEP, encoding=ENCODING):
    '''
    given a file in format:
    
      attribute-id<tab>attribute-name<tab>gene-identifier<tab>gene-identifer2<tab>more gene identifiers
 
    return a pair of tuples, one for the attributes and the second for the genes from each record,
    until exhaustion:
    
       ((attribute-name, attribute-identifier), (gene-identifier, gene-identifier2, ...)) 
    '''
    with codecs.open(filename, encoding=encoding, errors='replace') as f:
        for line in f:
            line = line.strip('\r\n')
            parts = line.split(sep)
            
            if len(parts) < 3:
                print("too few fields in record (skipping): >>>%s<<<" % line)
                continue
            
            attributes = (parts[0], parts[1])
            genes = parts[2:]
            yield (attributes, genes)

def attribute_metadata_reader(filename, sep=SEP, encoding=ENCODING):
    '''
    given a file in the format:
    
      attribute_id<tab>attribute_name<tab>attribute_desc
      ...
      
    return (attribute_id, attribute_name, attribute_desc) tuples until exhaustion
    
    description is optional, and will be returned as empty string if not present. attribute_name
    is also optional, and will be returned as the same as attribute_id if not given.  empty lines
    in the input are skipped. 
    '''
    
    with codecs.open(filename, encoding=encoding, errors='replace') as f:
        for line in f:
            line = line.strip('\n\r')
            parts = line.split(sep)
            
            if len(parts) == 0:
                continue
            elif len(parts) == 1:
                attr_id = parts[0]
                name = attr_id
                desc = ''
            elif len(parts) == 2:
                attr_id = parts[0]
                name = parts[1]
                desc = ''
            else:
                attr_id, name, desc = parts[0:3]
                
            yield (attr_id, name, desc)
    