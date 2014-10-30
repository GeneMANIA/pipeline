'''
run each of the individual attribute loading scripts
for the various different types of data we want to load
'''

import os, logging, shutil
from . import process_identifiers, process_attributes
from . import process_attribute_associations, process_attribute_associations2
from . import export_attributes
from . import dbtools


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = '/Users/khalid/bulk/r6b3'
ATTRIBUTES_DIR = os.path.join(BASE_DIR, 'attributes')

# each record is gene attribute. but maybe this should go away in favor of 2
ASSOC_FORMAT_SIMPLE = 1

# each record is attribute-id attribute-name  gene-id gene-name other-gene-identifiers
ASSOC_FORMAT_MULTI_ID = 2

class AttributeMetadata(object):
    def __init__(self, assoc_file, desc_file, name, code, desc, 
                 linkout_label, linkout_url, default_selected, 
                 pub_name, pub_url, attributes_identified_by, 
                 assoc_format):
        self.assoc_file = assoc_file
        self.desc_file = desc_file
        self.name = name
        self.code = code
        self.desc = desc
        self.linkout_label = linkout_label
        self.linkout_url = linkout_url
        self.default_selected = default_selected
        self.pub_name = pub_name
        self.pub_url = pub_url
        self.attributes_identified_by = attributes_identified_by # either name or external id, only relevant for simple assoc format
        self.assoc_format = assoc_format

#1    A. thaliana
#2    C. elegans
#3    D. melanogaster
#4    H. sapiens
#5    M. musculus
#6    S. cerevisiae
#7    R. norvegicus

organisms = {1: 'At', 2: 'Ce', 3: 'Dm',
             4: 'Hs', 5:'Mm', 6: 'Sc',
             7: 'Rn', }
    
attribute_metadata = [
                      
# protein domains
#                      
              AttributeMetadata(assoc_file = None, #os.path.join(ATTRIBUTES_DIR, 'interpro', '%(org_code)s', 'associations', 'assoc.txt'),
                                desc_file = None,  #os.path.join(ATTRIBUTES_DIR, 'interpro', '%(org_code)s', 'descriptions', 'desc.txt'),
                                name = "InterPro",
                                code = "PRDOM", # protein domains
                                desc = "Protein domain attributes",
                                linkout_label = "InterPro",
                                linkout_url = "http://www.ebi.ac.uk/interpro/IEntry?ac={1}",
                                default_selected = 0,
                                pub_name = "InterPro: the integrative protein signature database",
                                pub_url = "http://www.ncbi.nlm.nih.gov/pubmed/18940856",
                                attributes_identified_by = 'external_id',
                                assoc_format = ASSOC_FORMAT_SIMPLE),
                                                          
# miRNA targets from various sources
#                                                                                                                 
#              AttributeMetadata(assoc_file = os.path.join(ATTRIBUTES_DIR, 'mirbase', 'associations', 'processed', 'assoc.txt'),
#                                desc_file = os.path.join(ATTRIBUTES_DIR, 'mirbase', 'descriptions', 'postprocessed', 'mirna.txt'),
#                                name = "miRBASE",
#                                code = "mirbase", # protein domains
#                                desc = "MicroRNA Targets",
#                                linkout_label = "miRBase",
#                                linkout_url = "http://www.mirbase.org/cgi-bin/mirna_entry.pl?id={1}",
#                                default_selected = 0,
#                                pub_name = "miRBase: integrating microRNA annotation and deep-sequencing data",
#                                pub_url = "http://www.ncbi.nlm.nih.gov/pubmed/21037258",
#                                attributes_identified_by = 'name',
#                                assoc_format = ASSOC_FORMAT_SIMPLE),
#
#              AttributeMetadata(assoc_file = os.path.join(ATTRIBUTES_DIR, 'targetscan', 'processed', 'hsa_conserved_assoc.txt'),
#                                #desc_file = os.path.join(ATTRIBUTES_DIR, 'targetscan', 'descriptions', 'desc.txt'),
#                                desc_file = os.path.join(ATTRIBUTES_DIR, 'targetscan', 'postprocessed', 'hsa_conserved_idents.txt'),
#                                name = "TargetScan Conserved",
#                                code = "targetscanconserved", 
#                                desc = "MicroRNA Targets",
#                                linkout_label = "miRBase",
#                                linkout_url = "http://www.mirbase.org/cgi-bin/mirna_entry.pl?id={1}",
#                                default_selected = 0,
#                                pub_name = "TargetScan",
#                                pub_url = "http://targetscan.org",
#                                attributes_identified_by = 'name',
#                                assoc_format = ASSOC_FORMAT_SIMPLE),                            
#              
#              AttributeMetadata(assoc_file = os.path.join(ATTRIBUTES_DIR, 'targetscan', 'processed', 'hsa_nonconserved_assoc.txt'),
#                                #desc_file = os.path.join(ATTRIBUTES_DIR, 'targetscan', 'descriptions', 'desc.txt'),
#                                desc_file = os.path.join(ATTRIBUTES_DIR, 'targetscan', 'postprocessed', 'hsa_nonconserved_idents.txt'),
#                                name = "TargetScan Nonconserved",
#                                code = "targetscannonconserved", 
#                                desc = "MicroRNA Targets",
#                                linkout_label = "miRBase",
#                                linkout_url = "http://www.mirbase.org/cgi-bin/mirna_entry.pl?id={1}",
#                                default_selected = 0,
#                                pub_name = "TargetScan",
#                                pub_url = "http://targetscan.org",
#                                attributes_identified_by = 'name',
#                                assoc_format = ASSOC_FORMAT_SIMPLE),  
#              
#              AttributeMetadata(assoc_file = os.path.join(ATTRIBUTES_DIR, 'targetscan', 'processed', 'hsa_family_assoc.txt'),
#                                #desc_file = os.path.join(ATTRIBUTES_DIR, 'targetscan', 'descriptions', 'desc.txt'),
#                                desc_file = os.path.join(ATTRIBUTES_DIR, 'targetscan', 'postprocessed', 'hsa_family_idents.txt'),
#                                name = "TargetScan Family",
#                                code = "targetscanfamily", 
#                                desc = "MicroRNA Targets",
#                                linkout_label = "miRBase",
#                                linkout_url = "http://www.mirbase.org/cgi-bin/mirna_entry.pl?id={1}",
#                                default_selected = 0,
#                                pub_name = "TargetScan",
#                                pub_url = "http://targetscan.org",
#                                attributes_identified_by = 'name',
#                                assoc_format = ASSOC_FORMAT_SIMPLE),  
#                                            
#              AttributeMetadata(assoc_file = os.path.join(ATTRIBUTES_DIR, 'mirtarbase', 'processed', 'assoc.txt'),
#                                desc_file = os.path.join(ATTRIBUTES_DIR, 'mirtarbase', 'postprocessed', 'mirna.txt'), 
#                                name = "miRTarBase",  
#                                code = "mirtarbase", 
#                                desc = "MicroRNA Targets",
#                                linkout_label = "miRBase",
#                                linkout_url = "http://www.mirbase.org/cgi-bin/mirna_entry.pl?id={1}",
#                                #  http://www.mirbase.org/cgi-bin/query.pl?terms={1}
#                                default_selected = 0,
#                                pub_name = "miRTarBase: a database curates experimentally validated microRNA-target interactions",
#                                pub_url = "http://www.ncbi.nlm.nih.gov/pubmed/21071411",
#                                attributes_identified_by = 'name',
#                                assoc_format = ASSOC_FORMAT_SIMPLE),                   

              ]


def process_attribute_set(coredb, db, generic_db_dir, organism_id, metadata):
      
    logger.info("processing %s" % metadata.name)  
                         
    logger.info("attribute descriptions: %s" % metadata.desc_file)
    attributeLoader = process_attributes.AttributeLoader(coredb, db)  
    attribute_group_id = attributeLoader.process(organism_id, metadata.name, metadata.code, metadata.desc,
                                        metadata.linkout_label, metadata.linkout_url, 
                                        metadata.default_selected, 
                                        metadata.pub_name, metadata.pub_url,
                                        metadata.desc_file)
    
    # load associations
    logger.info("attribute-gene associations: %s" % metadata.assoc_file)
    if metadata.assoc_format == 1:
        assocLoader = process_attribute_associations.AttributeAssociationLoader(db, generic_db_dir)
        assocLoader.process(organism_id, attribute_group_id, metadata.assoc_file, metadata.attributes_identified_by)
    elif metadata.assoc_format == 2:
        assocLoader = process_attribute_associations2.AttributeAssociationLoader2(db, generic_db_dir)
        assocLoader.process(organism_id, attribute_group_id, metadata.assoc_file)
    else:
        raise Exception("unknown association file format: " + metadata.assoc_format)
    
def cleanup_generic_db_attributes(generic_db_dir):
    f = os.path.join(generic_db_dir, 'ATTRIBUTES.txt')
    if os.path.exists(f):
        os.remove(f)
    
    f = os.path.join(generic_db_dir, 'ATTRIBUTE_GROUPS.txt')
    if os.path.exists(f):
        os.remove(f)
    
    d = os.path.join(generic_db_dir, 'ATTRIBUTES')
    if os.path.isdir(d):
        shutil.rmtree(d)
    
def export(db, generic_db_dir):
    logger.info("metadata")
    exporter = export_attributes.AttributeExporter(db, generic_db_dir)
    exporter.export()     
    
def main():
#    organism_id = 4
    working_dir = os.path.join(BASE_DIR, 'working')    
    os.makedirs(working_dir)

    generic_db_dir = os.path.join(BASE_DIR, 'generic_db')
    cleanup_generic_db_attributes(generic_db_dir)
   
    coredbfile = os.path.join(working_dir, "core_metadata.sqlite")
    coredb = dbtools.CoreDB(coredbfile)
    coredb.drop_tables()
    coredb.create_tables()
#    core_loader = core_loader.CoreLoader(coredb)
     
    for org_id, org_code in organisms.items():
        logger.info("processing %s" % org_code)
        d = {'org_code': org_code}
        
#        core_loader.add_organism(org_id, org_code)
        
        identifier_file = os.path.join(BASE_DIR, 'mappings/processed/%(org_code)s_names.txt' % d)
        
        if not os.path.isdir(working_dir):
            os.makedirs(working_dir)
            
        dbfile = os.path.join(working_dir, "%d_metadata.sqlite" % org_id)
        db = dbtools.OrgDB(dbfile)
        db.drop_tables()
        db.create_tables()
    
        # load identifiers
        logger.info("identifiers")
        loader = process_identifiers.IdentifierLoader(db)
        loader.process(identifier_file)
                
        # process each set of attributes
        for metadata in attribute_metadata:
            # quick hack to fixup locations for multiple organisms
            metadata.assoc_file = os.path.join(ATTRIBUTES_DIR, 'interpro', '%(org_code)s' % d, 'associations', 'assoc.txt')
            metadata.desc_file = os.path.join(ATTRIBUTES_DIR, 'interpro', '%(org_code)s' % d, 'descriptions', 'desc.txt')
            
            process_attribute_set(coredb, db, generic_db_dir, org_id, metadata)
    
        # export to generic_db
        export(db, generic_db_dir)    
        db.close()
    
    logger.info("done")
    
if __name__ == '__main__':
    main()