'''
usage:

  attribute_processor db.cfg [load|export]
'''

import os, sys, shutil
import argparse
import unittest, logging
from attribute_loader import process_attributes, process_attribute_associations
from attribute_loader import process_attribute_associations2, export_attributes
from attribute_loader import process_identifiers
from attribute_loader import dbtools
from configobj import ConfigObj

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        
def process_attribute_set(coredb, db, generic_db_dir, organism_id, assoc_file, desc_file, metadata):
      
    logger.info("processing %s" % metadata['name'])  
                     
    logger.info("attribute descriptions: %s" % desc_file)
    attributeLoader = process_attributes.AttributeLoader(coredb, db)  
    attribute_group_id = attributeLoader.process(organism_id, metadata['name'], metadata['code'], metadata['desc'],
                                        metadata['linkout_label'], metadata['linkout_url'], 
                                        int(metadata['default_selected']), 
                                        metadata['pub_name'], metadata['pub_url'],
                                       desc_file)
    
    # load associations
    logger.info("attribute-gene associations: %s" % assoc_file)
    if int(metadata['assoc_format']) == 1:
        assocLoader = process_attribute_associations.AttributeAssociationLoader(db, generic_db_dir)
        assocLoader.process(organism_id, attribute_group_id, assoc_file, metadata['attributes_identified_by'])
    elif int(metadata['assoc_format']) == 2:
        assocLoader = process_attribute_associations2.AttributeAssociationLoader2(db, generic_db_dir)
        assocLoader.process(organism_id, attribute_group_id, assoc_file)
    else:
        raise Exception("unknown association file format: " + metadata['assoc_format'])
    
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

def load_attribute_metadata(config):
    
    configs = []
    
    for i in config['Attributes']:
        configs.append(config['Attributes'][i])
    return configs
        
def export(config):
    logger.info("exporting")
    attribute_dir = datalib.get_location(config, 'attribute_dir')
    generic_db_dir = datalib.get_location(config, 'generic_db_dir')
    work_dir = os.path.join(attribute_dir, "work")
  
    organisms = config['Organisms']['organisms']
    for organism in organisms:
        organism_name = config[organism]['name']
        organism_id = int(config[organism]['gm_organism_id'])
        
        logger.info("exporting %s" % organism_name)
        orgdbfile = os.path.join(work_dir, "%s_metadata.sqlite" % organism_id)    
        orgdb = dbtools.OrgDB(orgdbfile)
        exporter = export_attributes.AttributeExporter(orgdb, generic_db_dir)
        exporter.export()   

def load(config):
    attribute_dir = datalib.get_location(config, 'attribute_dir')
    generic_db_dir = datalib.get_location(config, 'generic_db_dir')
    work_dir = os.path.join(attribute_dir, "work")
    processed_mapping_dir = datalib.get_location(config, 'processed_mappings_dir')
    
    attribute_metadata = load_attribute_metadata(config)
    if not attribute_metadata:
        print("no attributes defined")
        return
    
    # cleanup lingering work dir from previous runs
    if (os.path.exists(work_dir)):
        shutil.rmtree(work_dir)
    
    os.makedirs(work_dir)
    
    # create core db for data shared betwen organisms
    coredbfile = os.path.join(work_dir, "core_metadata.sqlite")
    coredb = dbtools.CoreDB(coredbfile)
    coredb.drop_tables()
    coredb.create_tables()        
    
    organisms = config['Organisms']['organisms']
    for organism in organisms:
        organism_name = config[organism]['name']
        short_id = config[organism]['short_id']
        organism_id = int(config[organism]['gm_organism_id'])
        
        logger.info("loading %s" % organism_name)

        processed_mapping_file = os.path.join(processed_mapping_dir, "%s_names.txt" % short_id)
        
        if not os.path.exists(processed_mapping_file):
            raise Exception("failed to find identifier file: '%s'" % processed_mapping_file)
        
        # org specific database
        orgdbfile = os.path.join(work_dir, "%s_metadata.sqlite" % organism_id)
        orgdb = dbtools.OrgDB(orgdbfile)
        orgdb.drop_tables()
        orgdb.create_tables()
        
        # load identifiers
        logger.info("identifiers")
        loader = process_identifiers.IdentifierLoader(orgdb)
        loader.process(processed_mapping_file)
                
        # process each set of attributes defined the master config
        for metadata in attribute_metadata:
            
            # The configured attribute data does not need to be present for every organism,
            # check & process if found
            try: 
                assoc_file_pattern = os.path.join(datalib.get_location(config), metadata['assoc_file'])
                assoc_file = datalib.magic_organism_file_matcher(config, short_id, assoc_file_pattern)
                
                desc_file_pattern = os.path.join(datalib.get_location(config), metadata['desc_file'])
                desc_file = datalib.magic_organism_file_matcher(config, short_id, desc_file_pattern)
            except:
                logger.warn("failed to find attribute data '%s' for organism %s, skipping" % (metadata['name'], organism_name))
                continue
            
            process_attribute_set(coredb, orgdb, generic_db_dir, organism_id, assoc_file, desc_file, metadata)
    
        # process each set of attributes pulled in from the datamart, for this organism
        attribute_import_dir = os.path.join(attribute_dir, 'import')
        datamart_import_config_file = os.path.join(attribute_import_dir, "attribute-metadata.cfg")
        if os.path.exists(datamart_import_config_file):
            datamart_import_config = ConfigObj(datamart_import_config_file, encoding="utf8")
            for section in datamart_import_config:
                metadata = datamart_import_config[section]
                if metadata['org_code'] != short_id:
                    continue
                
                print("processing datamart import for", section.encode('utf8'))
                
                assoc_file = os.path.join(attribute_import_dir, metadata['assoc_file'])
                desc_file = os.path.join(attribute_import_dir, metadata['desc_file'])
                
                process_attribute_set(coredb, orgdb, generic_db_dir, organism_id, assoc_file, desc_file, metadata)
               
        orgdb.close()
    coredb.close()
    
def main(config, operation):
    
    if operation == 'load':
        load(config)
    elif operation == 'export':
        export(config)
    else:
        raise Exception('unknown operation: ' % operation)
    
    print("done")
    
if __name__ == '__main__':

#    config_file = sys.argv[1]
#    operation = sys.argv[2] # load or export
#    config = datalib.load_main_config(config_file)
#
#    main(config, operation)

    parser = argparse.ArgumentParser(description='scrub attributes against identifiers')

    parser.add_argument('filenames', metavar='files', type=str, nargs='+',
                help='list if input files')

    parser.add_argument('--output', type=str,
                help='name of clean output file, stdout if missing')

    parser.add_argument('--log', type=str,
                help='name of report log file')

    args = parser.parse_args()
    main(args.filenames, args.output, args.log)


def get_testdata_dir():
    this_dir = os.path.dirname(os.path.abspath(__file__))
    testdata_dir = os.path.join(this_dir, 'testdata')
    return testdata_dir

class Test(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def tearDown(self):
        pass
    
    def test_load(self):
        config_file = os.path.join(get_testdata_dir(), 'db.cfg')
        
        config = datalib.load_main_config(config_file)
        main(config, 'load')
        
    def test_export(self):
        config_file = os.path.join(get_testdata_dir(), 'db.cfg')
        
        config = datalib.load_main_config(config_file)
        main(config, 'export')        