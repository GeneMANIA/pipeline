'''
export attribute metadata from sqlite to generic db

 * table of attribute groups
 * table of individual attributes
  
'''

import logging, os
import unittest, tempfile, shutil
from . import process_identifiers, process_attributes, process_attribute_associations
from . import dbtools, utils, test_support
import codecs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# the fields don't have to be the same as what we are storing in sqlite, 
# so we don't create dynamically but specify here instead
ATTRIBUTE_GROUPS_FILE_NAME = 'ATTRIBUTE_GROUPS.txt'
ATTRIBUTE_GROUPS_FIELDS= ['ID', 'ORGANISM_ID', 'NAME', 'CODE', 'DESCRIPTION', 'LINKOUT_LABEL', 'LINKOUT_URL', 'DEFAULT_SELECTED', 'PUBLICATION_NAME', 'PUBLICATION_URL']

ATTRIBUTES_FILE_NAME = 'ATTRIBUTES.txt'
ATTRIBUTES_FIELDS = ['ID', 'ORGANISM_ID', 'ATTRIBUTE_GROUP_ID', 'EXTERNAL_ID', 'NAME', 'DESCRIPTION']

SCHEMA_FILE_NAME = 'SCHEMA.txt'

class AttributeExporter(object):

    def __init__(self, db, generic_db_dir):        
        self.db = db
        self.generic_db_dir = generic_db_dir
        
        if not os.path.isdir(generic_db_dir):
            os.makedirs(generic_db_dir)
    
    def export(self):        
        self.export_attribute_groups()
        self.export_attributes()
        self.update_schema_file()
        
    def export_attribute_groups(self):
        
        afile = codecs.open(os.path.join(self.generic_db_dir, ATTRIBUTE_GROUPS_FILE_NAME), encoding='utf8', mode='a')
        
        with afile:
            cursor = self.db.connection().execute("select id, organism_id, name, code, description, linkout_label, linkout_url, default_selected, publication_name, publication_url from attribute_groups")
            
            keys = [ field[0] for field in cursor.description ]
            if len(keys) != len(ATTRIBUTE_GROUPS_FIELDS):
                raise Exception("internal consistency error, incorrect number of attribute group fields")
 
            lines = []
            
            rows = cursor.fetchmany()
            while rows:
                for row in rows:
                    line = '\t'.join(str(item) if isinstance(item, int) else item for item in row) + '\n'
                    lines.append(line)
                
                rows = cursor.fetchmany()
 
            if lines:
                afile.writelines(lines)
                
            cursor.close()
            
    def export_attributes(self):
        afile = codecs.open(os.path.join(self.generic_db_dir, ATTRIBUTES_FILE_NAME), encoding='utf8', mode='a')
        
        with afile:
            cursor = self.db.connection().execute("select id, organism_id, attribute_group_id, external_id, name, description from attributes")

            keys = [ field[0] for field in cursor.description ]
            if len(keys) != len(ATTRIBUTES_FIELDS):
                raise Exception("internal consistency error, incorrect number of attribute fields")                
 
            lines = []
            
            rows = cursor.fetchmany()
            while rows:
                for row in rows:
                    # joining ints and unicode strings, have to be careful about conversion
                    line = '\t'.join(str(item) if isinstance(item, int) else item for item in row) + '\n'
                    lines.append(line)
                
                rows = cursor.fetchmany()
 
            if lines:
                afile.writelines(lines)
                
            cursor.close()
    
    def update_schema_file(self):
        '''
        write a pair of records into the schema file with the table and records
        for the attribute group and attribute tables
        '''
        
        schema_filename = os.path.join(self.generic_db_dir, SCHEMA_FILE_NAME)

        if self.is_schema_updated(schema_filename):
            logger.info('skipping schema update')
            return
    
        logger.info('updating schema')
                
        schema_file = open(schema_filename, 'a')
        
        with schema_file:
            fields = [os.path.splitext(ATTRIBUTE_GROUPS_FILE_NAME)[0]] + ATTRIBUTE_GROUPS_FIELDS
            line = utils.SEP.join(fields) + '\n'
            schema_file.write(line)

            fields = [os.path.splitext(ATTRIBUTES_FILE_NAME)[0]] + ATTRIBUTES_FIELDS
            line = utils.SEP.join(fields) + '\n'
            schema_file.write(line)
    
    def is_schema_updated(self, schema_filename):
        '''
        has the schema already been brought up to date?
        '''
        
        if not os.path.exists(schema_filename):
            return False
        
        lines = open(schema_filename).readlines()
        
        needle = os.path.splitext(ATTRIBUTE_GROUPS_FILE_NAME)[0]
        for line in lines:
            if line.startswith(needle):
                return True
            
        return False
        
class TestAttributeExporter(unittest.TestCase):
    def setUp(self):
        self.generic_db_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        shutil.rmtree(self.generic_db_dir)
           
    def test_export(self):

        coredbfile = tempfile.NamedTemporaryFile()
        coredb = dbtools.CoreDB(coredbfile.name)
        coredb.drop_tables()
        coredb.create_tables()
        
        dbfile = tempfile.NamedTemporaryFile()
        db = dbtools.OrgDB(dbfile.name)
        db.drop_tables()
        db.create_tables()
                
        organism_id = 1
        
        # load identifiers
        identifier_file = tempfile.NamedTemporaryFile()        
        test_support.testDataToFile(test_support.NODES, identifier_file)
        
        loader = process_identifiers.IdentifierLoader(db)
        loader.process(identifier_file.name)
        
        # load attributes
        attribute_file = tempfile.NamedTemporaryFile()        
        test_support.testDataToFile(test_support.NODE_ATTR_ASSOC, attribute_file)
  
        attributemeta_file = tempfile.NamedTemporaryFile()        
        test_support.testDataToFile(test_support.ATTR_METADATA, attributemeta_file)
                
        loader = process_attributes.AttributeLoader(coredb, db)
        attribute_group_id = loader.process(organism_id, 'group_name', 'code',  'group description', 
                                            'linkout label', 'http://linkout.template/{1}', 0, 
                                            'publication name', 'http://publication.url',
                                            attributemeta_file.name)
        
        # load associations
        attribute_file_with_unknowns = tempfile.NamedTemporaryFile()
        test_support.testDataToFile(test_support.NODE_ATTR_ASSOC_WITH_UNKNOWNS, attribute_file_with_unknowns)


        loader = process_attribute_associations.AttributeAssociationLoader(db, self.generic_db_dir)
        loader.process(organism_id, attribute_group_id, attribute_file_with_unknowns.name, 'external_id')
        
        # finally, export
        exporter = AttributeExporter(db, self.generic_db_dir)
        exporter.export()
        
        self.assertTrue(os.path.isfile(os.path.join(self.generic_db_dir, ATTRIBUTE_GROUPS_FILE_NAME)))
        self.assertTrue(os.path.isfile(os.path.join(self.generic_db_dir, ATTRIBUTES_FILE_NAME)))
        self.assertTrue(os.path.isfile(os.path.join(self.generic_db_dir, SCHEMA_FILE_NAME)))
        
        db.close()