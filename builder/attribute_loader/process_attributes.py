'''
load attribute group and attribute identifier data

some implementation notes:
 
 * load attribute metadata from a 3-column file, with fields interpro accession, name, description.
   
 * collate nocase on external_id on both table create and index so not case sensitive

 * input is an association file in sparse-profile format (gene-multiple attributes/line)
 
 * creates/updates a sqlite database as an intermediate output: sqlitedb
 
 * separate script to dump the sqlite db to create files in generic db
'''

import sqlite3, logging
import unittest, tempfile
from . import test_support
from . import utils, dbtools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AttributeLoader(object):
    '''
    identifiers table must exist
    '''

    def __init__(self, coredb, db):
        self.coredb = coredb
        self.db = db    
    
    def add_attribute_group(self, organism_id, name, code, desc, linkout_label, linkout_url, default_selected, pub_name, pub_url):
        
        # first create a group id by inserting into core table (since this id is global across organisms,
        # which is probably a design error)
        sql = "insert into attribute_groups (organism_id) values (%d);" % organism_id
        conn = self.coredb.connection()
        with conn:
            cursor = conn.execute(sql)
            last_row_id = cursor.lastrowid
        
        attribute_group_id = last_row_id

        # now insert the attribute group metadata record into the organism db
        sql = "insert into attribute_groups (id, organism_id, name, code, description, linkout_label, linkout_url, default_selected, publication_name, publication_url) values (%d, %d, '%s', '%s', '%s', '%s', '%s', '%d', '%s', '%s');" %  (attribute_group_id, organism_id, name, code, desc, linkout_label, linkout_url, default_selected, pub_name, pub_url)        
        conn = self.db.connection()
        with conn:
            cursor = conn.execute(sql)
        
        return attribute_group_id
    
    def load_attribute_data(self, organism_id, attribute_group_id, filename):
   
        # good grief, do the attribute ids need to be global?
        # for now, first insert into global table. this is ridiculous and evil, 
        # separate db's, think about the integrity non-guarantees here. should
        # try linked db's?
        for (attribute, name, desc) in utils.attribute_metadata_reader(filename):
            sql = "insert into attributes (attribute_group_id, organism_id) values (?, ?);"
            values = (attribute_group_id, organism_id)
            conn = self.coredb.connection()
            cursor = conn .execute(sql, values)
            attribute_id = cursor.lastrowid
                 
   #     for (attribute, name, desc) in utils.attribute_metadata_reader(filename):            
            sql = "insert into attributes (id, organism_id, attribute_group_id, external_id, name, description) values (?, ?, ?, ?, ?, ?);"
            values = (attribute_id, organism_id, attribute_group_id, attribute, name, desc)
            self.db.connection().execute(sql, values)
        
        self.coredb.connection().commit()
        self.db.connection().commit()
        
    def count_attributes(self, attribute_group_id):        
        cursor = self.db.connection().execute("select count(*) from attributes;")
        num = cursor.fetchone()[0]
        cursor.close()
        return num
    
    def count_attribute_groups(self):
        cursor = self.db.connection().execute("select count(*) from attribute_groups;")
        num = cursor.fetchone()[0]
        cursor.close()
        return num
    
    def process(self, organism_id, name, code, desc, linkout_label, linkout_url, default_selected, pub_name, pub_url, attribute_file_name):
        '''
        create an attribute group, loading attributes from given file.
        returns id of newly created group
        '''

        attribute_group_id = self.add_attribute_group(organism_id, name, code, desc, linkout_label, linkout_url, default_selected, pub_name, pub_url)
        self.load_attribute_data(organism_id, attribute_group_id, attribute_file_name)
        
        return attribute_group_id            
    
class TestAttributeLoader(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def tearDown(self):
        pass
    
    def test_load(self):
        '''
        load some attributes, include the same attribute with
        different case. make sure the correct # are inserted
        '''
        
        attributemeta_file = tempfile.NamedTemporaryFile()        
        test_support.testDataToFile(test_support.ATTR_METADATA, attributemeta_file)
        
        coredbfile = tempfile.NamedTemporaryFile()
        coredb = dbtools.CoreDB(coredbfile.name)
        coredb.drop_tables()
        coredb.create_tables()
        
        dbfile = tempfile.NamedTemporaryFile()
        db = dbtools.OrgDB(dbfile.name)
        db.drop_tables()
        db.create_tables()
                
        logger.info("running test with data %s db %s" % (attributemeta_file.name, dbfile.name))
        organism_id = 1
        
        loader = AttributeLoader(coredb, db)
        attribute_group_id = loader.process(organism_id, 'group_name', 'code', 'group description', 
                                            'linkout label', 'http://linkout.template/{1}', 0, 
                                            'publication name', 'http://publication.url',
                                            attributemeta_file.name)
        self.assertGreater(attribute_group_id, 0)
        
        num = loader.count_attribute_groups()
        self.assertEqual(1, num)
        
        num = loader.count_attributes(1)        
        self.assertEqual(4, num)
        
        db.close()
        