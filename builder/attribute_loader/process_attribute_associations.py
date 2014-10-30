'''
attribute associations are gene-attribute pairs. this script
loads attribute data, once gene identifiers and attribute identifiers
have been loaded
'''

import logging, os
import unittest, tempfile, shutil
from . import utils, dbtools, test_support, process_attributes, process_identifiers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
class AttributeAssociationLoader(object):

    def __init__(self, db, generic_db_dir):
        self.db = db
        self.generic_db_dir = generic_db_dir        
        self.clear_cache()
    
    def clear_cache(self):
        '''
        this shouldn't be needed, but the identifier lookups from the db 
        are too slow. the indices look correct so not sure what's wrong.
        need to check out the in-mem temp data pragma's, etc, but until
        then a quick in-memory cache makes things tolerable
        '''
        
        self.attribute_id_cache = {}
        self.node_id_cache = {}
                   
    def load(self, attribute_group_id, raw_filename, processed_filename, attributes_identified_by):
        
        # we don't have a unified identifier space yet for attributes, make
        # conditional for now
        if not attributes_identified_by in ['name', 'external_id']:
            raise Exception("unexpected value for attributes_identified_by: '%s'" % attributes_identified_by)

        self.clear_cache() # for safety accross multiple loads for different organisms/attribute groups        
        total = 0
        num_loaded = 0
        processed_file = open(processed_filename, 'w')
        
        with processed_file:
            for (gene, attribute) in utils.sparse_attribute_profile_reader(raw_filename):
                total += 1
 
#                if total % 100 == 0:
#                    print ".",
#                if total % 1000 == 0:
#                    print "\n",
                                           
                node_id = self.lookup_node_id(gene)
                
                if node_id == None:
                    #logger.warn("failed to find id %s %s %s %s" % (gene, node_id, attribute, attribute_id))
                    continue
                
                attribute_id = self.lookup_attribute_id(attribute_group_id, attribute, attributes_identified_by)
                if attribute_id == None:
                    continue
                
                num_loaded += 1
                line = '%d\t%d\n' % (node_id, attribute_id)
                processed_file.write(line)
            


        return num_loaded, total
    
    def lookup_node_id(self, gene):
        
        try:
            return self.node_id_cache[gene]
        except KeyError:
            cursor = self.db.connection().execute("select node_id from identifiers where symbol = ?", [gene])
            try:
                records = cursor.fetchall()
                if len(records) > 1:
                    raise Exception("multiple ids for gene '%s'" % gene)
                
                if len(records) == 0:
                    ID = None
                    logger.warn("Failed to find gene '%s'" % gene)
                else:
                    ID = int(records[0][0])
                
                self.node_id_cache[gene] = ID
                return ID
            finally:
                cursor.close()
    
    def lookup_attribute_id(self, attribute_group_id, attribute, attributes_identified_by):    
#        cursor = self.conn.execute("select id from attributes where attribute_group_id = %d and external_id = '%s'" % (attribute_group_id, attribute))
#        sql = "select id from attributes where attribute_group_id = %d and name = '%s'" % (attribute_group_id, attribute.lower())        
#        print sql

        try:
            return self.attribute_id_cache["%s-%s" % (attribute_group_id, attribute)]
        except KeyError:
            if attributes_identified_by == 'name':
                sql = "select id from attributes where attribute_group_id = %d and lower(name) = '%s'" % (attribute_group_id, attribute.lower())
            elif attributes_identified_by == 'external_id':
                sql = "select id from attributes where attribute_group_id = %d and external_id = '%s'" % (attribute_group_id, attribute)
            else:
                raise Exception("bad configuration") # should never get here
            
            cursor = self.db.connection().execute(sql)
            try:
                records = cursor.fetchall()
                if len(records) > 1:
                    raise Exception("multiple ids for attribute '%s'" % attribute)
                
                if len(records) == 0:
                    ID = None
                    logger.warn("Failed to find attribute '%s'" % attribute)
                else:
                    ID = int(records[0][0])
                
                self.attribute_id_cache["%s-%s" % (attribute_group_id, attribute)] = ID
                return ID
            
            finally:
                cursor.close()
    
    def process(self, organism_id, attribute_group_id, raw_filename, attributes_identified_by):
        attributes_dir = os.path.join(self.generic_db_dir, "ATTRIBUTES")
        if not os.path.isdir(attributes_dir):
            os.makedirs(attributes_dir)
        
        processed_filename = os.path.join(attributes_dir, "%s.txt" %  (attribute_group_id))
        num_loaded, total = self.load(attribute_group_id, raw_filename, processed_filename, attributes_identified_by)
        return processed_filename, num_loaded, total
    
class TestAttributeAssociationLoader(unittest.TestCase):
    
    def setUp(self):
        self.generic_db_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        shutil.rmtree(self.generic_db_dir)
        
    def test_load(self):

        dbfile = tempfile.NamedTemporaryFile()

        coredbfile = tempfile.NamedTemporaryFile()
        coredb = dbtools.CoreDB(coredbfile.name)
        coredb.drop_tables()
        coredb.create_tables()
        
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
        attribute_group_id = loader.process(organism_id, 'group_name', 'code', 'group description', 
                                            'linkout label', 'http://linkout.template/{1}', 0, 
                                            'publication name', 'http://publication.url',
                                            attributemeta_file.name)        
        # load associations
        attribute_file_with_unknowns = tempfile.NamedTemporaryFile()
        test_support.testDataToFile(test_support.NODE_ATTR_ASSOC_WITH_UNKNOWNS, attribute_file_with_unknowns)


        loader = AttributeAssociationLoader(db, self.generic_db_dir)
        processed_filename, num_loaded, total = loader.process(organism_id, attribute_group_id, attribute_file_with_unknowns.name, 'external_id')
        print("created", processed_filename)
                
        self.assertEqual(6, num_loaded, "check num loaded")
        self.assertEqual(8, total, "check total")

        db.close()
        