'''
creates a table 'identifiers' in the given sqlite database,
containing the processed identifier data.
'''

import sqlite3, logging
import unittest, tempfile
from . import dbtools, test_support

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SEP = '\t'

class IdentifierLoader(object):
    '''
    classdocs
    '''


    def __init__(self, db):
        '''
        Constructor
        '''
        self.db = db
            
    def load(self, idfile):
        # all into mem
        records = []
        for line in open(idfile):
            line = line.strip()
            parts = line.split('\t')
            if len(parts) != 3:
                raise Exception("invalid input file")
            
            # remove org id prefix eg 'Hs:' from node id
            id_parts = parts[0].split(':')
            if len(id_parts) > 1:
                parts[0] = id_parts[1]

            records.append(parts)
            
        # big batch insert
        self.db.connection().executemany("insert into identifiers (node_id, symbol, source) values (?, ?, ?);", records)
        self.db.connection().commit()

    def count(self):
        
        cursor = self.db.connection().execute("select count(*) from identifiers")
        
        try:
            num = cursor.fetchone()[0]
        finally:
            cursor.close()
            
        return num      
        
    def process(self, identifier_file):
        self.load(identifier_file)        

class TestIdentifierLoader(unittest.TestCase):
    
    def test_load(self):
        identifier_file = tempfile.NamedTemporaryFile()        
        test_support.testDataToFile(test_support.NODES, identifier_file)
        
        dbfile = tempfile.NamedTemporaryFile()
        db = dbtools.OrgDB(dbfile.name)
        db.drop_tables()
        db.create_tables()
                               
        loader = IdentifierLoader(db)
        loader.process(identifier_file.name)
        num = loader.count()
        
        self.assertEqual(4, num, "check identifier count")
        db.close()
        