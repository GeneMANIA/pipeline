'''
tables for data loading. each organism goes in a seperate db,
so no organism id column in here
'''

import sqlite3, unittest, tempfile

class BaseDB(object):
    '''
    somehow orm's never work for me
    try low-tech & simple
    '''
    
    def __init__(self, dbfile):
        self.conn = sqlite3.connect(dbfile)    
        self.conn.row_factory = sqlite3.Row
       
    def drop_tables(self):
        pass
         
    def create_tables(self):
        pass
    
    def connection(self):
        return self.conn
    
    def close(self):
        self.conn.close()
        
class CoreDB(BaseDB):

    def drop_tables(self):
        sql = """
         drop table if exists organisms;
         drop table if exists attribute_groups;
         drop table if exists attributes;
         """
         
        self.conn.executescript(sql)
        self.conn.commit()
         
    def create_tables(self):
        sql = """
        create table organisms (id integer primary key, org_code text);
        create table attribute_groups(id integer primary key, organism_id integer);
        create table attributes (id integer primary key, attribute_group_id integer, organism_id integer);
        """

        self.conn.executescript(sql)
        self.conn.commit()
    
        
class OrgDB(BaseDB):
    
    def drop_tables(self):
        sql = """
         drop index if exists ix_identifiers_symbol;
         drop table if exists identifiers;
         
         drop table if exists attribute_groups;
         
         drop index if exists ix_attributes;
         drop index if exists ix_attributes_names;
         drop table if exists attributes;
         """
         
        self.conn.executescript(sql)
        self.conn.commit()
         
    def create_tables(self):
        sql = """
        create table identifiers (node_id text, symbol text, source text);
        create index ix_identifers_symbol on identifiers (symbol);        
 
        create table attribute_groups (id integer primary key, organism_id integer, name text collate nocase, code text, description text, linkout_label text, linkout_url text, default_selected text, publication_name text, publication_url text);
        create table attributes (id integer primary key, organism_id integer, attribute_group_id integer, external_id text collate nocase, name text, description text);
        
        create index ix_attributes on attributes (attribute_group_id, external_id collate nocase);
        create index ix_attributes_names on attributes (attribute_group_id, name collate nocase);        
        """

        self.conn.executescript(sql)
        self.conn.commit()
        
class TestDB(unittest.TestCase):
    def test_dropcreate(self):
        
        dbfile = tempfile.NamedTemporaryFile()
        
        db = OrgDB(dbfile.name)
        db.drop_tables()
        db.create_tables()
        db.drop_tables()

        dbfile = tempfile.NamedTemporaryFile()        
        db = CoreDB(dbfile.name)
        db.drop_tables()
        db.create_tables()
        db.drop_tables()

        