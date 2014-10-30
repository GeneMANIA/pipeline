'''
Symbol cleanup: 

 * input is identifier mapping files produced by IDMapper, one record
   per entity (gene), multiple columns containing identifiers, synonyms,
   descriptions, and biotype flag. 
 * output is a normalized identifier file containing an id-symbol-source triplet
   for each record, with various processing applied (see below)
 * entities are filtered by biotype
 * all symbols in final output are unique to a single entity (ie gene)
 * case insensitive symbol comparisons
 * disallow empty symbols, the symbol 'N/A', and symbols containing space chars
 * symbol ambiguity in input data is resolved by:
     * using reverse mappings from entrez to ensembl to select an ensembl to entrez mapping
       when multiple ensembl ids point to the same entrez id
     * merging entities with the same 'Gene Name'
     * removing synonyms that clash with non-synonyms, the non-synonyms are kept
     * removing non-synonyms that clash with non-synonyms in different entities
     * selecting arbitrarily one symbol when clash is only with other symbols in
       the same entity
       
'''

import sqlite3, codecs, os, sys
import unittest
from io import StringIO
from . import parsers
import contextlib, difflib
from .constants import *

INSERT_BATCH_SIZE = 100

class IdentifierDB:

    def __init__(self, dbfile, report=None):
        '''
        report is a file-like object
        '''
  
        if not report:
            self.report = open(os.devnull, 'w')
        else:
            self.report = report
        
        self.conn = sqlite3.connect(dbfile)
        self.conn.row_factory = sqlite3.Row

        self.create_tables()
        self.index()    

    def create_tables(self):
        self.drop_tables()
        sql = """
        create table symbols (rowid integer primary key, node_id integer, symbol text collate nocase, source text);        
        create table entities (node_id integer primary key, biotype text, description text);
        create table entrez_to_ensembl (entrez_id text, name text, ensembl_id text, biotype text);
        """
        self.conn.executescript(sql)
        self.conn.commit()
        
    def drop_tables(self):
        sql = """
        drop table if exists symbols;
        drop table if exists entities;
        drop table if exists entrez_to_ensembl;
        """
        self.conn.executescript(sql)
        self.conn.commit()
    
    def index(self):
        
        self.drop_indices()
        sql = """        
        create index ix_symbols_symbol on symbols (symbol collate nocase);
        create index ix_symbols_node_id on symbols (node_id);
        create index ix_entrez_to_ensembl on entrez_to_ensembl (entrez_id);
        """
        self.conn.executescript(sql)
        self.conn.commit()        
    
    def drop_indices(self):
        sql = """
        drop index if exists ix_symbols_symbol;
        drop index if exists ix_symbols_node_id;
        drop index if exists ix_entrez_to_ensembl;
        """        
        self.conn.executescript(sql)
        self.conn.commit()
        
    def addId(self, id, biotype, desc):
        sql = "insert into entities (node_id, biotype, description) values (?,?,?)"
        try:
            self.conn.execute(sql, (id, biotype, desc))
        except:
            print(id, biotype, desc)
            raise

    def addSymbol(self, id, symbol, source ):
        sql = "insert into symbols (node_id, symbol, source) values (?,?,?)"
        self.conn.execute(sql, (id, symbol, source))
        self.conn.commit()

    def addSymbols(self, symbols):
        '''
        symbols is a list of (nodeid, symbol, source) tuples
        '''
        sql = "insert into symbols (node_id, symbol, source) values (?,?,?)"
        self.conn.executemany(sql, symbols)
 
    def addReverseSymbols(self, symbols):
        sql = "insert into entrez_to_ensembl (entrez_id, name, ensembl_id, biotype) values (?,?,?,?)"
        self.conn.executemany(sql, symbols)
               
    def count(self, sql):
        cursor = self.conn.execute(sql)
        return cursor.fetchone()[0]
        
    def symbols_size(self):
        return self.count("select count(*) from symbols")
    
    def entities_size(self):
        return self.count("select count(*) from entities")
    
    def changes(self):
        return self.count("select changes()")
    
    def selectToFile(self, sql, afile):
        cursor = self.conn.execute(sql)
        #print sql
        row = cursor.fetchone()
        while row:
            afile.write('%s\n' % '\t'.join(row))
            row = cursor.fetchone()
        
    def cleanup(self):
        '''
        remove symbols that are missing, or that contain whilespace
        '''
        self.conn.execute("delete from symbols where symbol = 'N/A';")
        self.conn.execute("delete from symbols where symbol = '';")
        self.conn.execute("delete from symbols where symbol is null;")
        self.conn.execute("delete from symbols where symbol like '% %';")
        self.conn.commit()
        
    def remove_unwanted_sources(self, filters):
        '''
        process blacklist of sources we want ignored
        '''
        
        if not filters:
            return
        
        ignores_clause = ["'" + i + "'" for i in filters]
        ignores_clause = '(' + ','.join(ignores_clause) + ')'
        sql = "delete from symbols where source in " + ignores_clause
        self.conn.execute(sql)
        self.conn.commit()
        
    def standardize_source_names(self):
        '''
        source names come from the header line of the input file. apply
        any naming convention requirements here
        '''
        
        self.conn.execute("update symbols set source = ? where source = ?;", (RAW_SYNONYM, RAW_SYNONYM_ORIG))

        # todo, if we have other spellings for the source 'Synonym'
        #self.conn.execute("update symbols set source = ? where source = ?;", (RAW_SYNONYM, 'Alias'))

        self.conn.commit()
        
    def clean_empties(self):
        '''
        after our various symbol cleanups, its possible
        that we've ended up with entities for which all
        symbols have been removed. remove such entities themselves
        '''
              
        sql = """
        delete from entities where node_id in 
        (select entities.node_id
        from entities left outer join symbols
        on entities.node_id = symbols.node_id
        group by entities.node_id
        having count(symbols.symbol) = 0);
        """
        self.conn.execute(sql)
        self.conn.commit()
        
    def biotype_filter(self, biotype_keepers):
        '''
        remove any entities and the corresponding symbols
        that don't match the given list of biotypes
        '''
        
        sql = """
        delete from symbols where rowid in 
        (select symbols.rowid from symbols, entities 
        where symbols.node_id = entities.node_id 
        and entities.biotype not in %s);
        """

        ignores_clause = ["'" + i + "'" for i in biotype_keepers]
        ignores_clause = '(' + ','.join(ignores_clause) + ')'

        sql = sql % ignores_clause    
        self.conn.execute(sql)
        
        sql = "delete from entities where biotype not in %s" % ignores_clause
        self.conn.execute(sql)
        self.conn.commit()
        
        n = self.changes()
        self.report.write("applying biotype filters %s, total records removed: %d\n" % (ignores_clause, n))
        
        # also filter reverse mappings by biotype
        ignores_clause = ["'" + i + "'" for i in REV_DEFAULT_BIOTYPE_KEEPERS]
        ignores_clause = '(' + ','.join(ignores_clause) + ')'
        sql = "delete from entrez_to_ensembl where biotype not in " + ignores_clause
        self.conn.execute(sql)
        self.conn.commit()
        
        return n

    def delink(self):
        '''
        use reverse mappings to break links in raw mappings
        '''        
        
        # create an ensembl to entrez table from the raw mapping data
        sql = """
        create temp table ensembl_to_entrez as 
        select t1.node_id, t3.symbol as name, t1.symbol as entrez_id, t2.symbol as ensembl_id,
        t1.rowid as entrez_rowid, t2.rowid as ensembl_rowid
        from symbols as t1, symbols as t2, symbols as t3
        where t1.node_id = t2.node_id
        and t1.node_id = t3.node_id
        and t1.source = 'Entrez Gene ID'
        and t2.source = 'Ensembl Gene ID'
        and t3.source = 'Gene Name';
        """
        self.conn.executescript(sql)
                
        # some indices help
        sql = """
        create index ix_1a on ensembl_to_entrez (entrez_id);
        create index ix_1b on ensembl_to_entrez (ensembl_id);
        create index ix_2a on entrez_to_ensembl (entrez_id);
        create index ix_2b on entrez_to_ensembl (ensembl_id);
        """
        self.conn.executescript(sql)
                
        # create a table containing only the consistent mappings,
        # that is where ensembl to entrez and entrez to ensembl
        # agree on both ensembl and entrez ids. gene name is
        # not considered
        sql = """
        create temp table consistent_ensembl_entrez as
        select t1.ensembl_id, t1.entrez_id, t1.entrez_rowid
        from ensembl_to_entrez as t1,
        entrez_to_ensembl as t2
        where t1.ensembl_id = t2.ensembl_id
        and t1.entrez_id = t2.entrez_id;
        """        
        self.conn.executescript(sql)
                
        sql = """
        create index ix3 on consistent_ensembl_entrez (entrez_rowid);
        """
        self.conn.executescript(sql)
                
        # we can now use the consistent mappings table to 
        # clean ambiguities in the raw data, but we don't want 
        # to apply it in cases where the raw data
        # doesn't have ambiguities, even if not consistent with the
        # reverse mappings (at least that's our approach for now).
        #
        # so create a table containing inconsistencies from the raw data.
        # an inconsistency is two different gene entities (node_id's)
        # having the same entrez id
        sql = """
        create temp table inconsistent_raw_entities as
        select t1.node_id as node_id1, t2.node_id as node_id2, t1.entrez_id, 
        t1.ensembl_id as ensembl_id1, t2.ensembl_id as ensembl_id2,
        t1.entrez_rowid as entrez_rowid1, t2.entrez_rowid as entrez_rowid2
        from ensembl_to_entrez as t1, ensembl_to_entrez as t2
        where t1.entrez_id = t2.entrez_id
        and t1.node_id < t2.node_id;
        """
        self.conn.executescript(sql)        
        
        #raise Exception("stop here for debugging")
        # to be extra conservative, drop any rows from the inconsistent table
        # that don't have an entry in the consistent table, as these remaining 
        # inconsistencies are the only ones we can actually resolve
        sql = """
        delete from inconsistent_raw_entities
        where entrez_id not in 
        (select entrez_id from consistent_ensembl_entrez);
        """
        self.conn.executescript(sql)        
        
        # now remove entrez ids from symbols where they are among
        # the inconsistencies in the raw data, and the aren't one of the
        # consistent entrez-ensembl mappings.
        sql = """
        delete from symbols where source = 'Entrez Gene ID' and
        rowid not in 
        (select entrez_rowid from consistent_ensembl_entrez)
        and rowid in 
        (select entrez_rowid1 from inconsistent_raw_entities 
        union select entrez_rowid2 from inconsistent_raw_entities);
        """
        self.conn.executescript(sql)
                
    def merge(self):       
        '''
        several helper/working tables:
        
          * merge: contains gene name and pairs of node_id's, for different nodes (entities)
            having the same gene name, with the first column getting the lower
            node_id
          * blessed: for each gene name, contains the node_id of an entity to keep (the 
            others will be merged into this one)
          * to_fix: contains row_id in symbols to be updated, and the corresponding node_id
            (from the blessed list) to be used in the update
        '''
        
        sql = """
        drop table if exists merge;
        drop table if exists blessed;
        create table merge (symbol text collate nocase, node_id1 int, node_id2 int);
        create table blessed (symbol text collate nocase, node_id int);
        """
        self.conn.executescript(sql)
        
        sql = """
        insert into merge (symbol, node_id1, node_id2)
        select t1.symbol, t1.node_id, t2.node_id 
        from symbols as t1, symbols as t2
        where t1.symbol = t2.symbol
        and t1.source = '%s'
        and t1.source = t2.source
        and t1.node_id < t2.node_id;
        """
        sql = sql % RAW_GENE_NAME
        
        self.conn.execute(sql)
        self.conn.commit()
        
        sql = """
        insert into blessed (symbol, node_id)
        select symbol, min(node_id1)
        from merge
        group by symbol;
        """
        
        self.conn.execute(sql)
        self.conn.commit()
        
        sql = """
        drop index if exists ix_merge1;
        drop index if exists ix_merge2;
        drop index if exists ix_blessed1;
        drop index if exists ix_blessed2;
        
        create index ix_merge1 on merge (symbol);
        create index ix_merge2 on merge(node_id2);
        create index ix_blessed1 on blessed (symbol);
        create index ix_blessed2 on blessed (node_id);
        """
        self.conn.executescript(sql)
                
        sql = """
        drop table if exists to_fix;
        drop index if exists ix_tofix;
        
        create table to_fix as 
        select symbols.rowid as rowid, blessed.node_id as node_id
        from symbols, blessed, merge
        where symbols.node_id = merge.node_id2
        and blessed.node_id = merge.node_id1;
        
        create index ix_tofix on to_fix (rowid);
        """
        self.conn.executescript(sql)
        self.conn.commit()

        # report what we're doing, before applying the fixes
        n = self.count("select count(distinct symbol) from merge;")
        self.report.write("%d gene names belong to multiple genes and will be merged\n" % n)
        if n > 0:
            sql = "select distinct symbol from merge;"
            self.selectToFile(sql, self.report)
        
        # apply the new entity ids to the symbols table
        sql = """
        update symbols set node_id =            
        (select to_fix.node_id from to_fix
        where to_fix.rowid = symbols.rowid)
        where rowid in (select rowid from to_fix);
        """
        self.conn.executescript(sql)
        self.conn.commit()
        
        # the entities that have been merged can be
        # removed from the entities table
        sql = """
        delete from entities where node_id in
        (select distinct node_id2 from merge);
        """
        self.conn.executescript(sql)
        self.conn.commit()
               
                
    def dedup(self):

        self.dedup_syn_vs_nonsyn()
        self.dedup_entity_vs_entity()
        self.dedup_entity_within_itself()
    
    def dedup_syn_vs_nonsyn(self):
        # synonyms that conflict with non synonyms
        sql = """
        drop table if exists dup_syn;
        
        create table dup_syn as 
        select t1.rowid as rowid 
        from symbols as t1, symbols as t2 
        where t1.source = '%s' 
        and t2.source != '%s'
        and t1.symbol = t2.symbol;
        """ % (RAW_SYNONYM, RAW_SYNONYM)
        
        self.conn.executescript(sql)
        self.conn.commit()

        # report
        n = self.count("select count(*) from dup_syn;")
        self.report.write("removing %d synonyms for conflict with non-synonyms:\n" % n)

        if n>0:                
            sql = """
            select symbols.symbol as symbol from dup_syn join symbols
            on symbols.rowid = dup_syn.rowid;
            """
    
            self.selectToFile(sql, self.report)
        
        # delete 
        sql = "delete from symbols where rowid in (select rowid from dup_syn);"
        self.conn.execute(sql)
        self.conn.commit()
    
    def dedup_entity_vs_entity(self):
        # conflict between different entities, need to delete these
        # symbols completely. create the table explicitly with collate nocase
        sql = """
        drop table if exists dup_between_entities;
        create table dup_between_entities (symbol text collate nocase);
         
        insert into dup_between_entities (symbol) 
        select t1.symbol as symbol 
        from symbols as t1, symbols as t2
        where t1.symbol = t2.symbol
        and t1.node_id != t2.node_id;
        """
        self.conn.executescript(sql)
        
        n = self.count("select count(distinct symbol) from dup_between_entities;")
        self.report.write("removing %d symbols for conflict between different genes\n" % n)
        
        if n > 0:
            sql = "select distinct symbol from dup_between_entities;"
            self.selectToFile(sql, self.report)

        sql = "delete from symbols where symbol in (select symbol from dup_between_entities);"
        self.conn.execute(sql)
        self.conn.commit()

    def dedup_entity_within_itself(self):
        # all that can be left are conflicts within an entity of the same type.
        # eg due to merging. if symbol and source are both the same, need to keep just one
        # copy. if the sources are different we pick on arbitrarily, at least it unambiguously
        # points to the correct entity. either way the arbitrary thing will work

        sql = """
        drop table if exists dup_within_entities2;
        drop table if exists dup_within_entities;
        create table dup_within_entities2 (node_id int, symbol text collate nocase);
        create table dup_within_entities (rowid int, node_id int, symbol text collate nocase);
        """
        self.conn.executescript(sql)
        
        sql = """
        insert into dup_within_entities2 (node_id, symbol)
        select distinct t1.node_id as node_id, t1.symbol as symbol                
        from symbols as t1, symbols as t2
        where t1.symbol = t2.symbol
        and t1.node_id = t2.node_id
        and t1.rowid != t2.rowid;
        """
        self.conn.execute(sql)
        self.conn.commit()

        sql = """
        insert into dup_within_entities (rowid, node_id, symbol)
        select symbols.rowid, symbols.node_id, symbols.symbol 
        from symbols, dup_within_entities2
        where symbols.symbol = dup_within_entities2.symbol
        and symbols.node_id = dup_within_entities2.node_id;
        """ 
        self.conn.execute(sql)
        self.conn.commit()
        
        # treat this as a kill list, drop first removing the record we'll want to keep
        sql = """
        delete from dup_within_entities
        where rowid in 
        (select min(rowid) from dup_within_entities
        group by node_id, symbol); 
        """
        self.conn.execute(sql)
        self.conn.commit()
        
        # now apply the kill list
        sql = "delete from symbols where rowid in (select rowid from dup_within_entities);"
        self.conn.execute(sql)
        self.conn.commit()
            
    def validate(self):
        
        # verify symbols are unique
        total = self.count("select count(symbol) from symbols;")
        unique = self.count("select count(distinct symbol) from symbols;")
        if total != unique:
            raise Exception("total %d not equal to unique %d" % (total, unique))        
        
        # make sure each entity has at least one symbol attached
        sql = """
        select count(*) from 
        (select entities.node_id
        from entities left outer join symbols
        on entities.node_id = symbols.node_id
        group by entities.node_id
        having count(symbols.symbol) = 0);
        """
        total = self.count(sql)
        if total > 0:
            raise Exception("%d entities have no symbols at all" % total)   
            
        # verify at most 1 gene name attached to each entity
        sql = """
        select count(*) from 
        (select entities.node_id
        from entities join symbols
        on entities.node_id = symbols.node_id
        where symbols.source = '%s'
        group by entities.node_id
        having count(symbols.symbol) > 1);
        """
        sql = sql % RAW_GENE_NAME
        total = self.count(sql)
        if total > 0:
            raise Exception("%d entities have more than one gene name" % total)

        # symbols wit
    def get_symbol(self, symbol = None):
        '''
        to facilitate unit testing
        '''
        
        if symbol:
            sql = "select node_id, symbol, source from symbols where symbol = ? order by node_id, source, symbol;"
            cursor = self.conn.execute(sql, [symbol]);
        else:
            sql = "select node_id, symbol, source from symbols order by node_id, source, symbol;"
            cursor = self.conn.execute(sql)
        return cursor.fetchall()
    
    def get_entities(self, node_id = None):
        
        if node_id:
            sql = "select * from entities where node_id = ?;"
            cursor = self.conn.execute(sql, [node_id])
        else:
            sql = "select * from entities;"
            cursor = self.conn.execute(sql)
        return cursor.fetchall()
                
    def export_processed(self, export_file, org_prefix = None):
        
        record_pattern = '%s\t%s\t%s\n'
        if org_prefix:
            record_pattern = org_prefix + ':' + record_pattern
            
        cursor = self.conn.execute('select * from symbols order by node_id, source, symbol')
        for row in cursor:
            rec = record_pattern % (row['node_id'], row['symbol'], row['source'])
            export_file.write(rec)
    
    def export_generic_db(self, filename):
        pass

    def commit(self):
        self.conn.commit()
        
    def close(self):
        self.conn.close()

    def load_raw(self, filename):
        '''
        read all identifier data from the output of idmapper,
        e.g. 'ENSEMBL_ENTREZ_Hs'. 
        
        takes a file object, not a filename
        '''
        
        parser = parsers.RawIdFileParser(filename)
        symbols = []
        for entity in parser.reader():
            self.addId(entity.gmid(), entity.biotype(), entity.desc())
            for symbol, source in entity.symbols():
                symbols.append((entity.gmid(), symbol, source))
            
            if len(symbols) == INSERT_BATCH_SIZE:
                self.addSymbols(symbols)
                symbols = []
                
        if len(symbols) > 0:
            self.addSymbols(symbols)
            symbols = []
    
        self.commit()
        self.index()
        
    def load_reverse_mappings(self, filename):
        '''
        entrez to ensembl mappings
        '''
        
        parser = parsers.EntrezToEnsemblParser(filename)
        symbols = []
        
        for entity in parser.reader():            
            for ensembl in entity.xrefs(REV_ENSEMBL):
                record = (entity.entrez(), entity.name(), ensembl, entity.biotype())
                symbols.append(record)

            if len(symbols) == INSERT_BATCH_SIZE:
                self.addReverseSymbols(symbols)
                symbols = []
                
        if len(symbols) > 0:
            self.addReverseSymbols(symbols)
            symbols = []
    
        self.commit()
        self.index()

    def load_3col(self, filename):
        '''
        id, symbol, source triplets

        '''

        parser = parsers.Col3Parser(filename)

        symbols = []
        for entity in parser.reader():
            record = (entity.gmid(), entity.symbol(), entity.source())
            symbols.append(record)

            if len(symbols) == INSERT_BATCH_SIZE:
                self.addSymbols(symbols)
                symbols = []


        if len(symbols) > 0:
            self.addSymbols(symbols)
            symbols = []

        self.commit()
        self.index()

    def process(self, biotype_keepers, filters, merge_names):
        '''
        apply cleaning, merging, deduplicating logic
        '''
    
        print('initial data, # symbols =', self.symbols_size(), '# entities =', self.entities_size())

        self.cleanup()
        print('removed empty symbols, size =', self.symbols_size(), '# entities =', self.entities_size())

        self.remove_unwanted_sources(filters)
        print('removed symbols belonging to unwanted sources, size =', self.symbols_size(), '# entities =', self.entities_size())
        
        self.standardize_source_names()
        
        self.biotype_filter(biotype_keepers)                
        print('applied biotype filter, size =', self.symbols_size(), '# entities =', self.entities_size())
        
        self.delink()
        print('delinked, size =', self.symbols_size(), '# entities =', self.entities_size())

        if merge_names:        
            self.merge()
            print('merged, size =', self.symbols_size(), '# entities =', self.entities_size())
        else:
            print('gene name merging disabled for this organism')
            
        self.dedup()
        print('deduped, size =', self.symbols_size(), '# entities =', self.entities_size())
        
        self.clean_empties()
        print('removed empties, size =', self.symbols_size(), '# entities =', self.entities_size())
    
def raw_to_processed(raw_filename, reverse_filename, processed_filename, report_filename, 
                     org_prefix, temp_dir, biotypes, filters, merge_names):

    if temp_dir:
        if not os.path.isdir(temp_dir):
            os.makedirs(temp_dir)
    
        db_filename = os.path.join(temp_dir, "%s_ids.sqlite" % org_prefix)
        if os.path.exists(db_filename):
            os.remove(db_filename)
    else:
        db_filename = ":memory:"       

    report = codecs.open(report_filename, 'w', 'utf8')
    with report:
        db = IdentifierDB(db_filename, report)
        db.drop_indices()
    
        raw_file = codecs.open(raw_filename, 'r', 'utf8')
        with raw_file:
            db.load_raw(raw_file)
    
        if reverse_filename:
            reverse_file = codecs.open(reverse_filename, 'r', 'utf8')
            with reverse_file:
                db.load_reverse_mappings(reverse_file)
        
        db.process(biotypes, filters, merge_names)
    
        db.validate()
        
        processed_file = codecs.open(processed_filename, 'w', 'utf8')
        with processed_file:
            db.export_processed(processed_file, org_prefix)
        
        db.close()

def triplets_to_processed(triplet_filenames, reverse_filename, processed_filename, report_filename,
                          org_prefix, temp_dir, biotypes, filters, merge_names):

    if temp_dir:
        if not os.path.isdir(temp_dir):
            os.makedirs(temp_dir)

        db_filename = os.path.join(temp_dir, "%s_ids.sqlite" % org_prefix)
        if os.path.exists(db_filename):
            os.remove(db_filename)
    else:
        db_filename = ":memory:"

    report = codecs.open(report_filename, 'w', 'utf8')
    with report:
        db = IdentifierDB(db_filename, report)
        db.drop_indices()

        for triplet_filename in triplet_filenames:
            triplet_file = codecs.open(triplet_filename, 'r', 'utf8')
            with triplet_file:
                db.load_3col(triplet_file)

        if reverse_filename:
            reverse_file = codecs.open(reverse_filename, 'r', 'utf8')
            with reverse_file:
                db.load_reverse_mappings(reverse_file)

        db.process(biotypes, filters, merge_names)

        db.validate()

        processed_file = codecs.open(processed_filename, 'w', 'utf8')
        with processed_file:
            db.export_processed(processed_file, org_prefix)

        db.close()

testdata = (
    ('GMID', 'Gene Name', 'Protein Coding',       'Synonyms',         'Definition'),
    (   '1',     'happy',           'True',    'silly;putty',         'happy gene'), 
    (   '2',       'sad', 'protein_coding',  'serious;putty;sad',           'sad gene'),
    (   '3',     'happy',           'True',        'naughty', 'another happy gene'),
    (   '4',   'strange',          'False',       'whatever', 'not protein coding'),
    (   '5',       'N/A',           'True',    'whatsmyname',       'missing name'),
    (   '6',       'sad',            'rna',      'ignore;me',         'not coding'),
)

testdata_output = (
    ('1',       'happy', 'Gene Name'),
    ('1',     'naughty',  'Synonym'),
    ('1',       'silly',  'Synonym'),
    ('2',         'sad', 'Gene Name'),
    ('2',     'serious',  'Synonym'),
    ('5', 'whatsmyname',  'Synonym'),
)

# test data for using entrez-to-ensembl identifier tables to resolve ambiguity
# in the mappings without throwing away identifiers 
#
#    Ensembl    Entrez   comment
#     ENSG01 -> 1          ok, ensembl and entrez both point to each other uniquely
#     ENSG01 <- 1
#
#     ENSG02 -> 2          ok, ensembl points to entrez which doesn't point back, but no conflict     
#
#     ENSG03 -> 3          ok, this isn't ambiguous we won't loose symbols
#     ENSG03 -> 4
#     ENSG03 <- 3
#
#     ENSG04 -> 5          remove ENSG05 -> 5 
#     ENSG05 -> 5
#     ENSG04 <- 5
# 
#     ENSG06 -> 6          not ambiguous
#     ENSG06 -> 7
#     ENSG06 <- 6
#     ENSG06 <- 7
#
#     ENSG07 -> 8          still ambiguous, can't remove anything
#     ENSG08 -> 8
#     ENSG07 <- 8
#     ENSG08 <- 8
#
#     ENSG09 -> 9          ok, we don't consider this ambiguous since ENSG10 -> 9 isn't in our raw map
#     ENSG10 <- 9
#
#     ENSG11 -> 10         inconsistent, but no reverse mapping. leave these alone for later steps to resolve or clean
#     ENSG12 -> 10
#
testdata_delink_input = (
    ('GMID', 'Ensembl Gene ID', 'Gene Name', 'Protein Coding', 'Entrez Gene ID', 'Synonyms', 'Definition'),
    (   '1',          'ENSG01',     'gene1', 'protein_coding',              '1', '', ''), 
    (   '2',          'ENSG02',     'gene2', 'protein_coding',              '2', '', ''),
    (   '3',          'ENSG03',     'gene3', 'protein_coding',            '3;4', '', ''),
    (   '4',          'ENSG04',     'gene4', 'protein_coding',              '5', '', ''),
    (   '5',          'ENSG05',     'gene5', 'protein_coding',              '5', '', ''),
    (   '6',          'ENSG06',     'gene6', 'protein_coding',            '6;7', '', ''),
    (   '7',          'ENSG07',     'gene7', 'protein_coding',              '8', '', ''),
    (   '8',          'ENSG08',     'gene8', 'protein_coding',              '8', '', ''),
    (   '9',          'ENSG09',     'gene9', 'protein_coding',              '9', '', ''),
    (  '11',          'ENSG11',    'gene11', 'protein_coding',             '10', '', ''),
    (  '12',          'ENSG12',    'gene12', 'protein_coding',             '10', '', ''),    
)

testdata_delink_reverse = (
    ('GeneID', 'Symbol', 'Synonyms', 'dbXrefs',               'description',  'type_of_gene'),
    ( '1',     'gene_1',         '', 'HGNC:1|Ensembl:ENSG01',            '','protein-coding'), 
    ( '2',     'gene_2',         '', 'HGNC:2',                           '','protein-coding'), 
    ( '3',     'gene_3',         '', 'Ensembl:ENSG03',                   '','protein-coding'),
    ( '4',     'gene_4',         '', '',                                 '','protein-coding'),
    ( '5',     'gene_5',         '', 'Ensembl:ENSG04',                   '','protein-coding'),
    ( '6',     'gene_6',         '', 'Ensembl:ENSG06',                   '','protein-coding'),
    ( '7',     'gene_7',         '', 'Ensembl:ENSG06',                   '','protein-coding'),
    ( '8',     'gene_8',         '', 'Ensembl:ENSG07|Ensembl:ENSG08',    '','protein-coding'),     
    ( '9',     'gene_9',         '', 'Ensembl:ENSG10',                   '','protein-coding'),
)

# order expected output by gmid, source, symbol ascending so that
# we can lexically compare with database query of computed results
testdata_delink_output = (
    ('1',      'ENSG01', 'Ensembl Gene ID'),
    ('1',           '1', 'Entrez Gene ID'),
    ('1',       'gene1', 'Gene Name'),

    ('2',      'ENSG02', 'Ensembl Gene ID'),
    ('2',           '2', 'Entrez Gene ID'),
    ('2',       'gene2', 'Gene Name'),

    ('3',      'ENSG03', 'Ensembl Gene ID'),
    ('3',           '3', 'Entrez Gene ID'),
    ('3',           '4', 'Entrez Gene ID'),
    ('3',       'gene3', 'Gene Name'),

    ('4',      'ENSG04', 'Ensembl Gene ID'),
    ('4',           '5', 'Entrez Gene ID'),
    ('4',       'gene4', 'Gene Name'),

    ('5',      'ENSG05', 'Ensembl Gene ID'),
    ('5',       'gene5', 'Gene Name'),

    ('6',      'ENSG06', 'Ensembl Gene ID'),
    ('6',           '6', 'Entrez Gene ID'),
    ('6',           '7', 'Entrez Gene ID'),
    ('6',       'gene6', 'Gene Name'),

    ('7',      'ENSG07', 'Ensembl Gene ID'),
    ('7',           '8', 'Entrez Gene ID'),
    ('7',       'gene7', 'Gene Name'),

    ('8',      'ENSG08', 'Ensembl Gene ID'),
    ('8',           '8', 'Entrez Gene ID'),
    ('8',       'gene8', 'Gene Name'),

    ('9',      'ENSG09', 'Ensembl Gene ID'),
    ('9',           '9', 'Entrez Gene ID'),
    ('9',       'gene9', 'Gene Name'),

    ('11',      'ENSG11', 'Ensembl Gene ID'),
    ('11',          '10', 'Entrez Gene ID'),
    ('11',      'gene11', 'Gene Name'),

    ('12',      'ENSG12', 'Ensembl Gene ID'),
    ('12',          '10', 'Entrez Gene ID'),
    ('12',      'gene12', 'Gene Name'),

)


def data_to_file(testdata):
    '''
    convert tuple of test data field values 
    into a file-like object we can use in 
    unit tests
    '''
    testdata = ['\t'.join(line) for line in testdata]
    testdata = '\n'.join(testdata) + '\n'
    return StringIO.StringIO(testdata)
    
class TestSomething(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def tearDown(self):
        pass
    
    def test(self):

        db = IdentifierDB(":memory:", sys.stdout)
        
        with contextlib.closing(db):
            print('load')
            f = data_to_file(testdata)
            db.load_raw(f)        
            dump(db)
            self.assertEqual(16, db.symbols_size())
            self.assertEqual(6, db.entities_size())
            
            print('clean')
            db.cleanup()
            dump(db)
            self.assertEqual(15, db.symbols_size())
            self.assertEqual(6, db.entities_size())
            
            print('remove unwanted sources')
            db.remove_unwanted_sources(RAW_DEFAULT_SOURCES_TO_REMOVE)
            self.assertEqual(15, db.symbols_size())
            self.assertEqual(6, db.entities_size())
            
            print('standardize source names')
            db.standardize_source_names()
            rows = db.get_symbol('silly')
            self.assert_(len(rows)==1)
            self.assertEqual('Synonym', rows[0]['source'])
            
            print('biotype filter')
            db.biotype_filter(RAW_DEFAULT_BIOTYPE_KEEPERS)                
            dump(db)
            self.assertEqual(10, db.symbols_size())
            self.assertEqual(4, db.entities_size())
    
            print('delink')
            db.delink()
            dump(db)
            self.assertEqual(10, db.symbols_size())
            self.assertEqual(4, db.entities_size())
                    
            print('merge')
            db.merge()
            dump(db)
            self.assertEqual(10, db.symbols_size())
            self.assertEqual(3, db.entities_size())
                
            print('dedup')
            db.dedup()
            dump(db)
            self.assertEqual(6, db.symbols_size())
            self.assertEqual(3, db.entities_size())        
    
            print('clean empties')
            db.clean_empties()
            dump(db)
            self.assertEqual(6, db.symbols_size())
            self.assertEqual(3, db.entities_size())
                    
            rows = db.get_symbol('happy')
            self.assertEqual(1, len(rows))
            row = rows[0]
            row = dict(zip(row.keys(), row))
            self.assertEqual(row['node_id'], 1)
            
            db.validate()
            
            output_file = StringIO.StringIO()
            db.export_processed(output_file)
            expected_output = data_to_file(testdata_output).getvalue()
            output = output_file.getvalue()
            print(expected_output)
            print(output)
            
            self.assertEquals(expected_output, output)
        

    def test_delink(self):
        db = IdentifierDB(":memory:", sys.stdout)
        
        with contextlib.closing(db):
            print('load')

            dump(db)
            f = data_to_file(testdata_delink_input)
            db.load_raw(f)
            
            freverse = data_to_file(testdata_delink_reverse)
            db.load_reverse_mappings(freverse)
    
            print('clean')
            db.cleanup()
            
            print('start state')
            dump(db)
            self.assertEqual(35, db.symbols_size())
            self.assertEqual(11, db.entities_size())
                
            print('delink')
            db.delink()            
            dump(db)

            result = symbols_as_string(db)
            expected_result = data_to_file(testdata_delink_output).getvalue()
            
            diffs = difflib.unified_diff(expected_result.splitlines(), result.splitlines(), "expected", "actual", lineterm='\n')
            for diff in diffs:
                print(diff)

            self.assertEqual(34, db.symbols_size())
            self.assertEqual(expected_result, result)
            self.assertEqual(11, db.entities_size())
            
def symbols_as_string(db):
    rows = db.get_symbol()
    result = []
    for row in rows:
        fields = [str(field) for field in row]
        result.append('\t'.join(fields))
        
    return '\n'.join(result) + '\n'
    
    
def dump(db):
    print("----------------")
    print("symbols")
    rows = db.get_symbol()
    for row in rows:
        print(list(row))
        
    print("entities")
    rows = db.get_entities()
    for row in rows:
        print(list(row))
