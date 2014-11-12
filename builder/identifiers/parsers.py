'''
parsers for identifier files
'''

import unittest
from .constants import *

class BaseParser(object):
    def __init__(self, theFile, header=None):
        self.file = theFile

        if header:
            self.header = header
        else:
            headerline = self.file.readline()
            headerline = headerline.rstrip('\r\n')
            self.header = headerline.split('\t')
            self.header = [field.strip() for field in self.header]

    def dictify(self, row):
        d = {}
        fields = row.split('\t')
        for name, value in zip(self.header, fields):
            d[name] = value.strip()
        
        return d
    
class RawGeneEntity():
    '''
    represents a gene entity with multiple symbols attached. 
    '''

    def __init__(self, d):
        self.d = d
        
    def symbols(self):
        for source, value in self.d.items():
            if source !=  RAW_GMID and source != RAW_DEFINITION and source != RAW_BIOTYPE:
                if source in RAW_NON_DELIMITED_FIELDS:
                    yield (value, source)
                else:
                    symbols_list = value.split(';')
                    for symbol in symbols_list:
                        yield(symbol, source)
                                    
    def gmid(self):
        return self.d[RAW_GMID]
    
    def desc(self):
        return self.d[RAW_DEFINITION]
    
    def biotype(self):
        return self.d[RAW_BIOTYPE]
    
class RawIdFileParser(BaseParser):
    '''
    takes a file-like object. it should have been opened as
    codecs.open(filename, 'r', 'utf8') to be sure we don't mess up
    non-ascii chars in the description field
    '''
    def __init__(self, theFile):   
        super(RawIdFileParser, self).__init__(theFile)
       
    def reader(self):
        
        for row in self.file:
            row = row.rstrip('\r\n')
            d = self.dictify(row)
            yield RawGeneEntity(d)
        
class EntrezToEnsemblGeneEntity:
    
    def __init__(self, d):
        self.d = d
        
    def entrez(self):
        return self.d[REV_ENTREZ_ID]

    def name(self):
        return self.d[REV_GENE_NAME]
    
    def synonyms(self):
        raise Exception('not implemented')
    
    def xrefs(self, wanted_source):
        '''
        xref record from entrez looks something like:
        
            HGNC:5|MIM:138670|Ensembl:ENSG00000121410|HPRD:00726
            
        this extracts the ensembl ids. there can be none, one, or several, the
        return value is a list of the ensembl ids found.
        '''

        xrefs = self.d[REV_XREFS]

        idents = []
        
        parts = xrefs.split('|')
        for part in parts:
            try:
                source, ident = part.split(':')
            except:
                source, ident = None, None
                
            if source == wanted_source:
                idents.append(ident)
    
        return idents       
        
    def biotype(self):
        return self.d[REV_BIOTYPE]


class Col3GeneEntity:
    def __init__(self, row):
        row = row.split('\t')
        if len(row) != 3:
            raise Exception('invalid row len' + row)
        self.row = row
        #self.gmid, self.symbol, self.source = row

    def gmid(self):
        return self.row[0]

    def symbol(self):
        return self.row[1]

    def source(self):
        return self.row[2]


class Col3Parser(BaseParser):

    def __init__(self, theFile):
        header = [COL3_GMID, COL3_SYMBOL, COL3_SOURCE]
        super(Col3Parser, self).__init__(theFile, header)

    def reader(self):

        for row in self.file:
            row = row.strip('\r\n')
            yield Col3GeneEntity(row)


class EntrezToEnsemblParser(BaseParser):
    
    def __init__(self, theFile):
        super(EntrezToEnsemblParser, self).__init__(theFile)

    def reader(self):
        
        for row in self.file:
            row = row.rstrip('\r\n')
            row = row.replace('\r', ' ')
            d = self.dictify(row)
            yield EntrezToEnsemblGeneEntity(d)        
    
class TestEntrezToEnsemblParser(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def tearDown(self):
        pass
    
    def test1(self):
        parser = EntrezToEnsemblParser(open('/tmp/entrez_ids.9606.txt'))
        
        for entity in parser.reader():
            print(entity.entrez())
            print(entity.name())
            print(entity.xrefs('Ensembl'))
            print(entity.biotype())