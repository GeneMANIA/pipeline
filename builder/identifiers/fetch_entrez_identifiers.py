'''
based on the sql from id-mapper, just fetch entrez ids & a couple of related fields into a flat file
'''

import pymysql, unittest, contextlib

def fetch(tax_id, filename, dbhost, dbport, dbuser, dbpass, dbname):
    '''
    Write out entrez id, gene name, synonyms, xrefs, description, and biotype for each gene
    for the given tax_id. basically just a dump of the results from the sql query
    '''
    
    print filename    
    sql = "select GeneID, Symbol, Synonyms, dbXrefs, description, type_of_gene from Entrez.Gene_Info where tax_id = %s;"
    
    with contextlib.closing(pymysql.connect(host=dbhost, port=dbport, user=dbuser, passwd=dbpass, db=dbname)) as conn:   
        with contextlib.closing(conn.cursor()) as cur:    
            cur.execute(sql, (tax_id,))
            
            outfile = open(filename, 'w')
            
            with outfile:        
                line = '\t'.join(str(f[0]) for f in cur.description) + '\n'
                outfile.write(line);
                    
                for r in cur.fetchall():
                    fields = [str(f).strip() for f in r] # stringify
                    fields = [f.replace('\r', ' ') for f in fields] # clean embedded control chars
                    line = '\t'.join(fields) + '\n'
                    outfile.write(line)
                                       
class TestFetch(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def tearDown(self):
        pass
    
    def test(self):
        pass # how can i test this without hardcoding a mysql instance?
    
#    @unittest.skip("later")
#    def testFetch(self):
#        fetch(3702, '/tmp/entrez_ids.3702.txt')
#        fetch(6239, '/tmp/entrez_ids.6239.txt')
#        fetch(7227, '/tmp/entrez_ids.7227.txt')
#        fetch(9606, '/tmp/entrez_ids.9606.txt')
#        fetch(10090, '/tmp/entrez_ids.10090.txt')
#        fetch(4932, '/tmp/entrez_ids.4932.txt')
#        fetch(10116, '/tmp/entrez_ids.10116.txt')

        