
"""
this script exists because we made a design mistake in the
organism data structures [TODO explain!]
"""

import os, shutil, glob
import argparse
import pandas as pd

# TODO: sort outputs, so easy to diff and reproducible


class GenericDbIO(object):
    def __init__(self, schema_file):
        self.schema = schema2dict(schema_file)

    def load_table_file(self, filename, cols):
        '''
        helper to load table with column names from schema file

        return pandas dataframe
        '''


        df = pd.read_csv(filename, sep='\t',
                         na_filter=False, header=None,
                         names=cols)
        return df

    def load_table(self, location, name):
        return load_table(os.path.join(location, name + '.txt'),
                          self.schema[name])

    def store_table_file(self, df, filename):
        df.to_csv(filename, sep='\t',
                  header=False, index=False)

    def store_table(self, df, location, name):
        self.store_table_file(df, os.path.join(location, name+'.txt'))

    def create_empty_table(self, name):
        return pd.DataFrame(columns=self.schema[name])

    def list_tables(self):
        return self.schema.keys()


def schema2dict(schema_file):
    # little dictionary of columns from the schema file
    schema = open(schema_file).read().splitlines()
    schema = [line.split('\t') for line in schema]
    schema = dict((line[0], line[1:]) for line in schema)

    return schema

# TODO: remove after refactoring
def my_schema2dict(db):
    return schema2dict(db + '/SCHEMA.txt')

def load_table(filename, schema):
    '''
    helper to load table with column names from schema file

    return pandas dataframe
    '''


    df = pd.read_csv(filename, sep='\t',
                     na_filter=False, header=None,
                     names=schema[filename])
    return df

def my_load_table(dirname, filename, schema):
    '''
    helper to load table with column names from schema file

    return pandas dataframe
    '''


    df = pd.read_csv(dirname + '/' + filename + '.txt', sep='\t',
                     na_filter=False, header=None,
                     names=schema[filename])
    return df


def store_table(df, dirname, filename):
    df.to_csv(dirname + '/' + filename + '.txt', sep='\t',
              header=False, index=False)


def mergeone(newdb, olddb, mergeddb):

    schema = schema2dict(newdb)

    def load_table(dirname, filename):
        return my_load_table(dirname, filename, schema)


    # organisms
    org_old = load_table(olddb, 'ORGANISMS')
    org_new = load_table(newdb, 'ORGANISMS')

    # can only merge single organisms dbs into a multi organism one
    assert len(org_old) == 1

    org_merged = pd.concat([org_old, org_new], ignore_index=True)

    org_merged.to_csv(mergeddb + '/ORGANISMS.txt', sep='\t', header=False, index=False)

    # network groups
    nwgroups_old = load_table(olddb, 'NETWORK_GROUPS')
    nwgroups_new = load_table(newdb, 'NETWORK_GROUPS')

    max_nwg_id = max(nwgroups_old['ID'], default=0)
    nwgroups_new['ID'] += max_nwg_id
    nwgroups_merged = pd.concat([nwgroups_old, nwgroups_new], ignore_index=True)
    # make sure no collisions
    #print(len(nwgroups_merged), nwgroups_merged['ID'].nunique())

    # networks
    networks_old = load_table(olddb, 'NETWORKS')
    networks_new = load_table(newdb, 'NETWORKS')

    n = max(networks_old['ID'], default=0)

    # there may be gaps in the numbering,  maybe networks that didn't
    # make it through extract because they were empty?
    # anyhow, the next valid network id to use is n+1. create a mapping table

    nwmap = networks_old[['ID', 'METADATA_ID']].copy()

    # don't assume network id and metadata id are the same
    n_md = max(networks_old['METADATA_ID'])

    networks_new['ID'] += n
    networks_new['METADATA_ID'] += n_md
    networks_new['GROUP_ID'] += max_nwg_id

    networks_merged = pd.concat([networks_old, networks_new], ignore_index=True)
    # make sure no collisions
    #print (len(networks_merged), networks_merged['ID'].nunique(),     networks_merged['METADATA_ID'].nunique())

    networks_merged.to_csv(mergeddb + '/NETWORKS.txt', sep='\t', header=False,
                           index=False)

    nwgroups_merged.to_csv(mergeddb + '/NETWORK_GROUPS.txt', sep='\t',
                           header=False, index=False)

    ## network metadata
    nwmd_old = load_table(olddb, 'NETWORK_METADATA')
    nwmd_new = load_table(newdb, 'NETWORK_METADATA')

    nwmd_new['ID'] += n_md

    nwmd_merged = pd.concat([nwmd_old, nwmd_new], ignore_index=True)

    nwmd_merged.to_csv(mergeddb + '/NETWORK_METADATA.txt', sep='\t', header=False, index=False)


    ## ontology categories
    ontcat_old = load_table(olddb, 'ONTOLOGY_CATEGORIES')
    ontcat_new = load_table(newdb, 'ONTOLOGY_CATEGORIES')

    n_ontcat = max(ontcat_old['ID'])

    ontcat_new['ID'] += n_ontcat
    ontcat_new['ONTOLOGY_ID'] = 9

    ontcat_merged = pd.concat([ontcat_old, ontcat_new], ignore_index=True)

    ontcat_merged.to_csv(mergeddb + '/ONTOLOGY_CATEGORIES.txt', sep='\t',
                        header=False, index=False)

    ## ontologies
    ont_old = load_table(olddb, 'ONTOLOGIES')
    ont_new = load_table(newdb, 'ONTOLOGIES')

    ont_new['ID'] = 9

    ont_merged = pd.concat([ont_old, ont_new], ignore_index=True)

    ont_merged.to_csv(mergeddb + '/ONTOLOGIES.txt', sep='\t',
                      header=False, index=False)

    ## attributes, schema
    #
    # these can just be copied from old since new adds nothing
    #

    shutil.copyfile(olddb+'/SCHEMA.txt', mergeddb+'/SCHEMA.txt')
    shutil.copyfile(olddb+'/ATTRIBUTES.txt', mergeddb+'/ATTRIBUTES.txt')
    shutil.copyfile(olddb+'/ATTRIBUTE_GROUPS.txt', mergeddb+'/ATTRIBUTE_GROUPS.txt')
    shutil.copyfile(olddb+'/INTERACTIONS.txt', mergeddb+'/INTERACTIONS.txt') # just an empty file

    # copy attribute data from old as well
    old_attribdata = olddb+'/ATTRIBUTES'
    merged_attribdata = mergeddb+'/ATTRIBUTES'

    if not os.path.exists(merged_attribdata):
        os.makedirs(merged_attribdata)

    old_attribdata_files = glob.glob(old_attribdata + '/*')
    for filename in old_attribdata_files:
        link_name = merged_attribdata + '/' + os.path.basename(filename)
        rel = os.path.relpath(filename, merged_attribdata)
        os.symlink(rel, link_name)

    ## nodes
    nodes_old = load_table(olddb, 'NODES')
    nodes_new = load_table(newdb, 'NODES')

    n_nodes_id = max(nodes_old['ID'])
    n_nodes_gene_data_id = max(nodes_old['GENE_DATA_ID'])

    nodes_new['ID'] += n_nodes_id
    nodes_new['GENE_DATA_ID'] += n_nodes_gene_data_id

    nodes_merged = pd.concat([nodes_old, nodes_new], ignore_index=True)
    store_table(nodes_merged, mergeddb, 'NODES')

    ## gene naming sources TODO
    ns_old = load_table(olddb, 'GENE_NAMING_SOURCES')
    ns_new = load_table(newdb, 'GENE_NAMING_SOURCES')

    # looks like i can just use the new sources for this, since the first 13 rows are the same
    shutil.copyfile(olddb+'/GENE_NAMING_SOURCES.txt', mergeddb+'/GENE_NAMING_SOURCES.txt')

    ## gene

    genes_old = load_table(olddb, 'GENES')
    genes_new = load_table(newdb, 'GENES')

    # slightly confused here with the ids in GENE and GENE_DATA.
    #
    # ok, reviewing a bit, i think
    #
    #  * genes contains the different names, points to node via node_id
    #  * gene_data contains descriptions, node points to gene_data via gene_data_id
    #
    #

    n_genes_id = max(genes_old['ID'])

    genes_new['ID'] += n_genes_id
    genes_new['NODE_ID'] += n_nodes_id

    genes_merged = pd.concat([genes_old, genes_new], ignore_index=True)
    store_table(genes_merged, mergeddb, 'GENES')

    ## gene_data
    genedata_old = load_table(olddb, 'GENE_DATA')
    genedata_new = load_table(newdb, 'GENE_DATA')

    genedata_new['ID'] += n_nodes_gene_data_id

    genedata_merged = pd.concat([genedata_old, genedata_new], ignore_index=True)
    store_table(genedata_merged, mergeddb, 'GENE_DATA')

    ## statistics
    stats_old = load_table(olddb, 'STATISTICS')
    stats_new = load_table(newdb, 'STATISTICS')

    stats_merged = stats_old.copy()

    for col in ['organisms', 'networks', 'interactions', 'genes']:
        stats_merged[col] = stats_old[col] + stats_new[col]

    # TODO
    stats_merged['data'] = ['2014-10-16']
    store_table(stats_merged, mergeddb, 'STATISTICS')

    ## tags
    tags_old = load_table(olddb, 'TAGS')
    tags_new = load_table(newdb, 'TAGS')

    tags_old.head()

    # i'm not going to care if some of the tag names are duplicated when putting together,
    # shouldn't matter.
    n_tags_id = max(tags_old['ID'])
    tags_new['ID'] += n_tags_id

    tags_merged = pd.concat([tags_old, tags_new], ignore_index=True)
    store_table(tags_merged, mergeddb, 'TAGS')

    ## network tag associations
    tag_assoc_old = load_table(olddb, 'NETWORK_TAG_ASSOC')
    tag_assoc_new = load_table(newdb, 'NETWORK_TAG_ASSOC')

    n_tag_assoc_id = max(tag_assoc_old['ID'])

    tag_assoc_new['ID'] += n_tag_assoc_id
    tag_assoc_new['NETWORK_ID'] += n
    tag_assoc_new['TAG_ID'] += n_tags_id

    tag_assoc_merged = pd.concat([tag_assoc_old, tag_assoc_new], ignore_index=True)
    store_table(tag_assoc_merged, mergeddb, 'NETWORK_TAG_ASSOC')

    ## copy interaction files
    old_interactions = olddb+'/INTERACTIONS'
    new_interactions = newdb+'/INTERACTIONS'

    merged_interactions = mergeddb+'/INTERACTIONS'

    if not os.path.exists(merged_interactions):
        os.makedirs(merged_interactions)

    # lots of interaction data if copying, TODO how about just symlinking?
    old_interaction_files = glob.glob(old_interactions + '/*')
    for filename in old_interaction_files:
        link_name = merged_interactions + '/' + os.path.basename(filename)
        rel = os.path.relpath(filename, merged_interactions)
        os.symlink(rel, link_name)

    # for the new interactions, have to map the node ids, so can't just copy or symlink the files
    new_interaction_files = glob.glob(new_interactions +'/*')
    for filename in new_interaction_files:
        parts = os.path.basename(filename).split('.')

        org_id, nw_id, ext = parts
        nw_id = int(nw_id) + n
        new_filename = merged_interactions + '/%s.%s.%s' % (org_id, nw_id, ext)

        network_data = pd.read_csv(filename, sep='\t', header=None, names=['node_a', 'node_b', 'weight'])
        network_data['node_a'] += n_nodes_id
        network_data['node_b'] += n_nodes_id

        network_data.to_csv(new_filename, sep='\t', header=False, index=False)

    ## copy go category files
    old_gocats = olddb+'/../GoCategories'
    new_gocats = newdb+'/../GoCategories'

    merged_gocats = mergeddb+'/../GoCategories'

    if not os.path.exists(merged_gocats):
        os.makedirs(merged_gocats)

    old_gocats_files = glob.glob(old_gocats + '/*')
    for filename in old_gocats_files:
        link_name = merged_gocats + '/' + os.path.basename(filename)
        rel = os.path.relpath(filename, merged_gocats)
        os.symlink(rel, link_name)

    new_gocats_files = glob.glob(new_gocats + '/*')
    for filename in new_gocats_files:
        link_name = merged_gocats + '/' + os.path.basename(filename)
        rel = os.path.relpath(filename, merged_gocats)
        os.symlink(rel, link_name)

class merger(object):

    def __init__(self, schema_file):
        self.gio = GenericDbIO(schema_file)

        # initialize empty table dataframes
        self.merged = {}
        for table in self.gio.list_tables():
            self.merged[table] = self.gio.create_empty_table(table)

    def append_table(self, name, df):
        """helper to append the given dataframe
        to the merged version we are accumulating
        """

        self.merged[name] = pd.concat([self.merged[name], df], ignore_index=True)

    def merge_db(self, location):
        org_id = self.merge_organisms(location)

        network_group_id_inc = self.merge_network_groups(location, id)
        network_id_inc = self.merge_networks(location, network_group_id_inc)
        self.merge_network_metadata(location, network_id_inc)

        self.merge_ontologies(location, org_id)
        self.merge_ontology_categories(location, org_id)

        self.merge_attributes_groups(location)
        self.merge_attributes(location)

        self.merge_schema(location)

        node_id_inc = self.merge_nodes(location)
        self.merge_gene_naming_sources(location)

        self.merge_genes(location, node_id_inc)
        self.merge_gene_data(location, node_id_inc)

        self.merge_statistics(location)

        self.merge_tags(location)
        self.merge_network_tag_assoc(location)

    def merge(self, input_location_list, merged_location):
        for location in input_location_list:
            self.merge_db(location)

        self.write_db(merged_location)

    def write_db(self, location):
        pass

    def merge_organisms(self, location):
        organisms = self.gio.load_table(location, 'ORGANISMS')

        # can only merge single organisms dbs into a multi organism one
        assert len(organisms) == 1

        # if an ontology id is defined, we require for simplicity
        # that it's id is the same as the organism id TODO fix the
        # missing value case, is it NAN, 0, '', or what?
        assert organisms.loc[0]['ID'] == organisms.loc[0]['ONTOLOGY_ID']

        # determine if we need to assign a new organism id
        # this is one place where we try to preserve id's instead
        # of just enumerating new ids, so that we have continuity
        # of organism ids between releases of a dataset
        id = max(organisms['ID'])  # assumes only single org as above
        if id in dict(self.merged['ORGANISMS']['ID']):
            id = max(self.merged['ORGANISMS']['ID'])
            organisms['ID'] = id

        self.append_table('ORGANISMS', organisms)
        return id

    def merge_network_groups(self, location):
        network_groups = self.gio.load_table(location, 'NETWORK_GROUPS')

        max_id = max(self.merged['NETWORK_GROUPS']['ID'])
        network_groups['ID'] += max_id

        self.append_table('NETWORK_GROUPS', network_groups)
        return max_id

    def merge_networks(self, location, network_group_inc):

        networks = self.gio.load_table(location, 'NETWORKS')

        # network ids and network metadata ids are 1-1,
        # for simplicity require they've been constructed with the same ids
        # (but check to be sure)
        assert (networks['ID'] == networks['METADATA_ID']).all()

        # map ids by incrementing into empty id space
        n = max(self.merge['NETWORKS']['ID'], default=0)

        networks['ID'] += n
        networks['METADATA_ID'] += n
        networks['GROUP_ID'] += network_group_inc

        self.append_table('NETWORKS', networks)
        return n


    def merge_network_metadata(self, location, network_inc):

        network_metadata = self.gio.load_table(location, 'NETWORK_METADATA')

        # use the same increment as networks, since we are
        # assuming network_id == network_metadata_id
        network_metadata['ID'] += network_inc

        self.append_table('NETWORK_METADATA', network_metadata)

    def merge_ontologies(self, location, org_id):

        ontologies = self.gio.load_table(location, 'ONTOLOGIES')

        # for simplicity we require a single set of enrichment annotations
        # is defined, and that it is given the same id as the organism id
        # no ontologies is ok too

        if len(ontologies) == 0:
            return 0

        assert len(ontologies) == 1
        assert ontologies.loc[0]['ID'] == org_id

        self.append_table('ONTOLOGIES', ontologies)

    def merge_ontology_categories(self, location, org_id):

        ontology_categories = self.gio.load_table('ONTOLOGY_CATEGORIES')

        n = max(self.merged['ONTOLOGY_CATEGORIES']['ID'], default=0)
        ontology_categories['ID'] += n

        # set ontology id to org id, which we are requiring to be the
        # same for simplicity
        ontology_categories['ONTOLOGY_ID'] = org_id

        self.append_table('ONTOLOGY_CATEGORIES', ontology_categories)

    def merge_attribute_groups(self, location):
        pass #TODO

    def merge_attributes(self, location):
        pass #TODO

    def merge_schema(self, location):
        pass #TODO, maybe should just load the original schema, and assert the new one matches?

    def merge_nodes(self, location):

        nodes = self.gio.load_table(location, 'NODES')

        # for simplicity we require gene_data_id (confusing name) has consistent
        # ids with node id, since they are 1-1
        assert (nodes['ID'] == nodes['GENE_DATA_ID']).all()

        # update ids
        n = max(self.merged['NODES']['ID'])
        nodes['ID'] += n
        nodes['GENE_DATA_ID'] += n

        self.append_table('NODES', nodes)

        return n

    def merge_gene_naming_sources(self, location):
        pass #TODO this one could be tricky, since these are global and not per organism, unless duplicates are allowed

    def merge_genes(self, location, node_id_inc):

        genes = self.gio.load_table(location, 'GENES')

        n = max(self.merged['GENES']['ID'], default=0)

        genes['ID'] += n
        genes['NODE_ID'] += node_id_inc

        self.append_table('GENES', genes)

    def merge_gene_data(self, location, node_id_inc):

        gene_data = self.gio.load_table(location)

        # since we are requiring node_id == gene_data_id
        gene_data['ID'] += node_id_inc

        self.append_table('GENE_DATA', gene_data)

    def merge_statistics(self, location):
        pass # TODO: maybe initialize the date in gio, update counts here

    def merge_tags(self, location):
        # no longer supported

        tags = self.gio.load_table(location, 'TAGS')
        assert len(tags) == 0

    def merge_network_tag_assoc(self, location):
        # no longer supported

        network_tag_assoc = self.gio.load_table(location, 'NETWORK_TAG_ASSOC')
        assert len(network_tag_assoc) == 0


def main(newdb, olddbs_list):
    pass

if __name__ == '__main__':
    pass

