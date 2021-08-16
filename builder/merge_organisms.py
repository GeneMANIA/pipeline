
"""
this script exists because we made a design mistake in the
organism data structures [TODO explain!]
"""

import os, shutil, glob, datetime
import argparse
import pandas as pd

# TODO: sort outputs, so easy to diff and reproducible


class GenericDbIO(object):
    def __init__(self, schema_file):
        self.schema_file = schema_file
        self.schema = schema2dict(schema_file)

    def load_table_file(self, filename, cols):
        '''
        helper to load table with column names from schema file
        reads everything as strings so we don't get floats where
        we want ints, except columns named 'ID' or '*_ID' (with
        exceptions, yes its a bit fiddly, ideally the schema would
        include a type. which would make it more sql-ish)

        return pandas dataframe
        '''

        df = pd.read_csv(filename, sep='\t', dtype=str,
                         na_filter=False, header=None,
                         names=cols)

        # not all columns ending with ID are ints, blacklist such cases
        ignore_ids = set(['EXTERNAL_ID', ])

        for column_name in df.columns:
            if (column_name == 'ID' or column_name.endswith('_ID')) and column_name not in ignore_ids:
                df[column_name] = df[column_name].astype(int)

        return df

    def load_table(self, location, name):
        return self.load_table_file(os.path.join(location, name + '.txt'),
                          self.schema[name])

    def store_table_file(self, df, filename):
        # we don't have any floats, all the numeric vals are ints
        # convert on write via float_foramt in case loading or
        # re-indexing math gave us floats
        df.to_csv(filename, sep='\t',  float_format='%.f',
                  header=False, index=False)

    def store_table(self, df, location, name):

        if not os.path.exists(location):
            os.makedirs(location)

        self.store_table_file(df, os.path.join(location, name+'.txt'))

    def create_empty_table(self, name):
        return pd.DataFrame(columns=self.schema[name])

    def list_tables(self):
        return self.schema.keys()

    def store_schema(self, location):

        if not os.path.exists(location):
            os.makedirs(location)

        new_schema_file = os.path.join(location, os.path.basename(self.schema_file))
        shutil.copyfile(self.schema_file, new_schema_file)


def schema2dict(schema_file):
    # little dictionary of columns from the schema file
    schema = open(schema_file).read().splitlines()
    schema = [line.split('\t') for line in schema]
    schema = dict((line[0], line[1:]) for line in schema)

    return schema


class Merger(object):

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

    def merge_db(self, location, merged_location=None):
        """merge metadata tables for generic_db in given location
        into in-memory tables.

        If merged_location given, the bulk interaction data for the given
        location is copied.  The merged tables are not written out here however,
        since they require all dbs to be processed. see write_db() instead.
        """
        org_id = self.merge_organisms(location)

        network_group_id_inc = self.merge_network_groups(location, org_id)
        network_id_inc = self.merge_networks(location, network_group_id_inc)
        self.merge_network_metadata(location, network_id_inc)

        self.merge_ontologies(location, org_id)
        self.merge_ontology_categories(location, org_id)

        attribute_group_id_inc = self.merge_attribute_groups(location)
        attribute_id_inc = self.merge_attributes(location, attribute_group_id_inc)

        self.merge_schema(location)

        node_id_inc = self.merge_nodes(location)
        naming_source_id_inc = self.merge_gene_naming_sources(location)

        self.merge_genes(location, node_id_inc, naming_source_id_inc)
        self.merge_gene_data(location, node_id_inc)

        self.merge_statistics(location)

        self.merge_tags(location)
        self.merge_network_tag_assoc(location)

        if merged_location:
            self.copy_interaction_data(location, merged_location, org_id, network_id_inc, node_id_inc)
            self.copy_attribute_data(location, merged_location, attribute_group_id_inc, attribute_id_inc, node_id_inc)
            self.copy_function_data(location, merged_location)

    def merge(self, input_location_list, merged_location):
        for location in input_location_list:
            print("processing %s" % location)
            self.merge_db(location, merged_location)

        self.write_db(merged_location)

    def write_db(self, location):

        # write out the merged db tabls we've built up
        for table in self.gio.list_tables():
            self.gio.store_table(self.merged[table], location, table)

        # copy over schema file
        self.gio.store_schema(location)

    def merge_organisms(self, location):
        organisms = self.gio.load_table(location, 'ORGANISMS')

        # can only merge single organisms dbs into a multi organism one
        assert len(organisms) == 1

        # if an ontology id is defined, we require for simplicity
        # that it's id is the same as the organism id TODO fix the
        # missing value case, is it NAN, 0, '', or what?
        assert organisms.loc[0]['ID'] == organisms.loc[0]['ONTOLOGY_ID'], \
            "organism id must be same as ontology id"

        # determine if we need to assign a new organism id
        # this is one place where we try to preserve id's instead
        # of just enumerating new ids, so that we have continuity
        # of organism ids between releases of a dataset
        org_id = max(organisms['ID'])  # assumes only single org as above
        org_id = int(org_id)
        if org_id in dict(self.merged['ORGANISMS']['ID']):
            org_id = max(self.merged['ORGANISMS']['ID']) + 1
            org_id = int(org_id)
            organisms['ID'] = org_id

        self.append_table('ORGANISMS', organisms)
        return org_id

    def merge_network_groups(self, location, org_id):
        network_groups = self.gio.load_table(location, 'NETWORK_GROUPS')

        max_id = max(self.merged['NETWORK_GROUPS']['ID'], default=0)
        max_id = int(max_id)
        network_groups['ID'] += max_id
        network_groups['ORGANISM_ID'] = org_id

        self.append_table('NETWORK_GROUPS', network_groups)
        return max_id

    def merge_networks(self, location, network_group_inc):

        networks = self.gio.load_table(location, 'NETWORKS')

        # network ids and network metadata ids are 1-1,
        # for simplicity require they've been constructed with the same ids
        # (but check to be sure)
        assert (networks['ID'] == networks['METADATA_ID']).all()

        # map ids by incrementing into empty id space
        n = max(self.merged['NETWORKS']['ID'], default=0)
        n = int(n)

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

        ontology_categories = self.gio.load_table(location, 'ONTOLOGY_CATEGORIES')

        n = max(self.merged['ONTOLOGY_CATEGORIES']['ID'], default=0)
        n = int(n)
        ontology_categories['ID'] += n

        # set ontology id to org id, which we are requiring to be the
        # same for simplicity
        ontology_categories['ONTOLOGY_ID'] = org_id

        self.append_table('ONTOLOGY_CATEGORIES', ontology_categories)

    def merge_attribute_groups(self, location):

        attribute_groups = self.gio.load_table(location, 'ATTRIBUTE_GROUPS')

        n = max(self.merged['ATTRIBUTE_GROUPS']['ID'], default=0)
        n = int(n)
        attribute_groups['ID'] += n
        self.append_table('ATTRIBUTE_GROUPS', attribute_groups)

        return n

    def merge_attributes(self, location, attribute_group_id_inc):

        attributes = self.gio.load_table(location, 'ATTRIBUTES')

        n = max(self.merged['ATTRIBUTES']['ID'], default=0)
        n = int(n)
        attributes['ID'] += n
        attributes['ATTRIBUTE_GROUP_ID'] += attribute_group_id_inc
        self.append_table('ATTRIBUTES', attributes)

        return n

    def merge_schema(self, location):
        pass #TODO, maybe should just load the original schema, and assert the new one matches?


    def merge_nodes(self, location):

        nodes = self.gio.load_table(location, 'NODES')

        # for simplicity we require gene_data_id (confusing name) has consistent
        # ids with node id, since they are 1-1
        assert (nodes['ID'] == nodes['GENE_DATA_ID']).all()

        # update ids
        n = max(self.merged['NODES']['ID'], default=0)
        n = int(n)
        nodes['ID'] += n
        nodes['GENE_DATA_ID'] += n

        self.append_table('NODES', nodes)

        return n

    def merge_gene_naming_sources(self, location):
        '''this one could be tricky, since these are global and not per organism
        we'll ignore merging, and effectively create a set of naming sources
        for each organism
        '''

        gene_naming_sources = self.gio.load_table(location, 'GENE_NAMING_SOURCES')
        
        #get the max id value in the incoming file.
        max_in_new_file = max(gene_naming_sources['ID'], default = 0)
        max_in_new_file = int(max_in_new_file)

        #get the max of the running merged file
        n = max(self.merged['GENE_NAMING_SOURCES']['ID'], default=0)
        n = int(n)
        
        #get the max value of the ids for ids less than 100
        #max_less_than100 = max(i for i in self.merged['GENE_NAMING_SOURCES']['ID'] if i < 100)
        max_less_than100 = max(filter(lambda i: i < 100, self.merged['GENE_NAMING_SOURCES']['ID']),default=0)
        max_less_than100 = int(max_less_than100)

        # to accomadate issues with merging and yeast added this hack.  Once all organism use the correct numbering won't need to increment any of th ids.
        if max_in_new_file < 100:
            gene_naming_sources['ID'] += max_less_than100

        self.append_table('GENE_NAMING_SOURCES', gene_naming_sources)

        return n

    def merge_genes(self, location, node_id_inc, namging_source_id_inc):

        genes = self.gio.load_table(location, 'GENES')

        n = max(self.merged['GENES']['ID'], default=0)
        n = int(n)

        genes['ID'] += n
        genes['NODE_ID'] += node_id_inc

        self.append_table('GENES', genes)

    def merge_gene_data(self, location, node_id_inc):

        gene_data = self.gio.load_table(location, "GENE_DATA")

        # since we are requiring node_id == gene_data_id
        gene_data['ID'] += node_id_inc

        self.append_table('GENE_DATA', gene_data)

    def merge_statistics(self, location):

        statistics = self.gio.load_table(location, "STATISTICS")
        assert len(statistics) == 1, 'expected exactly 1 record in statistics table, found %s' % len(statistics)

        merged_statistics = self.merged['STATISTICS']

        if len(merged_statistics) == 0:
            today = datetime.date.today()
            date = today.strftime('%Y-%m-%d')

            merged_statistics.append({'ID': 1, 'organisms': 0, 'networks': 0,
                                      'interactions': 0, 'genes': 0, 'predictions': 0, 'date': date},
                                     ignore_index=True)

        merged_statistics[['organisms', 'networks', 'interactions', 'genes']] += statistics[['organisms', 'networks',
                                                                                             'interactions', 'genes']]
    def merge_tags(self, location):
        # no longer supported

        tags = self.gio.load_table(location, 'TAGS')
        assert len(tags) == 0

    def merge_network_tag_assoc(self, location):
        # no longer supported

        network_tag_assoc = self.gio.load_table(location, 'NETWORK_TAG_ASSOC')
        assert len(network_tag_assoc) == 0

    def copy_interaction_data(self, location, merged_location, org_id, network_id_inc, node_id_inc):

        from_dir = os.path.join(location, 'INTERACTIONS')
        data_files = glob.glob(os.path.join(from_dir, '*'))

        to_dir = os.path.join(merged_location, 'INTERACTIONS')
        if not os.path.exists(to_dir):
            os.makedirs(to_dir)

        for filename in data_files:
            parts = os.path.basename(filename).split('.')

            old_org_id, network_id, ext = parts
            network_id = int(network_id) + network_id_inc
            new_filename = os.path.join(to_dir, '%s.%s.%s' % (org_id, network_id, ext))

            network_data = pd.read_csv(filename, sep='\t', header=None, names=['node_a', 'node_b', 'weight'])
            network_data['node_a'] += node_id_inc
            network_data['node_b'] += node_id_inc

            network_data.to_csv(new_filename, sep='\t', header=False, index=False)

    def copy_attribute_data(self, location, merged_location, attribute_group_id_inc, attribute_id_inc, node_id_inc):

        data_files = glob.glob(os.path.join(location, 'ATTRIBUTES', '*'))

        to_dir = os.path.join(merged_location, 'ATTRIBUTES')
        if not os.path.exists(to_dir):
            os.makedirs(to_dir)

        for filename in data_files:
            parts = os.path.basename(filename).split('.')
            attribute_group_id, ext = parts
            attribute_group_id = int(attribute_group_id) + attribute_group_id_inc

            new_filename = os.path.join(to_dir, '%s.%s' % (attribute_group_id, ext))

            attribute_data = pd.read_csv(filename, sep='\t', header=None, names=['node', 'attribute'])
            attribute_data['node'] += node_id_inc
            attribute_data['attribute'] += attribute_id_inc

            attribute_data.to_csv(new_filename, sep='\t', header=False, index=False)

    def copy_function_data(self, location, merged_location):
        # TODO: this just copies over the file, review naming conventions and content format
        # and update

        from_dir = os.path.join(location, 'GO_CATEGORIES')
        to_dir = os.path.join(merged_location, 'GO_CATEGORIES')
        if not os.path.exists(to_dir):
            os.makedirs(to_dir)

        data_files = glob.glob(os.path.join(from_dir, '*'))

        for filename in data_files:
            new_filename = os.path.join(to_dir, os.path.basename(filename))
            shutil.copyfile(filename, new_filename)

def main(input_dbs, merged_db):

    assert len(input_dbs) > 1, "need more than 1 db to merge"

    print('merging %s into %s' % (', '.join(input_dbs), merged_db))

    # grab a schema file from the first db to merge
    schema_file = os.path.join(input_dbs[0], "SCHEMA.txt")

    #
    merger = Merger(schema_file)

    merger.merge(input_dbs, merged_db)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='combine multiple single organism generic_dbs')

    parser.add_argument('input_dbs', help='list of input db folders to include', nargs='+')
    parser.add_argument('merged_db', help='output merged db folder')

    args = parser.parse_args()
    main(args.input_dbs, args.merged_db)


