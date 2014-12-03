
import os, shutil, glob
import argparse
import pandas as pd

# TODO: sort outputs, so easy to diff and reprodicble

def schema2dict(db):
    # little dictionary of columns from the schema file
    schema = open(db + '/SCHEMA.txt').read().splitlines()
    schema = [line.split('\t') for line in schema]
    schema = dict((line[0], line[1:]) for line in schema)

    return schema

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

    max_nwg_id = max(nwgroups_old['ID'])
    nwgroups_new['ID'] += max_nwg_id
    nwgroups_merged = pd.concat([nwgroups_old, nwgroups_new], ignore_index=True)
    # make sure no collisions
    #print(len(nwgroups_merged), nwgroups_merged['ID'].nunique())

    # networks
    networks_old = load_table(olddb, 'NETWORKS')
    networks_new = load_table(newdb, 'NETWORKS')

    n = max(networks_old['ID'])

    # there may be gaps in the numbering,  maybe networks that didn't
    # make it through extract because they were empty?
    # anyhow, the next valid network id to use is n+1. create a mapping table

    nwmap = networks_old[['ID', 'METADATA_ID']].copy()

    # don't assume network id and metadata id are the same
    networks_new[networks_new['ID'] != networks_new['METADATA_ID']].head()

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


    # ## ontology categories
    #

    ontcat_old = load_table(olddb, 'ONTOLOGY_CATEGORIES')
    ontcat_new = load_table(newdb, 'ONTOLOGY_CATEGORIES')


    n_ontcat = max(ontcat_old['ID'])
    n_ontcat



    ontcat_new['ID'] += n_ontcat
    ontcat_new['ONTOLOGY_ID'] = 9



    ontcat_merged = pd.concat([ontcat_old, ontcat_new], ignore_index=True)



    ontcat_merged.to_csv(mergeddb + '/ONTOLOGY_CATEGORIES.txt', sep='\t',
                        header=False, index=False)


    # ## ontologies


    ont_old = load_table(olddb, 'ONTOLOGIES')
    ont_new = load_table(newdb, 'ONTOLOGIES')


    ont_new['ID'] = 9

    ont_merged = pd.concat([ont_old, ont_new], ignore_index=True)


    # In[150]:

    ont_merged.to_csv(mergeddb + '/ONTOLOGIES.txt', sep='\t',
                      header=False, index=False)


    # # attributes, schema
    #
    # these can just be copied from old since new adds nothing
    #

    import shutil

    shutil.copyfile(olddb+'/SCHEMA.txt', mergeddb+'/SCHEMA.txt')
    shutil.copyfile(olddb+'/ATTRIBUTES.txt', mergeddb+'/ATTRIBUTES.txt')
    shutil.copyfile(olddb+'/ATTRIBUTE_GROUPS.txt', mergeddb+'/ATTRIBUTE_GROUPS.txt')
    shutil.copyfile(olddb+'/INTERACTIONS.txt', mergeddb+'/INTERACTIONS.txt') # just an empty file


    # copy attribute data from old as well


    old_attribdata = olddb+'/ATTRIBUTES'

    merged_attribdata = mergeddb+'/ATTRIBUTES'


    if not os.path.exists(merged_attribdata):
        os.makedirs(merged_attribdata)


    #old_attribdata_files = get_ipython().getoutput('ls $old_attribdata/*')
    old_attribdata_files = glob.glob(old_attribdata + '/*')
    for filename in old_attribdata_files:
        link_name = merged_attribdata + '/' + os.path.basename(filename)
        rel = os.path.relpath(filename, merged_attribdata)
        os.symlink(rel, link_name)


    # ## nodes

    nodes_old = load_table(olddb, 'NODES')
    nodes_new = load_table(newdb, 'NODES')


    nodes_old.head()

    n_nodes_id = max(nodes_old['ID'])
    n_nodes_gene_data_id = max(nodes_old['GENE_DATA_ID'])
    n_nodes_id, n_nodes_gene_data_id


    nodes_new.head()


    nodes_new['ID'] += n_nodes_id
    nodes_new['GENE_DATA_ID'] += n_nodes_gene_data_id


    nodes_merged = pd.concat([nodes_old, nodes_new], ignore_index=True)
    store_table(nodes_merged, mergeddb, 'NODES')


    ## gene naming sources
    ns_old = load_table(olddb, 'GENE_NAMING_SOURCES')
    ns_new = load_table(newdb, 'GENE_NAMING_SOURCES')


    # looks like i can just use the new sources for this, since the first 13 rows are the same

    shutil.copyfile(olddb+'/GENE_NAMING_SOURCES.txt', mergeddb+'/GENE_NAMING_SOURCES.txt')


    # ## gene

    genes_old = load_table(olddb, 'GENES')
    genes_new = load_table(newdb, 'GENES')

    genes_old.head()


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

    # ## gene_data


    genedata_old = load_table(olddb, 'GENE_DATA')
    genedata_new = load_table(newdb, 'GENE_DATA')


    genedata_old.head()


    genedata_new['ID'] += n_nodes_gene_data_id


    genedata_merged = pd.concat([genedata_old, genedata_new], ignore_index=True)
    store_table(genedata_merged, mergeddb, 'GENE_DATA')


    # ## statistics


    stats_old = load_table(olddb, 'STATISTICS')
    stats_new = load_table(newdb, 'STATISTICS')



    stats_merged = stats_old.copy()



    for col in ['organisms', 'networks', 'interactions', 'genes']:
        stats_merged[col] = stats_old[col] + stats_new[col]

    # TODO
    stats_merged['data'] = ['2014-10-16']



    store_table(stats_merged, mergeddb, 'STATISTICS')


    # ## tags
    #


    tags_old = load_table(olddb, 'TAGS')
    tags_new = load_table(newdb, 'TAGS')


    tags_old.head()


    # i'm not going to care if some of the tag names are duplicated when putting together,
    # shouldn't matter.

    n_tags_id = max(tags_old['ID'])


    tags_new['ID'] += n_tags_id


    tags_merged = pd.concat([tags_old, tags_new], ignore_index=True)
    store_table(tags_merged, mergeddb, 'TAGS')


    # ## network tag associations


    tag_assoc_old = load_table(olddb, 'NETWORK_TAG_ASSOC')
    tag_assoc_new = load_table(newdb, 'NETWORK_TAG_ASSOC')



    n_tag_assoc_id = max(tag_assoc_old['ID'])


    tag_assoc_new['ID'] += n_tag_assoc_id
    tag_assoc_new['NETWORK_ID'] += n
    tag_assoc_new['TAG_ID'] += n_tags_id



    tag_assoc_merged = pd.concat([tag_assoc_old, tag_assoc_new], ignore_index=True)
    store_table(tag_assoc_merged, mergeddb, 'NETWORK_TAG_ASSOC')



    # ## copy interaction files


    old_interactions = olddb+'/INTERACTIONS'
    new_interactions = newdb+'/INTERACTIONS'

    merged_interactions = mergeddb+'/INTERACTIONS'



    if not os.path.exists(merged_interactions):
        os.makedirs(merged_interactions)


    # lots of interaction data if copying, TODO how about just symlinking?


    #old_interaction_files = get_ipython().getoutput('ls $old_interactions/*')
    old_interaction_files = glob.glob(old_interactions + '/*')
    for filename in old_interaction_files:
        link_name = merged_interactions + '/' + os.path.basename(filename)
        #print filename, link_name
        rel = os.path.relpath(filename, merged_interactions)
        #print rel, link_name
        #os.symlink(filename, link_name)
        os.symlink(rel, link_name)


    # for the new interactions, have to map the node ids, so can't just copy or symlink the files

    # In[268]:

    #new_interaction_files = get_ipython().getoutput('ls $new_interactions/*')
    new_interaction_files = glob.glob(new_interactions +'/*')
    for filename in new_interaction_files:
        parts = os.path.basename(filename).split('.')

        org_id, nw_id, ext = parts
        nw_id = int(nw_id) + n
        new_filename = merged_interactions + '/%s.%s.%s' % (org_id, nw_id, ext)

        network_data = pd.read_csv(filename, sep='\t', header=None, names=['node_a', 'node_b', 'weight'])
        network_data['node_a'] += n_nodes_id
        network_data['node_b'] += n_nodes_id

        #print new_filename

        network_data.to_csv(new_filename, sep='\t', header=False, index=False)



    # ## copy go category files


    old_gocats = olddb+'/../GoCategories'
    new_gocats = newdb+'/../GoCategories'

    merged_gocats = mergeddb+'/../GoCategories'



    if not os.path.exists(merged_gocats):
        os.makedirs(merged_gocats)




    #old_gocats_files = get_ipython().getoutput('ls $old_gocats/*')
    old_gocats_files = glob.glob(old_gocats + '/*')
    for filename in old_gocats_files:
        link_name = merged_gocats + '/' + os.path.basename(filename)
        rel = os.path.relpath(filename, merged_gocats)
        os.symlink(rel, link_name)



    #new_gocats_files = get_ipython().getoutput('ls $new_gocats/*')
    new_gocats_files = glob.glob(new_gocats + '/*')
    for filename in new_gocats_files:
        link_name = merged_gocats + '/' + os.path.basename(filename)
        rel = os.path.relpath(filename, merged_gocats)
        os.symlink(rel, link_name)


    # ## all done??

def main(newdb, olddbs_list):
    pass

if __name__ == '__main__':
    pass

