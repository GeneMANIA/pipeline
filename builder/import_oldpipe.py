
"""populate pipeline with data from a single organism
from the old pipeline.
"""

import argparse, os, shutil, glob
from configobj import ConfigObj


class Importer(object):
    def __init__(self, old_dir, short_id, new_dir):
        self.old_dir = old_dir
        self.short_id = short_id
        self.new_dir = new_dir

    def import_all(self):
        self.import_config()
        self.import_identifiers()
        self.import_networks()
        self.import_go_categories()
        self.import_attributes()

    def import_config(self):

        # load old config
        old_cfg = ConfigObj(os.path.join(self.old_dir, 'db.cfg'), encoding='UTF8')

        # only need organism specific params, copy into new config
        new_cfg = ConfigObj(encoding='UTF8')

        new_cfg['name'] = old_cfg[self.short_id]['name']
        new_cfg['short_name'] = old_cfg[self.short_id]['short_name']
        new_cfg['common_name'] = old_cfg[self.short_id]['common_name']
        new_cfg['gm_organism_id'] = old_cfg[self.short_id]['gm_organism_id']
        new_cfg['default_genes'] = old_cfg['Defaults']['Genes'][self.short_id]

        # write 'em out to new folder with
        if not os.path.exists(self.new_dir):
            os.makedirs(self.new_dir)

        new_cfg.filename = os.path.join(self.new_dir, 'organism.cfg')
        new_cfg.write()

        # stash old config for use later in the import
        self.master_cfg = old_cfg


    def import_identifiers(self):

        # 'raw' mapping file from old pipeline
        raw_fn = 'ENSEMBL_ENTREZ_' + self.short_id
        old_file = os.path.join(self.old_dir, 'mappings', 'raw', raw_fn)

        # the actual file name doesn't matter in the output dir as it does
        # in the input dir, but i'll resist the temptation to randomize for proof
        new_subdir = os.path.join(self.new_dir, 'identifiers', 'mixed_table')
        if not os.path.exists(new_subdir):
            os.makedirs(new_subdir)

        new_file = os.path.join(new_subdir, raw_fn)
        shutil.copyfile(old_file, new_file)

    def import_networks(self):
        """all of them"""

        # pick out network data from under the old tree belonging
        # to the organism. this was files under 'db/data/{collection}/{short_id}/*.cfg
        # for the configs, and the config files themselves contained vars that
        # pointed to processed data files in various states (raw, profiles, networks, etc).

        pattern = os.path.join(self.old_dir, 'data', '*', self.short_id, '*.cfg')
        cfg_files = glob.glob(pattern)

        for cfg_file in cfg_files:
            self.import_network(cfg_file)

    def import_network(self, cfg_file):
        """just the given one"""

        # create a config
        old_cfg = ConfigObj(cfg_file, encoding='UTF8')

        new_cfg = ConfigObj(encoding='UTF8')

        new_cfg['group'] = old_cfg['dataset']['group']
        new_cfg['default_selected'] = old_cfg['dataset']['default_selected']
        new_cfg['name'] = old_cfg['dataset']['name']
        new_cfg['description'] = old_cfg['dataset']['description']
        new_cfg['pubmed_id'] = old_cfg['gse']['pubmed_id']

        # use generic source_id instead of gse_id
        source = old_cfg['dataset']['source']
        new_cfg['source'] = source

        if source == 'GEO':
            source_id = old_cfg['gse']['gse_id']
        else:
            source_id = ''

        new_cfg['source_id'] = source_id


        # extract the 'collection' (e.g. biogrid) from the old path, its between
        # data and the short_id
        path_parts = os.path.dirname(cfg_file).split(os.sep)
        collection = path_parts[-2]

        # check that the master config has a processing recipe for this collection
        # the destination location for the file depends on the processing type
        assert collection in self.master_cfg['processing']

        # tempting to make the processing type classification ultra-general,
        # based ont the params in the master config, but this script should
        # be pretty throw-away and the configs don't change, so just
        # use the collection name to hard-code a classification
        #
        # proctype corresponds to one of the folders under data/networks in the
        # new pipeline, data in each of those folders is processed in a particular way

        if collection == 'pfam':
            proctype = 'sharedneighbour'
            data_filename_field, data_folder = 'raw_data', 'raw'

        elif collection == 'interpro':
            proctype = 'sharedneighbour'
            data_filename_field, data_folder = 'raw_data', 'raw'

        elif collection == 'biogrid_direct':
            proctype = 'direct'
            data_filename_field, data_folder = 'raw_data', 'raw'

        elif collection == 'iref_direct':
            proctype = 'direct'
            data_filename_field, data_folder = 'raw_data', 'raw'

        elif collection == 'i2d_direct':
            proctype = 'direct'
            data_filename_field, data_folder = 'raw_data', 'raw'

        elif collection == 'misc_network':
            proctype = 'direct'
            data_filename_field, data_folder = 'raw_data', 'raw'

        elif collection == 'misc_network_normalized':
            proctype = 'direct'
            data_filename_field, data_folder = 'raw_data', 'raw'

        elif collection == 'mousefunc':
            proctype = 'direct'
            data_filename_field, data_folder = 'raw_data', 'raw'

        elif collection == 'pathwaycommons_direct':
            proctype = 'direct'
            data_filename_field, data_folder = 'raw_data', 'raw'

        elif collection == 'web_import_network':
            proctype = 'direct'
            data_filename_field, data_folder = 'raw_data', 'raw'

        elif collection == 'web_import_profile':
            proctype = 'profile'
            data_filename_field, data_folder = 'raw_data', 'raw'

        elif collection == 'geo':
            proctype = 'profile'
            data_filename_field, data_folder = 'profile', 'profile'

        elif collection == 'misc_profile':
            proctype = 'profile'
            data_filename_field, data_folder = 'raw_data', 'raw'

        else:
            print('skipping collection', collection)
            return

        # check the corresponding data file exists
        data_file = old_cfg['gse'][data_filename_field]
        data_file = os.path.join(os.path.dirname(cfg_file), data_folder, data_file)
        assert os.path.exists(data_file)

        # naming of the new files, both config and data file have the same
        # base name with specific extensions, and no pointers from the
        # config file to the corresponding data file
        basename = os.path.basename(data_file)
        if basename.endswith('.txt'):
            basename = basename[:-4]

        new_path = os.path.join(self.new_dir, 'networks', proctype, collection)
        if not os.path.exists(new_path):
            os.makedirs(new_path)

        new_data_name = os.path.join(new_path, basename + '.txt')
        new_cfg_name = os.path.join(new_path, basename + '.cfg')

        # write the config, and copy the data
        new_cfg.filename = new_cfg_name
        new_cfg.write()
        shutil.copyfile(data_file, new_data_name)

    def import_go_categories(self):

        org_id = self.master_cfg[self.short_id]['gm_organism_id']
        old_file = os.path.join(self.old_dir, 'GoCategories', '%s.annos.txt' % org_id)
        new_file = os.path.join(self.new_dir, 'go_annos.txt')

        shutil.copyfile(old_file, new_file)

    def import_attributes(self):
        """two places to find attributes, in the web-import folder
        and in locations specified in the master config's Attributes section

        looks like the data file paths are not consistent in the folder
        they are relative to, so fix up

        also fix up the MAGIC_ORG_IDENTIFIER hack by applying the organism short id
        """

        # from web-import
        path = os.path.join(self.old_dir, 'attributes', 'import')
        attrib_metadata_file = os.path.join(path, 'attribute-metadata.cfg')
        attrib_metadata = ConfigObj(attrib_metadata_file, encoding='UTF8')

        for name in attrib_metadata:
            attrib = attrib_metadata[name]

            # file path fixes
            data_file = attrib['assoc_file']
            desc_file = attrib['desc_file']

            data_file = os.path.join('attributes', 'import', data_file)
            desc_file = os.path.join('attributes', 'import', desc_file)

            attrib['assoc_file'] = data_file
            attrib['desc_file'] = desc_file

            # import
            self.import_attribute_file('web_import', name, attrib)

        # from master-config
        attrib_metadata = self.master_cfg['Attributes']
        for name in attrib_metadata:
            attribs = attrib_metadata[name]

            # file path fixes
            data_file = attribs['assoc_file']
            desc_file = attribs['desc_file']

            data_file = data_file.replace('MAGIC_ORG_IDENTIFIER', self.short_id)
            desc_file = desc_file.replace('MAGIC_ORG_IDENTIFIER', self.short_id)

            attribs['assoc_file'] = data_file
            attribs['desc_file'] = desc_file

            # import
            self.import_attribute_file('manual', name, attrib)

    def import_attribute_file(self, collection, name, attrib):

        print(collection, name)

        # check if presence of the data files
        data_file = attrib['assoc_file']
        desc_file = attrib['desc_file']

        data_file = os.path.join(self.old_dir, data_file)
        desc_file = os.path.join(self.old_dir, desc_file)

        assert os.path.exists(data_file)
        assert os.path.exists(desc_file)

        # create attribute metadata

        cfg = ConfigObj(encoding='UTF8')

        cfg['name'] = attrib['name']
        cfg['desc'] = attrib['desc']
        cfg['linkout_label'] = attrib['linkout_label']
        cfg['linkout_url'] = attrib['linkout_url']
        cfg['default_selected'] = attrib['default_selected']
        cfg['pub_name'] = attrib['pub_name']
        cfg['pub_url'] = attrib['pub_url']

        # think this is always true
        assert attrib['attributes_identified_by'] == 'external_id'

        # drop into correct processing location depending on the
        # data format
        data_format = attrib['assoc_format']
        if data_format == '1':
            proctype = 'gene-attrib-list'
        elif data_format == '2':
            proctype = 'attrib-desc-gene-list'
        else:
            raise Exception('unexpected data format: ' + data_format)

        # naming of the new files, config, data and descriptions, have the same
        # base name with specific extensions, and no pointers from the
        # config file to the corresponding data file
        basename = os.path.basename(data_file)
        if basename.endswith('.txt'):
            basename = basename[:-4]

        new_path = os.path.join(self.new_dir, 'attributes', proctype, collection)

        if not os.path.exists(new_path):
            os.makedirs(new_path)

        new_data_name = os.path.join(new_path, basename + '.txt')
        new_desc_name = os.path.join(new_path, basename + '.desc')
        new_cfg_name = os.path.join(new_path, basename + '.cfg')

        # write the config, and copy the data
        cfg.filename = new_cfg_name
        cfg.write()
        shutil.copyfile(data_file, new_data_name)
        shutil.copyfile(desc_file, new_desc_name)

def main(old_dir, short_id, new_dir):

    importer = Importer(old_dir, short_id, new_dir)
    importer.import_all()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='import data files from old to new pipeline')

    parser.add_argument('old_dir', help='db/ folder containing old pipeline data')
    parser.add_argument('short_id', help='organism short id in old pipeline, e.g. Hs for human')
    parser.add_argument('new_dir', help='data/ dir of new pipeline to populate')

    args = parser.parse_args()
    main(args.old_dir, args.short_id, args.new_dir)









