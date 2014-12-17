
# for merging multiple organisms into a single db
#
# this short-circuits part of the build process, creating
# a generic_db from a list of previously constructed individual
# generic_db's, creating corresponding flag files, cooking up
# a merged organism config.


# either set manually, as in: 
#
#   MERGE_ROOT_FOLDERS = ['../human', '../yeast']
#
# or provide via command line input as comma delimited list:
#
#   snakemake --config merge=1 orgs=../human,../yeast
#
# else leave assigned to [] to cause a scan for any folders
# that are peers of this pipeline folder that look like they 
# contain organisms for merging 

MERGE_ROOT_FOLDERS = []

# check config
if 'orgs' in config:
    orgs = config['orgs']
    MERGE_ROOT_FOLDERS = orgs.split(',')

# or scan for peers
if MERGE_ROOT_FOLDERS == []:
    PEERS = glob_wildcards('../{folder}/result/generic_db/SCHEMA.txt')
    MERGE_ROOT_FOLDERS = expand('../{folder}', folder=PEERS.folder)

MERGE_FOLDERS=[folder + '/result/generic_db' for folder in MERGE_ROOT_FOLDERS]
MERGE_CONFIGS=[folder + '/data/organism.cfg' for folder in MERGE_ROOT_FOLDERS]

rule MERGE_GENERIC_DBS:
    input: MERGE_FOLDERS
    output: flags=[WORK+'/flags/generic_db.attribute_data.flag', \
        WORK+'/flags/generic_db.interaction_data.flag', \
        WORK+'/flags/generic_db.function_data.flag'],
        files=GENERIC_DB_METADATA_FILES
    params: merged_folder=RESULT+'/generic_db'
    shell: "python builder/merge_organisms.py {input} {params.merged_folder} && touch {output}"

rule MERGED_LUCENE_CONFIG:
    message: "build a merged config file in format compatible with generic db lucene indexer"
    input: MERGE_CONFIGS
    output: WORK+"/lucene.cfg"
    shell: "python builder/create_compat_config.py {input} {output}"

# override the rule in lucene.snakefile to make the same a single organism lucene.cfg
ruleorder: MERGED_LUCENE_CONFIG > LUCENE_CONFIG
