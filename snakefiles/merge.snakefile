
# for merging multiple organisms into a single db
#
# this short-circuits part of the build process, creating
# a generic_db from a list of previously constructed individual
# generic_db's, creating corresponding flag files, cooking up
# a merged organism config.

MERGE_FOLDERS=['../human/result/generic_db', '../yeast/result/generic_db']
MERGE_INPUTS=['../human/result/generic_db', '../yeast/result/generic_db']
MERGE_CFGS=['../human/data/organism.cfg', '../yeast/data/organism.cfg']

#rule FAKE_GENERIC_DB_FLAG_FILES:
#    message: "creating fake flag file {output}"
#    output: WORK+"flags/generic_db.{part}.flag"
#    shell: "touch {output}"

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
    input: MERGE_CFGS
    output: WORK+"/lucene.cfg"
    shell: "python builder/create_compat_config.py {input} {output}"

# override the rule in lucene.snakefile to make the same a single organism lucene.cfg
ruleorder: MERGED_LUCENE_CONFIG > LUCENE_CONFIG
