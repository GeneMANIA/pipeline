
# not sure about the naming of the lucene index files,
# so try using a marker flag file to start: lucene.flag

rule LUCENE_INDEX:
    message: "target rule for lucene index files"
    input: WORK+"/flags/lucene.flag"

rule LUCENE_CONFIG:
    message: "build a config file in format compatible with generic db lucene indexer"
    input: DATA+"/organism.cfg"
    output: WORK+"/lucene.cfg"
    shell: "python builder/create_compat_config.py {input} {output}"

rule BUILD_LUCENE_INDEX:
    message: "build lucene index from generic db files"
    input: gdb=GENERIC_DB_METADATA_FILES, cfg=WORK+"/lucene.cfg", colours="config/colours.txt"
    params: base_dir=RESULT, lucene_index_dir=RESULT+"/lucene_index", profile="none"
    output: WORK+"/flags/lucene.flag"
    shell: "java -cp {JAR_FILE} org.genemania.mediator.lucene.exporter.Generic2LuceneExporter \
        {input.cfg} {params.base_dir} {input.colours} {params.profile} {params.lucene_index_dir} && touch {output}"

rule CLEAN_LUCENE_INDEX:
    shell: """rm -rf {RESULT}/lucene_index/*
        rm -f {WORK}/flags/lucene.flag
        """
