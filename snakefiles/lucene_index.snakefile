
# not sure about the naming of the lucene index files,
# so try using a marker flag file to start: lucene_ready.txt

rule LUCENE_INDEX:
    input: "work/flags/lucene.flag"

rule LUCENE_CFG:
    input: "data/organism.cfg"
    output: "work/lucene.cfg"
    shell: "python builder/create_compat_config.py {input} {output}"

rule BUILD_LUCENE_INDEX:
    input: gdb=GENERIC_DB_FILES, cfg="work/lucene.cfg", colours="config/colours.txt"
    params: base_dir="result", lucene_index_dir="result/lucene_index", profile="none"
    output: "work/flags/lucene.flag"
    shell: "java -cp {JAR_FILE} org.genemania.mediator.lucene.exporter.Generic2LuceneExporter \
        {input.cfg} {params.base_dir} {input.colours} {params.profile} {params.lucene_index_dir} && touch {output}"

rule CLEAN_LUCENE_INDEX:
    shell: """rm -rf result/lucene_index/*
        rm -f work/flags/lucene.flag
        """
