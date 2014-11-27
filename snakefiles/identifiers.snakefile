
## process identifiers

# create a cleaned set of identifiers and descriptions from a set of input files

SYMBOL_FILES = glob_wildcards('data/identifiers/symbols/{fn}')
DESCRIPTION_FILES = glob_wildcards('data/identifiers/descriptions/{fn}')
RAW_FILES = glob_wildcards('data/identifiers/mixed_table/{fn}')

# filter out files starting with '.', e.g. .gitignore
ignores = [i for i in range(len(SYMBOL_FILES.fn)) if SYMBOL_FILES.fn[i].startswith('.')]
for i in range(len(SYMBOL_FILES)):
    for ignore in ignores:
        del(SYMBOL_FILES[i][ignore])

COMBINED_SYMBOL_FILES = expand('data/identifiers/symbols/{fn}', fn=SYMBOL_FILES.fn) + expand('work/identifiers/symbols/{fn}.triplets', fn=RAW_FILES.fn)
COMBINED_DESCRIPTION_FILES = expand('data/identifiers/descriptions/{fn}', fn=DESCRIPTION_FILES.fn) + expand('work/identifiers/descriptions/{fn}.desc', fn=RAW_FILES.fn)


# raw_file, should only be one. reformat
# into 3 column format, with a bit of cleaning applied
rule MELT_RAW_IDENTIFIERS:
    message: "convert raw identifiers into id/symbol/source triplets, and filter biotypes"
    input: "data/identifiers/mixed_table/{fn}"
    output: symbols="work/identifiers/symbols/{fn}.triplets", descriptions="work/identifiers/descriptions/{fn}.desc"
    shell: "python builder/melt_raw_identifiers.py {input} {output.symbols} {output.descriptions} --biotypes protein_coding True"

# target rule for symbol scrubbing
rule SCRUB_SYMBOLS:
    message: "target rule for cleaned gene identifiers"
    input: "work/identifiers/symbols.txt"

rule APPLY_SYMBOL_SCRUBBING:
    message: "load all identifier input files containing id/symbol/source triplets and produce a single clean file remove duplicates and clashes"
    #input: expand("data/identifiers/symbols/{fn}", fn=SYMBOL_FILES.fn)
    input: COMBINED_SYMBOL_FILES
    output: "work/identifiers/symbols.txt"
    log: "work/identifiers/symbols.log"
    shell: "python builder/clean_identifiers.py {input} --output {output} --log {log}"

rule CLEAN_SYMBOLS:
    shell: """
        rm -f work/identifiers/symbols.txt
        rm -f work/identifiers/symbols.log
    """

rule IDENTIFIER_DESCRIPTIONS:
    message: "create table containing descriptions for only the clean gene symbols"
    input: "work/identifiers/symbols.txt"
    output: "work/identifiers/descriptions.txt"
    shell: "python builder/clean_identifier_descriptions.py {input} --output {output}"

rule GENERIC_DB_NODES:
    message: "create generic db file NODES.txt"
    input: symbols="work/identifiers/symbols.txt", cfg="data/organism.cfg"
    output: "result/generic_db/NODES.txt"
    shell: """ORGANISM_ID=$(python builder/getparam.py {input.cfg} gm_organism_id --default 1)
        python builder/extract_identifiers.py nodes $ORGANISM_ID {input.symbols} {output}
        """

rule GENERIC_DB_GENES:
    message: "create generic db file GENES.txt containing all identifier synonyms"
    input: idents="work/identifiers/symbols.txt", naming_sources="result/generic_db/GENE_NAMING_SOURCES.txt", organism_cfg="data/organism.cfg"
    output: "result/generic_db/GENES.txt"
    shell: "python builder/extract_identifiers.py genes {input.organism_cfg} {input.idents} {input.naming_sources} {output}"

rule GENERIC_DB_GENE_DATA:
    message: "create generic db file GENE_DATA.txt containing gene descriptions"
    input: idents="work/identifiers/symbols.txt", descs="work/identifiers/descriptions.txt"
    output: "result/generic_db/GENE_DATA.txt"
    shell: "python builder/extract_identifiers.py gene_data {input.idents} {input.descs} {output}"

rule GENERIC_DB_GENE_NAMING_SOURCES:
    message: "create generic db file GENE_NAMING_SOURCES.txt enumerating all identifier source types"
    input: "work/identifiers/symbols.txt"
    output: "result/generic_db/GENE_NAMING_SOURCES.txt"
    shell: "python builder/extract_identifiers.py naming_sources {input} {output}"

