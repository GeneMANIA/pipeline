
## process identifiers

# create a cleaned set of identifiers and descriptions from a set of input files

SYMBOL_FILES = glob_wildcards('data/identifiers/symbols/{fn}')
DESCRIPTIONS_FILES = glob_wildcards('data/identifiers/descriptions/{fn}')

# filter out files starting with '.', e.g. .gitignore
ignores = [i for i in range(len(SYMBOL_FILES.fn)) if SYMBOL_FILES.fn[i].startswith('.')]
for i in range(len(SYMBOL_FILES)):
    for ignore in ignores:
        del(SYMBOL_FILES[i][ignore])

# target rule for symbol scrubbing
rule SCRUB_SYMBOLS:
    input: "work/identifiers/symbols.txt"

rule APPLY_SYMBOL_SCRUBBING:
    message: "load all identifier input files containing id/symbol/source triplets and produce a single cleaned file"
    input: expand("data/identifiers/symbols/{fn}", fn=SYMBOL_FILES.fn)
    output: "work/identifiers/symbols.txt"
    log: "work/identifiers/symbols.log"
    shell: "python builder/clean_identifiers.py {input} --output {output} --log {log}"

rule CLEAN_SYMBOLS:
    shell: """
        rm -f work/identifiers/symbols.txt
        rm -f work/identifiers/symbols.log
    """

rule IDENTIFIER_DESCRIPTIONS:
    input: "work/identifiers/symbols.txt"
    output: "work/identifiers/descriptions.txt"
    shell: "python builder/clean_identifier_descriptions.py {input} --output {output}"

rule GENERIC_DB_NODES:
    input: "work/identifiers/symbols.txt"
    output: "result/generic_db/NODES.txt"
    shell: "python builder/extract_identifiers.py nodes {ORGANISM_ID} {input} {output}"

rule GENERIC_DB_GENES:
    input: idents="work/identifiers/symbols.txt", naming_sources="result/generic_db/GENE_NAMING_SOURCES.txt", organism_cfg="data/organism.cfg"
    output: "result/generic_db/GENES.txt"
    shell: "python builder/extract_identifiers.py genes {input.organism_cfg} {input.idents} {input.naming_sources} {output}"

rule GENERIC_DB_GENE_DATA:
    input: idents="work/identifiers/symbols.txt", descs="work/identifiers/descriptions.txt"
    output: "result/generic_db/GENE_DATA.txt"
    shell: "python builder/extract_identifiers.py gene_data {input.idents} {input.descs} {output}"

rule GENERIC_DB_GENE_NAMING_SOURCES:
    input: "work/identifiers/symbols.txt"
    output: "result/generic_db/GENE_NAMING_SOURCES.txt"
    shell: "python builder/extract_identifiers.py naming_sources {input} {output}"

