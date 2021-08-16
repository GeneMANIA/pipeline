
## process identifiers

# create a cleaned set of identifiers and descriptions from a set of input files

SYMBOL_FILES = glob_wildcards(DATA+'/identifiers/symbols/{fn}')
DESCRIPTION_FILES = glob_wildcards(DATA+'/identifiers/descriptions/{fn}')
RAW_FILES = glob_wildcards(DATA+'/identifiers/mixed_table/{fn}')

# filter out files starting with '.', e.g. .gitignore
ignores = [i for i in range(len(SYMBOL_FILES.fn)) if SYMBOL_FILES.fn[i].startswith('.')]
for i in range(len(SYMBOL_FILES)):
    for ignore in ignores:
        del(SYMBOL_FILES[i][ignore])

COMBINED_SYMBOL_FILES = expand(DATA+'/identifiers/symbols/{fn}', fn=SYMBOL_FILES.fn) + expand(WORK+'/identifiers/symbols/{fn}.triplets', fn=RAW_FILES.fn)
COMBINED_DESCRIPTION_FILES = expand(DATA+'/identifiers/descriptions/{fn}', fn=DESCRIPTION_FILES.fn) + expand(WORK+'/identifiers/descriptions/{fn}.desc', fn=RAW_FILES.fn)


# raw_file, should only be one. reformat
# into 3 column format, with a bit of cleaning applied
rule MELT_RAW_IDENTIFIERS:
    message: "convert raw identifiers into id/symbol/source triplets, and filter biotypes"
    input: ident_file=DATA+"/identifiers/mixed_table/{fn}", cfg=DATA+"/organism.cfg"
    output: symbols=WORK+"/identifiers/symbols/{fn}.triplets", descriptions=WORK+"/identifiers/descriptions/{fn}.desc"
    shell: "python builder/melt_raw_identifiers.py {input.ident_file} {output.symbols} {output.descriptions} \
        --biotypes $(python builder/getparam.py {input.cfg} identifier_biotypes --default protein_coding,True --empty_as_default)"

# target rule for symbol scrubbing
rule SCRUB_SYMBOLS:
    message: "target rule for cleaned gene identifiers"
    input: WORK+"/identifiers/symbols.txt"

rule APPLY_SYMBOL_SCRUBBING:
    message: "load all identifier input files containing id/symbol/source triplets and produce a single clean file remove duplicates and clashes"
    #input: expand(DATA+"/identifiers/symbols/{fn}", fn=SYMBOL_FILES.fn)
    input: files=COMBINED_SYMBOL_FILES, cfg=DATA+"/organism.cfg"
    output: WORK+"/identifiers/symbols.txt"
    log: WORK+"/identifiers/symbols.log"
    shell: "python builder/clean_identifiers.py {input.files} --output {output} --log {log} \
        --merge_names $(python builder/getparam.py {input.cfg} identifier_merging_enabled --default true) \
        --ignore '$(python builder/getparam.py {input.cfg} identifier_sources_to_ignore --default ignore_nothing --empty_as_default)'"

rule CLEAN_SYMBOLS:
    shell: """
        rm -f {WORK}/identifiers/symbols.txt
        rm -f {WORK}/identifiers/symbols.log
    """

rule IDENTIFIER_DESCRIPTIONS:
    message: "create table containing descriptions for only the clean gene symbols"
    input: symbols=WORK+"/identifiers/symbols.txt", descriptions=COMBINED_DESCRIPTION_FILES
    output: WORK+"/identifiers/descriptions.txt"
    shell: "python builder/clean_identifier_descriptions.py {input.symbols} {input.descriptions} --output {output}"

rule GENERIC_DB_NODES:
    message: "create generic db file NODES.txt"
    input: symbols=WORK+"/identifiers/symbols.txt", cfg=DATA+"/organism.cfg"
    output: RESULT+"/generic_db/NODES.txt"
    shell: """ORGANISM_ID=$(python builder/getparam.py {input.cfg} gm_organism_id --default 1)
        python builder/extract_identifiers.py nodes $ORGANISM_ID {input.symbols} {output}
        """

rule GENERIC_DB_GENES:
    message: "create generic db file GENES.txt containing all identifier synonyms"
    input: idents=WORK+"/identifiers/symbols.txt", naming_sources=RESULT+"/generic_db/GENE_NAMING_SOURCES.txt", organism_cfg=DATA+"/organism.cfg"
    output: RESULT+"/generic_db/GENES.txt"
    shell: "python builder/extract_identifiers.py genes {input.organism_cfg} {input.idents} {input.naming_sources} {output}"

rule GENERIC_DB_GENE_DATA:
    message: "create generic db file GENE_DATA.txt containing gene descriptions"
    input: idents=WORK+"/identifiers/symbols.txt", descs=WORK+"/identifiers/descriptions.txt"
    output: RESULT+"/generic_db/GENE_DATA.txt"
    shell: "python builder/extract_identifiers.py gene_data {input.idents} {input.descs} {output}"

rule GENERIC_DB_GENE_NAMING_SOURCES:
    message: "create generic db file GENE_NAMING_SOURCES.txt enumerating all identifier source types"
    input: symbols=WORK+"/identifiers/symbols.txt", cfg=DATA+"/organism.cfg"
    output: RESULT+"/generic_db/GENE_NAMING_SOURCES.txt"
    shell: """ORGANISM_ID=$(python builder/getparam.py {input.cfg} gm_organism_id --default 1) 
	python builder/extract_identifiers.py naming_sources {input.symbols} {output} $ORGANISM_ID
	"""

