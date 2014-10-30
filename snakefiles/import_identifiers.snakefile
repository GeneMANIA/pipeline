
## process identifiers

# create a cleaned set of identifiers and descriptions from a set of input files

TABULAR_FILES = glob_wildcards('data/identifiers/tabular/{fn}')

rule IDENTIFIER_SYMBOLS:
    input: "data/identifier_symbols"

rule TRIPLET_FILES:
    message: "triplet files that need to be produced from tabular identifier files"
    input: expand("work/identifiers/triplets/tabular_{fn}", fn=TABULAR_FILES.fn)

rule TABULAR_TO_TRIPLETS:
    message: "convert tabular identifier files into triplet files"
    input: "data/identifiers/tabular/{fn}"
    output: "work/identifiers/triplets/tabular_{fn}"
    shell: "cp {input} {output}"

rule CLEAN_TRIPLETS:
    shell: "rm -rf work/identifiers/triplets/*"

rule PROCESS_SYMBOLS:
    input: "data/identifiers/symbols/triplets/*"
    output: "data/identifier_symbols"
