

# grab names of files containing functional categories for enrichment analysis
#FNS, = glob_wildcards("work/functions/processed/{fn}")
FNS=[]

ANNOS_FILES = glob_wildcards("data/functions/{fn}.txt")
assert len(ANNOS_FILES) == 1 # sorry, only implemented for one file so far


rule CLEAN_FUNCTIONS:
    shell: """rm -f result/generic_db/ONTOLOGIES.txt
        rm -f result/generic_db/ONTOLOGY_CATEGORIES.txt
        """



# wanted to call this CLEAN_QUERIED... but
# the rules that start with CLEAN usually mean
# removing old build files, so
rule TIDY_QUERIED_FUNCTIONS:
    input: annos=expand("data/functions/{fn}.txt", fn=ANNOS_FILES.fn), symbols="work/identifiers/symbols.txt"
    output: annos="work/functions/clean_annos.txt", names="work/functions/clean_anno_names.txt"
    shell: "python builder/filter_go_annotations.py clean {input.annos} {input.symbols} {output.annos} {output.names}"

rule ENRICHMENT_FUNCTIONS:
    input: "work/functions/clean_annos.txt"
    output: "work/functions/enrichment_functions.txt"
    shell: "python builder/filter_go_annotations.py filter {input} {output} 10 300"

rule COMBINING_FUNCTIONS_BP:
    input: "work/functions/clean_annos.txt"
    output: "work/functions/BP.txt"
    shell: "python builder/filter_go_annotations.py filter {input} {output} 3 300 --branch biological_process"

rule COMBINING_FUNCTIONS_MF:
    input: "work/functions/clean_annos.txt"
    output: "work/functions/MF.txt"
    shell: "python builder/filter_go_annotations.py filter {input} {output} 3 300 --branch molecular_function"

rule COMBINING_FUNCTIONS_CC:
    input: "work/functions/clean_annos.txt"
    output: "work/functions/CC.txt"
    shell: "python builder/filter_go_annotations.py filter {input} {output} 3 300 --branch cellular_component"

rule GENERIC_DB_FUNCTIONS:
    input: function_files = "work/functions/enrichment_functions.txt", function_groups="result/generic_db/ONTOLOGIES.txt"
    output: "result/generic_db/ONTOLOGY_CATEGORIES.txt"
    shell: "python builder/extract_functions.py functions {output} {input.function_groups} {input.function_files}"

rule GENERIC_DB_FUNCTION_GROUPS:
    input: "work/functions/enrichment_functions.txt"
    output: "result/generic_db/ONTOLOGIES.txt"
    shell: "python builder/extract_functions.py function_groups {output} {input}"

rule COPY_GENERIC_DB_FUNCTION_DATA:
    input: "work/functions/enrichment_functions.txt",
        "work/functions/BP.txt",
        "work/functions/MF.txt",
        "work/functions/CC.txt"
    output: "result/generic_db/GO_CATEGORIES/1.annos.txt",  # TODO org id
        "result/generic_db/GO_CATEGORIES/1_BP.txt",
        "result/generic_db/GO_CATEGORIES/1_MF.txt",
        "result/generic_db/GO_CATEGORIES/1_CC.txt"
    shell: """cp -f {input[0]} {output[0]}
        cp -f {input[1]} {output[1]}
        cp -f {input[2]} {output[2]}
        cp -f {input[3]} {output[3]}
        """
