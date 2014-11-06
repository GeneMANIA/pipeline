

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
    output: annos="work/functions/all_annos.txt", names="work/functions/all_anno_names.txt"
    shell: "python builder/filter_go_annotations.py clean {input.annos} {input.symbols} {output.annos} {output.names}"

rule ENRICHMENT_FUNCTIONS:
    input: "work/functions/all_annos.txt"
    output: "work/functions/enrichment/enrichment_annos.txt"
    shell: "python builder/filter_go_annotations.py filter {input} {output} 10 300"

rule COMBINING_FUNCTIONS_BP:
    input: "work/functions/all_annos.txt"
    output: "work/functions/combining/BP_annos.txt"
    shell: "python builder/filter_go_annotations.py filter {input} {output} 3 300 --branch biological_process"

rule COMBINING_FUNCTIONS_MF:
    input: "work/functions/all_annos.txt"
    output: "work/functions/combining/MF_annos.txt"
    shell: "python builder/filter_go_annotations.py filter {input} {output} 3 300 --branch molecular_function"

rule COMBINING_FUNCTIONS_CC:
    input: "work/functions/all_annos.txt"
    output: "work/functions/combining/CC_annos.txt"
    shell: "python builder/filter_go_annotations.py filter {input} {output} 3 300 --branch cellular_component"

rule GENERIC_DB_FUNCTIONS:
    input: function_file = "work/functions/enrichment/enrichment_annos.txt", function_groups="result/generic_db/ONTOLOGIES.txt", function_names="work/functions/all_anno_names.txt"
    output: "result/generic_db/ONTOLOGY_CATEGORIES.txt"
    shell: "python builder/extract_functions.py functions {input.function_file} {input.function_groups} {input.function_names} {output}"

rule GENERIC_DB_FUNCTION_GROUPS:
    input: annos="work/functions/enrichment/enrichment_annos.txt"
    output: "result/generic_db/ONTOLOGIES.txt"
    shell: "python builder/extract_functions.py function_groups {output} {input}"

# TODO: split this up
rule COPY_GENERIC_DB_FUNCTION_DATA:
    input: "work/functions/enrichment/enrichment_annos.txt",
        "work/functions/combining/BP_annos.txt",
        "work/functions/combining/MF_annos.txt",
        "work/functions/combining/CC_annos.txt"
    output: "result/generic_db/GO_CATEGORIES/1.annos.txt",  # TODO org id
        "result/generic_db/GO_CATEGORIES/1_BP.txt",
        "result/generic_db/GO_CATEGORIES/1_MF.txt",
        "result/generic_db/GO_CATEGORIES/1_CC.txt",
        "work/flags/generic_db.function_data.flag"
    shell: """sed '1d;$d' {input[0]} > {output[0]} && \
           sed '1d;$d' {input[1]} > {output[1]} && \
           sed '1d;$d' {input[2]} > {output[2]} && \
           sed '1d;$d' {input[3]} > {output[3]} && \
           touch {output[4]}
           """
