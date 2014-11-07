

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
    input: annos="work/functions/enrichment/enrichment_annos.txt", cfg="data/organism.cfg"
    output: "result/generic_db/ONTOLOGIES.txt"
    shell: """ORGANISM_ID=$(python builder/getparam.py {input.cfg} gm_organism_id --default 1)
        python builder/extract_functions.py function_groups $ORGANISM_ID {output} {input.annos}
        """


# this next part is a bit tricky. our old file naming
# conventions have the organism id in the go category files.
# load the id from the organism config file, expand the list
# of target file names, and setup copy rules that just strip
# the header from the corresponding source files. create
# a flag file to connect everything together, until we
# work out how to wire things together with the file lists
# as direct dependencies.


from builder import getparam
TEMP_ORGANISM_ID = getparam.getparam("data/organism.cfg", "gm_organism_id", 1)

GOCAT_FILES = expand(["result/generic_db/GO_CATEGORIES/{ORGANISM_ID}.annos.txt",
    "result/generic_db/GO_CATEGORIES/{ORGANISM_ID}_BP.txt",
    "result/generic_db/GO_CATEGORIES/{ORGANISM_ID}_MF.txt",
    "result/generic_db/GO_CATEGORIES/{ORGANISM_ID}_CC.txt"],
     ORGANISM_ID=TEMP_ORGANISM_ID)

rule COPY_GOCAT_COMBINING_FILES:
    input: "work/functions/combining/{GO_BRANCH}_annos.txt"
    output: "result/generic_db/GO_CATEGORIES/{ORGANISM_ID}_{GO_BRANCH}.txt"
    shell: "sed '1d;$d' {input} > {output}"


rule COPY_GOCAT_ENRICHMENT_FILES:
    input: "work/functions/enrichment/enrichment_annos.txt"
    output: "result/generic_db/GO_CATEGORIES/{ORGANISM_ID}.annos.txt"
    shell: "sed '1d;$d' {input} > {output}"


rule CONTROL_COPY:
    input: cfg="data/organism.cfg", files=GOCAT_FILES
    output: "work/flags/generic_db.function_data.flag"
    shell: "touch {output}"


