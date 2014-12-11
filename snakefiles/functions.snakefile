

# grab names of files containing functional categories for enrichment analysis
ANNOS_FILES = glob_wildcards(DATA+"/functions/{fn}.txt")
assert len(ANNOS_FILES) == 1 # sorry, only implemented for one file so far

rule CLEAN_FUNCTIONS:
    shell: """rm -f {RESULT}/generic_db/ONTOLOGIES.txt
        rm -f {RESULT}/generic_db/ONTOLOGY_CATEGORIES.txt
        """

# wanted to call this CLEAN_QUERIED... but
# the rules that start with CLEAN usually mean
# removing old build files, so
rule TIDY_QUERIED_FUNCTIONS:
    message: "filter functional annotations by recognized gene symbols"
    input: annos=expand(DATA+"/functions/{fn}.txt", fn=ANNOS_FILES.fn), symbols=WORK+"/identifiers/symbols.txt"
    output: annos=WORK+"/functions/all_annos.txt", names=WORK+"/functions/all_anno_names.txt"
    shell: "python builder/filter_go_annotations.py clean {input.annos} {input.symbols} {output.annos} {output.names}"

rule ENRICHMENT_FUNCTIONS:
    message: "filter functional annotation categories by size for enrichment analysis"
    input: WORK+"/functions/all_annos.txt"
    output: WORK+"/functions/enrichment/enrichment_annos.txt"
    shell: "python builder/filter_go_annotations.py filter {input} {output} 10 300"

rule COMBINING_FUNCTIONS_BP:
    message: "filter functional annotation categories by size and branch BP combining"
    input: WORK+"/functions/all_annos.txt"
    output: WORK+"/functions/combining/BP_annos.txt"
    shell: "python builder/filter_go_annotations.py filter {input} {output} 3 300 --branch biological_process"

rule COMBINING_FUNCTIONS_MF:
    message: "filter functional annotation categories by size and branch MF combining"
    input: WORK+"/functions/all_annos.txt"
    output: WORK+"/functions/combining/MF_annos.txt"
    shell: "python builder/filter_go_annotations.py filter {input} {output} 3 300 --branch molecular_function"

rule COMBINING_FUNCTIONS_CC:
    message: "filter functional annotation categories by size and branch CC combining"
    input: WORK+"/functions/all_annos.txt"
    output: WORK+"/functions/combining/CC_annos.txt"
    shell: "python builder/filter_go_annotations.py filter {input} {output} 3 300 --branch cellular_component"

rule GENERIC_DB_FUNCTIONS:
    message: "build generic db file ONTOLOGY_CATEGORIES.txt for sets of functional annotations available for enrichment analysis"
    input: function_file = WORK+"/functions/enrichment/enrichment_annos.txt", function_groups=RESULT+"/generic_db/ONTOLOGIES.txt", function_names=WORK+"/functions/all_anno_names.txt"
    output: RESULT+"/generic_db/ONTOLOGY_CATEGORIES.txt"
    shell: "python builder/extract_functions.py functions {input.function_file} {input.function_groups} {input.function_names} {output}"

rule GENERIC_DB_FUNCTION_GROUPS:
    message: "build generic db file ONTOLGOIES.txt with names of function categories for display in enrichment analysis"
    input: annos=WORK+"/functions/enrichment/enrichment_annos.txt", cfg=DATA+"/organism.cfg"
    output: RESULT+"/generic_db/ONTOLOGIES.txt"
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
TEMP_ORGANISM_ID = getparam.getparam(DATA+"/organism.cfg", "gm_organism_id", 1)

GOCAT_FILES = expand([RESULT+"/generic_db/GO_CATEGORIES/{ORGANISM_ID}.annos.txt",
    RESULT+"/generic_db/GO_CATEGORIES/{ORGANISM_ID}_BP.txt",
    RESULT+"/generic_db/GO_CATEGORIES/{ORGANISM_ID}_MF.txt",
    RESULT+"/generic_db/GO_CATEGORIES/{ORGANISM_ID}_CC.txt"],
     ORGANISM_ID=TEMP_ORGANISM_ID)

rule COPY_GOCAT_COMBINING_FILES:
    message: "copy functional annotation data files for GO based combining to generic db"
    input: WORK+"/functions/combining/{GO_BRANCH}_annos.txt"
    output: RESULT+"/generic_db/GO_CATEGORIES/{ORGANISM_ID}_{GO_BRANCH}.txt"
    shell: "sed '1d;$d' {input} > {output}"

rule COPY_GOCAT_ENRICHMENT_FILES:
    message: "copy functional annotation data files for enrichment analysis to generic db"
    input: WORK+"/functions/enrichment/enrichment_annos.txt"
    output: RESULT+"/generic_db/GO_CATEGORIES/{ORGANISM_ID}.annos.txt"
    shell: "sed '1d;$d' {input} > {output}"

rule GENERIC_DB_FUNCTIONS_ALL:
    message: "create flag file marking functional annotation data in generic_db as constructed"
    input: cfg=DATA+"/organism.cfg", files=GOCAT_FILES
    output: WORK+"/flags/generic_db.function_data.flag"
    shell: "touch {output}"


