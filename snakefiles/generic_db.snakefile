

rule GENERIC_DB:
  input: GENERIC_DB_FILES

# no longer support this scheme of tagging networks,
# may revise in future. just create empty files for now
# leaving support in rest of the system

rule GDB_TAGS:
    output: "result/generic_db/TAGS.txt"
    shell: "touch {output}"

rule GDB_NETWORK_TAG_ASSOC:
    output: "result/generic_db/NETWORK_TAG_ASSOC.txt"
    shell: "touch {output}"

rule GDB_SCHEMA:
    input: "config/SCHEMA.txt"
    output: "result/generic_db/SCHEMA.txt"
    shell: "cp {input} {output}"

rule GDB_STATISTICS:
    output: "result/generic_db/STATISTICS.txt"
    shell: "touch {output}"

rule CLEAN_GENERIC_DB:
    shell: """rm -rf result/generic_db
        rm -f work/flags/generic_db.*.flag
        """

rule GDB_ORGANISMS:
    input: "data/organism.cfg"
    output: "result/generic_db/ORGANISMS.txt"
    shell: "python builder/extract_organisms.py {input} {output}"

GO_BRANCHES = ['BP', 'MF', 'CC']

rule GDB_GO_CATEGORIES:
    input: expand("result/generic_db/GO_CATEGORIES/{ORGANISM_ID}_{BRANCH}.txt", ORGANISM_ID=ORGANISM_ID, BRANCH=GO_BRANCHES)

rule GDB_CP_GO_CATEGORIES:
    input: "work/functions/{BRANCH}.txt"
    output: "result/generic_db/GO_CATEGORIES/{ORGANISM_ID}_{BRANCH}.txt"
    shell: "cp -f {input} {output}"

# might want to move this rule to a networks.snakefile, that combines direct, shared-neighbour, and profile rules
# as well as populating this part of generic_db

# dup'd again
NW_PROCESSED_FILES = glob_wildcards("data/networks/{proctype}/{collection}/{fn}.txt")

# dynamic() needs more investigation, use flag file
#rule GDB_INTERACTIONS:
#    input: dynamic("result/generic_db/INTERACTIONS/{ORG_ID}.{NW_ID}.txt")
#
#rule COPY_INTERACTIONS:
#    input: mapfile="work/networks/network_metadata.txt", networks=expand("work/networks/{proctype}/{collection}/{fn}.txt.nn", zip, proctype=NW_PROCESSED_FILES.proctype, collection=NW_PROCESSED_FILES.collection, fn=NW_PROCESSED_FILES.fn)
#    output: dynamic("result/generic_db/INTERACTIONS/{ORG_ID}.{NW_ID}.txt")
#    params: newdir='result/generic_db/INTERACTIONS'
#    shell: "python builder/rename_data_files.py interactions {input.mapfile} {params.newdir} {ORGANISM_ID} {input.networks}"

rule GDB_INTERACTIONS:
    input: "work/flags/generic_db.interaction_data.flag"

rule COPY_INTERACTIONS:
    input: mapfile="work/networks/network_metadata.txt", networks=expand("work/networks/{proctype}/{collection}/{fn}.txt.nn", zip, proctype=NW_PROCESSED_FILES.proctype, collection=NW_PROCESSED_FILES.collection, fn=NW_PROCESSED_FILES.fn)
    output: "work/flags/generic_db.interaction_data.flag"
    params: newdir='result/generic_db/INTERACTIONS'
    shell: "python builder/rename_data_files.py interactions {input.mapfile} {params.newdir} {ORGANISM_ID} {input.networks} --key_lstrip='work/' --key_rstrip='.txt.nn' && touch {output}"
