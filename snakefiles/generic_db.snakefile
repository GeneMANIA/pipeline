

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

# might want to move this rule to a networks.snakefile, that combines direct, shared-neighbour, and profile rules
# as well as populating this part of generic_db

# dup'd again
NW_PROCESSED_FILES = glob_wildcards("data/networks/{proctype}/{collection}/{fn}.txt")

# dynamic() needs more investigation, use flag file
rule GDB_INTERACTIONS:
    input: "work/flags/generic_db.interaction_data.flag"

# TODO: should we be using the processed metadata file here to guide the copying?
rule COPY_INTERACTIONS:
    input: mapfile="work/networks/network_metadata.txt",
        networks=expand("work/networks/{proctype}/{collection}/{fn}.txt.nn", \
            zip, proctype=NW_PROCESSED_FILES.proctype, collection=NW_PROCESSED_FILES.collection, fn=NW_PROCESSED_FILES.fn),
            cfg="data/organism.cfg"
    output: "work/flags/generic_db.interaction_data.flag"
    params: newdir='result/generic_db/INTERACTIONS'
    #shell: """ORGANISM_ID=$(python builder/getparam.py {input.cfg} gm_organism_id --default 1)
    #    python builder/rename_data_files.py interactions {input.mapfile} {params.newdir} $ORGANISM_ID {input.networks} \
    #    --key_lstrip='work/' --key_rstrip='.txt.nn' && touch {output}
    #    """
    run:
        quoted_input_networks = ' '.join('"%s"' % o for o in input.networks)
        shell("""ORGANISM_ID=$(python builder/getparam.py {input.cfg} gm_organism_id --default 1) && \
              python builder/rename_data_files.py interactions {input.mapfile} {params.newdir} ${{ORGANISM_ID}} \
              {quoted_input_networks} --key_lstrip='work/' --key_rstrip='.txt.nn' && touch {output}
              """)

