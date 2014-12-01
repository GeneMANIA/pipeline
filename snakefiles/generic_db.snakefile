

rule GENERIC_DB:
  input: GENERIC_DB_FILES

# no longer support this scheme of tagging networks,
# may revise in future. just create empty files for now
# leaving support in rest of the system

rule GENERIC_DB_TAGS:
    message: "create empty generic db file TAGS.txt, network tags no longer supported"
    output: RESULT+"/generic_db/TAGS.txt"
    shell: "touch {output}"

rule GENERIC_DB_NETWORK_TAG_ASSOC:
    message: "create empty generic_db file NETWORK_TAG_ASSOC"
    output: RESULT+"/generic_db/NETWORK_TAG_ASSOC.txt"
    shell: "touch {output}"

rule GENERIC_DB_SCHEMA:
    message: "create generic db file SCHEMA.txt, describing file layouts"
    input: "config/SCHEMA.txt"
    output: RESULT+"/generic_db/SCHEMA.txt"
    shell: "cp {input} {output}"

# TODO: add up interaction counts to construct statistics
rule GENERIC_DB_STATISTICS:
    message: "create generic db file STATISTICS.txt containing interaction total count, dataset production date"
    output: RESULT+"/generic_db/STATISTICS.txt"
    shell: "touch {output}"

rule CLEAN_GENERIC_DB:
    shell: """rm -rf {RESULT}/generic_db
        rm -f {WORK}/flags/generic_db.*.flag
        """

rule GENERIC_DB_ORGANISMS:
    message: "create generic db file ORGANISMS.txt containing list with descriptive names and ids"
    input: DATA+"/organism.cfg"
    output: RESULT+"/generic_db/ORGANISMS.txt"
    shell: "python builder/extract_organisms.py {input} {output}"

GO_BRANCHES = ['BP', 'MF', 'CC']

# might want to move this rule to a networks.snakefile, that combines direct, shared-neighbour, and profile rules
# as well as populating this part of generic_db

# dup'd again
NW_PROCESSED_FILES = glob_wildcards(DATA+"/networks/{proctype}/{collection}/{fn}.txt")

# dynamic() needs more investigation, use flag file
rule GENERIC_DB_INTERACTIONS:
    message: "target rule for interaction data in generic_db format"
    input: WORK+"/flags/generic_db.interaction_data.flag"

# TODO: should we be using the processed metadata file here to guide the copying?
rule GENERIC_DB_COPY_INTERACTIONS:
    message: "copy network interaction files to generic_db"
    input: mapfile=WORK+"/networks/network_metadata.txt",
        networks=expand(WORK+"/networks/{proctype}/{collection}/{fn}.txt.nn", \
            zip, proctype=NW_PROCESSED_FILES.proctype, collection=NW_PROCESSED_FILES.collection, fn=NW_PROCESSED_FILES.fn),
            cfg=DATA+"/organism.cfg"
    output: WORK+"/flags/generic_db.interaction_data.flag"
    params: newdir=RESULT+"/generic_db/INTERACTIONS"
    #shell: """ORGANISM_ID=$(python builder/getparam.py {input.cfg} gm_organism_id --default 1)
    #    python builder/rename_data_files.py interactions {input.mapfile} {params.newdir} $ORGANISM_ID {input.networks} \
    #    --key_lstrip='work/' --key_rstrip='.txt.nn' && touch {output}
    #    """
    run:
        quoted_input_networks = ' '.join('"%s"' % o for o in input.networks)
        shell("""ORGANISM_ID=$(python builder/getparam.py {input.cfg} gm_organism_id --default 1) && \
              python builder/rename_data_files.py interactions {input.mapfile} {params.newdir} ${{ORGANISM_ID}} \
              {quoted_input_networks} --key_lstrip='{WORK}/' --key_rstrip='.txt.nn' && touch {output}
              """)

