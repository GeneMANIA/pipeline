

# this would be simpler if i had all the networks under a subdirectory, like say 'networks', e.g.
# networks/direct/{collection}/, networks/sharedneibhgour/{collection}/, etc
# then i could make one rule

NW_CFGS = glob_wildcards(DATA+"/networks/{proctype}/{collection}/{fn}.cfg")

# TODO: this scan is already done elsewhere, reuse? also the naming is wrong
NW_PROCESSED_FILES = glob_wildcards(DATA+"/networks/{proctype}/{collection}/{fn}.txt")

# need a better name for the target rule for the network metadata
rule TABULATE_NETWORK_METADATA:
    message: "target rule for network metadata table"
    input: WORK+"/networks/network_metadata.txt"

rule APPLY_NETWORK_METADATA_TABULATION:
    message: "combine metadata from individual network config files into a single table"
    input: expand(DATA+"/networks/{proctype}/{collection}/{fn}.cfg", zip, proctype=NW_CFGS.proctype, collection=NW_CFGS.collection, fn=NW_CFGS.fn)
    output: WORK+"/networks/network_metadata.txt"
    #shell: "python builder/tabulate_cfgs.py {input} {output} --key_lstrip='data/' --key_rstrip='.cfg'"
    run:
        quoted_input = ' '.join('"%s"' % o for o in input)
        shell("python builder/tabulate_cfgs.py {quoted_input} {output} --key_lstrip='{DATA}/' --key_rstrip='.cfg'")

rule CREATE_BATCH_EDIT_FILE:
    message: "create emtpy metadata edit file, if one doesn't already exist"
    output: DATA+"/metadata_fixes.txt"
    shell: "touch {output}"

rule APPLY_METADATA_EDITS:
    message: "apply ad-hoc fixes to network metadata"
    input: edit_file=DATA+"/metadata_fixes.txt", metadata=WORK+"/networks/network_metadata.txt"
    output: WORK+"/networks/network_metadata.txt.fixed"
    shell: "python builder/batch_edit_metadata.py {input.edit_file} {input.metadata} {output} --key_rstrip='.cfg'"

rule SET_MISSING_NETWORK_METADATA:
    message: "set default values where no metadata was provided"
    input: WORK+"/networks/network_metadata.txt.fixed"
    output: WORK+"/networks/network_metadata.txt.defaulted"
    shell: "python builder/set_missing_network_metadata.py {input} {output}"

rule NETWORK_STATS_FILES:
    message: "target rule for computing interaction stats for all individual networks"
    input: expand(WORK+"/networks/{proctype}/{collection}/{fn}.txt.nn.stats", zip, proctype=NW_PROCESSED_FILES.proctype, collection=NW_PROCESSED_FILES.collection, fn=NW_PROCESSED_FILES.fn)

rule COMPUTE_NETWORK_STATS:
    message: "compute interaction stats for individual networks"
    input: WORK+"/networks/{proctype}/{collection}/{fn}.txt.nn"
    output: WORK+"/networks/{proctype}/{collection}/{fn}.txt.nn.stats"
    shell: 'python builder/network_stats.py "{input}" "{output}"'

rule TABULATED_NETWORK_STATS:
    message: "target rule for table combining all individual network stats files"
    input: WORK+"/networks/stats.txt"

rule TABULATE_NETWORK_STATS:
    message: "combine individual network stats files into a single table"
    input: expand(WORK+"/networks/{proctype}/{collection}/{fn}.txt.nn.stats", zip, proctype=NW_PROCESSED_FILES.proctype, collection=NW_PROCESSED_FILES.collection, fn=NW_PROCESSED_FILES.fn)
    output: WORK+"/networks/stats.txt"
    #shell: "python builder/tabulate_cfgs.py {input} {output} --enumerate=false --key_lstrip 'work/' --key_rstrip='.txt.nn.stats'"
    run:
        quoted_input = ' '.join('"%s"' % o for o in input)
        shell("python builder/tabulate_cfgs.py {quoted_input} {output} --enumerate=false --key_lstrip '{WORK}/' --key_rstrip='.txt.nn.stats'")


rule INIT_PUBMED_CACHE:
    message: "empty pubmed data cache file if first time through"
    output: WORK+"/cache/pubmed.txt"
    shell: "cp config/EMPTY_PUBMED_METADATA.txt {output}"


rule FETCH_PUBMED_METADATA:
    message: "retrieve publication metadata from pubmed, where available. create new metadata file adding pubmed info"
    input: metadata=WORK+"/networks/network_metadata.txt.defaulted", pubmed_cache=WORK+"/cache/pubmed.txt"
    output: WORK+"/networks/network_metadata.txt.pubmed_extended"
    params: pubmed_cache=WORK+"/cache/pubmed.txt", fetchsize="200"
    shell: "python builder/fetch_pubmed_metadata.py {input.metadata} {output} {input.pubmed_cache} --fetchsize={params.fetchsize}"


rule GENERATE_NETWORK_NAMES:
    message: "compute network names from publication metadata, if not given explicitly"
    input: WORK+"/networks/network_metadata.txt.pubmed_extended"
    output: WORK+"/networks/network_metadata.txt.named"
    shell: "python builder/generate_network_names.py {input} {output}"

rule JOIN_NICE_NETWORK_GROUP_NAMES:
    message: "add group names like Co-expression for codes like coexp"
    input: metadata=WORK+"/networks/network_metadata.txt.named", group_names="config/NETWORK_GROUP_NAMES.txt"
    output: WORK+"/networks/network_metadata.txt.nicegroups"
    shell: "python builder/nicen_network_group_names.py {input.metadata} {input.group_names} {output}"

rule GENERATE_NETWORK_PROCESSING_TYPES:
    message: "add processing type description strings"
    input: metadata=WORK+"/networks/network_metadata.txt.nicegroups", proc_types="config/NETWORK_PROCESSING_TYPES.txt"
    output: WORK+"/networks/network_metadata.txt.proctypes"
    shell: "python builder/generate_network_processing_types.py {input.metadata} {input.proc_types} {output}"

rule JOIN_NETWORK_INTERACTION_COUNTS:
    message: "incorporate network interaction counts to network metadata"
    input: network_metadata=WORK+"/networks/network_metadata.txt.proctypes", stats=WORK+"/networks/stats.txt"
    output: WORK+"/networks/network_metadata.txt.processed"
    shell: "python builder/table_joiner.py {input.network_metadata} {input.stats} {output} 'dataset_key'"

rule EXTRACT_NETWORKS:
    message: "create generic db file NETWORKS.txt, listing all networks"
    input: metadata=WORK+"/networks/network_metadata.txt.processed", groups=RESULT+"/generic_db/NETWORK_GROUPS.txt"
    output: RESULT+"/generic_db/NETWORKS.txt"
    shell: "python builder/extract_networks.py networks {input.metadata} {input.groups} {output}"


rule EXTRACT_NETWORK_GROUPS:
    message: "create generic db file NETWORK_GROUPS.txt"
    input: metadata=WORK+"/networks/network_metadata.txt.processed", cfg=DATA+"/organism.cfg"
    output: RESULT+"/generic_db/NETWORK_GROUPS.txt"
    shell: """ORGANISM_ID=$(python builder/getparam.py {input.cfg} gm_organism_id --default 1)
        python builder/extract_networks.py network_groups $ORGANISM_ID {input.metadata} {output}
        """


rule EXTRACT_NETWORK_METADATA:
    message: "create generic db file NETWORK_METADATA.txt, containing references and comments"
    input: WORK+"/networks/network_metadata.txt.processed"
    output: RESULT+"/generic_db/NETWORK_METADATA.txt"
    shell: "python builder/extract_networks.py network_metadata {input} {output}"


