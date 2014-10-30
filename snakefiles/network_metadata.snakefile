

# this would be simpler if i had all the networks under a subdirectory, like say 'networks', e.g.
# networks/direct/{collection}/, networks/sharedneibhgour/{collection}/, etc
# then i could make one rule

NW_CFGS = glob_wildcards("data/networks/{proctype}/{collection}/{fn}.cfg")

# TODO: this scan is already done elsewhere, reuse? also the naming is wrong
NW_PROCESSED_FILES = glob_wildcards("data/networks/{proctype}/{collection}/{fn}.txt")

# need a better name for the target rule for the network metadata
rule TABULATE_NETWORK_METADATA:
    input: "work/networks/network_metadata.txt"

rule APPLY_NETWORK_METADATA_TABULATION:
    input: expand("data/networks/{proctype}/{collection}/{fn}.cfg", zip, proctype=NW_CFGS.proctype, collection=NW_CFGS.collection, fn=NW_CFGS.fn)
    output: "work/networks/network_metadata.txt"
    shell: "python builder/tabulate_cfgs.py {input} {output} --key_ext='.cfg'"

rule NETWORK_STATS_FILES:
    input: expand("work/networks/{proctype}/{collection}/{fn}.txt.nn.stats", zip, proctype=NW_PROCESSED_FILES.proctype, collection=NW_PROCESSED_FILES.collection, fn=NW_PROCESSED_FILES.fn)

rule COMPUTE_NETWORK_STATS:
    input: "work/networks/{proctype}/{collection}/{fn}.txt.nn"
    output: "work/networks/{proctype}/{collection}/{fn}.txt.nn.stats"
    shell: "python builder/network_stats.py {input} {output}"

rule TABULATED_NETWORK_STATS:
    input: "work/networks/stats.txt"

rule TABULATE_NETWORK_STATS:
    input: expand("work/networks/{proctype}/{collection}/{fn}.txt.nn.stats", zip, proctype=NW_PROCESSED_FILES.proctype, collection=NW_PROCESSED_FILES.collection, fn=NW_PROCESSED_FILES.fn)
    output: "work/networks/stats.txt"
    shell: "python builder/tabulate_cfgs.py {input} {output} --enumerate=false --key_ext='.txt.nn.stats'"


rule PROCESS_NETWORK_METADATA:
    input: "work/networks/network_metadata.txt"
    output: "work/networks/network_metadata.txt.processed"
    shell: "python builder/process_network_metadata.py {input} {output}"


rule EXTRACT_NETWORKS:
    input: metadata="work/networks/network_metadata.txt.processed", groups="result/generic_db/NETWORK_GROUPS.txt"
    output: "result/generic_db/NETWORKS.txt"
    shell: "python builder/extract_networks.py networks {input.metadata} {input.groups} {output}"


rule EXTRACT_NETWORK_GROUPS:
    input: "work/networks/network_metadata.txt.processed"
    output: "result/generic_db/NETWORK_GROUPS.txt"
    shell: "python builder/extract_networks.py network_groups {input} {output}"


rule EXTRACT_NETWORK_METADATA:
    input: "work/networks/network_metadata.txt.processed"
    output: "result/generic_db/NETWORK_METADATA.txt"
    shell: "python builder/extract_networks.py network_metadata {input} {output}"
