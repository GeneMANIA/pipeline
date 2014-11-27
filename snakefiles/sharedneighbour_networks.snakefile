

FNS = glob_wildcards("data/networks/sharedneighbour/{collection}/{fn}.txt")

rule SHAREDNEIGHBOUR_NETWORKS:
    message: "target rule for interaction networks created from shared neighbour profile data"
    input: expand("work/networks/sharedneighbour/{collection}/{fn}.txt.nn", zip, collection=FNS.collection, fn=FNS.fn)

rule PROCESS_SHAREDNEIGHBOUR_NETWORKS_P2N:
    message: "convert shared neighbour profiles to networks"
    input: data="data/networks/sharedneighbour/{collection}/{fn}.txt", mapping="work/identifiers/symbols.txt"
    output: "work/networks/sharedneighbour/{collection}/{fn}.txt.p2n"
    log: "work/networks/sharedneighbour/{collection}/{fn}.txt.p2n.log"
    shell: 'java -Xmx512m -cp {JAR_FILE} org.genemania.engine.core.evaluation.ProfileToNetworkDriver -in "{input.data}" -out "{output}" -log "{log}" -syn "{input.mapping}" -proftype binary -cor pearson_bin_log_no_norm -threshold auto -keepAllTies -limitTies'

rule PROCESS_SHAREDNEIGHBOUR_NETWORKS_NN:
    message: "network normalization"
    input: data="work/networks/sharedneighbour/{collection}/{fn}.txt.p2n", mapping="work/identifiers/symbols.txt"
    output: "work/networks/sharedneighbour/{collection}/{fn}.txt.nn"
    log: "work/networks/sharedneighbour/{collection}/{fn}.txt.nn.log"
    shell: 'java -Xmx512m -cp {JAR_FILE}  org.genemania.engine.apps.NetworkNormalizer -outtype uid -norm true  -in "{input.data}" -out "{output}" -log "{log}" -syn "{input.mapping}"'

rule CLEAN_SHAREDNEIGHBOUR_NETWORKS:
    shell: """
    rm -rf work/networks/sharedneighbour/*
    """

