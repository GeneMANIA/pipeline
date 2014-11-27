

DIRECT_NETWORKS_FNS = glob_wildcards("data/networks/direct/{collection}/{fn}.txt")

rule DIRECT_NETWORKS:
    message: "target rule for direct networks"
    input: expand("work/networks/direct/{collection}/{fn}.txt.nn", zip, collection=DIRECT_NETWORKS_FNS.collection, fn=DIRECT_NETWORKS_FNS.fn)

rule CLEANED_DIRECT_NETWORKS:
    message: "clean direct networks, adding an implicit '1' weight if missing, removing <=0 weights"
    input: "data/networks/direct/{collection}/{fn}.txt"
    output: "work/networks/direct/{collection}/{fn}.txt.cleaned"
    log: "work/networks/direct/{collection}/{fn}.txt.cleaned.log"
    shell: 'python builder/fix_weights.py "{input}" "{output}"'

rule PROCESS_DIRECT_NETWORKS:
    message: "network normalization"
    input: data="work/networks/direct/{collection}/{fn}.txt.cleaned", mapping="work/identifiers/symbols.txt"
    output: "work/networks/direct/{collection}/{fn}.txt.nn"
    log: "work/networks/direct/{collection}/{fn}.txt.nn.log"
    shell: 'java -Xmx512m -cp {JAR_FILE}  org.genemania.engine.apps.NetworkNormalizer -outtype uid -norm true  -in "{input.data}" -out "{output}" -log "{log}" -syn "{input.mapping}"'

rule CLEAN_DIRECT_NETWORKS:
    shell: """
    rm -rf work/networks/direct/*
    """

