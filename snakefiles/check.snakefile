
FNS = glob_wildcards("data/attributes/{collection}/{fn}.txt")
CFGS = glob_wildcards("data/attributes/{collection}/{fn}.cfg")
DESCS = glob_wildcards("data/attributes/{collection}/{fn}.desc")

rule FIX_MISSING_ATTRIBUTE_METADATA_CFG:
    input: expand('data/attributes/{collection}/{fn}.cfg', zip, collection=FNS.collection, fn=FNS.fn)

rule APPLY_FIX:
    output: 'data/attributes/{collection}/{fn}.cfg'
    shell: 'touch {output}'

NW_DATA = glob_wildcards("data/networks/{proctype}/{collection}/{fn}.txt")

rule FIX_MISSING_NETWORK_METADATA:
    input: expand('data/networks/{proctype}/{collection}/{fn}.cfg', zip, proctype=NW_DATA.proctype, collection=NW_DATA.collection, fn=NW_DATA.fn)

rule APPLY_MISSING_NW_METADATA_FIX:
    output: 'data/networks/{proctype}/{collection}/{fn}.cfg'
    shell: 'touch {output}'
