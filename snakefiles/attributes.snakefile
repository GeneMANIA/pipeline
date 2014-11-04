
'''

major targets:

  ATTRIBUTES: processes attribute metadata into generic_db (well not yet)

  CLEAN_ATTRIBUTES: removes all files under work/attributes/

process attribute data located in the subdirectories

  data/attributes/{collection}/

where collection is any name, for convenience of organizing collections of files,
e.g. by source (ensembl, drug interactions db, etc).

each set of attributes under collection has 3 files associated,
 '.txt', descriptions '.desc', and metadata '.cfg'

 ../{collection}/filename.txt     : attribute data, gene<tab>attribute<tab>attribute...
 ../{collection}/filename.desc    : descriptions for attributes, id<tab>description e.g IPR0001<tab>amazing important domain
 ../{collection}/filename.cfg     : metadata: name, description, links to source data etc. in 'var = value' format


processing requires a clean identifiers file as input in

  work/identifiers/symbols.txt

and proceeds to the following intermediate files:

  work/attributes/{collection}/
  work/attributes/metadata.txt:    metadata in the various *.cfg files, converted to columns, tab delim with header
  work/attributes/enumeration.txt:

'''

FNS = my_glob_wildcards("data/attributes/{collection}/{fn}.txt")
CFGS = my_glob_wildcards("data/attributes/{collection}/{fn}.cfg")
DESCS = my_glob_wildcards("data/attributes/{collection}/{fn}.desc")

# make sure the file sets are consistent in size, so no
# .cfg and .desc files are missing
# assert len(FNS[0]) == len(CFGS[0]) == len(DESCS[0]), "Files are missing, try check.py --fix"

rule ATTRIBUTES:
    message: "target rule for clean attribute files"
    input: expand("work/attributes/{collection}/{fn}.txt.mapped", zip, collection=FNS.collection, fn=FNS.fn)

rule MELT_ATTRIBUTES:
    message: "convert ragged input files into tall thin tables"
    input: "data/attributes/{collection}/{fn}.txt"
    output: "work/attributes/{collection}/{fn}.melted"
    shell: "python builder/ragged_melter.py {input} {output}"

rule PROCESS_ATTRIBUTES:
    message: "remove unknown gene symbols"
    input: data="work/attributes/{collection}/{fn}.melted", mapping="work/identifiers/symbols.txt"
    output: "work/attributes/{collection}/{fn}.scrubbed"
    shell: "python builder/scrubber.py {input.data} {input.mapping} {output}"

rule DEDUP_ATTRIBUTES:
    message: "remove duplicate attributes"
    input: data="work/attributes/{collection}/{fn}.scrubbed", mapping="work/identifiers/symbols.txt"
    output: "work/attributes/{collection}/{fn}.clean"
    shell: "python builder/dedup.py {input.data} {input.mapping} {output}"

rule MAP_ATTRIBUTES_TO_IDS:
    message: "convert gene and attribute symbols to internal genemania ids"
    input: data="data/attributes/{collection}/{fn}.txt", mapping="work/identifiers/symbols.txt", desc="work/attributes/{collection}/{fn}.desc.cleaned"
    output: "work/attributes/{collection}/{fn}.txt.mapped"
    shell: "python builder/map_attributes_to_ids.py {input.data} {input.desc} {input.mapping} {output}"

rule ATTRIBUTE_METADATA:
    input: expand("work/attributes/{collection}/{fn}.cfg.cp", zip, collection=CFGS.collection, fn=CFGS.fn)

rule UPDATE_ATTRIBUTE_METADATA:
    input: "data/attributes/{collection}/{fn}.cfg"
    output: "work/attributes/{collection}/{fn}.cfg.cp"
    shell: "cp {input} {output}"

# need a better name for the target rule for the network metadata
rule TABULATE_ATTRIBUTE_METADATA:
    input: "work/attributes/metadata.txt"

rule APPLY_TABULATION:
    input: expand("work/attributes/{collection}/{fn}.cfg.cp", zip, collection=CFGS.collection, fn=CFGS.fn)
    output: "work/attributes/metadata.txt"
    shell: "python builder/tabulate_cfgs.py {input} {output} --key_lstrip='work/' --key_rstrip='.cfg.cp'"

# enumerate, needs name. not needed anymore if tabulation good enough?
rule ENUMERATE_ATTRIBUTE_METADATA:
    input: "work/attributes/enumeration.txt"

rule APPLY_ENUMERATION:
    input: expand("work/attributes/{collection}/{fn}.txt.mapped", zip, collection=FNS.collection, fn=FNS.fn)
    output: "work/attributes/enumeration.txt"
    shell: "python builder/enum_files.py {input} {output}"

rule ATTRIBUTE_DESCRIPTIONS:
    input: expand("work/attributes/{collection}/{fn}.desc.cleaned", zip, collection=DESCS.collection, fn=DESCS.fn)

rule UPDATE_ATTRIBUTE_DESCRIPTIONS:
    message: """attributeid, description pairs, for all attribute ids in
    input after cleaning, with empty descriptions added in if necessary
    """
    input: desc="data/attributes/{collection}/{fn}.desc", data="data/attributes/{collection}/{fn}.txt"
    output: "work/attributes/{collection}/{fn}.desc.cleaned"
    shell: "python builder/update_attribute_descriptions.py {input.data} {input.desc} {output}"

# generic db files for attributes
rule GENERIC_DB_ATTRIBUTE_GROUPS:
    input: "work/attributes/metadata.txt"
    output: "result/generic_db/ATTRIBUTE_GROUPS.txt"
    shell: "python builder/extract_attributes.py attribute_groups {output} {input}"

# generic db files for attributes
rule GENERIC_DB_ATTRIBUTES:
    input: desc=expand("work/attributes/{collection}/{fn}.desc.cleaned", zip, collection=DESCS.collection, fn=DESCS.fn),
        metadata='work/attributes/metadata.txt'
    output: "result/generic_db/ATTRIBUTES.txt"
    shell: "python builder/extract_attributes.py attributes {output} {input.metadata} {input.desc} --key_lstrip='work/' --key_rstrip='.desc.cleaned'"

# generic db attribute data files for engine cache
#
# dynamic() mechanism giving problems ... use flag file

#rule GENERIC_DB_ATTRIBUTE_DATA:
#    input: dynamic("result/generic_db/ATTRIBUTES/{attr_id}.txt")
#
#rule GENERIC_DB_COPY_ATTRIBUTE_DATA:
#    input: mapfile="work/attributes/metadata.txt",  attribs=expand("work/attributes/{collection}/{fn}.txt.mapped", zip, collection=FNS.collection, fn=FNS.fn)
#    output: dynamic("result/generic_db/ATTRIBUTES/{attr_id}.txt")
#    params: newdir='result/generic_db/ATTRIBUTES'
#    shell: "python builder/rename_data_files.py attribs {input.mapfile} {params.newdir} {input.attribs} --key_lstrip='work/' --key_rstrip='.txt.mapped'"

rule GENERIC_DB_ATTRIBUTE_DATA:
    input: "work/flags/generic_db.attribute_data.flag"

rule GENERIC_DB_COPY_ATTRIBUTE_DATA:
    input: mapfile="work/attributes/metadata.txt",  attribs=expand("work/attributes/{collection}/{fn}.txt.mapped", zip, collection=FNS.collection, fn=FNS.fn)
    output: "work/flags/generic_db.attribute_data.flag"
    params: newdir='result/generic_db/ATTRIBUTES'
    shell: "python builder/rename_data_files.py attribs {input.mapfile} {params.newdir} {input.attribs} --key_lstrip='work/' --key_rstrip='.txt.mapped' && touch {output}"

rule CLEAN_ATTRIBUTES:
    shell: """
        rm -rf work/attributes
    """
