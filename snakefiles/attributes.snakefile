
'''
major targets:

  ATTRIBUTES: processes attribute metadata into generic_db (well not yet)

  CLEAN_ATTRIBUTES: removes all files under work/attributes/

process attribute data located in the subdirectories

  data/attributes/{proctype}/{collection}/

proctype specifies the type of data layout, either gene-attribute-list,
gene-attribute-list, or attribute-desc-gene-list (.gmt style). attribute-gene
pairs and gene-attribute pairs are just special cases of the other formats
and are supported.

collection is any name, for convenience of organizing collections of files,
e.g. by source (ensembl, drug interactions db, etc).

each set of attributes under gene-attribute collections has 3 files associated,
 'txt', descriptions '.desc', and metadata '.cfg'

 ../{collection}/filename.txt     : attribute data, gene<tab>attribute<tab>attribute...
 ../{collection}/filename.desc    : descriptions for attributes, id<tab>description e.g IPR0001<tab>amazing important domain
 ../{collection}/filename.cfg     : metadata: name, description, links to source data etc. in 'var = value' format


processing requires a clean identifiers file as input in

  work/identifiers/symbols.txt

and proceeds to the following intermediate files:

  work/attributes/{proctype}/{collection}/
  work/attributes/metadata.txt:    metadata in the various *.cfg files, converted to columns, tab delim with header
  work/attributes/enumeration.txt:

'''

## processing based on input data files:

# naming mnemonic, G=genes, A=attributes, D=descriptions, L=list
GAL_FNS = glob_wildcards(DATA+"/attributes/gene-attrib-list/{collection}/{fn}.txt")
AGL_FNS = glob_wildcards(DATA+"/attributes/attrib-gene-list/{collection}/{fn}.txt")
ADGL_FNS = glob_wildcards(DATA+"/attributes/attrib-desc-gene-list/{collection}/{fn}.txt")

## construct lists of file targets composed from the names in the file scans above

# all data files in their normalized forms
ALL_FNS = expand(WORK+"/attributes/gene-attrib-list/{collection}/{fn}.txt.mapped", \
    zip, collection=GAL_FNS.collection, fn=GAL_FNS.fn) + \
    expand(WORK+"/attributes/attrib-gene-list/{collection}/{fn}.txt.mapped", \
    zip, collection=AGL_FNS.collection, fn=AGL_FNS.fn) + \
    expand(WORK+"/attributes/attrib-desc-gene-list/{collection}/{fn}.txt.mapped", \
    zip, collection=ADGL_FNS.collection, fn=ADGL_FNS.fn)

# all metadata files
ALL_CFGS = expand(DATA+"/attributes/gene-attrib-list/{collection}/{fn}.cfg", \
    zip, collection=GAL_FNS.collection, fn=GAL_FNS.fn) + \
    expand(DATA+"/attributes/attrib-gene-list/{collection}/{fn}.cfg", \
    zip, collection=AGL_FNS.collection, fn=AGL_FNS.fn) + \
    expand(DATA+"/attributes/attrib-desc-gene-list/{collection}/{fn}.cfg", \
    zip, collection=ADGL_FNS.collection, fn=ADGL_FNS.fn)

# all cleaned attribute description files
ALL_DESCS = expand(WORK+"/attributes/gene-attrib-list/{collection}/{fn}.desc.cleaned", \
    zip, collection=GAL_FNS.collection, fn=GAL_FNS.fn) + \
    expand(WORK+"/attributes/attrib-gene-list/{collection}/{fn}.desc.cleaned", \
    zip, collection=AGL_FNS.collection, fn=AGL_FNS.fn) + \
    expand(WORK+"/attributes/attrib-desc-gene-list/{collection}/{fn}.desc.cleaned", \
    zip, collection=ADGL_FNS.collection, fn=ADGL_FNS.fn)

#print(ALL_FNS)

# make sure the file sets are consistent in size, so no
# .cfg and .desc files are missing
# assert len(FNS[0]) == len(CFGS[0]) == len(DESCS[0]), "Files are missing, try check.py --fix"

rule ATTRIBUTES:
    message: "target rule for clean attribute files"
    input: ALL_FNS

# reformat various input data files into standardized files
# for common processing

rule MELT_ATTRIBUTES:
    message: "convert ragged gene-attrib input files into tall thin tables"
    input: DATA+"/attributes/gene-attrib-list/{collection}/{fn}.txt"
    output: WORK+"/attributes/gene-attrib-list/{collection}/{fn}.txt.melted"
    shell: "python builder/ragged_melter.py {input} {output}"

rule MELT_ATTRIBUTES2:
    message: "convert ragged attrib-gene input files into tall thin tables"
    input: DATA+"/attributes/attrib-gene-list/{collection}/{fn}.txt"
    output: WORK+"/attributes/attrib-gene-list/{collection}/{fn}.txt.melted-transposed"
    shell: "python builder/ragged_melter.py {input} {output}"

rule TRANSPOSE_ATTRIBS:
    message: "convert attrib-gene to gene-attrib pairs"
    input: WORK+"/attributes/attrib-gene-list/{collection}/{fn}.txt.melted-transposed"
    output: WORK+"/attributes/attrib-gene-list/{collection}/{fn}.txt.melted"
    shell: """awk -F'\t' '{{ print $2 "\t" $1}}'  {input} > {output}"""


rule MELT_ATTRIBUTES3:
    message: "convert gmt-format ragged input files into tall thin tables"
    input: DATA+"/attributes/attrib-desc-gene-list/{collection}/{fn}.txt"
    output: WORK+"/attributes/attrib-desc-gene-list/{collection}/{fn}.txt.melted"
    #shell: "python builder/ragged_melter.py {input} {output}"
    shell: "false" # because not implemented, issue #12

#
# common processing for all attributes
#


# process attribute data
# {fn}.txt -> {fn}.txt.scrubbed (remove unknown genes")
#          -> {fn}.txt.clean (remove duplicate attribs)
#          -> {fn}.txt.mapped (symbols to ids)
#
rule PROCESS_ATTRIBUTES:
    message: "remove unknown gene symbols"
    input: data=WORK+"/attributes/{proctype}/{collection}/{fn}.txt.melted", mapping=WORK+"/identifiers/symbols.txt"
    output: WORK+"/attributes/{proctype}/{collection}/{fn}.txt.scrubbed"
    shell: "python builder/scrubber.py {input.data} {input.mapping} {output}"

rule DEDUP_ATTRIBUTES:
    message: "remove duplicate attributes"
    input: data=WORK+"/attributes/{proctype}/{collection}/{fn}.txt.scrubbed", mapping=WORK+"/identifiers/symbols.txt"
    output: WORK+"/attributes/{proctype}/{collection}/{fn}.txt.clean"
    shell: "python builder/dedup.py {input.data} {input.mapping} {output}"

rule MAP_ATTRIBUTES_TO_IDS:
    message: "convert gene and attribute symbols to internal genemania ids"
    input: data=WORK+"/attributes/{proctype}/{collection}/{fn}.txt.clean", mapping=WORK+"/identifiers/symbols.txt",
        lin_attr_id=WORK+"/attributes/linearized_attributes.txt"
    output: WORK+"/attributes/{proctype}/{collection}/{fn}.txt.mapped"
    shell: "python builder/map_attributes_to_ids.py {input.data} {input.lin_attr_id} {input.mapping} {output}"

# need a better name for the target rule for the network metadata
rule TABULATED_ATTRIBUTE_METADATA:
    input: WORK+"/attributes/metadata.txt"

rule TABULATE_ATTRIBUTE_METADATA:
    input: ALL_CFGS
    output: WORK+"/attributes/metadata.txt"
    #shell: "python builder/tabulate_cfgs.py {input} {output} --key_lstrip='{DATA}/' --key_rstrip='.cfg'"
    run:
        quoted_input = ' '.join('"%s"' % o for o in input)
        shell("python builder/tabulate_cfgs.py {quoted_input} {output} --key_lstrip='{DATA}/' --key_rstrip='.cfg'")

# enumerate, needs name. not needed anymore if tabulation good enough?
rule ENUMERATE_ATTRIBUTE_METADATA:
    input: WORK+"/attributes/enumeration.txt"

rule APPLY_ATTRIBUTE_ENUMERATION:
    message: "assign internal genemania id's to each attribute group"
    input: ALL_FNS
    output: WORK+"/attributes/enumeration.txt"
    shell: "python builder/enum_files.py {input} {output}"

rule ATTRIBUTE_DESCRIPTIONS:
    input: ALL_DESCS

rule UPDATE_ATTRIBUTE_DESCRIPTIONS:
    message: """create attributeid, description pairs, for all attribute ids in
    input after cleaning, with empty descriptions added in if necessary
    """
    input: desc=DATA+"/attributes/{proctype}/{collection}/{fn}.desc", \
        data=WORK+"/attributes/{proctype}/{collection}/{fn}.txt.scrubbed"
    output: WORK+"/attributes/{proctype}/{collection}/{fn}.desc.cleaned"
    shell: "python builder/update_attribute_descriptions.py {input.data} {input.desc} {output}"

rule LINEARIZE_ATTRIBUTE_IDS:
    message: """make sure attribute ids are unique for all attribute groups
    belonging to t he organism
    """
    input: desc=ALL_DESCS, metadata=WORK+'/attributes/metadata.txt', cfg=DATA+"/organism.cfg"
    output: WORK+"/attributes/linearized_attributes.txt"
    shell: """ORGANISM_ID=$(python builder/getparam.py {input.cfg} gm_organism_id --default 1)
    python builder/linearize_attribute_ids.py $ORGANISM_ID {output} {input.metadata} {input.desc} \
        --key_lstrip='{WORK}/' --key_rstrip='.desc.cleaned'
    """

# generic db files for attributes
rule GENERIC_DB_ATTRIBUTE_GROUPS:
    message: "create generic_db ATTRIBUTE_GROUPS.txt file"
    input: metadata=WORK+"/attributes/metadata.txt", cfg=DATA+"/organism.cfg"
    output: RESULT+"/generic_db/ATTRIBUTE_GROUPS.txt"
    shell: """ORGANISM_ID=$(python builder/getparam.py {input.cfg} gm_organism_id --default 1)
        python builder/extract_attributes.py attribute_groups $ORGANISM_ID {output} {input.metadata}
        """

# generic db files for attributes
rule GENERIC_DB_ATTRIBUTES:
    message: "create generic_db ATTRIBUTES.txt file with name of each attribute in each group"
    input: "work/attributes/linearized_attributes.txt"
    output: RESULT+"/generic_db/ATTRIBUTES.txt"

    shell: "python builder/extract_attributes.py attributes {input} {output}"

# generic db attribute data files for engine cache
#
# dynamic() mechanism giving problems ... use flag file

rule GENERIC_DB_ATTRIBUTE_DATA:
    input: WORK+"/flags/generic_db.attribute_data.flag"

rule GENERIC_DB_COPY_ATTRIBUTE_DATA:
    message: "copy attribute data files to generic_db"
    input: mapfile=WORK+"/attributes/metadata.txt",  attribs=ALL_FNS
    output: WORK+"/flags/generic_db.attribute_data.flag"
    params: newdir=RESULT+'/generic_db/ATTRIBUTES'
    #shell: "python builder/rename_data_files.py attribs {input.mapfile} {params.newdir} {input.attribs} \
    #    --key_lstrip='{WORK}/' --key_rstrip='.txt.mapped' && touch {output}"
    run:
        quoted_input_attribs = ' '.join('"%s"' % o for o in input.attribs)
        shell("python builder/rename_data_files.py attribs {input.mapfile} {params.newdir} {quoted_input_attribs} " \
         "--key_lstrip='{WORK}/' --key_rstrip='.txt.mapped' && touch {output}")

rule CLEAN_ATTRIBUTES:
    shell: """
        rm -rf {WORK}/attributes
    """
