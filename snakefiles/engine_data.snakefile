

# organized by flag files for now,
# need to reorganize low-level control of data files


# TODO: move to common location
LUCENE_INDEX_DIR = "result/lucene_index"
GENERIC_DB_DIR = "result/generic_db"

NETWORK_CACHE_DIR = "result/network_cache"
LOG_DIR = "work/logs"
NETWORK_DIR = "result/generic_db/INTERACTIONS"
GO_CATEGORIES_DIR = "result/generic_db/GO_CATEGORIES"


# target rule to make all engine data, set to
# flag file of last step in processing pipeline
rule ENGINE_DATA:
    input: "work/flags/engine.precombine_networks.flag"

rule BUILD_NETWORKS_CACHE:
    message: "CacheBuilder: convert interaction networks to binary engine format"
    #input: "work/flags/lucene.flag", dynamic("result/generic_db/INTERACTIONS/{ORG_ID}.{NW_ID}.txt")
    input: "work/flags/lucene.flag", "work/flags/generic_db.interaction_data.flag"
    output: "work/flags/engine.cachebuilder.flag"
    shell: """java -cp {JAR_FILE} -Xmx2G org.genemania.engine.apps.CacheBuilder \
        -indexDir "{LUCENE_INDEX_DIR}" -cachedir "{NETWORK_CACHE_DIR}" \
        -log "{LOG_DIR}/CacheBuilder.log" \
        -networkDir "{NETWORK_DIR}" \
        && touch {output}
        """

rule ATTRIBUTE_DATA:
    message: "AttributeBuilder: convert attributes to binary engine format"
    #input: "work/flags/engine.cachebuilder.flag", dynamic("result/generic_db/ATTRIBUTES/{attr_id}.txt")
    input: "work/flags/engine.cachebuilder.flag", "work/flags/generic_db.attribute_data.flag"
    output: "work/flags/engine.attribute_data.flag"
    shell: """java -cp {JAR_FILE} -Xmx2G org.genemania.engine.apps.AttributeBuilder \
        -indexDir "{LUCENE_INDEX_DIR}" -cachedir "{NETWORK_CACHE_DIR}" \
        -genericDbDir "{GENERIC_DB_DIR}" \
        -log "{LOG_DIR}/AttributeBuilder.log" \
        && touch {output}
        """


rule POST_SPARSIFY:
    message: "PostSparsifier: filter co-expression networks removing unsupported interactions"
    input: "work/flags/engine.attribute_data.flag"
    output: "work/flags/engine.postsparsifier.flag"
    shell: """java -cp {JAR_FILE} -Xmx2G org.genemania.engine.apps.PostSparsifier \
        -indexDir "{LUCENE_INDEX_DIR}" -cachedir "{NETWORK_CACHE_DIR}" \
        -log "{LOG_DIR}/PostSparsifier.log" \
        &&  touch {output}
        """
    # disable this step if needed
    #shell: "touch {output}"

rule NODE_DEGREES:
    message: "NodeDegreeComputer: count interactions for each gene"
    input: "work/flags/engine.postsparsifier.flag"
    output: "work/flags/engine.node_degrees.flag"
    shell: """java -cp {JAR_FILE} -Xmx2G org.genemania.engine.apps.NodeDegreeComputer \
        -indexDir "{LUCENE_INDEX_DIR}" -cachedir "{NETWORK_CACHE_DIR}" \
        -log "{LOG_DIR}/NodeDegreeComputer.log" \
        && touch {output}
        """

rule ANNOTATION_DATA:
    message: "AnnotationCacheBuilder: load functional annotation data into engine binary format"
    input: "work/flags/engine.node_degrees.flag", "work/flags/generic_db.function_data.flag"
    output: "work/flags/engine.annotation_data.flag"
    shell: """java -cp {JAR_FILE} -Xmx2G org.genemania.engine.apps.AnnotationCacheBuilder \
        -geneCol 2 -termCol 1 \
        -indexDir "{LUCENE_INDEX_DIR}" -cachedir "{NETWORK_CACHE_DIR}" \
        -log "{LOG_DIR}/AnnotationCacheBuider.log" -annodir "{GO_CATEGORIES_DIR}" \
        && touch {output}
        """

rule FAST_WEIGHTING:
    message: "FastWeightCacheBuilder: build precomputed data structures for GO-based network weighting"
    input: "work/flags/engine.annotation_data.flag"
    output: "work/flags/engine.fast_weighting.flag"
    shell: """java -cp {JAR_FILE} -Xmx2G org.genemania.engine.apps.FastWeightCacheBuilder \
        -indexDir "{LUCENE_INDEX_DIR}" -cachedir "{NETWORK_CACHE_DIR}" \
        -log "{LOG_DIR}/FastWeightCacheBuilder.log" -qdir "{GO_CATEGORIES_DIR}" \
        && touch {output}
        """

rule ENRICHMENT_ANALYSIS:
    message: "EnrichmentCategoryBuilder: build data structures for functional enrichment analysis"
    input: "work/flags/engine.fast_weighting.flag"
    output: "work/flags/engine.enrichment_analysis.flag"
    shell: """java -cp {JAR_FILE} -Xmx2G org.genemania.engine.apps.EnrichmentCategoryBuilder \
        -indexDir "{LUCENE_INDEX_DIR}" -cachedir "{NETWORK_CACHE_DIR}" \
        -log "{LOG_DIR}/EnrichmentCategoryBuilder.log" \
        && touch {output}
        """

rule DEFAULT_COEXPRESSION:
    message: "DefaultNetworkSelector: select subset of co-expression networks to use as default networks"
    input: "work/flags/engine.enrichment_analysis.flag"
    output: "work/flags/engine.default_coexpression.flag"
    shell: """java -cp {JAR_FILE} -Xmx2G org.genemania.engine.apps.DefaultNetworkSelector \
        -indexDir "{LUCENE_INDEX_DIR}" -cachedir "{NETWORK_CACHE_DIR}" \
        -log "{LOG_DIR}/DefaultNetworkSelector.log" \
        && touch {output}
        """
    # disable this step if needed
    #shell: "touch {output}"


rule PRECOMBINE_NETWORKS:
    message: "NetworkPrecombiner: build precoputed combined networks for common queries like single-gene"
    input: "work/flags/engine.default_coexpression.flag"
    output: "work/flags/engine.precombine_networks.flag"
    shell: """java -cp {JAR_FILE} -Xmx2G org.genemania.engine.apps.NetworkPrecombiner \
        -indexDir "{LUCENE_INDEX_DIR}" -cachedir "{NETWORK_CACHE_DIR}" \
        -log "{LOG_DIR}/NetworkPrecombiner.log" \
        && touch {output}
        """

rule CLEAN_ENGINE_DATA:
    shell: """rm -f work/flags/engine.*.flag
        rm -rf {NETWORK_CACHE_DIR}/*
        """

