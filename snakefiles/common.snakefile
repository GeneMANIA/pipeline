

# non-configuration values shared by various snakefiles
# load this before other included snakefiles

# list of files which make up generic_db
GENERIC_DB_FILES = ["ORGANISMS.txt",
  		"NODES.txt", "GENES.txt", "GENE_DATA.txt", "GENE_NAMING_SOURCES.txt",
  		"ATTRIBUTES.txt", "ATTRIBUTE_GROUPS.txt",
        "NETWORKS.txt", "NETWORK_GROUPS.txt", "NETWORK_METADATA.txt",
  		"ONTOLOGIES.txt", "ONTOLOGY_CATEGORIES.txt",
  		"TAGS.txt", "NETWORK_TAG_ASSOC.txt",
  		"STATISTICS.txt", "SCHEMA.txt"
  		]

# add directory location to generic db file list
import os
GENERIC_DB_FILES = [os.path.join("result", "generic_db", filename) for filename in GENERIC_DB_FILES]

# flag files for the subdirs in generic db
GENERIC_DB_FLAG_FILES = ["work/flags/generic_db.interaction_data.flag",
                         "work/flags/generic_db.attribute_data.flag",
                         "work/flags/generic_db.function_data.flag"]

GENERIC_DB_FILES = GENERIC_DB_FILES + GENERIC_DB_FLAG_FILES

# intermediate file containing cleaned up identifier symbols,
# and corresponding descriptions
IDENTIFIER_SYMBOLS = "identifiers.txt"
IDENTIFIER_DESCRIPTIONS = "descriptions.txt"


# pull in some util functions
from builder.snakeutils import glob_datafiles, glob_configfiles, my_glob_wildcards
