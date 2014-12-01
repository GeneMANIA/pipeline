
## configuration

# define jar file containing engine programs, by default look
# under the lib/ folder.
LOADER_VER, = glob_wildcards("lib/genemania-{version}-jar-with-dependencies.jar")

if LOADER_VER:
    JAR_FILE = "lib/genemania-%s-jar-with-dependencies.jar" % (LOADER_VER[0])
else:
    print("WARNING: genemania jar not found")
    JAR_FILE = "lib/genemania.jar"

#DATA = 'data'
#WORK = 'work'
#RESULT = 'result'
DATA = 'data_worm'
WORK = 'work_worm'
RESULT = 'result_worm'

## pipeline rules
# set a default rule

rule ALL:
    message: "build everything"
    input: WORK+"/flags/all.flag"

# connect the last thing currnently in the pipeline to the 'all' flag file
rule ALL_ENGINE_DATA:
    input: WORK+"/flags/engine.precombine_networks.flag"
    output: WORK+"/flags/all.flag"
    shell: "touch {output}"

# generic cleaning rules
rule CLEAN:
    shell: "rm -rf {WORK}"

rule VERY_CLEAN:
    shell: """rm -rf {WORK}
        rm -rf {RESULT}
        """

# load all the snakefiles
include: 'snakefiles/common.snakefile'
include: 'snakefiles/identifiers.snakefile'
include: 'snakefiles/generic_db.snakefile'
include: 'snakefiles/functions.snakefile'
include: 'snakefiles/direct_networks.snakefile'
include: 'snakefiles/sharedneighbour_networks.snakefile'
include: 'snakefiles/profiles.snakefile'
include: 'snakefiles/attributes.snakefile'
include: 'snakefiles/network_metadata.snakefile'
include: 'snakefiles/lucene_index.snakefile'
include: 'snakefiles/engine_data.snakefile'

