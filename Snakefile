
## configuration

# define jar file containing engine programs, by default look
# under the lib/ folder.
LOADER_VER, = glob_wildcards("lib/genemania-{version}-jar-with-dependencies.jar")

if LOADER_VER:
    JAR_FILE = "lib/genemania-%s-jar-with-dependencies.jar" % (LOADER_VER[0])
else:
    print("WARNING: genemania jar not found")
    JAR_FILE = "lib/genemania.jar"

# we could set a path in the snakefile by:
#
#  workdir: "path/to/workdir"
#
# to control data location, but it seems to also
# affect paths to included snakefiles and configs.
# so try configuring by command line options:
#
# e.g. for test run as
#
#  snakemake --config test=1
#
if 'test' in config and config['test']:
    DATA = 'test/data'
    WORK = 'test/work'
    RESULT = 'test/result'
else:
    DATA = 'data'
    WORK = 'work'
    RESULT = 'result'

print("DATA:", DATA)
print("WORK:", WORK)
print("RESULT:", RESULT)

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

