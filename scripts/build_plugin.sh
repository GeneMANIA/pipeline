#!/bin/bash

# Note this overwites the xml file for each organism each time. So we should make a copy of it before
# we overwrite it so we can concatenate them at the end. 
# This xml file is needed to display what data is available for download by the plugin. 

srcdb="~/dev/r12.2/db/"
staging="/usr/local/genemania/db_build/r12.2"
dbcfg="db_at.cfg"

rm -rf build
./build_indices.sh ${srcdb}/${dbcfg} ${staging} colours.txt refresh all

./build_networks.sh ${srcdb}/${dbcfg} ${staging} refresh
