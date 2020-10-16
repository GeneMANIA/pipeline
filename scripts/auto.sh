#!/bin/bash

#source ./setpath.sh

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 longname shortname srcdb"
    echo "Eg   : $0 human Hs /gm/dev/r14.3/db/"
    exit 1
fi


#srcdb="/gm/dev/r14.3/db"
#srcdb="/gm/dev/r14/db"
#srcdb="/gm/dev/r-test/db"

lname=$1
sname=$2
srcdb=$3

if [[ ! -d ${srcdb} ]]; then
    echo "[!] can't find ${srcdb}"
    ./message_slack.sh "[!] can't find ${srcdb}"
    exit 1
fi

#echo "[+] deleting old $lname"
rm -rf $lname

echo "[+] cloning git repo for $lname"
git clone https://github.com/GeneMANIA/pipeline.git $lname
if [[ $? -ne 0 ]]; then 
    "[!] git clone failed"
    exit 1
fi

#cp fetch_pubmed_metadata.py $lname/builder

# sync date otherwise snakemake complains
#ssh root@192.168.81.219 ./sync_date.sh

cd $lname
echo "[+] workdir: `pwd`"
echo "[+] copying jar files to lib"
cp ../lib/*.jar lib

echo "[+] running import_oldpipe.py on $sname"
echo "[+] python builder/import_oldpipe.py $srcdb $sname data/ --force_default_selected"
python builder/import_oldpipe.py $srcdb $sname data/ --force_default_selected
if [[ $? -ne 0 ]]; then 
    echo "[!] import_oldpipe.py failed"
    ./message_slacks.sh "[!] import_oldpipe.py failed"
    exit 1
fi 

# fix pfam.cfg and all.cfg names
echo "[+] fixing names for PFAM and INTERPRO"
sed -i 's/name = \"\"/name = \"PFAM\"/g' data/networks/sharedneighbour/pfam/pfam.cfg
sed -i 's/name = \"\"/name = \"INTERPRO\"/g' data/networks/sharedneighbour/interpro/all.cfg

echo "[+] running snakemake"
time snakemake -j4 -p
if [[ $? -ne 0 ]]; then 
    echo "[!] snakemake failed"
    ./message_slack.sh "[!] snakemake failed for $lname"
    exit 1
fi 

./message_slack.sh "[+] snakemake for $lname completed successfully"

cd ..
