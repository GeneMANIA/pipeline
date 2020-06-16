#!/bin/bash

if [[ -z $1 ]]; then
    echo "Need to provide srcdb as arg1"
    exit
fi
srcdb=${1}
build_version=${2}

echo "Building all organisms"
./auto.sh worm Ce $srcdb > worm.log 2>&1

./auto.sh human Hs $srcdb > human.log 2>&1
./auto.sh yeast Sc $srcdb > yeast.log 2>&1
./auto.sh fly Dm $srcdb > fly.log 2>&1
./auto.sh zebrafish Dr $srcdb > zebrafish.log 2>&1
./auto.sh mouse Mm $srcdb > mouse.log 2>&1
./auto.sh rat Rn $srcdb > rat.log 2>&1
./auto.sh arabidopsis At $srcdb > arabidopsis.log 2>&1
./auto.sh ecoli Ec $srcdb > ecoli.log 2>&1

echo "Merging all data"

if [[ -d merged ]]; then
    tmpname=$(basename $(mktemp -u))
    mv merged merged.${tmpname}
fi

git clone https://github.com/GeneMANIA/pipeline.git merged
pushd merged
cp /home/gmbuild/dev/${build_version}/src/loader/target/*.jar lib
cp /home/gmbuild/dev/${build_version}/src/loader/target/*.jar ~/pipeline/lib
snakemake -j4 --config merge=1 orgs=../arabidopsis,../worm,../fly,../human,../mouse,../yeast,../rat,../zebrafish,../ecoli
cp -r result/* /gm/db_build/${build_version}/
popd


