#!/bin/bash

for i in arabidopsis ecoli fly human mouse rat worm yeast zebrafish; do 
    rm -rf $i
    rm -rf ${i}.log
done

