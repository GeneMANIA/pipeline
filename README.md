GeneMANIA Pipeline
==================

Builds an organism database for the
[GeneMANIA](http://genemania.org) website,
given input files describing genes and
their interactions.



## Installation

The pipeline consists of a set of python scripts and java programs controlled by the [snakemake](https://bitbucket.org/johanneskoester/snakemake/wiki/Home) bioinformatics workflow engine. The programs are developed on Linux and OSX systems, Windows may work but is untested.

Platform requirements are:

 1. Python 3.4+
 1. Java 1.6+
 
The [Conda](http://conda.pydata.org/) Python package manager makes it easy to install Python 3 along with associated numeric libraries, particularly on Mac and Windows systems, and is recommended. The following python packages are required:
 

 1. [pandas](http://pandas.pydata.org/)
 1. [snakemake](https://bitbucket.org/johanneskoester/snakemake/wiki/Home)
 1. [biopython](http://biopython.org/)
 1. [configobj](https://github.com/DiffSK/configobj)
 

The needed java programs are included in GeneMANIA application itself; currently a snapshot from the latest codebase is required ([#9](https://github.com/GeneMANIA/pipeline/issues/9)).


## Usage

Configuration, data formats, and pipeline execution are describe in
the [project documenation](https://github.com/GeneMANIA/pipeline/wiki).
