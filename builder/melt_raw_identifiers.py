
"""the so-called 'raw' identifiers file is the output of a process that
runs against the Ensembl database to produce identifiers in a particular
format

note there is an old parser for these in identifiers/parsers.py. we're now
mostly in the habit of using pandas for data-munging, but we'll reuse the
previous parser and cleaning logic for now
"""


import argparse, contextlib
import pandas as pd
from identifiers import identifier_merger
from identifiers import constants


def main(inputfile, symbols_outputfile, descriptions_outputfile, biotypes=None):

    # symbols as id/symbol/source triplets
    db = identifier_merger.IdentifierDB(":memory:")
    with contextlib.closing(db):
        db.load_raw(open(inputfile, encoding='UTF8'))
        db.cleanup()
        db.remove_unwanted_sources(constants.RAW_DEFAULT_SOURCES_TO_REMOVE)
        db.standardize_source_names()

        if biotypes:
            db.biotype_filter(biotypes)

        db.export_processed(open(symbols_outputfile, 'w', encoding='UTF8'))


    # descriptions
    data = pd.read_csv(inputfile, sep='\t', header=0,
                       encoding='UTF8', na_filter=False)

    data['Definition'].replace('N/A', '', inplace=True)

    data.to_csv(descriptions_outputfile, sep='\t', header=False, index=False,
                columns=['GMID', 'Definition'], encoding='UTF8')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='split raw identifiers file into triplets' )

    parser.add_argument('inputfile')
    parser.add_argument('symbols_outputfile')
    parser.add_argument('descriptions_outputfile')
    parser.add_argument('--biotypes', nargs='*',
                        help='only load records having one of these biotypes')

    args = parser.parse_args()
    main(args.inputfile, args.symbols_outputfile, args.descriptions_outputfile, args.biotypes)