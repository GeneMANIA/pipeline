
"""apply misc changes to network metadata, based
on an auxiliary tab-delimited datafile with the columns:

  dataset_key
  var_name
  var_value

metadata records matching the given key (path to .cfg file, less the .cfg extension itself)
are modified to have var_name set to the given value. e.g:

  networks/direct/biogrid/somenetwork group gi

will edit the 'group' value setting it to 'gi'. Why bother and not change the
value in the .cfg file directly? actually yes, that's what you should do. this
mechanism exists only for recording changes to programatically generated data files
that need to have edits repeated when replaced with refreshed downloads.

if you can avoid using this, please do
"""

import argparse
import numpy as np
import pandas as pd
from buildutils import strip_key


def main(edit_file, metadata_in, metadata_out, key_rstrip=None):

    # load the table of edits
    edits = pd.read_csv(edit_file, sep='\t', names=['filename', 'var', 'value'], encoding='UTF8')

    # load metadata
    metadata = pd.read_csv(metadata_in, sep='\t', encoding='UTF8', dtype=str)

    # metadata as the var's in columns. if any of the var's we are editing don't already
    # exist, add them as new cols initialized to missing

    edit_vars = pd.Series(edits['var'].unique())
    new_vars = edit_vars[~edit_vars.isin(metadata.columns)]

    for new_var in new_vars:
        metadata[new_var] = np.NAN

    # cfg file name should match the dataset key in the metadata file
    edits['dataset_key'] = edits['filename'].apply(lambda x: strip_key(x, None, key_rstrip))

    # log all edit files we don't have in the metadata table
    missing_files = edits[~edits['dataset_key'].isin(metadata['dataset_key'])]['dataset_key']
    if len(missing_files) > 0:
        print("metadata edits for these missing files will be skipped: ",
              ' '.join(list(missing_files)))

    # apply remaining edits. could probably vectorize this into some kind
    # of join, but its not much data, and simpler to think about in a loop,
    # at least for me :)
    edits = edits[edits['dataset_key'].isin(metadata['dataset_key'])]

    for index, row in edits.iterrows():
        filename, var, value = row['dataset_key'], row['var'], row['value']
        metadata.loc[metadata['dataset_key'] == filename, var] = value

    # write new edited metadata file
    metadata.to_csv(metadata_out, sep='\t', header=True, index=True, encoding='UTF8')

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='apply batch edits to config files')

    parser.add_argument('edit_file', help='input file containing edits to apply')

    parser.add_argument('metadata_in', help='file containing metadata to update')

    parser.add_argument('metadata_out', help='updated metadata file')

    parser.add_argument('--key_rstrip', type=str,
                        help='remove given string from right of filename to compute dataset key')


    args = parser.parse_args()
    main(args.edit_file, args.metadata_in, args.metadata_out, args.key_rstrip)