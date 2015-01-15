

def strip_key(filename, key_lstrip, key_rstrip):
    """convenience helper to strip strings from the beginning and/or end
    of a filename, e.g.:

      strip_key("data/networks/file.txt", "data/", ".txt")

    gives

      networks/file
    """

    dataset_key = filename
    if key_lstrip and dataset_key.startswith(key_lstrip):
        dataset_key = dataset_key[len(key_lstrip):]
    if key_rstrip and dataset_key.endswith(key_rstrip):
        dataset_key = dataset_key[:-len(key_rstrip)]

    return dataset_key

def str2bool(v):
    """convert a string in common forms into a boolean

    because bool doesn't work as naively expected in argparse, see
    http://stackoverflow.com/questions/15008758/parsing-boolean-values-with-argparse
    """

    return v.lower() in ("yes", "true", "t", "1")
