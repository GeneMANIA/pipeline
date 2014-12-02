
def strip_key(filename, key_lstrip, key_rstrip):
    dataset_key = filename
    if key_lstrip and dataset_key.startswith(key_lstrip):
        dataset_key = dataset_key[len(key_lstrip):]
    if key_rstrip and dataset_key.endswith(key_rstrip):
        dataset_key = dataset_key[:-len(key_rstrip)]

    return dataset_key