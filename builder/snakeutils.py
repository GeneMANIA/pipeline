
'''
various utilities for use with snakemake
'''


from snakemake.io import glob_wildcards
import re


def my_glob_wildcards(pattern, whitelist=(), blacklist=()):
    '''
    returns  a wildcard object with whitelist/blacklist filters applied. can have multiple regex's
    per wildcard token.

    example, ignore files with leading '.' char

       FNS = my_glob_wildcards('path/to/{dir}/{filename}', blacklist=(('filename','^\..*$)))

    putting a regex directly in the glob_wildcards pattern is possible, but somehow fiddly
    to get working, plus its easier this way to negate the matching as well as provide multiple
    patterns for a single wildcard token

    :param pattern: search pattern as per glob_wildcards()
    :param whitelist: optional sequence of (wildcard, regex) pairs, only matches are retained
    :param blacklist: optional sequence of (wildcard, regex) pairs, matches are dropped
    :return: wildcards object (named tuple)
    '''

    wildcards = glob_wildcards(pattern)
    return filter_wildcards(wildcards, whitelist, blacklist)


def filter_wildcards(wildcards, whitelist, blacklist):
    for wc, filter in whitelist:
        hits = getattr(wildcards, wc)
        to_delete = [i for i in range(len(hits)) if not re.search(filter, hits[i])]
        delete_from_wildcards(wildcards, to_delete)

    for wc, filter in blacklist:
        hits = getattr(wildcards, wc)
        to_delete = [i for i in range(len(hits)) if re.search(filter, hits[i])]
        delete_from_wildcards(wildcards, to_delete)

    return wildcards


def delete_from_wildcards(wildcards, to_delete):
    '''
    modifies the given namedtuple wildcards object,
    dropping entries with indexes in the given to_delete list

    :param wildcards: wildcards object (namedtuple)
    :param to_delete: sequence of index values to drop
    :return: None
    '''

    # the wildcards object has a list of hits for each wildcard (named element),
    # need to remove the ignore item from each of these lists

    to_delete.sort()
    for wc in range(len(wildcards)):
        hits = wildcards[wc]
        for n, i in enumerate(to_delete):
            # delete in place, with n accounting for mutating
            # the list as we loop
            del(hits[i-n])


def glob_datafiles(pattern, filter_wc='fn'):
    '''
    return glob_wildcards() matches after removing files with a leading '.'
    or ending with '.cfg'

    :param pattern: search pattern as per glob_wildcards()
    :param filter_wc: name of wildcard in pattern to which to apply filename
           filtering, e.g. 'fn' when pattern contains {fn}
    :return: wildcards object
    '''

    blacklist = ((filter_wc, '^\..*$'),     # remove files with leading '.'
                 (filter_wc, '^.*\.cfg$'))  # remove files ending with '.cfg'

    return my_glob_wildcards(pattern, blacklist=blacklist)


def glob_configfiles(pattern, filter_wc='fn'):
    '''
    return glob_wildcards() matches with files only ending in .cfg, and
    removing files with leading '.'

    :param pattern: search pattern as per glob_wildcards()
    :param filter_wc: name of wildcard in pattern to which to apply filename
           filtering, e.g. 'fn' when pattern contains {fn}
    :return: wildcards object
    '''

    whitelist = ((filter_wc, '^.*\.cfg$'))  # keep only files ending with '.cfg'
    blacklist = ((filter_wc, '^\..*$'))     # remove files with leading '.'

    return my_glob_wildcards(pattern, whitelist=whitelist, blacklist=blacklist)

