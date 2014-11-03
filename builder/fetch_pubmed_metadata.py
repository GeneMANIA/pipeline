
import argparse, os
import pandas as pd
import numpy as np
from itertools import zip_longest
from Bio import Entrez

MAX_IDS_PER_REQUEST = 200

PUBMED_FIELDS = ['pubmed_id', 'title', 'journal', 'journal_short',
                 'year', 'first_author', 'last_author']

def fetch(pmid_list, email=None):

    Entrez.email = email
    pmid_list = [pmid for pmid in pmid_list if pmid is not None]
    ids = ','.join(str(id) for id in pmid_list)
    handle = Entrez.efetch(db='pubmed', id=ids, retmode='xml')
    result = Entrez.read(handle)
    handle.close()

    return result


def to_series(metadata):
    '''
    extract needed pieces from the metadata hierarchy
    into a nice flat list (pandas Series with field names
    in the index)
    '''

    pubmed_id = str(metadata['MedlineCitation']['PMID'])

    # publication year
    article = metadata['MedlineCitation']['Article']
    pubdate = article['Journal']['JournalIssue']['PubDate']

    try:
        year = str(pubdate['Year'])
    except KeyError as e:
        medline_date = str(pubdate['MedlineDate'])
        year = medline_date.split(' ')[0]

    # article title
    title = str(article['ArticleTitle'])

    # authors ... needs handling for CollectiveName tag?
    authors = article['AuthorList']

    first_author = str(authors[0]['LastName'])

    if len(authors) > 1:
        last_author = str(authors[-1]['LastName'])
    else:
        last_author = ''

    # journal
    journal = str(article['Journal']['Title'])
    journal_short = str(metadata['MedlineCitation']['MedlineJournalInfo']['MedlineTA'])

    # return series
    return pd.Series([pubmed_id, title, journal, journal_short,
                      year, first_author, last_author],
                     index=PUBMED_FIELDS)


def cache_filename(cachedir, pubmed_id):
    return os.path.join(cachedir, "%s.cfg" % pubmed_id)


# https://docs.python.org/3/library/itertools.html#itertools-recipes
def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def main(inputfile, outputfile, pubmed_datafile, fetchsize):

    network_metadata = pd.read_csv(inputfile, sep='\t', header=0, index_col=0,
                                   na_filter=False, encoding='UTF8')

    if os.path.exists(pubmed_datafile):
        # give the header fields explicitly since we create this as an empty
        # file when no cache has been built up
        pubmed_data = pd.read_csv(pubmed_datafile, sep='\t', header=0, na_filter=False,
                                  encoding='UTF8')
        assert list(pubmed_data.columns) == PUBMED_FIELDS # consistency check
    else:
        pubmed_data = pd.DataFrame(columns=PUBMED_FIELDS)

    # networks for which we need metadata
    pmids = network_metadata[-network_metadata['pubmed_id'].isin(pubmed_data['pubmed_id'])]
    pmids = pmids['pubmed_id'].copy()
    pmids.replace('', np.nan, inplace=True)
    pmids.dropna(inplace=True)
    pmids.drop_duplicates(inplace=True)

    if len(pmids) > 0:
        # fetch rest from pubmed, in groups of fetchsize
        groups = grouper(pmids, fetchsize)

        all_fetched = []
        for group in groups:
            parsed_metadata = fetch(group)

            fetched = [to_series(item) for item in parsed_metadata]
            all_fetched.extend(fetched)

        # glue the series together into a dataframe
        new_pubmed_data = pd.concat(all_fetched, axis=1).transpose()

        # update previously retrieved metadata
        pubmed_data = pd.concat([pubmed_data, new_pubmed_data])
        pubmed_data.to_csv(pubmed_datafile, sep='\t', header=True, index=False,
                           na_filter=False, encoding='UTF8')

    # join with network metadata
    # pushing index preserve id in from the left dataframe
    network_metadata.reset_index(inplace=True)
    updated_network_metadata = pd.merge(network_metadata, pubmed_data,
                                        on='pubmed_id', how='left')

    updated_network_metadata.set_index('id', inplace=True)
    updated_network_metadata.to_csv(outputfile, sep='\t', header=True, index=True,
                                    na_filter=False, encoding='UTF8')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='fetch article metadata from pubmed')

    parser.add_argument('inputfile', help='input network metadata file')
    parser.add_argument('outputfile', help='output network metadata file')
    parser.add_argument('pubmed_data', help='file containing previously retrieved pubmed data, to be ')
    parser.add_argument('--fetchsize', help='# of pubmed entries to retrieve in a single fetch, default 200',
                        type=int, default=200)

    args = parser.parse_args()
    main(args.inputfile, args.outputfile, args.pubmed_data, args.fetchsize)







