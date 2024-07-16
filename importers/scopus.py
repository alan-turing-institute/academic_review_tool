from ..utils.basics import results_cols

import pandas as pd
import numpy as np

api_key = 'e015290bc75d27a1814cde5c468523e7'

import pybliometrics # type: ignore
    
pybliometrics.scopus.create_config(keys = [api_key])

from pybliometrics.scopus import AbstractRetrieval, ScopusSearch # type: ignore

def search(query: str = 'request_input',
           refresh=True, 
           view=None, 
           verbose=False, 
           download=True, 
           integrity_fields=None, 
           integrity_action='raise', 
           subscriber=False):
    
    if query == 'request_input':
        query = input('Search query: ')
    
    res = ScopusSearch(query=query, 
                       refresh=refresh,
                       view=view,
                       verbose=verbose,
                       download=download,
                       integrity_fields=integrity_fields,
                       integrity_action=integrity_action,
                       subscriber=subscriber)
    
    res_len = res._n

    print(f'{res_len} results returned') # type: ignore

    res_list = res.results
    res_df = pd.DataFrame(data=res_list, dtype=object)

    res_df = res_df.rename(columns={
                                    'eid': 'scopus_id',
                                    'subtypeDescription': 'type',
                                    'creator': 'authors',
                                    'affilname': 'author_affiliations',
                                    'coverDate': 'date',
                                    'publicationName': 'source',
                                    'eIssn': 'issn',
                                    'citedby_count': 'cited_by_count',
                                    'authkeywords': 'keywords',
                                    'openaccess': 'access_type'
                                    })

    res_df['author_affiliations'] = res_df['author_affiliations'].astype(str).replace('None', '').replace('none', '')
    res_df['affiliation_city'] = res_df['affiliation_city'].astype(str).replace('None', '').replace('none', '')
    res_df['affiliation_country'] = res_df['affiliation_country'].astype(str).replace('None', '').replace('none', '')
    res_df['author_affiliations'] = res_df['author_affiliations'] + ', ' + res_df['affiliation_city'] + ', ' + res_df['affiliation_country']

    
    res_df['access_type'] = res_df['access_type'].replace(1, 'open_access').replace(0, None)

    res_df = res_df.drop(['subtype', 'coverDisplayDate', 'affiliation_city', 'affiliation_country'], axis=1)

    res_df = res_df.dropna(axis=1, how='all')

    return res_df