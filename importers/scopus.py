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

def lookup(work_id: str = 'request_input',
           refresh = False,
           view = 'META',
           id_type = None):

    if work_id == 'request_input':
        work_id = input('ID: ')
    
    work_id = work_id.strip()

    res = AbstractRetrieval(
                            identifier=work_id,
                            refresh=refresh,
                            view=view,
                            id_type=id_type
                            )
    
    if type(res) == AbstractRetrieval:

        js = res._json
        js_keys = js.keys()

        if 'affiliation' in js_keys:
            affils = js['affiliation']
        else:
            affils = []
        
        if 'coredata' in js_keys:
            data = js['coredata']
        else:
            data = {}

        df = pd.DataFrame.from_dict(data, orient='index').T

        df = df.rename(columns={
                                    'eid': 'scopus_id',
                                    'dc:title': 'title',
                                    'subtypeDescription': 'type',
                                    'dc:creator': 'authors',
                                    'prism:coverDate': 'date',
                                    'prism:publicationName': 'source',
                                    'citedby_count': 'cited_by_count',
                                    'openaccess': 'access_type',
                                    'link': 'other_links',
                                    'prism:url': 'link',
                                    'prism:doi': 'doi',
                                    'prism:issn': 'issn',
                                    'dc:publisher': 'publisher'
                                    })
        
        df.at[0, 'author_affiliations'] = affils
        df['access_type'] = df['access_type'].replace(1, 'open_access').replace(0, None)
        
        other_links = df.at[0, 'other_links']

        if (type(other_links) == list) and (len(other_links) > 0):

            for i in other_links:

                link = i['@href']

                if (type(link) == str) and (link.startswith('https://api.elsevier.com/content/abstract/')):
                     df.at[0, 'abstract'] = link
                     continue
                
                if (type(link) == str) and (link.startswith('https://www.scopus.com/inward/citedby')):
                     df.at[0, 'cited_by_data'] = link
                     continue

                    

        
    else:
        df = pd.DataFrame(columns=results_cols, dtype=object)

    return df