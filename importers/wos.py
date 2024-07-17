from ..utils.basics import results_cols

import os
import time
from pprint import pprint

import pandas as pd
import numpy as np

import metaknowledge as mk

import clarivate.wos_starter.client as wos_client # type: ignore
from clarivate.wos_starter.client.rest import ApiException # type: ignore
from clarivate.wos_starter.client.models.documents_list import DocumentsList # type: ignore




configuration = wos_client.Configuration(
    host = "http://api.clarivate.com/apis/wos-starter/v1"
)

configuration.api_key['ClarivateApiKeyAuth'] = '7a6bd360df2d18446f24bc26c85ab72fdbe4091f'

def import_wos(file_path: str = 'request_input'):

    if file_path == 'request_input':
        file_path = input('File path: ')
    
    file_path = file_path.strip()

    RC = mk.RecordCollection(file_path)

    return RC


def search_engine(query: str = 'request_input', 
           database: str = 'WOK',
           limit: int = 10,
           page: int = 1,
           sort_field: str = 'RS+D',
           modified_time_span = None,
           tc_modified_time_span = None,
           detail = None
           ):
    
    if query == 'request_input':
        query = input('Search query: ')

    global configuration

    with wos_client.ApiClient(configuration) as api_client:
        
        api_instance = wos_client.DocumentsApi(api_client)

        try:
            # Query Web of Science documents 
            api_response = api_instance.documents_get(q=query, db=database, limit=limit, page=page, sort_field=sort_field, modified_time_span=modified_time_span, tc_modified_time_span=tc_modified_time_span, detail=detail)
            return api_response

        except ApiException as e:
            print("Exception when calling DocumentsApi->documents_get: %s\n" % e)

def search(query: str = 'request_input', 
           database: str = 'WOK',
           limit: int = 10,
           page: int = 1,
           sort_field: str = 'RS+D',
           modified_time_span = None,
           tc_modified_time_span = None,
           detail = None
           ):
    
    api_response = search_engine(query=query, 
           database=database,
           limit=limit,
           page=page,
           sort_field=sort_field,
           modified_time_span = modified_time_span,
           tc_modified_time_span = tc_modified_time_span,
           detail = detail
           )
    
    if (api_response is not None) and (type(api_response) == DocumentsList):

        res_dict = api_response.to_dict()
        meta = res_dict['metadata']
        found = meta['total']
        lim = meta['limit']
        page_num = meta['page']
        print(f'{found} results found. {lim} results returned from page {page_num}') # type: ignore

        df = pd.DataFrame(res_dict['hits'], dtype=object)
        df = df.rename(columns={
                                'uid': 'wos_id',
                                'sourceTypes': 'type',
                                'types': 'other_types'
                                'names': 'authors',
                                'links': 'link'
                            })
        
        for c in results_cols:
            if c not in df.columns:
                df[c] = pd.Series(dtype=object)
        
        df = df.replace(np.nan, None)
    
    else:
        df = pd.DataFrame(columns=results_cols, dtype=object)
    
    return df