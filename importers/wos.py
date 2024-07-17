from ..utils.basics import results_cols

import os
import time
from pprint import pprint

import metaknowledge as mk

import clarivate.wos_starter.client as wos_client # type: ignore
from clarivate.wos_starter.client.rest import ApiException # type: ignore




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