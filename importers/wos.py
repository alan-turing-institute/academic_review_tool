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
from clarivate.wos_starter.client.models.journals_list import JournalsList # type: ignore




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

def operator_logic(default_operator: str, string: str):

    operator = default_operator

    if string.startswith('AND '):
            operator = 'AND'
        
    else:
        if string.startswith('OR '):
            operator = 'OR'
        
        else:
            if string.startswith('NOT '):
                operator = 'NOT'
            
            else:
                if string.startswith('NEAR '):
                    operator = 'NEAR'
                
                else:
                    if string.startswith('SAME '):
                        operator = 'SAME'
                    
                    else:
                        operator = default_operator

    string_stripped = string.strip(f'{operator} ').strip()
    

    return (operator, string_stripped)

def query_builder(default_operator = 'AND',
                    all_fields = None,
                    title = None,
                    year = None,
                    author = None,
                    author_identifier = None,
                    affiliation = None,
                    doctype = None,
                    doi = None,
                    issn = None,
                    isbn = None,
                    pubmed_id = None,
                    source_title = None,
                    volume = None,
                    page = None,
                    issue = None,
                    topics = None
                    ):
    
    query = ''
    
    if (all_fields is not None) and (type(all_fields) == str): # type: ignore
        query = query + 'TS=' + all_fields

    if (title is not None) and (type(title) == str): # type: ignore
        title_tuple = operator_logic(default_operator=default_operator, string=title)
        query = query + ' ' + title_tuple[0] + ' ' + 'TI=' + title_tuple[1]
    
    if (year is not None) and (type(year) == str): # type: ignore
        year_tuple = operator_logic(default_operator=default_operator, string=year)
        query = query + ' ' + year_tuple[0] + ' PY=' + year_tuple[1]
    
    if (author is not None) and (type(author) == str): # type: ignore
        auth_tuple = operator_logic(default_operator=default_operator, string=author)
        query = query + ' ' + auth_tuple[0] + ' AU=' + auth_tuple[1]
    
    if (author_identifier is not None) and (type(author_identifier) == str): # type: ignore
        auth_id_tuple = operator_logic(default_operator=default_operator, string=author_identifier)
        query = query + ' ' + auth_id_tuple[0] + ' AI=' + auth_id_tuple[1]
    
    if (affiliation is not None) and (type(affiliation) == str): # type: ignore
        affil_tuple = operator_logic(default_operator=default_operator, string=affiliation)
        query = query + ' ' + affil_tuple[0] + ' OG=' + affil_tuple[1]
    
    if (doctype is not None) and (type(doctype) == str): # type: ignore
        doctype_tuple = operator_logic(default_operator=default_operator, string=doctype)
        query = query + ' ' + doctype_tuple[0] + ' DT=' + doctype_tuple[1]
    
    if (doi is not None) and (type(doi) == str): # type: ignore
        doi_tuple = operator_logic(default_operator=default_operator, string=doi)
        query = query + ' ' + doi_tuple[0] + ' DO=' + doi_tuple[1]
    
    if (issn is not None) and (type(issn) == str): # type: ignore
        issn_tuple = operator_logic(default_operator=default_operator, string=issn)
        query = query + ' ' + issn_tuple[0] + ' IS=' + issn_tuple[1]
    
    if (isbn is not None) and (type(isbn) == str): # type: ignore
        isbn_tuple = operator_logic(default_operator=default_operator, string=isbn)
        query = query + ' ' + isbn_tuple[0] + ' IS=' + isbn_tuple[1]
    
    if (pubmed_id is not None) and (type(pubmed_id) == str): # type: ignore
        pubmed_tuple = operator_logic(default_operator=default_operator, string=pubmed_id)
        query = query + ' ' + pubmed_tuple[0] + ' PMID=' + pubmed_tuple[1]
    
    if (source_title is not None) and (type(source_title) == str): # type: ignore
        st_tuple = operator_logic(default_operator=default_operator, string=source_title)
        query = query + ' ' + st_tuple[0] + ' SO=' + st_tuple[1]
    
    if (volume is not None) and (type(volume) == str): # type: ignore
        volume_tuple = operator_logic(default_operator=default_operator, string=volume)
        query = query + ' ' + volume_tuple[0] + ' VL=' + volume_tuple[1]
    
    if (page is not None) and (type(page) == str): # type: ignore
        page_tuple = operator_logic(default_operator=default_operator, string=page)
        query = query + ' ' + page_tuple[0] + ' PG=' + page_tuple[1]
    
    if (issue is not None) and (type(issue) == str): # type: ignore
        issue_tuple = operator_logic(default_operator=default_operator, string=issue)
        query = query + ' ' + issue_tuple[0] + ' CS=' + issue_tuple[1]
    
    if (topics is not None) and (type(topics) == str): # type: ignore
        topics_tuple = operator_logic(default_operator=default_operator, string=topics)
        query = query + ' ' + topics_tuple[0] + ' TS=' + topics_tuple[1]
    
    query = query.strip()
    if query.startswith('AND ') == True:
        query = query[4:]
    if query.startswith('OR ') == True:
        query = query[3:]
    
    return query
        
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

def search(
            all_fields = None,
            title = None,
            year = None,
            author = None,
            author_identifier = None,
            affiliation = None,
            doctype = None,
            doi = None,
            issn = None,
            isbn = None,
            pubmed_id = None,
            source_title = None,
            volume = None,
            page = None,
            issue = None,
            topics = None,
            default_operator = 'AND',
           database: str = 'WOK',
           limit: int = 10,
           page_limit: int = 1,
           sort_field: str = 'RS+D',
           modified_time_span = None,
           tc_modified_time_span = None,
           detail = None
           ):
    
    query = query_builder(default_operator = default_operator,
                    all_fields = all_fields,
                    title = title,
                    year = year,
                    author = author,
                    author_identifier = author_identifier,
                    affiliation = affiliation,
                    doctype = doctype,
                    doi = doi,
                    issn = issn,
                    isbn = isbn,
                    pubmed_id = pubmed_id,
                    source_title = source_title,
                    volume = volume,
                    page = page,
                    issue = issue,
                    topics = topics
                    )

    api_response = search_engine(query=query, 
           database=database,
           limit=limit,
           page=page_limit,
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
                                'types': 'other_types',
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

def journals_search_engine(issn: str = 'request_input'):

    if issn == 'request_input':
        issn = input('ISSN to search: ')
    
    global configuration
    
    with wos_client.ApiClient(configuration) as api_client:

        api_instance = wos_client.JournalsApi(api_client)

        try:
            # Query Web of Science documents 
            api_response = api_instance.journals_get(issn=issn)
            return api_response

        except ApiException as e:
            print("Exception when calling JournalsApi->journals_get: %s\n" % e)

def search_journals(
            query = 'request_input'
           ):
    

    api_response = journals_search_engine(issn=query)
    
    if (api_response is not None) and (type(api_response) == JournalsList):

        res_dict = api_response.to_dict()
        meta = res_dict['metadata']
        found = meta['total']
        lim = meta['limit']
        page_num = meta['page']
        print(f'{found} results found. {lim} results returned from page {page_num}') # type: ignore

        df = pd.DataFrame(res_dict['hits'], dtype=object)
        df = df.rename(columns={
                                'id': 'wos_id',
                                'links': 'link'
                            })
        
        df = df.replace(np.nan, None)
    
    else:
        df = pd.DataFrame(columns=results_cols, dtype=object)
    
    return df



