from ..utils.basics import results_cols

import os
import time
from pprint import pprint

import pandas as pd
import numpy as np

# import metaknowledge as mk

from ..wosstarter_python_client_master.clarivate.wos_starter import client as wos_client
from ..wosstarter_python_client_master.clarivate.wos_starter.client.rest import ApiException
from ..wosstarter_python_client_master.clarivate.wos_starter.client.models.documents_list import DocumentsList
from ..wosstarter_python_client_master.clarivate.wos_starter.client.models.journals_list import JournalsList

configuration = wos_client.Configuration(
    host = "http://api.clarivate.com/apis/wos-starter/v1"
)

configuration.api_key['ClarivateApiKeyAuth'] = '7a6bd360df2d18446f24bc26c85ab72fdbe4091f'

# def import_wos(file_path: str = 'request_input'):

#     if file_path == 'request_input':
#         file_path = input('File path: ')
    
#     file_path = file_path.strip()

#     RC = mk.RecordCollection(file_path)

#     return RC

def extract_source(source_dict):

    """
    Extracts publication source from Web of Science API result.
    """

    source = None

    if (type(source_dict) == list) and (len(source_dict)>0):
        source_dict = source_dict[0]

    if type(source_dict) == dict:

        if 'sourceTitle' in source_dict.keys():
            source = source_dict['sourceTitle']

    return source

def extract_cite_counts(citations_dict):

    """
    Extracts citation counts from Web of Science API result.
    """

    count = None

    if (type(citations_dict) == list) and (len(citations_dict)>0):
        citations_dict = citations_dict[0]

    if type(citations_dict) == dict:

        if 'count' in citations_dict.keys():
            count = citations_dict['count']

    return count

def extract_isbn(identifiers):

    """
    Extracts publication ISBN from Web of Science API result.
    """

    isbn = None

    if (type(identifiers) == list) and (len(identifiers)>0):
        identifiers = identifiers[0]

    if type(identifiers) == dict:

        if 'isbn' in identifiers.keys():
            isbn = identifiers['isbn']

    return isbn

def extract_issn(identifiers):

    """
    Extracts publication ISSN from Web of Science API result.
    """

    issn = None

    if (type(identifiers) == list) and (len(identifiers)>0):
        identifiers = identifiers[0]

    if type(identifiers) == dict:

        if 'issn' in identifiers.keys():
            issn = identifiers['issn']

    return issn

def extract_doi(identifiers):

    """
    Extracts publication DOI from Web of Science API result.
    """

    doi = None

    if (type(identifiers) == list) and (len(identifiers)>0):
        identifiers = identifiers[0]

    if type(identifiers) == dict:

        if 'doi' in identifiers.keys():
            doi = identifiers['doi']

    return doi

def extract_keywords(keywords_dict):

    """
    Extracts publication keywords from Web of Science API result.
    """

    kws = None

    if (type(keywords_dict) == list) and (len(keywords_dict)>0):
        keywords_dict = keywords_dict[0]

    if type(keywords_dict) == dict:

        if 'authorKeywords' in keywords_dict.keys():
            kws = keywords_dict['authorKeywords']

    return kws

def extract_related(links_dict):

    """
    Extracts related publications from Web of Science API result.
    """

    link = None

    if (type(links_dict) == list) and (len(links_dict)>0):
        links_dict = links_dict[0]

    if type(links_dict) == dict:

        if 'related' in links_dict.keys():
            link = links_dict['related']

    return link

def extract_links(links_dict):

    """
    Extracts links from Web of Science API result.
    """

    link = None

    if (type(links_dict) == list) and (len(links_dict)>0):
        links_dict = links_dict[0]

    if type(links_dict) == dict:

        if 'record' in links_dict.keys():
            link = links_dict['record']
        
        else:
            if 'references' in links_dict.keys():
                link = links_dict['references']
            else:
                if 'related' in links_dict.keys():
                    link = links_dict['related']

    return link

def operator_logic(default_operator: str, string: str):

    """
    Takes Web of Science API search string, detects the logical operator used, and separates the operator and string. Returns a tuple.
    """

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
    
    """
    Takes queries for specific search fields and returns a string which is formatted for input into the Web of Science API.

    Parameters
    ----------
    default_operator : str
        default logical operator to build search. Defaults to 'AND'.
    all_fields : str
    title : str
    year : str
    author : str
    author_identifier : str
    affiliation : str
    doctype : str
    doi : str
    issn : str
    isbn : str
    pubmed_id : str
    source_title : str
    volume : str
    page : str
    issue : str
    topics : str

    Returns
    -------
    query : str
        a query formatted for input into the Web of Science API.
    """

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
    
    """
    Core functionality for making Web of Science publication search API calls.

    Parameters
    ----------
    query: str
        a query formatted for input into the Web of Science API.
    database : str 
    limit : int
    page : int
    sort_field : str
    modified_time_span
    tc_modified_time_span
    detail

    Returns
    -------
    api_response : DocumentsList
        Web of Science API response.
    """

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
    
    """
        Searches Web of Science API for published works. Returns the results as a Pandas DataFrame.

        Parameters
        ----------
        all_fields : str
        title : str
        year : str
        author : str
        author_identifier : str
        affiliation : str
        doctype : str
        doi : str
        issn : str
        isbn : str
        pubmed_id : str
        source_title : str
        volume : str
        page : str
        issue : str
        topics : str
        default_operator : str
            default logical operator to build search. Defaults to 'AND'.
        database : str 
        limit : int
        page : int
        sort_field : str
        modified_time_span
        tc_modified_time_span
        detail
        
        Returns
        -------
        df : pandas.DataFrame
            results from Web of Science API search.
    """

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
        df['authors_data'] = df['authors'].copy(deep=True)
        df['source'] = df['source'].apply(extract_source)
        df['doi'] = df['identifiers'].apply(extract_doi)
        df['isbn'] = df['identifiers'].apply(extract_isbn)
        df['issn'] = df['identifiers'].apply(extract_issn)
        df['keywords'] = df['keywords'].apply(extract_keywords)
        df['recommendations'] = df['link'].apply(extract_related)
        df['link'] = df['link'].apply(extract_links)
        df['citation_count'] = df['citations'].apply(extract_cite_counts)
        df['citations_data'] = df['citations']
        df['repository'] = database

        df = df.drop('identifiers', axis=1)
    
    else:
        df = pd.DataFrame(columns=results_cols, dtype=object)
    

    return df

def journals_search_engine(issn: str = 'request_input'):

    """
    Core functionality for making Web of Science journal search API calls.

    Parameters
    ----------
    issn: str
        an ISSN to search.

    Returns
    -------
    api_response : JournalsList
        Web of Science API response.
    """

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
    
    """
        Searches Web of Science API for journals. Returns the results as a Pandas DataFrame.

        Parameters
        ----------
        query : str
            search query.
        
        Returns
        -------
        df : pandas.DataFrame
            results from Web of Science API search.
    """

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
        df['link'] = df['link'].apply(extract_links)
    
    else:
        df = pd.DataFrame(columns=results_cols, dtype=object)
    
    return df
