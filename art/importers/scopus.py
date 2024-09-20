from ..utils.basics import results_cols, blockPrint, enablePrint

import pandas as pd
import numpy as np

api_key = 'e015290bc75d27a1814cde5c468523e7'

import pybliometrics # type: ignore

blockPrint()
pybliometrics.scopus.init(keys = [api_key])
enablePrint()

from pybliometrics.scopus import AbstractRetrieval, ScopusSearch # type: ignore

def operator_logic(default_operator: str, string: str):

    """
    Takes Scopus API search string, detects the logical operator used, and separates the operator and string. Returns a tuple.
    """

    string = string.replace('NOT ', 'AND NOT ').replace('AND AND NOT ', 'AND NOT ')

    operator = default_operator

    if string.startswith('AND '):
            operator = 'AND'
        
    else:
        if string.startswith('OR '):
            operator = 'OR'
        
        else:
            if string.startswith('AND NOT '):
                operator = 'AND NOT'
            
                    
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
                    editor = None,
                    publisher = None,
                    funder = None,
                    abstract = None,
                    keywords = None,
                    doctype = None,
                    doi = None,
                    issn = None,
                    isbn = None,
                    pubmed_id = None,
                    source_title = None,
                    volume = None,
                    page = None,
                    issue = None,
                    language = None,
                    link = None,
                    references = None,
                    tile_abs_key_auth = None
                    ):
    
    """
    Takes queries for specific search fields and returns a string which is formatted for input into the Scopus API.
    
    Parameters
    ----------
        default_operator : str
            default logical operator to build search. Defaults to 'AND'.
        tile_abs_key_auth : str
            a combined search. Searches for titles, abstracts, keywords, and author names. Defaults to None.
        all_fields : str
            searches all fields. Defaults to None.
        title : str
            searches for titles containing string. Defaults to None.
        year : str
            searches for matching publication years. Defaults to None.
        author : str
            searches for authors containing string. Defaults to None.
        author_identifier : str
            searches for Scopus author IDs. Defaults to None.
        affiliation : str
            searches for author affiliations containing string. Defaults to None.
        editor : str
            searches for editor names containing string. Defaults to None.
        publisher : str
            searches for publisher names containing string. Defaults to None.
        funder : str
            searches for funder names containing string. Defaults to None.
        abstract : str
            searches for abstracts containing string. Defaults to None.
        keywords : str
            searches for matching keywords. Defaults to None.
        doctype : str
            searches for types of entries containing string. Defaults to None.
        doi : str
            searches for matching DOIs. Defaults to None.
        issn : str
            searches for matching ISSNs. Defaults to None.
        isbn : str
            searches for matching ISBNs. Defaults to None.
        pubmed_id : str
            searches for matching PubMed IDs (PMIDs). Defaults to None.
        source_title : str
            searches for source titles (e.g. journals, books) containing string. Defaults to None.
        volume : str
            searches for journal entries with matching volume numbers. Defaults to None.
        page : str
            searches for entries with matching page numbers. Defaults to None.
        issue : str
            searches for journal entries with matching issue numbers. Defaults to None.
        language : str
            searches for entries by language Defaults to None.
        link : str
            searches for entry links containing string. Defaults to None.
        references : str
            searches for entries with citations that contain matching strings. Defaults to None.
    
    Returns
    -------
        query : str
            a query formatted for input into the Scopus API.
    """

    query = ''
    
    if (all_fields is not None) and (type(all_fields) == str): # type: ignore
        query = query + 'ALL(' + all_fields + ')'

    if (title is not None) and (type(title) == str): # type: ignore
        title_tuple = operator_logic(default_operator=default_operator, string=title)
        query = query + ' ' + title_tuple[0] + ' ' + 'TITLE(' + title_tuple[1] + ')'
    
    if (year is not None) and (type(year) == str): # type: ignore
        year_tuple = operator_logic(default_operator=default_operator, string=year)
        query = query + ' ' + year_tuple[0] + ' PUBYEAR'
        if (year_tuple[1].startswith('>') == False) and (year_tuple[1].startswith('<') == False):
            query = query + ' = '
        query = query + year_tuple[1]
    
    if (author is not None) and (type(author) == str): # type: ignore
        auth_tuple = operator_logic(default_operator=default_operator, string=author)
        query = query + ' ' + auth_tuple[0] + ' AUTHOR-NAME(' + auth_tuple[1] + ')'
    
    if (author_identifier is not None) and (type(author_identifier) == str): # type: ignore
        auth_id_tuple = operator_logic(default_operator=default_operator, string=author_identifier)
        query = query + ' ' + auth_id_tuple[0] + ' AU-ID(' + auth_id_tuple[1] + ')'
    
    if (affiliation is not None) and (type(affiliation) == str): # type: ignore
        affil_tuple = operator_logic(default_operator=default_operator, string=affiliation)
        query = query + ' ' + affil_tuple[0] + ' AFFIL(' + affil_tuple[1] + ')'
    
    if (abstract is not None) and (type(abstract) == str): # type: ignore
        abs_tuple = operator_logic(default_operator=default_operator, string=abstract)
        query = query + ' ' + abs_tuple[0] + ' ABS(' + abs_tuple[1] + ')'
    
    if (keywords is not None) and (type(keywords) == str): # type: ignore
        kws_tuple = operator_logic(default_operator=default_operator, string=keywords)
        query = query + ' ' + kws_tuple[0] + ' KEY(' + kws_tuple[1] + ')'
    
    if (doctype is not None) and (type(doctype) == str): # type: ignore
        doctype_tuple = operator_logic(default_operator=default_operator, string=doctype)
        query = query + ' ' + doctype_tuple[0] + ' DOCTYPE(' + doctype_tuple[1] + ')'
    
    if (doi is not None) and (type(doi) == str): # type: ignore
        doi_tuple = operator_logic(default_operator=default_operator, string=doi)
        query = query + ' ' + doi_tuple[0] + ' DOI(' + doi_tuple[1] + ')'
    
    if (editor is not None) and (type(editor) == str): # type: ignore
        ed_tuple = operator_logic(default_operator=default_operator, string=editor)
        query = query + ' ' + ed_tuple[0] + ' EDITOR(' + ed_tuple[1] + ')'
    
    if (publisher is not None) and (type(publisher) == str): # type: ignore
        pub_tuple = operator_logic(default_operator=default_operator, string=publisher)
        query = query + ' ' + pub_tuple[0] + ' PUBLISHER(' + pub_tuple[1] + ')'
    
    if (funder is not None) and (type(funder) == str): # type: ignore
        funder_tuple = operator_logic(default_operator=default_operator, string=funder)
        query = query + ' ' + funder_tuple[0] + ' FUND-SPONSOR(' + funder_tuple[1] + ')'
    
    if (issn is not None) and (type(issn) == str): # type: ignore
        issn_tuple = operator_logic(default_operator=default_operator, string=issn)
        query = query + ' ' + issn_tuple[0] + ' ISSN(' + issn_tuple[1] + ')'
    
    if (isbn is not None) and (type(isbn) == str): # type: ignore
        isbn_tuple = operator_logic(default_operator=default_operator, string=isbn)
        query = query + ' ' + isbn_tuple[0] + ' ISBN(' + isbn_tuple[1] + ')'
    
    if (language is not None) and (type(language) == str): # type: ignore
        lang_tuple = operator_logic(default_operator=default_operator, string=language)
        query = query + ' ' + lang_tuple[0] + ' LANGUAGE(' + lang_tuple[1] + ')'
    
    if (pubmed_id is not None) and (type(pubmed_id) == str): # type: ignore
        pubmed_tuple = operator_logic(default_operator=default_operator, string=pubmed_id)
        query = query + ' ' + pubmed_tuple[0] + ' PMID(' + pubmed_tuple[1] + ')'
    
    if (source_title is not None) and (type(source_title) == str): # type: ignore
        st_tuple = operator_logic(default_operator=default_operator, string=source_title)
        query = query + ' ' + st_tuple[0] + ' SRCTITLE(' + st_tuple[1] + ')'
    
    if (volume is not None) and (type(volume) == str): # type: ignore
        volume_tuple = operator_logic(default_operator=default_operator, string=volume)
        query = query + ' ' + volume_tuple[0] + ' VOLUME(' + volume_tuple[1] + ')'
    
    if (page is not None) and (type(page) == str): # type: ignore
        page_tuple = operator_logic(default_operator=default_operator, string=page)
        query = query + ' ' + page_tuple[0] + ' PAGES(' + page_tuple[1] + ')'
    
    if (issue is not None) and (type(issue) == str): # type: ignore
        issue_tuple = operator_logic(default_operator=default_operator, string=issue)
        query = query + ' ' + issue_tuple[0] + ' ISSUE(' + issue_tuple[1] + ')'
    
    if (link is not None) and (type(link) == str): # type: ignore
        link_tuple = operator_logic(default_operator=default_operator, string=link)
        query = query + ' ' + link_tuple[0] + ' WEBSITE(' + link_tuple[1] + ')'
    
    if (references is not None) and (type(references) == str): # type: ignore
        refs_tuple = operator_logic(default_operator=default_operator, string=references)
        query = query + ' ' + refs_tuple[0] + ' REF(' + refs_tuple[1] + ')'
    
    if (tile_abs_key_auth is not None) and (type(tile_abs_key_auth) == str): # type: ignore
        focused_tuple = operator_logic(default_operator=default_operator, string=tile_abs_key_auth)
        query = query + ' ' + focused_tuple[0] + ' TITLE-ABS-KEY-AUTH(' + focused_tuple[1] + ')'
    
    query = query.strip()
    if query.startswith('AND ') == True:
        query = query[4:]
    if query.startswith('OR ') == True:
        query = query[3:]
    
    return query
       
def search(tile_abs_key_auth = None,
                    all_fields = None,
                    title = None,
                    year = None,
                    author = None,
                    author_identifier = None,
                    affiliation = None,
                    editor = None,
                    publisher = None,
                    funder = None,
                    abstract = None,
                    keywords = None,
                    doctype = None,
                    doi = None,
                    issn = None,
                    isbn = None,
                    pubmed_id = None,
                    source_title = None,
                    volume = None,
                    page = None,
                    issue = None,
                    language = None,
                    link = None,
                    references = None,
                    default_operator = 'AND',
                    refresh=True, 
                    view=None, 
                    verbose=False, 
                    download=True, 
                    integrity_fields=None, 
                    integrity_action='raise', 
                    subscriber=False):
    
    """
        Searches Scopus API and returns the results as a Pandas DataFrame.

        Parameters
        ----------
        tile_abs_key_auth : str
            a combined search. Searches for titles, abstracts, keywords, and author names. Defaults to None.
        all_fields : str
            searches all fields. Defaults to None.
        title : str
            searches for titles containing string. Defaults to None.
        year : str
            searches for matching publication years. Defaults to None.
        author : str
            searches for authors containing string. Defaults to None.
        author_identifier : str
            searches for Scopus author IDs. Defaults to None.
        affiliation : str
            searches for author affiliations containing string. Defaults to None.
        editor : str
            searches for editor names containing string. Defaults to None.
        publisher : str
            searches for publisher names containing string. Defaults to None.
        funder : str
            searches for funder names containing string. Defaults to None.
        abstract : str
            searches for abstracts containing string. Defaults to None.
        keywords : str
            searches for matching keywords. Defaults to None.
        doctype : str
            searches for types of entries containing string. Defaults to None.
        doi : str
            searches for matching DOIs. Defaults to None.
        issn : str
            searches for matching ISSNs. Defaults to None.
        isbn : str
            searches for matching ISBNs. Defaults to None.
        pubmed_id : str
            searches for matching PubMed IDs (PMIDs). Defaults to None.
        source_title : str
            searches for source titles (e.g. journals, books) containing string. Defaults to None.
        volume : str
            searches for journal entries with matching volume numbers. Defaults to None.
        page : str
            searches for entries with matching page numbers. Defaults to None.
        issue : str
            searches for journal entries with matching issue numbers. Defaults to None.
        language : str
            searches for entries by language Defaults to None.
        link : str
            searches for entry links containing string. Defaults to None.
        references : str
            searches for entries with citations that contain matching strings. Defaults to None.
        default_operator : str
            the default Boolean operator to build the search. Defaults to 'AND'.
        refresh : bool 
        view : bool 
        verbose : bool 
        download : bool 
        integrity_fields : None
        integrity_action : str
        subscriber : bool
        
        Returns
        -------
        df : pandas.DataFrame
            results from Scopus API search.
        
        Options
        -------
        Options for default_operator:
            * 'AND'
            * 'AND NOT'
            * 'NOT'
            * 'OR'
    """

    query = query_builder(default_operator = default_operator,
                    all_fields = all_fields,
                    title = title,
                    year = year,
                    author = author,
                    author_identifier = author_identifier,
                    affiliation = affiliation,
                    editor = editor,
                    publisher = publisher,
                    funder = funder,
                    abstract = abstract,
                    keywords = keywords,
                    doctype = doctype,
                    doi = doi,
                    issn = issn,
                    isbn = isbn,
                    pubmed_id = pubmed_id,
                    source_title = source_title,
                    volume = volume,
                    page = page,
                    issue = issue,
                    language = language,
                    link = link,
                    references = references,
                    tile_abs_key_auth = tile_abs_key_auth
                    )
    
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

    res_df['title'] = res_df['title'].str.replace('&amp;', '&')
    
    res_df['access_type'] = res_df['access_type'].replace(1, 'open_access').replace(0, None)

    res_df['authors_data'] = res_df['authors']

    res_df = res_df.drop(['subtype', 'coverDisplayDate', 'affiliation_city', 'affiliation_country'], axis=1)

    res_df = res_df.dropna(axis=1, how='all')

    return res_df

def lookup(uid: str = 'request_input',
           refresh = False,
           view = 'META',
           id_type = None):

    """
        Looks up publication using the Scopus API.

        Parameters
        ----------
        uid : str
            Scopus ID, DOI, ISBN, ISSN, or Pubmed ID (PMID) to look up. Defaults to requesting from user input.
        refresh : bool
            whether to refresh the Scopus session.
        view : str
            sets the amount of detail returned. Defaults to 'META'.
        add_to_results : bool
            whether to add results to Review.
        drop_duplicates : bool
            whether to remove duplicated rows when adding to results.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data when adding to results.
        id_type : None

        Returns
        -------
        df : pandas.DataFrame
            results from publication lookup on Scopus API.
    """

    if uid == 'request_input':
        uid = input('ID: ')
    
    uid = uid.strip()

    res = AbstractRetrieval(
                            identifier=uid,
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
        
        df['author_affiliations'] = pd.Series(dtype=object).replace(np.nan, None)

        if len(df) > 0:

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