"""Functions to interface with the Crossref API"""

from ..utils.basics import results_cols
from ..utils.cleaners import is_int
from ..internet.webanalysis import is_url

from time import sleep

from crossref.restful import Works, Journals, Funders, Etiquette # type: ignore
import pandas as pd

filters = ['alternative_id', 
    'archive', 
    'article_number', 
    'assertion', 
    'assertion_group', 
    'award__funder', 
    'award__number', 
    'category_name', 
    'clinical_trial_number', 
    'container_title', 
    'content_domain', 
    'directory', 
    'doi', 
    'from_accepted_date', 
    'from_created_date', 
    'from_deposit_date', 
    'from_event_end_date', 
    'from_event_start_date', 
    'from_index_date', 
    'from_issued_date', 
    'from_online_pub_date', 
    'from_posted_date', 
    'from_print_pub_date', 
    'from_pub_date', 
    'from_update_date', 
    'full_text__application', 
    'full_text__type', 
    'full_text__version', 
    'funder', 
    'funder_doi_asserted_by', 
    'group_title', 
    'has_abstract', 
    'has_affiliation', 
    'has_archive', 
    'has_assertion', 
    'has_authenticated_orcid', 
    'has_award', 
    'has_clinical_trial_number', 
    'has_content_domain', 
    'has_domain_restriction', 
    'has_event', 
    'has_full_text', 
    'has_funder', 
    'has_funder_doi', 
    'has_license', 
    'has_orcid', 
    'has_references', 
    'has_relation', 
    'has_update', 
    'has_update_policy', 
    'is_update', 
    'isbn', 
    'issn', 
    'license__delay', 
    'license__url', 
    'license__version', 
    'location', 
    'member', 
    'orcid', 
    'prefix', 
    'relation__object', 
    'relation__object_type', 
    'relation__type', 
    'type', 
    'type_name', 
    'until_accepted_date', 
    'until_created_date', 
    'until_deposit_date', 
    'until_event_end_date', 
    'until_event_start_date', 
    'until_index_date', 
    'until_issued_date', 
    'until_online_pub_date', 
    'until_posted_date', 
    'until_print_pub_date', 
    'until_pub_date', 
    'until_update_date', 
    'update_type', 
    'updates'
    ]

my_etiquette = Etiquette('Academic Review Tool (ART)', '1.10-beta', 'https://github.com/alan-turing-institute/academic_review_tool', 'academic_review_tool@outlook.com')

def items_to_df(items: list) -> pd.DataFrame:

    """
    Takes list containing items from CrossRef API call and returns as a Pandas DataFrame.
    """

    global results_cols
    df = pd.DataFrame(columns = results_cols)

    for i in items:
        
        if 'title' in i.keys():
            title = i['title']
            if type(title) == list:
                title = title[0]
        else:
            title = None

        if 'container-title' in i.keys():
            source = i['container-title']
            if type(source) == list:
                source = source[0]
        else:
            source = None

        if 'published' in i.keys():
            date = i['published']['date-parts'][0][0]
        else:
            date = None

        if 'abstract' in i.keys():
            abstract = i['abstract']
        else:
            abstract = None

        if 'publisher' in i.keys():
            publisher = i['publisher']
        else:
            publisher = None

        if 'funder' in i.keys():
            funder = i['funder']
        else:
            funder = None

        if 'doi' in i.keys():
            doi = i['doi']
        else:
            doi = None

        if 'type' in i.keys():
            entry_type = i['type']
        else:
            entry_type = None

        if 'author' in i.keys():
            authors_data = i['author']
        else:
            authors_data = []

        if 'references-count' in i.keys():
            citations_count = i['references-count']
        else:
            citations_count = None

        if 'reference' in i.keys():
            citations_data = i['reference']
        else:
            citations_data = None

        if 'is-referenced-by-count' in i.keys():
            cited_by_count = i['is-referenced-by-count']
        else:
            cited_by_count = None

        if 'URL' in i.keys():
            link = i['URL']
        else:
            link = None
        
        if 'score' in i.keys():
            crossref_score = i['score']
        else:
            crossref_score = None
        
        if 'language' in i.keys():
            language = i['language']
        else:
            language = None
        
        authors = []
        for a in authors_data:
            if 'given' in a.keys():
                first_name = a['given']
            else:
                first_name = ''
            
            if 'family' in a.keys():
                last_name = a['family']
            else:
                last_name = ''
            
            name = first_name + ' ' + last_name

            authors.append(name)
        
        if doi == None:
            if link != None:
                if 'doi.org/' in link:
                    doi = link.replace('https', '').replace('http', '').replace('://', '').replace('dx.', '').replace('www.', '').replace('doi.org/', '')

        index = len(df)
        df.loc[index, 'title'] = title
        df.loc[index, 'source'] = source
        df.loc[index, 'date'] = date
        df.loc[index, 'publisher'] = publisher
        df.loc[index, 'funder'] = funder
        df.loc[index, 'abstract'] = abstract
        df.loc[index, 'doi'] = doi
        df.loc[index, 'type'] = entry_type
        df.loc[index, 'authors_data'] = authors_data
        df.at[index, 'authors'] = authors
        df.at[index, 'language'] = language
        df.at[index, 'citation_count'] = citations_count
        df.at[index, 'citations_data'] = citations_data
        df.at[index, 'cited_by_count'] = cited_by_count
        df.at[index, 'crossref_score'] = crossref_score
        df.loc[index, 'link'] = link
    
    return df

def reference_to_df(reference: dict, update_from_doi = False) -> pd.DataFrame:

    """
    Takes reference (i.e. citation) dictionary from CrossRef API result and returns as a Pandas DataFrame.

    Parameters
    ----------
    reference : dict
        a dictionary containing data on a reference associated with a CrossRef API result.
    update_from_doi : bool
        whether to update the reference data using the CrossRef API. Defaults to False.
    
    Returns
    -------
    df : pandas.DataFrame
        the reference formatted as a Pandas DataFrame.
    """

    keys = list(reference.keys())

    df_data = {}

    
    if 'doi' in keys:

        if update_from_doi == True:
            try:
                doi = reference['doi']
                df = lookup_doi(doi)
                return df
            
            except:
                pass
        else:
            doi = reference['doi']
    
    if 'URL' in keys:

        if 'doi.org/' in reference['URL']:
            doi = reference['URL']

            if update_from_doi == True:
                try:
                    df = lookup_doi(doi)
                    return df
                
                except:
                    pass

    if 'unstructured' in keys:

        unstr = reference['unstructured'].split('. ')
        
        df_data['authors'] = unstr[0]
        df_data['title'] = None
        df_data['date'] = None
        df_data['link'] = None

        for i in unstr:
            
            if i != df_data['authors']:

                if is_int(i) == True:
                    df_data['date'] = i

                else:
                    if is_url(i) == True:
                        df_data['link'] = i

                    else:

                        if df_data['title'] == None:
                            df_data['title'] = i

        if (df_data['link'] != None) and ('doi.org/' in df_data['link']):
            try:
                df = lookup_doi(df_data['link'])
                return df
            
            except:
                pass
    
    if 'year' in keys:
        df_data['date'] = reference['year']

    if 'author' in keys:
        df_data['authors'] = reference['author']
    
    if 'title' in keys:
        df_data['title'] = reference['title']
    else:
        if 'article-title' in keys:
            df_data['title'] = reference['article-title']
        
        else:
            if 'volume-title' in keys:
                df_data['title'] = reference['volume-title']
        
    if 'book-title' in keys:
        df_data['source'] = reference['book-title']
    
    if 'journal-title' in keys:
        df_data['source'] = reference['journal-title']

    if len(df_data.keys()) == 0:
        df_data = reference

    df = pd.DataFrame(columns = list(df_data.keys()), dtype=object)

    for key in df_data.keys():
        df.at[0, key] = df_data[key]
    
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    return df

def references_to_df(references_list: list, update_from_doi = False) -> pd.DataFrame:

    """
    Takes a list of references (i.e. citations) from a CrossRef API result and returns as a Pandas DataFrame.

    Parameters
    ----------
    references : list
        a list containing data on references associated with a CrossRef API result.
    update_from_doi : bool
        whether to update the reference data using the CrossRef API. Defaults to False.
    
    Returns
    -------
    df : pandas.DataFrame
        the reference formatted as a Pandas DataFrame.
    """

    df = pd.DataFrame(columns = results_cols, dtype=object)

    for i in references_list:

        row = reference_to_df(i, update_from_doi)
        df = pd.concat([df, row])
    
    df = df.reset_index().drop('index', axis=1)

    try:
        df = df.drop('doi-asserted-by', axis=1).drop('key', axis=1)
    except:
        pass

    return df

def operator_logic(default_operator: str, string: str):

    """
    Takes CrossRef API search string, detects the logical operator used, and separates the operator and string. Returns a tuple.
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
                operator = default_operator

    string_stripped = string.strip(f'{operator} ').strip()
    

    return (operator, string_stripped)

def query_builder(default_operator = 'AND',
                  bibliographic: str = None, # type: ignore
                    title: str = None, # type: ignore
                    author: str = None, # type: ignore
                    author_affiliation: str = None, # type: ignore
                    editor: str = None, # type: ignore
                    entry_type: str = None, # type: ignore
                    published_date: str = None, # type: ignore
                    doi: str = None, # type: ignore
                    issn: str = None, # type: ignore
                    publisher_name: str = None, # type: ignore
                    funder_name = None, # type: ignore
                    source: str = None, # type: ignore
                    link: str = None, # type: ignore
                    ):
    
    """
    Takes queries for specific search fields and returns a string which is formatted for input into the CrossRef API.

    Parameters
    ----------
        default_operator : str
            default logical operator to build search. Defaults to 'AND'.
        bibliographic : str
            a combined search. Searches for titles, abstracts, authors, publishers, dates etc. Defaults to None.
        title : str
            searches for titles containing string. Defaults to None.
        author : str
            searches for authors containing string. Defaults to None.
        author_affiliation : str
            searches for author affiliations containing string. Defaults to None.
        editor : str
            searches for editor names containing string. Defaults to None.
        entry_type : str
            searches for types of entries containing string. Defaults to None.
        published_date : str
            searches for matching publication dates. Defaults to None.
        doi : str
            searches for matching DOIs.
        issn : str
            searches for matching ISSNs.
        publisher_name : str
             searches for publisher names containing string. Defaults to None.
        funder_name : str
            searches for funder names containing string. Defaults to None.
        source : str
            searches for sources (e.g. journals, books) containing string. Defaults to None.
        link : str
            searches for entry links containing string. Defaults to None.
    
    Returns
    -------
        query : str
            a query formatted for input into the CrossRef API. 
    """

    query = ''
    
    if (bibliographic is not None) and (type(bibliographic) == str): # type: ignore
        bib_tuple = operator_logic(default_operator=default_operator, string=bibliographic)
        query = query + ' ' + bib_tuple[0] + ' ' + 'BIBLIOGRAPHIC: ' + bib_tuple[1]

    if (title is not None) and (type(title) == str): # type: ignore
        title_tuple = operator_logic(default_operator=default_operator, string=title)
        query = query + ' ' + title_tuple[0] + ' ' + 'TITLE: ' + title_tuple[1]
    
    if (published_date is not None) and (type(published_date) == str): # type: ignore
        year_tuple = operator_logic(default_operator=default_operator, string=published_date)
        query = query + ' ' + year_tuple[0] + ' DATE: ' + year_tuple[1]
    
    if (author is not None) and (type(author) == str): # type: ignore
        auth_tuple = operator_logic(default_operator=default_operator, string=author)
        query = query + ' ' + auth_tuple[0] + ' AUTHOR: ' + auth_tuple[1]
    
    if (author_affiliation is not None) and (type(author_affiliation) == str): # type: ignore
        affil_tuple = operator_logic(default_operator=default_operator, string=author_affiliation)
        query = query + ' ' + affil_tuple[0] + ' AUTHOR AFFIL:' + affil_tuple[1]
    
    if (entry_type is not None) and (type(entry_type) == str): # type: ignore
        doctype_tuple = operator_logic(default_operator=default_operator, string=entry_type)
        query = query + ' ' + doctype_tuple[0] + ' TYPE: ' + doctype_tuple[1]
    
    if (doi is not None) and (type(doi) == str): # type: ignore
        doi_tuple = operator_logic(default_operator=default_operator, string=doi)
        query = query + ' ' + doi_tuple[0] + ' DOI: ' + doi_tuple[1]
    
    if (editor is not None) and (type(editor) == str): # type: ignore
        ed_tuple = operator_logic(default_operator=default_operator, string=editor)
        query = query + ' ' + ed_tuple[0] + ' EDITOR: ' + ed_tuple[1]
    
    if (publisher_name is not None) and (type(publisher_name) == str): # type: ignore
        pub_tuple = operator_logic(default_operator=default_operator, string=publisher_name)
        query = query + ' ' + pub_tuple[0] + ' PUBLISHER: ' + pub_tuple[1]
    
    if (funder_name is not None) and (type(funder_name) == str): # type: ignore
        funder_tuple = operator_logic(default_operator=default_operator, string=funder_name)
        query = query + ' ' + funder_tuple[0] + ' FUNDER: ' + funder_tuple[1]
    
    if (issn is not None) and (type(issn) == str): # type: ignore
        issn_tuple = operator_logic(default_operator=default_operator, string=issn)
        query = query + ' ' + issn_tuple[0] + ' ISSN: ' + issn_tuple[1]
    
    if (source is not None) and (type(source) == str): # type: ignore
        st_tuple = operator_logic(default_operator=default_operator, string=source)
        query = query + ' ' + st_tuple[0] + ' SOURCE:' + st_tuple[1]
    
    if (link is not None) and (type(link) == str): # type: ignore
        link_tuple = operator_logic(default_operator=default_operator, string=link)
        query = query + ' ' + link_tuple[0] + ' LINK: ' + link_tuple[1]
        
    
    query = query.strip()
    if query.startswith('AND ') == True:
        query = query[4:]
    if query.startswith('NOT ') == True:
        query = query[4:]
    if query.startswith('OR ') == True:
        query = query[3:]
    query = query.strip()
    
    return query
   
def search_works(
                bibliographic = None, # type: ignore
                title: str = None, # type: ignore
                author: str = None, # type: ignore
                author_affiliation: str = None, # type: ignore
                editor: str = None, # type: ignore
                entry_type: str = None, # type: ignore
                published_date: str = None, # type: ignore
                doi: str = None, # type: ignore
                issn: str = None, # type: ignore
                publisher_name: str = None, # type: ignore
                funder_name = None, # type: ignore
                source: str = None, # type: ignore
                link: str = None, # type: ignore
                filter: dict = None, # type: ignore
                select: list = None, # type: ignore
                sample: int = None, # type: ignore
                limit: int = 20,
                rate_limit: float = 0.05,
                timeout = 60
                ) -> pd.DataFrame:

    """
        Searches CrossRef API for published works. Returns the results as a Pandas DataFrame.

        Parameters
        ----------
        bibliographic : str
            a combined search. Searches for titles, abstracts, authors, publishers, dates etc. Defaults to None.
        title : str
            searches for titles containing string. Defaults to None.
        author : str
            searches for authors containing string. Defaults to None.
        author_affiliation : str
            searches for author affiliations containing string. Defaults to None.
        editor : str
            searches for editor names containing string. Defaults to None.
        entry_type : str
            searches for types of entries containing string. Defaults to None.
        published_date : str
            searches for matching publication dates. Defaults to None.
        doi : str
            searches for matching DOIs.
        issn : str
            searches for matching ISSNs.
        publisher_name : str
             searches for publisher names containing string. Defaults to None.
        funder_name : str
            searches for funder names containing string. Defaults to None.
        source : str
            searches for sources (e.g. journals, books) containing string. Defaults to None.
        link : str
            searches for entry links containing string. Defaults to None.
        sample : int
            optional: select which results to return.
        limit : int
            optional: set a limit to the number of results returned.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        filter : dict
        select : list
        
        Returns
        -------
        df : pandas.DataFrame
            results from CrossRef API search.
    """

    if bibliographic == None:
        bibliographic = ''
    
    if title != None:
        bibliographic = bibliographic + ', ' + str(title)

    if entry_type != None:
        bibliographic = bibliographic + ', ' + str(entry_type)

    if doi != None:
        bibliographic = bibliographic + ', ' + str(doi)

    if issn != None:
        bibliographic = bibliographic + ', ' + str(issn)
    
    if published_date != None:
        bibliographic = bibliographic + ', ' + str(published_date)
    
    if funder_name != None:
        bibliographic = bibliographic + ', ' + str(funder_name)
    
    if link != None:
        bibliographic = bibliographic + ', ' + str(link)

    if bibliographic == '':
        bibliographic = None # type: ignore

    global my_etiquette
    works = Works(etiquette=my_etiquette, timeout=timeout)
    result = works.query(
                        bibliographic = bibliographic,
                        author = author,
                        affiliation = author_affiliation,
                        editor = editor,
                        publisher_name = publisher_name,
                        container_title = source
                        )
    
    if filter != None:

        filter_input = ''

        global filters
        for f in filters:
            if f in filter.keys():
                val = filter[f]
                filter_input = filter_input + f'{f}="{val}", '
        
        filter_input = filter_input.strip(', ').strip()
        
        filter_code = f'result.filter({filter_input})'
        result = exec(filter_code)
    
    if select != None:

        select_input = ''
        for i in select:
            select_input = select_input + f'"{i}", '
        select_input = select_input.strip(', ').strip()

        select_code = f'result.select({select_input})'
        result = exec(select_code)

    print(f'{result.count()} results found') # type: ignore

    if (limit == None) or (limit < 1):
        if result.count() > 1000: # type: ignore
            limit_decision = input(f'No limit set for the number of results to download, but {result.count()} results found. Would you like to set a limit? (yes/no) ') # type: ignore

            if limit_decision.lower().strip() == 'yes':
                new_limit = input('New limit: ').strip()

                if new_limit == '':
                    new_limit = 1000
                
                limit = int(new_limit)
            
            if limit_decision.lower().strip() == 'no':
                limit = None # type: ignore
            
            if limit_decision.lower().strip() == '':
                limit = 1000




    items = []
    if result != None:

        iteration = 1
        try:
            for item in result: # type: ignore
                try:
                    if limit != None:
                        if iteration > limit:
                            break

                    items.append(item)

                    iteration += 1

                    sleep(rate_limit)

                except Exception as e:
                    print(f'Search retrieval ran into an error. {e}')
                    continue
                
        except Exception as e:
            print(f'Search retrieval ran into an error. {e}')
    
    df = items_to_df(items)

    return df

def lookup_doi(doi = 'request_input', timeout = 60):

    """
        Looks up DOI using the CrossRef API.

        Parameters
        ----------
        doi : str
            DOI to look up. Defaults to requesting from user input.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.

        Returns
        -------
        df : pandas.DataFrame
            results from DOI lookup on CrossRef API.
    """

    if doi == 'request_input':
        doi = input('doi: ')

    global my_etiquette
    works = Works(etiquette=my_etiquette, timeout=timeout)
    
    result = works.doi(doi)

    item = [result]

    df = items_to_df(item)

    return df

def lookup_dois(dois_list: list = [], rate_limit: float = 0.05, timeout = 60):

    """
        Looks up a list of DOIs using the CrossRef API. Returns a Pandas DataFrame.

        Parameters
        ----------
        dois_list : list
            list of DOIs to look up. Defaults to an empty list.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.

        Returns
        -------
        result : pandas.DataFrame
            result of DOI lookups.
    """

    items = []

    global my_etiquette
    works = Works(etiquette=my_etiquette, timeout=timeout)
    for doi in dois_list:
        result = works.doi(doi)
        items.append(result)
        sleep(rate_limit)

    df = items_to_df(items)

    return df

def lookup_journal(issn = 'request_input', timeout = 60):

    """
        Looks up a journal by its ISSN using the CrossRef API. Returns a Pandas DataFrame.

        Parameters
        ----------
        issn : str
            ISSN to look up. Defaults to requesting from user input.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.

        Returns
        -------
        result : pandas.DataFrame
            journal records.
    """

    if issn == 'request_input':
        issn = input('Journal issn: ')

    global my_etiquette
    journals = Journals(etiquette=my_etiquette, timeout=timeout)
    result = journals.journal(issn)

    return pd.DataFrame.from_dict(result, orient='index').T

def lookup_journals(issns_list: list = [], rate_limit: float = 0.05, timeout = 60):

    """
        Looks up a list of journal ISSNs using the CrossRef API. Returns a Pandas DataFrame.

        Parameters
        ----------
        issns_list : str
            list of ISSNs to look up. Defaults to an empty list.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.

        Returns
        -------
        output : pandas.DataFrame
            journal records.
    """

    global my_etiquette
    journals = Journals(etiquette=my_etiquette, timeout=timeout)

    output = pd.DataFrame(dtype=object)

    for issn in issns_list:
        result = journals.journal(issn)
        df = pd.DataFrame.from_dict(result, orient='index').T
        output = pd.concat([output, df])
        sleep(rate_limit)

    return output

def search_journals(*args, limit: int = 1000, rate_limit: float = 0.05, timeout = 60):

    """
        Searches CrossRef API for journal records and returns the results as a Pandas DataFrame.

        Parameters
        ----------
        *args
            search fields.
        limit : int
            optional: set a limit to the number of results returned. Defaults to 1000.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        
        Returns
        -------
        results : pandas.DataFrame
            results from CrossRef API search.
    """

    global my_etiquette
    journals = Journals(etiquette=my_etiquette, timeout=timeout).query(*args)

    results = pd.DataFrame()

    if journals != None:

        iteration = 1
        for item in journals:

            if limit != None:
                if iteration > limit:
                    break
            
            row = pd.DataFrame.from_dict(item, orient='index').T
            results = pd.concat([results, row])

            iteration += 1

            sleep(rate_limit)
    
    results = results.reset_index().drop('index', axis=1)

    return results
    
def get_journal_entries(issn = 'request_input',
                        filter: dict = None, # type: ignore
                        select: list = None, # type: ignore
                        sample: int = None, # type: ignore
                        limit: int = 20,
                        rate_limit: float = 0.05,
                        timeout = 60):

    """
        Looks up a journal using the CrossRef API and returns associated entries as a Pandas DataFrame.

        Parameters
        ----------
        issn : str
            ISSN to look up. Defaults to requesting from user input.
        sample : int
            optional: select which results to return.
        limit : int
            optional: set a limit to the number of results returned.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        filter : dict
        select : list

        Returns
        -------
        df : pandas.DataFrame
            journal entry records.
    """

    if issn == 'request_input':
        issn = input('Journal issn: ')

    global my_etiquette
    journals = Journals(etiquette=my_etiquette, timeout=timeout)
    result = journals.works(issn)

    if filter != None:

        filter_input = ''

        global filters
        for f in filters:
            if f in filter.keys():
                val = filter[f]
                filter_input = filter_input + f'{f}="{val}", '
        
        filter_input = filter_input.strip(', ').strip()
        
        filter_code = f'result.filter({filter_input})'
        result = exec(filter_code)
    
    if select != None:

        select_input = ''
        for i in select:
            select_input = select_input + f'"{i}", '
        select_input = select_input.strip(', ').strip()

        select_code = f'result.select({select_input})'
        result = exec(select_code)

    print(f'{result.count()} results found') # type: ignore

    items = []
    if result != None:

        iteration = 1
        for item in result: # type: ignore

            if limit != None:
                if iteration > limit:
                    break

            items.append(item)

            iteration += 1

            sleep(rate_limit)
    
    df = items_to_df(items)

    return df

def search_journal_entries(issn = 'request_input',
                        bibliographic: str = None, # type: ignore
                        title: str = None, # type: ignore
                        author: str = None, # type: ignore
                        author_affiliation: str = None, # type: ignore
                        editor: str = None, # type: ignore
                        entry_type: str = None, # type: ignore
                        published_date: str = None, # type: ignore
                        doi: str = None, # type: ignore
                        publisher_name: str = None, # type: ignore
                        funder_name = None, # type: ignore
                        source: str = None, # type: ignore
                        link: str = None, # type: ignore
                        filter: dict = None, # type: ignore
                        select: list = None, # type: ignore
                        sample: int = None, # type: ignore
                        limit: int = 1000,
                        rate_limit: float = 0.05,
                        timeout = 60):
    
    """
            Searches for journal entries and articles associated with an ISSN using the CrossRef API.

            Parameters
            ----------
            issn : str
                ISSN to look up. Defaults to requesting from user input.
            bibliographic : str
                a combined search. Searches for titles, abstracts, authors, publishers, dates etc. Defaults to None.
            title : str
                searches for titles containing string. Defaults to None.
            author : str
                searches for authors containing string. Defaults to None.
            author_affiliation : str
                searches for author affiliations containing string. Defaults to None.
            editor : str
                searches for editor names containing string. Defaults to None.
            entry_type : str
                searches for types of entries containing string. Defaults to None.
            published_date : str
                searches for matching publication dates. Defaults to None.
            doi : str
                searches for matching DOIs.
            issn : str
                searches for matching ISSNs.
            publisher_name : str
                searches for publisher names containing string. Defaults to None.
            funder_name : str
                searches for funder names containing string. Defaults to None.
            source : str
                searches for sources (e.g. journals, books) containing string. Defaults to None.
            link : str
                searches for entry links containing string. Defaults to None.
            sample : int
                optional: select which results to return.
            limit : int
                optional: set a limit to the number of results returned.
            rate_limit : float
                time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
            timeout : int
                maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
            filter : dict
            select : list
            
            Returns
            -------
            df : pandas.DataFrame
                results from CrossRef API search.
    """

    if issn == 'request_input':
        issn = input('Journal issn: ')

    if bibliographic == None:
        bibliographic = ''
    
    if title != None:
        bibliographic = bibliographic + ', ' + str(title)

    if entry_type != None:
        bibliographic = bibliographic + ', ' + str(entry_type)

    if doi != None:
        bibliographic = bibliographic + ', ' + str(doi)

    if published_date != None:
        bibliographic = bibliographic + ', ' + str(published_date)
    
    if funder_name != None:
        bibliographic = bibliographic + ', ' + str(funder_name)
    
    if link != None:
        bibliographic = bibliographic + ', ' + str(link)

    if bibliographic == '':
        bibliographic = None  # type: ignore

    global my_etiquette
    journals = Journals(etiquette=my_etiquette, timeout=timeout)
    result = journals.works(issn)

    result = result.query(
                        bibliographic = bibliographic,
                        author = author,
                        affiliation = author_affiliation,
                        editor = editor,
                        publisher_name = publisher_name,
                        container_title = source
                        )
    
    if filter != None:

        filter_input = ''

        global filters
        for f in filters:
            if f in filter.keys():
                val = filter[f]
                filter_input = filter_input + f'{f}="{val}", '
        
        filter_input = filter_input.strip(', ').strip()
        
        filter_code = f'result.filter({filter_input})'
        result = exec(filter_code)
    
    if select != None:

        select_input = ''
        for i in select:
            select_input = select_input + f'"{i}", '
        select_input = select_input.strip(', ').strip()

        select_code = f'result.select({select_input})'
        result = exec(select_code)
    try:
        print(f'{result.count()} results found') # type: ignore
    except:
        pass

    items = []
    if result != None:

        iteration = 1
        for item in result:  # type: ignore

            if limit != None:
                if iteration > limit:
                    break

            items.append(item)

            iteration += 1

            sleep(rate_limit)
    
    df = items_to_df(items)

    return df

def lookup_funder(funder_id = 'request_input', timeout = 60):

    """
        Looks up a funder using the CrossRef API. Returns a Pandas DataFrame.

        Parameters
        ----------
        funder_id : str
            CrossRef Funder ID to look up. Defaults to requesting from user input.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.

        Returns
        -------
        output : pandas.DataFrame
            funder records.
    """

    if funder_id == 'request_input':
        funder_id = input('Funder ID: ')

    global my_etiquette
    funders = Funders(etiquette=my_etiquette, timeout=timeout)
    result = funders.funder(funder_id)

    if (result != None) and (type(result) == dict):
        output = pd.DataFrame(columns = list(result.keys()), dtype=object)

        for key in result.keys():

                data = result[key]
                if type(data) == dict:
                    data = list(data.keys()) + list(data.values())
                    if len(data) == 1:
                        data = data[0]

                output.at[0, key] = data
    else:
        output = pd.DataFrame(dtype=object)

    return output

def lookup_funders(funder_ids: list = [], rate_limit: float = 0.05, timeout = 60):

    """
        Looks up a list of funders using the CrossRef API. Returns a Pandas DataFrame.

        Parameters
        ----------
        funder_ids : list
            list of CrossRef Funder IDs to look up. Defaults to an empty list.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.

        Returns
        -------
        output : pandas.DataFrame
            funder records.
    """

    global my_etiquette
    funders = Funders(etiquette=my_etiquette, timeout=timeout)

    output = pd.DataFrame(dtype=object)

    for id in funder_ids:

        result = funders.funder(id)
        results = pd.DataFrame(columns = list(item.keys()), dtype=object)
        index = len(results)

        item = {}
        if (result != None) and (type(result) == dict):
            for key in result.keys():
                data = result[key]
                if type(data) == dict:
                    data = list(data.keys()) + list(data.values())
                    if len(data) == 1:
                        data = data[0]

                results.at[index, key] = data
        
        output = pd.concat([output, results])
        

        sleep(rate_limit)

    output = output.reset_index().drop('index', axis=1)

    return output

def search_funders(*args, limit: int = 1000, rate_limit: float = 0.05, timeout = 60):

    """
        Searches CrossRef API for funder records and returns the results as a Pandas DataFrame.

        Parameters
        ----------
        *args
            search fields.
        limit : int
            optional: set a limit to the number of results returned. Defaults to 1000.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        
        Returns
        -------
        output : pandas.DataFrame
            results from CrossRef API search.
    """

    global my_etiquette
    funders = Funders(etiquette=my_etiquette, timeout=timeout).query(*args)

    output = pd.DataFrame()

    if funders != None:

        iteration = 1
        try:
            for item in funders:

                if limit != None:
                    if iteration > limit:
                        break
                
                results = pd.DataFrame(columns = list(item.keys()), dtype=object)
                index = len(results)

                for key in set(item.keys()):
                    data = item[key]
                    if type(data) == dict:
                        data = list(data.keys()) + list(data.values())
                        if len(data) == 1:
                            data = data[0]

                    results.at[index, key] = data

                output = pd.concat([output, results])

                iteration += 1

                sleep(rate_limit)
        except Exception as e:

            raise e
    
    output = output.reset_index().drop('index', axis=1)

    return output
    
def get_funder_works(funder_id = 'request_input',
                        filter: dict = None, # type: ignore
                        select: list = None, # type: ignore
                        sample: int = None, # type: ignore
                        limit: int = 1000,
                        rate_limit: float = 0.05,
                        timeout = 60):

    """
        Looks up a funder using the CrossRef API and returns associated publications as a Pandas DataFrame.

        Parameters
        ----------
        funder_id : str
            CrossRef Funder ID to look up. Defaults to requesting from user input.
        sample : int
            optional: select which results to return.
        limit : int
            optional: set a limit to the number of results returned.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        filter : dict
        select : list

        Returns
        -------
        df : pandas.DataFrame
            publication records.
    """

    if funder_id == 'request_input':
        funder_id = input('Funder ID: ')

    global my_etiquette
    funders = Funders(etiquette=my_etiquette, timeout=timeout)
    result = funders.works(funder_id)

    if filter != None:

        filter_input = ''

        global filters
        for f in filters:
            if f in filter.keys():
                val = filter[f]
                filter_input = filter_input + f'{f}="{val}", '
        
        filter_input = filter_input.strip(', ').strip()
        
        filter_code = f'result.filter({filter_input})'
        result = exec(filter_code)
    
    if select != None:

        select_input = ''
        for i in select:
            select_input = select_input + f'"{i}", '
        select_input = select_input.strip(', ').strip()

        select_code = f'result.select({select_input})'
        result = exec(select_code)

    try:
        print(f'{result.count()} results found') # type: ignore
    except:
        pass

    items = []
    if result != None:

        iteration = 1
        for item in result: # type: ignore

            if limit != None:
                if iteration > limit:
                    break

            items.append(item)

            iteration += 1

            sleep(rate_limit)
    
    df = items_to_df(items)

    return df

def search_funder_works(funder_id = 'request_input',
                        bibliographic: str = None, # type: ignore
                        title: str = None, # type: ignore
                        author: str = None, # type: ignore
                        author_affiliation: str = None, # type: ignore
                        editor: str = None, # type: ignore
                        entry_type: str = None, # type: ignore
                        published_date: str = None, # type: ignore
                        doi: str = None, # type: ignore
                        publisher_name: str = None, # type: ignore
                        funder_name = None, # type: ignore
                        source: str = None, # type: ignore
                        link: str = None, # type: ignore
                        filter: dict = None, # type: ignore
                        select: list = None, # type: ignore
                        sample: int = None, # type: ignore
                        limit: int = 1000,
                        rate_limit: float = 0.05,
                        timeout = 60):
    
    """
        Searches for publications associated with a funder using the CrossRef API.

        Parameters
        ----------
        funder_id : str
            CrossRef Funder ID to look up. Defaults to requesting from user input.
        bibliographic : str
            a combined search. Searches for titles, abstracts, authors, publishers, dates etc. Defaults to None.
        title : str
            searches for titles containing string. Defaults to None.
        author : str
            searches for authors containing string. Defaults to None.
        author_affiliation : str
            searches for author affiliations containing string. Defaults to None.
        editor : str
            searches for editor names containing string. Defaults to None.
        entry_type : str
            searches for types of entries containing string. Defaults to None.
        published_date : str
            searches for matching publication dates. Defaults to None.
        doi : str
            searches for matching DOIs.
        issn : str
            searches for matching ISSNs.
        publisher_name : str
             searches for publisher names containing string. Defaults to None.
        funder_name : str
            searches for funder names containing string. Defaults to None.
        source : str
            searches for sources (e.g. journals, books) containing string. Defaults to None.
        link : str
            searches for entry links containing string. Defaults to None.
        sample : int
            optional: select which results to return.
        limit : int
            optional: set a limit to the number of results returned.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        filter : dict
        select : list
        
        Returns
        -------
        df : pandas.DataFrame
            results from CrossRef API search.
        """

    if funder_id == 'request_input':
        funder_id = input('Funder ID: ')

    if bibliographic == None:
        bibliographic = ''
    
    if title != None:
        bibliographic = bibliographic + ', ' + str(title)

    if entry_type != None:
        bibliographic = bibliographic + ', ' + str(entry_type)

    if doi != None:
        bibliographic = bibliographic + ', ' + str(doi)

    if published_date != None:
        bibliographic = bibliographic + ', ' + str(published_date)
    
    if funder_name != None:
        bibliographic = bibliographic + ', ' + str(funder_name)
    
    if link != None:
        bibliographic = bibliographic + ', ' + str(link)

    if bibliographic == '':
        bibliographic = None # type: ignore

    global my_etiquette
    funders = Funders(etiquette=my_etiquette, timeout=timeout)
    result = funders.works(funder_id)

    result = result.query(
                        bibliographic = bibliographic,
                        author = author,
                        affiliation = author_affiliation,
                        editor = editor,
                        publisher_name = publisher_name,
                        container_title = source
                        )
    
    if filter != None:

        filter_input = ''

        global filters
        for f in filters:
            if f in filter.keys():
                val = filter[f]
                filter_input = filter_input + f'{f}="{val}", '
        
        filter_input = filter_input.strip(', ').strip()
        
        filter_code = f'result.filter({filter_input})'
        result = exec(filter_code)
    
    if select != None:

        select_input = ''
        for i in select:
            select_input = select_input + f'"{i}", '
        select_input = select_input.strip(', ').strip()

        select_code = f'result.select({select_input})'
        result = exec(select_code)

    try:
        print(f'{result.count()} results found') # type: ignore
    except:
        pass

    items = []
    if result != None:

        iteration = 1
        for item in result: # type: ignore

            if limit != None:
                if iteration > limit:
                    break

            items.append(item)

            iteration += 1

            sleep(rate_limit)
    
    df = items_to_df(items)

    return df
