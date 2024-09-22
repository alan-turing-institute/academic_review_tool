from .scopus import search as search_scopus
# from .wos import search as search_wos
from .crossref import search_works as search_crossref
from .orcid import search as search_orcid

import pandas as pd
import numpy as np

def search(default_query = None,
                    all_fields = None,
                    title = None,
                    year = None,
                    author = None,
                    author_identifier = None,
                    entry_type: str = None, # type: ignore
                    affiliation = None,
                    editor = None,
                    publisher = None,
                    funder = None,
                    abstract = None,
                    keywords = None,
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
                    topics = None,
                    default_operator = 'AND',
                    limit_per_api: int = 20,
                    rate_limit: float = 0.05,
                    timeout = 60,
                    crossref = True,
                    scopus = True,
                    wos = True,
                    orcid = False
                    ):
    
    """
        Searches multiple APIs and returns the results as a Pandas DataFrame. API options:
            * CrossRef
            * Scopus
            * Web of Science (WoS)
            * ORCID

        Parameters
        ----------
        default_query : str
            a combined search. Searches for titles, abstracts, authors, publishers, dates etc. Defaults to None.
        all_fields : str
            Scopus only: searches all fields. Defaults to None.
        title : str
            searches for titles containing string. Defaults to None.
        year : str
            searches for matching publication years. Defaults to None.
        author : str
            searches for authors containing string. Defaults to None.
        author_identifier : str
            searches for API-specific author IDs (e.g. CrossRef, Scopus, WoS, Orcid). Defaults to None.
        entry_type : str
            searches for types of entries containing string. Defaults to None.
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
        doi : str
            searches for matching DOIs.
        issn : str
            searches for matching ISSNs.
        isbn : str
            searches for matching ISBNs. Defaults to None.
        pubmed_id : str
            searches for matching PubMed IDs (PMIDs). Defaults to None.
        source_title : str
            searches for sources with titles (e.g. journals, books) containing string. Defaults to None.
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
        topics : str
            searches for entries tagged with matching topic names and keywords. Defaults to None.
        default_operator : str
            the default Boolean operator to build searches. Defaults to 'AND'.
        limit_per_api : int
            sets limits for the number of results to return per API. Used to limit impact on API servers. Defaults to 20.
        rate_limit : float
            CrossRef only: time delay in seconds per result. Used to limit impact on API servers. Defaults to 0.05 seconds.
        timeout : int
            CrossRef only: maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        crossref : bool
            whether to search using the CrossRef API. Defaults to True.
        scopus : bool
            whether to search using the Scopus API. Defaults to True.
        wos : bool
            whether to search using the Web of Science (WoS) API. Defaults to False.
        orcid : bool
            whether to search using the ORCID API. Defaults to False.
        
        Returns
        -------
        df : pandas.DataFrame
            combined results from API searches.
    """

    df = pd.DataFrame(dtype=object)

    if crossref == True:
        try:
            cr_result = search_crossref(
                    bibliographic = default_query, # type: ignore
                    title = title, # type: ignore
                    author = author, # type: ignore
                    author_affiliation = affiliation, # type: ignore
                    editor = editor, # type: ignore
                    entry_type = entry_type, # type: ignore
                    published_date = year, # type: ignore
                    doi = doi, # type: ignore
                    issn = issn, # type: ignore
                    publisher_name = publisher, # type: ignore
                    funder_name = funder,
                    source = source_title, # type: ignore
                    link = link, # type: ignore
                    limit = limit_per_api,
                    rate_limit = rate_limit,
                    timeout = timeout)
            
            cr_result['repository'] = 'crossref'

            df = pd.concat([df, cr_result])
            df = df.reset_index().drop('index',axis=1)
        except Exception as e:
            print(f'Encountered Crossref search error: {e}')
            pass
    
    if scopus == True:
        try:
            scopus_result = search_scopus(tile_abs_key_auth = default_query,
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
                        doctype = entry_type,
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
                        default_operator = default_operator)

            scopus_result['repository'] = 'scopus'

            df = pd.concat([df, scopus_result])
            df = df.reset_index().drop('index',axis=1)
        except Exception as e:
            print(f'Encountered Scopus search error: {e}')
            pass
    
    # if wos == True:

        # if (all_fields is None) and (default_query is not None):
        #     all_fields_updated = default_query
        # else:
        #     all_fields_updated = all_fields

        # try:
        #     wos_result = search_wos(
        #         all_fields = all_fields_updated,
        #         title = title,
        #         year = year,
        #         author = author,
        #         author_identifier = author_identifier,
        #         affiliation = affiliation,
        #         doctype = entry_type,
        #         doi = doi,
        #         issn = issn,
        #         isbn = isbn,
        #         pubmed_id = pubmed_id,
        #         source_title = source_title,
        #         volume = volume,
        #         page = page,
        #         issue = issue,
        #         topics = topics,
        #         default_operator = default_operator,
        #         limit = limit_per_api)

        #     wos_result['repository'] = 'WOK'

        #     df = pd.concat([df, wos_result])
        #     df = df.reset_index().drop('index',axis=1)
        # except Exception as e:
        #     print(f'Encountered Web of Science search error: {e}')
        #     pass
    
    if orcid == True:

        if (all_fields is None) and (default_query is not None):
            all_fields_updated = default_query
        else:
            all_fields_updated = all_fields

        try:
            orcid_result = search_orcid(query = all_fields_updated, # type: ignore
                limit = limit_per_api)

            if type(orcid_result) == pd.DataFrame:
                orcid_result['repository'] = 'ORCID'
                df = pd.concat([df, orcid_result])
                df = df.reset_index().drop('index',axis=1)
        except Exception as e:
            print(f'Encountered ORCID search error: {e}')
            pass
    
    df = df.dropna(axis=0, how='all').dropna(axis=1, how='all').reset_index().drop('index', axis=1)

    return df