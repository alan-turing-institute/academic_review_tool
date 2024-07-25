from .scopus import search as search_scopus
from .wos import search as search_wos
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
    
    if wos == True:

        if (all_fields is None) and (default_query is not None):
            all_fields_updated = default_query
        else:
            all_fields_updated = all_fields

        try:
            wos_result = search_wos(
                all_fields = all_fields_updated,
                title = title,
                year = year,
                author = author,
                author_identifier = author_identifier,
                affiliation = affiliation,
                doctype = entry_type,
                doi = doi,
                issn = issn,
                isbn = isbn,
                pubmed_id = pubmed_id,
                source_title = source_title,
                volume = volume,
                page = page,
                issue = issue,
                topics = topics,
                default_operator = default_operator,
                limit = limit_per_api)

            wos_result['repository'] = 'WOK'

            df = pd.concat([df, wos_result])
            df = df.reset_index().drop('index',axis=1)
        except Exception as e:
            print(f'Encountered Web of Science search error: {e}')
            pass
    
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