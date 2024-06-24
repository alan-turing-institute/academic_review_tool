"""Functions to load and parse JSTOR database files"""

from ..utils.basics import results_cols
from ..utils.cleaners import is_int
from ..internet.webanalysis import is_url

from time import sleep

from crossref.restful import Works, Journals, Funders, Etiquette
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

my_etiquette = Etiquette('Academic Review Tool (ART)', '0.01', 'https://github.com/alan-turing-institute/academic-review-tool', 'academic_review_tool@outlook.com')

def items_to_df(items: list) -> pd.DataFrame:

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
            doi = i['DOI']
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

        if 'reference' in i.keys():
            citations_data = i['reference']
        else:
            citations_data = None

        if 'URL' in i.keys():
            link = i['URL']
        else:
            link = None
        
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
        df.at[index, 'citations_data'] = citations_data
        df.loc[index, 'link'] = link
    
    return df

def reference_to_df(reference: dict) -> pd.DataFrame:

    keys = list(reference.keys())

    df_data = {}

    if 'DOI' in keys:

        try:
            doi = reference['DOI']
            df = lookup_doi(doi)
            return df
        
        except:
            pass
    
    if 'URL' in keys:

        if 'doi.org/' in reference['URL']:
            doi = reference['URL']

            try:
                df = lookup_doi(doi)
                return df
            
            except:
                pass

    if 'unstructured' in keys:

        unstr = reference['unstructured'].split('. ')

        df_data['title'] = None
        df_data['authors'] = unstr[0]
        df_data['date'] = None
        df_data['link'] = None

        for i in unstr:
            
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


def references_to_df(references_list: list) -> pd.DataFrame:

    df = pd.DataFrame(columns = results_cols, dtype=object)

    for i in references_list:

        row = reference_to_df(i)
        df = pd.concat([df, row])
    
    df = df.reset_index().drop('index', axis=1)

    try:
        df = df.drop('doi-asserted-by', axis=1).drop('key', axis=1)
    except:
        pass

    return df

def search_works(
                bibliographic: str = None,
                title: str = None,
                author: str = None,
                author_affiliation: str = None,
                editor: str = None,
                entry_type: str = None,
                published_date: str = None,
                DOI: str = None,
                ISSN: str = None,
                publisher_name: str = None,
                funder_name = None,
                source: str = None,
                link: str = None,
                filter: dict = None,
                select: list = None,
                sample: int = None,
                limit: int = None,
                rate_limit: float = 0.1,
                timeout = 60
                ) -> pd.DataFrame:

    if bibliographic == None:
        bibliographic = ''
    
    if title != None:
        bibliographic = bibliographic + ', ' + str(title)

    if entry_type != None:
        bibliographic = bibliographic + ', ' + str(entry_type)

    if DOI != None:
        bibliographic = bibliographic + ', ' + str(DOI)

    if ISSN != None:
        bibliographic = bibliographic + ', ' + str(ISSN)
    
    if published_date != None:
        bibliographic = bibliographic + ', ' + str(published_date)
    
    if funder_name != None:
        bibliographic = bibliographic + ', ' + str(funder_name)
    
    if link != None:
        bibliographic = bibliographic + ', ' + str(link)

    if bibliographic == '':
        bibliographic = None

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

    print(f'{result.count()} results found')

    items = []
    if result != None:

        iteration = 1
        for item in result:

            if limit != None:
                if iteration > limit:
                    break

            items.append(item)

            iteration += 1

            sleep(rate_limit)
    
    df = items_to_df(items)

    return df

def lookup_doi(doi = 'request_input', timeout = 60):

    if doi == 'request_input':
        doi = input('DOI: ')

    global my_etiquette
    works = Works(etiquette=my_etiquette, timeout=timeout)
    
    result = works.doi(doi)

    item = [result]

    df = items_to_df(item)

    return df

def lookup_dois(dois_list: list = [], rate_limit: float = 0.1, timeout = 60):

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

    if issn == 'request_input':
        issn = input('Journal ISSN: ')

    global my_etiquette
    journals = Journals(etiquette=my_etiquette, timeout=timeout)
    result = journals.journal(issn)

    return pd.DataFrame.from_dict(result, orient='index').T

def lookup_journals(issns_list: list = [], rate_limit: float = 0.1, timeout = 60):

    global my_etiquette
    journals = Journals(etiquette=my_etiquette, timeout=timeout)

    output = pd.DataFrame(dtype=object)

    for issn in issns_list:
        result = journals.journal(issn)
        df = pd.DataFrame.from_dict(result, orient='index').T
        output = pd.concat([output, df])
        sleep(rate_limit)

    return output

def search_journals(*args, limit: int = None, rate_limit: float = 0.1, timeout = 60):

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
                        filter: dict = None,
                        select: list = None,
                        sample: int = None,
                        limit: int = None,
                        rate_limit: float = 0.1,
                        timeout = 60):

    if issn == 'request_input':
        issn = input('Journal ISSN: ')

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

    print(f'{result.count()} results found')

    items = []
    if result != None:

        iteration = 1
        for item in result:

            if limit != None:
                if iteration > limit:
                    break

            items.append(item)

            iteration += 1

            sleep(rate_limit)
    
    df = items_to_df(items)

    return df

def search_journal_entries(issn = 'request_input',
                        bibliographic: str = None,
                        title: str = None,
                        author: str = None,
                        author_affiliation: str = None,
                        editor: str = None,
                        entry_type: str = None,
                        published_date: str = None,
                        DOI: str = None,
                        publisher_name: str = None,
                        funder_name = None,
                        source: str = None,
                        link: str = None,
                        filter: dict = None,
                        select: list = None,
                        sample: int = None,
                        limit: int = None,
                        rate_limit: float = 0.1,
                        timeout = 60):
    

    if issn == 'request_input':
        issn = input('Journal ISSN: ')

    if bibliographic == None:
        bibliographic = ''
    
    if title != None:
        bibliographic = bibliographic + ', ' + str(title)

    if entry_type != None:
        bibliographic = bibliographic + ', ' + str(entry_type)

    if DOI != None:
        bibliographic = bibliographic + ', ' + str(DOI)

    if published_date != None:
        bibliographic = bibliographic + ', ' + str(published_date)
    
    if funder_name != None:
        bibliographic = bibliographic + ', ' + str(funder_name)
    
    if link != None:
        bibliographic = bibliographic + ', ' + str(link)

    if bibliographic == '':
        bibliographic = None

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

    print(f'{result.count()} results found')

    items = []
    if result != None:

        iteration = 1
        for item in result:

            if limit != None:
                if iteration > limit:
                    break

            items.append(item)

            iteration += 1

            sleep(rate_limit)
    
    df = items_to_df(items)

    return df

def lookup_funder(funder_id = 'request_input', timeout = 60):

    if funder_id == 'request_input':
        funder_id = input('Funder ID: ')

    global my_etiquette
    funders = Funders(etiquette=my_etiquette, timeout=timeout)
    result = funders.funder(funder_id)
    output = pd.DataFrame(columns = list(result.keys()), dtype=object)

    for key in result.keys():

            data = result[key]
            if type(data) == dict:
                data = list(data.keys()) + list(data.values())
                if len(data) == 1:
                    data = data[0]

            output.at[0, key] = data

    return output

def lookup_funders(funder_ids: list = [], rate_limit: float = 0.1, timeout = 60):

    global my_etiquette
    funders = Funders(etiquette=my_etiquette, timeout=timeout)

    output = pd.DataFrame(dtype=object)

    for id in funder_ids:

        result = funders.funder(id)
        results = pd.DataFrame(columns = list(item.keys()), dtype=object)
        index = len(results)

        item = {}
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

def search_funders(*args, limit: int = None, rate_limit: float = 0.1, timeout = 60):

    global my_etiquette
    funders = Funders(etiquette=my_etiquette, timeout=timeout).query(*args)

    output = pd.DataFrame()

    if funders != None:

        iteration = 1
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
    
    output = output.reset_index().drop('index', axis=1)

    return output
    
def get_funder_works(funder_id = 'request_input',
                        filter: dict = None,
                        select: list = None,
                        sample: int = None,
                        limit: int = None,
                        rate_limit: float = 0.1,
                        timeout = 60):

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

    print(f'{result.count()} results found')

    items = []
    if result != None:

        iteration = 1
        for item in result:

            if limit != None:
                if iteration > limit:
                    break

            items.append(item)

            iteration += 1

            sleep(rate_limit)
    
    df = items_to_df(items)

    return df

def search_funder_works(funder_id = 'request_input',
                        bibliographic: str = None,
                        title: str = None,
                        author: str = None,
                        author_affiliation: str = None,
                        editor: str = None,
                        entry_type: str = None,
                        published_date: str = None,
                        DOI: str = None,
                        publisher_name: str = None,
                        funder_name = None,
                        source: str = None,
                        link: str = None,
                        filter: dict = None,
                        select: list = None,
                        sample: int = None,
                        limit: int = None,
                        rate_limit: float = 0.1,
                        timeout = 60):
    

    if funder_id == 'request_input':
        funder_id = input('Funder ID: ')

    if bibliographic == None:
        bibliographic = ''
    
    if title != None:
        bibliographic = bibliographic + ', ' + str(title)

    if entry_type != None:
        bibliographic = bibliographic + ', ' + str(entry_type)

    if DOI != None:
        bibliographic = bibliographic + ', ' + str(DOI)

    if published_date != None:
        bibliographic = bibliographic + ', ' + str(published_date)
    
    if funder_name != None:
        bibliographic = bibliographic + ', ' + str(funder_name)
    
    if link != None:
        bibliographic = bibliographic + ', ' + str(link)

    if bibliographic == '':
        bibliographic = None

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

    print(f'{result.count()} results found')

    items = []
    if result != None:

        iteration = 1
        for item in result:

            if limit != None:
                if iteration > limit:
                    break

            items.append(item)

            iteration += 1

            sleep(rate_limit)
    
    df = items_to_df(items)

    return df
