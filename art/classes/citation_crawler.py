from ..utils.basics import results_cols
from ..importers.crossref import lookup_doi
from ..internet.scrapers import get_final_url, scrape_url, scrape_article, can_scrape, get_domain, scrape_google_scholar
from ..internet.crawlers import check_crawl_permission

from ..internet.crawlers import correct_seed_errors as correct_seed_url_errors
from .authors import format_authors
from .references import format_references

import queue
import time

import pandas as pd
import numpy as np

def crawler_scrape_url(url) -> pd.DataFrame:

    """
    Core functionality for the citation crawler's web scraper. Takes a URL and returns a Pandas DataFrame.
    """

    scrape_res = scrape_url(url=url)

    global results_cols
    result = pd.DataFrame(index = [0], columns=results_cols, dtype=object)

    keys = scrape_res.keys() # type: ignore

    if 'title' in keys:
        result.loc[0, 'title'] = scrape_res['title'] # type: ignore
    
    if 'author' in keys:
        auths = scrape_res['author'] # type: ignore
        if type(auths) == str:
            auths = auths.replace('[','').replace(']','').replace('{','').replace('}','').replace(';','').replace(', ',',').replace(' ,',',').split(',')
        result.at[0, 'authors'] = auths
        result.at[0, 'authors_data'] = auths

    if 'date' in keys:
        result.loc[0, 'date'] = scrape_res['date'] # type: ignore
    
    if ('sitename' in keys) and (scrape_res['sitename'] is not None) and (scrape_res['sitename'] != ''):  # type: ignore
            result.loc[0, 'source'] = scrape_res['sitename'] # type: ignore
    else:
        if ('url' in keys) and (scrape_res['url'] is not None) and (scrape_res['url'] != ''):  # type: ignore
            domain = get_domain(scrape_res['url']) # type: ignore
            result.loc[0, 'source'] = domain
    
    if ('type' in keys):
        result.loc[0, 'type'] = scrape_res['type'] # type: ignore
    else:
        if ('pagetype' in keys) and (scrape_res['pagetype'] is not None) and (scrape_res['pagetype'] != ''):  # type: ignore
            result.loc[0, 'type'] = scrape_res['pagetype'] # type: ignore
        else:
            result.loc[0, 'type'] = 'website'
    
    if ('publisher' in keys) and (scrape_res['publisher'] is not None) and (scrape_res['publisher'] != ''):  # type: ignore
        result.loc[0, 'publisher'] = scrape_res['publisher'] # type: ignore
    else:
        if ('sitename' in keys) and (scrape_res['sitename'] is not None) and (scrape_res['sitename'] != ''):  # type: ignore
            result.loc[0, 'publisher'] = scrape_res['sitename'] # type: ignore
        else:
            if 'url' in keys:
                domain = get_domain(scrape_res['url']) # type: ignore
                result.loc[0, 'publisher'] = domain
            else:
                result.loc[0, 'publisher'] = None
    
    if ('tags' in keys) and (scrape_res['tags'] is not None) and (scrape_res['tags'] != ''):  # type: ignore
        tags = scrape_res['tags'] # type: ignore
        tags_list = tags.replace('[','').replace(']','').replace('{','').replace('}','').replace(';','').replace(', ',',').replace(' ,',',').split(',')
        result.at[0, 'keywords'] = tags_list
    else:
        if ('categories' in keys) and (scrape_res['categories'] is not None) and (scrape_res['categories'] != ''):  # type: ignore
            cats = scrape_res['categories'] # type: ignore
            cats_list = cats.replace('[','').replace(']','').replace('{','').replace('}','').replace(';','').replace(', ',',').replace(' ,',',').split(',')
            result.at[0, 'keywords'] = cats_list
        else:
            result.at[0, 'keywords'] = []
    
    if ('description' in keys) and (scrape_res['description'] is not None) and (scrape_res['description'] != ''):  # type: ignore
        result.at[0, 'description'] = scrape_res['description'] # type: ignore

    if ('excerpt' in keys) and (scrape_res['excerpt'] is not None) and (scrape_res['excerpt'] != ''):  # type: ignore
        result.at[0, 'extract'] = scrape_res['excerpt'] # type: ignore
    
    if ('text' in keys) and (scrape_res['text'] is not None) and (scrape_res['text'] != ''):  # type: ignore
        result.at[0, 'full_text'] = scrape_res['text'] # type: ignore
    else:
        if ('raw_text' in keys) and (scrape_res['raw_text'] is not None) and (scrape_res['raw_text'] != ''):  # type: ignore
            result.at[0, 'full_text'] = scrape_res['raw_text'] # type: ignore
        else:
            if ('html' in keys) and (scrape_res['html'] is not None) and (scrape_res['html'] != ''):  # type: ignore
                result.at[0, 'full_text'] = scrape_res['html'] # type: ignore
    
    if ('links' in keys) and (scrape_res['links'] is not None) and (scrape_res['links'] != '') and (scrape_res['links'] != []): # type: ignore

        citations = scrape_res['links'] # type: ignore

        if type(citations) == str:
            citations = citations.replace('[','').replace(']','').replace('{','').replace('}','').replace(';','').replace(', ',',').replace(' ,',',').split(',')
        
        result.at[0, 'citations'] = citations
        result.at[0, 'citations_data'] = citations
        result.loc[0, 'citation_count'] = len(citations)
    
    if ('language' in keys) and (scrape_res['language'] is not None) and (scrape_res['language'] != ''):  # type: ignore
        result.at[0, 'language'] = scrape_res['language'] # type: ignore

    if ('url' in keys) and (scrape_res['url'] is not None) and (scrape_res['url'] != ''):  # type: ignore
        result.loc[0, 'link'] = scrape_res['url'] # type: ignore
    
    return result

def citation_crawler_site_test(url: str):

    """
    Checks whether the citation crawler can crawl a given URL. Returns True if yes; False if no.
    """

    global can_scrape

    for i in can_scrape:
        if i in url:
            return True
    
    return False

def academic_scraper(url, be_polite = False):

    """
    Bespoke web scraper for academic repository websites.

    Parameters
    ----------
    url : str
        a URL to scrape.
    be_polite : bool
        whether to follow respect scraping permissions contained in websites' robots.txt files.
    
    Returns
    -------
    res_df : pandas.DataFrame
        a Pandas DataFrame containing scraped web data.
    """

    # Checking if URL is bad. If True, tries to correct it.
    url = correct_seed_url_errors(url)
    domain = get_domain(url)
        
    # If be_polite is True, checks if crawler has permission to crawl/scrape URL
    if be_polite == True:
        if domain != 'acm.org': # ACM blanket refuses crawler permissions
            try:
                # If the crawler does not have permission, skips URL
                if check_crawl_permission(url) == False:
                    return pd.DataFrame(columns=results_cols)
            except:
                pass
    
    if 'scholar.google.com' in url:

        try:
            res_df = scrape_google_scholar(url)
        except:
            res_df = pd.DataFrame(columns=results_cols)

    else:
        if citation_crawler_site_test(url) == True:

            try:
                res_df = scrape_article(url)
            except:
                res_df = pd.DataFrame(columns=results_cols)

        else:
            try:
                res_df = crawler_scrape_url(url)
            except:
                res_df = pd.DataFrame(columns=results_cols)
    
    return res_df

def citation_crawler_scraper(entry: pd.Series, be_polite = True):
    
    """
    Bespoke web scraper for use by citation crawler.

    Parameters
    ----------
    entry : pandas.Series
        citation crawler entry.
    be_polite : bool
        whether to follow respect scraping permissions contained in websites' robots.txt files.
    
    Returns
    -------
    entry : pandas.Series
        citation crawler entry.
    """

    url = entry['link']

    res_df = academic_scraper(url=url, be_polite=be_polite)
    
    if len(res_df) > 0: 
                res_series = res_df.loc[0]
                for i in res_series.index:
                    entry[i] = res_series[i]
    
    return entry
        
def citation_crawler_doi_retriver(entry: pd.Series, be_polite = True, timeout = 60):

    """
    Takes citation crawler entry. If it contains a DOI, looks up the record using the CrossRef API. If not, scrapes the URL.

    Parameters
    ----------
    entry : pandas.Series
        citation crawler entry.
    be_polite : bool
        whether to follow respect scraping permissions contained in websites' robots.txt files.
    timeout : int
        maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
    
    Returns
    -------
    entry : pandas.Series
        citation crawler entry.
    """

    doi = entry['doi']
    link = entry['link']

    if type(doi) == str:
        try:
            res_df = lookup_doi(doi, timeout=timeout)

            if len(res_df) > 0:
                res_series = res_df.loc[0]
                for i in res_series.index:
                    entry[i] = res_series[i]
            
            return entry
    
        except:
                doi = doi.replace('https://', '').replace('http://', '').replace('dx.', '').replace('doi.org/', '')
                doi = 'https://doi.org' + doi
                
                try:
                    entry['link'] = doi
                    return citation_crawler_scraper(entry, be_polite = be_polite)
                except:

                    try:
                        entry['link'] = link
                        return citation_crawler_scraper(entry, be_polite = be_polite)
                    except:

                        try:
                            return crawler_scrape_url(doi)
                        except:
                            return entry
        
    else:
        if link != None:
            try:
                    return citation_crawler_scraper(entry, be_polite = be_polite)
            except:
                    try:
                        return crawler_scrape_url(link)
                    except:
                        return entry
            
        else:
            return entry

def update_citation_crawler_data(entry: pd.Series, be_polite = True, timeout = 60):

    """
    Takes citation crawler entry and updates the data using the CrossRef API if a record is available.

    Parameters
    ----------
    entry : pandas.Series
        citation crawler entry.
    be_polite : bool
        whether to follow respect scraping permissions contained in websites' robots.txt files.
    timeout : int
        maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
    
    Returns
    -------
    entry : pandas.Series
        citation crawler entry.
    """

    doi = entry['doi']
    link = entry['link']

    if (doi != None) and (doi != 'None') and (doi != ''):
        try:
            return citation_crawler_doi_retriver(entry, be_polite = be_polite, timeout = timeout)
        except:
            return citation_crawler_scraper(entry, be_polite = be_polite)

    else:
        if (link != None) and (type(link) == str) and ('doi.org' in link):
            doi = link.replace('https://', '').replace('http://', '').replace('dx.', '').replace('doi.org/', '')
            return citation_crawler_doi_retriver(entry, be_polite = be_polite, timeout = timeout)
        
        else:
            if link != None:
                
                if type(link) == dict:
                    link = list(link.values())

                if type(link) == list:
                    link = link[0]
                
                if type(link) == str:
                    return citation_crawler_scraper(entry, be_polite = be_polite)
            
            else:
                return entry

def citation_crawler_engine(
                    to_crawl,
                    data: pd.DataFrame,
                    use_api: bool,
                    crawl_limit, 
                    depth_limit,
                    be_polite = True,
                    rate_limit = 0.05,
                    timeout = 60
                ):
    
    """
    Core functionality for citation crawler. Takes inputted review entries, parses their citations, retrieves data, adds to results, and repeats. and returns a dataframe of results.
    
    Parameters
    ---------- 
    to_crawl : queue 
        records to crawl.
    data : pandas.DataFrame
        a dataframe of data gathered by the crawler.
    use_api : bool
        whether to lookup entries and update their data using APIs. Required for the crawler to find new and add new data. Defaults to True.
    crawl_limit : int 
        how many URLs the crawler should visit before it stops.
    depth_limit : int
        maximum number of crawler iterations to perform.
    be_polite : bool 
        whether to respect websites' permissions for crawlers.
    rate_limit : float
        time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
    timeout : int
        maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
    
    Returns
    -------
    data : pandas.DataFrame 
        a Pandas DataFrame containing results from the crawl.
    """
    
    # Intiailising variables to store the pages already visited
    data = data.copy(deep=True)
    
    crawled_entries = []
    iteration = 1
    
    added_in_cycle = 0
    processed_in_cycle = 0
    depth_marker = len(data)
    depth = 1

    # until all pages have been visited
    
    while (not to_crawl.empty() == True) and (depth <= depth_limit) and (len(crawled_entries) <= crawl_limit):
        
        # Checking if crawler has crawled the maximum number of entries. If so, halts.
        if len(crawled_entries) > crawl_limit:
            print('\nCrawl limit reached')
            break
            
        # Checking if crawler has reached the depth limit. If so, halts.
        if depth > depth_limit:
            print('\nDepth limit reached')
            break
        
        # Getting the entry index to process from the queue
        _, current_index = to_crawl.get()
        
        # Checking if entry index has already been processed. If True, skips.
        if current_index in crawled_entries:
            continue
        
        old_len = len(data)

        # Retreiving entry data
        entry = pd.Series(data.loc[current_index])
        
        if use_api == True:
        # Checking if the entry has a valid DOI. If yes, updating data in the entry using Crossref API. If not, updating data using web scraper.
            entry = update_citation_crawler_data(entry, be_polite = be_polite, timeout = timeout)
            entry = pd.Series(entry)  # type: ignore


        # Formatting entry citations data
        refs = format_references(entry['citations_data'], add_work_ids = True, update_from_doi = False)
        entry.at['citations'] = refs

        # Formatting entry authors data
        entry.at['authors'] = format_authors(entry['authors']) # type: ignore

        data.loc[current_index] = entry
        refs_df = refs.copy(deep=True) # type: ignore
        data = pd.concat([data, refs_df]).reset_index().drop('index', axis=1) # type: ignore

        # Adding current current index to list of indexes already processed
        crawled_entries.append(current_index)

        new_len = len(data)
        new_indexes = list(range(old_len, new_len))

        added_in_cycle = added_in_cycle + len(refs_df)


        for i in new_indexes:
            priority_score = 0.0001
            to_crawl.put((priority_score, i))
        
        
        # Pausing crawler for a brief interval to reduce server loads and avoid being blocked
        time.sleep(rate_limit) 
        
        # Displaying status to user

        if iteration > depth_marker:

            # print(f'\nCrawl depth {depth}:\n    - Entries processed: {processed_in_cycle}\n    - Results added: {added_in_cycle}')

            depth += 1
            added_in_cycle = 0
            processed_in_cycle = 0
            depth_marker = len(data)
            

        # Incrementing iteration count
        iteration += 1
        processed_in_cycle += 1

    # Updating newly added entries
    if use_api == True:
        index = set(data.index)
        not_crawled = list(index.difference(set(crawled_entries)))
        for i in not_crawled:
            entry = data.loc[i]
            entry = update_citation_crawler_data(entry, be_polite = be_polite, timeout = timeout)
            data.loc[i] = entry

    return data

def citation_crawler(
                    data: pd.DataFrame,
                    use_api: bool = True,
                    crawl_limit: int = 5, 
                    depth_limit: int = 2,
                    be_polite: bool = True,
                    rate_limit: float = 0.05,
                    timeout: int = 60
                    ) -> pd.DataFrame:
    
        """
        Crawls results, their citations, and so on.
        
        The crawler iterates through queue of works; extracts their citations; runs checks to validate each reference;
        based on these, selects a source to retrieve data from: (a) Crossref API (if has a valid DOI); (b) bespoke web scraping for 
        if so, scrapes them and adds links found to queue.
        
        Parameters
        ---------- 
        data : pandas.DataFrame
            a dataframe of data gathered by the crawler.
        use_api : bool
            whether to lookup entries and update their data using APIs. Required for the crawler to find new and add new data. Defaults to True.
        crawl_limit : int 
            how many records the crawler should visit before it stops.
        depth_limit : int
            maximum number of crawler iterations to perform.
        be_polite : bool 
            whether to respect websites' permissions for crawlers.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        
        Returns
        -------
        output : pd.DataFrame 
            an object containing the results from the crawl.
        """

        # See https://www.zenrows.com/blog/web-crawler-python#transitioning-to-a-real-world-web-crawler

        seeds = data.index.to_list()
        
        # Storing seed indexes to crawl in a specific order
        to_crawl = queue.PriorityQueue()
        
        # Queing seeds with highest priority
        for seed in seeds:
            to_crawl.put((0.0001, seed))
        
        # Running crawler engine
        output = citation_crawler_engine(
                    to_crawl,
                    data,
                    use_api,
                    crawl_limit, 
                    depth_limit,
                    be_polite,
                    rate_limit,
                    timeout
                )
        
        
        
        # Printing end status for user
        print('\n\n----------------------\nCrawl complete\n----------------------')
        
        return output

