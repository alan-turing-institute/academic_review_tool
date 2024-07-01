from ..utils.basics import results_cols
from ..importers.crossref import lookup_doi
from ..internet.scrapers import get_final_url, scrape_url, scrape_article, can_scrape
from ..internet.crawlers import check_crawl_permission

from ..internet.crawlers import correct_seed_errors as correct_seed_url_errors
from .references import extract_references

import queue
import time

import pandas as pd
import numpy as np


def crawler_scrape_url(url):

    global results_cols
    return pd.Series(index=results_cols, dtype=object)

def citation_crawler_site_test(url: str):

    final_url = get_final_url(url)

    global can_scrape

    for i in can_scrape:
        if i in final_url:
            return True
    
    return False

def citation_crawler_scraper(entry: pd.Series, be_polite = True):
    
    url = entry['link']

    # Checking if URL is bad. If True, tries to correct it.
    url = correct_seed_url_errors(url)
        
    # If be_polite is True, checks if crawler has permission to crawl/scrape URL
    if be_polite == True:
        try:
            # If the crawler does not have permission, skips URL
            if check_crawl_permission(url) == False:
                return entry
        except:
            pass
    
    if citation_crawler_site_test(url) == True:

        try:
            res_df = scrape_article(url)
            if len(res_df) > 0: 
                res_series = res_df.loc[0]
                for i in res_series.index:
                    entry[i] = res_series[i]
        
        except:
            pass

    else:
        try:
            res = crawler_scrape_url(url)
        except:
            pass
    
    return entry
        
def citation_crawler_doi_retriver(entry: pd.Series, be_polite = True, timeout = 60):

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
    urls : queue 
        ordered queue of URLs to be crawled.
    required_keywords : list 
        list of keywords which sites must contain to be crawled.
    excluded_keywords : list 
        list of keywords which sites must *not* contain to be crawled.
    excluded_url_terms : list 
        list of strings; link will be ignored if it contains any string in list.
    case_sensitive : bool 
        whether or not to ignore string characters' case.
    crawl_limit : int 
        how many URLs the crawler should visit before it stops.
    ignore_urls : list 
        list of URLs to ignore.
    ignore_domains : list 
        list of domains to ignore.
    be_polite : bool 
        whether to respect websites' permissions for crawlers.
    full : bool 
        whether to run a full scrape on each site. This takes longer.
    
    
    Returns
    -------
    output_dict : dict 
        a dictionary containing results from each crawled site.
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
            entry = pd.Series(entry)


        # Formatting entry citations data
        refs = extract_references(entry['citations_data'], add_work_ids = True, update_from_doi = False)
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
        seeds : str or list 
            one or more URLs from which to crawl.
        crawl_limit : int 
            how many URLs the crawler should visit before it stops.
        excluded_url_terms : list 
            list of strings; link will be ignored if it contains any string in list.
        required_keywords : list 
            list of keywords which sites must contain to be crawled.
        excluded_keywords : list 
            list of keywords which sites must *not* contain to be crawled.
        case_sensitive : bool 
            whether or not to ignore string characters' case.
        ignore_urls : list 
            list of URLs to ignore.
        ignore_domains : list 
            list of domains to ignore.
        be_polite : bool 
            whether respect websites' permissions for crawlers.
        full : bool 
            whether to run a full scrape on each site. This takes longer.
        output_as : str 
            the format to output results in. Defaults to a pandas.DataFrame.
        
        
        Returns
        -------
        result : pd.DataFrame 
            an object containing the results of a crawl.
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

