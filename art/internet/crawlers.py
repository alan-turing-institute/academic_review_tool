"""Functions for crawling web data"""

from ..utils.basics import map_inf_to_1
from ..text.textanalysis import cosine_sim
from .webanalysis import correct_url, get_domain
from .scrapers import scrape_google_search, iterate_scholar_pages, crawler_scraper

import requests
import queue
import time
import random

import numpy as np
import pandas as pd

from Levenshtein import distance as lev

import itertools

from trafilatura import feeds, sitemaps
from trafilatura.spider import focused_crawler

import urllib
from urllib import robotparser

from courlan import clean_url, scrub_url, is_external

from igraph import Graph as Graph


def is_external_link(source_url: str = 'request_input', linked_url: str = 'request_input', ignore_suffix: bool = False) -> bool:
    
    """
    Checks if provided link directs to a URL which is external to a source's website.
    
    Parameters
    ----------
    source_url : str
        URL to check. Defaults to requesting from user input.
    linked_url : str
        link to check. Defaults to requesting from user input.
    ignore_suffix : bool
        whether to ignore the site's suffix. Defaults to False.
    
    Returns
    -------
    result : bool
        whether the link is internal or not.
    """
    
    # Requesting source URL from user input if none given
    if source_url == 'request_input':
        source_url = input('Source URL: ')
        
    # Requesting linked URL from user input if none given
    if linked_url == 'request_input':
        linked_url = input('Linked URL: ')
    
    # Correcting URLs if needed (e.g., adding missing HTTPS prefix)
    source_url = correct_url(source_url)
    linked_url = correct_url(linked_url)
    
    # Checking if link is external
    return is_external(source_url, linked_url, ignore_suffix=ignore_suffix)

def fetch_feed_urls(url: str = 'request_input') -> list:
    
    """
    Retrieves URLs from web feed.
    
    Parameters
    ----------
    url : str
        URL to fetch from. Defaults to requesting from user input.
    
    Returns
    -------
    result : list
        web feed result as a list.
    """
    
    # Requesting source URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting URL if needed (e.g., adding missing HTTPS prefix)
    url = correct_url(url)
    
    # Retrieving URLs from feed
    out_list = feeds.find_feed_urls(url)
    
    return out_list

def fetch_sitemap(url: str = 'request_input') -> list:
    
    """
    Retrieves website sitemap. Returns a list of URLs in sitemap.
    
    Parameters
    ----------
    url : str
        URL to fetch from. Defaults to requesting from user input.
    
    Returns
    -------
    result : list
        sitemap as a list of URLs.
    """
    
    # Requesting source URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting URL if needed (e.g., adding missing HTTPS prefix)
    url = correct_url(url)
    
    # Fetching sitemap
    result = sitemaps.sitemap_search(url)
    
    return result

def fetch_url_rules(url = 'request_input'):
    
    """
    Retrieves website's rules for robots from robots.txt.
    
    Parameters
    ----------
    url : str
        URL to fetch from. Defaults to requesting from user input.
    
    Returns
    -------
    result : RobotFileParser
        Robots.txt ruleset.
    """
    
    # Requesting source URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting URL if needed (e.g., adding missing HTTPS prefix)
    url = correct_url(url)
    
    # Loading the necessary components; fetching and parsing the file
    rules = robotparser.RobotFileParser()
    rules.set_url(url + '/robots.txt')
    rules.read()
    
    return rules

def check_crawl_permission(url: str = 'request_input') -> bool:
    
    """
    Checks if web crawler has permission to crawl website.
    
    Parameters
    ----------
    url : str
        URL to check. Defaults to requesting from user input.
    
    Returns
    -------
    result : bool
        whether the URL allows for crawlers.
    """
    
    # Retrieving site's rules from robots.txt
    rules = fetch_url_rules(url = url)

    # Determining if a page can be fetched by all crawlers
    result = rules.can_fetch("*", "https://www.example.org/page1234.html")
    # returns True or False
    
    return result

def check_bad_url(url: str = 'request_input') -> bool:
    
    """
    Checks if URL leads to a real website.
    
    Parameters
    ----------
    url : str
        URL to check. Defaults to requesting from user input.
    
    Returns
    -------
    result : bool
        whether the URL is a bad link (True == bad).
    """
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting URL if needed (e.g., adding missing HTTPS prefix)
    url = correct_url(url)
    
    # Trying to make request to URL. If this is successful, URL is not bad; returns False.
    try:
        requests.get(url)
        return False
    
    except:
        
        return True
    
def append_domain(url: str, domain: str) -> str:
    
    """
    Appends domain to the start of a URL.
    
    Parameters
    ----------
    url : str
        URL to append domain to
    domain : str
        domain to append.
    
    Returns
    -------
    result : str
        the fixed URL.
    """
    
    # Correcting URL if needed (e.g., adding missing HTTPS prefix)
    url = correct_url(url)
    
    # Trying to make request to cleaned URL. If successful, no further action is necessary
    try:
        requests.get(url)
        return url
    
    # If request fails, appending domain to start of URL and checking if this is now a good URL.
    except:
        
        url = domain + url
        
        # Checking if URL is bad
        is_bad = check_bad_url(url)
        
        # If not, append has been successful
        if is_bad == False:
            return url
        else:
            raise ValueError('URL may not be a valid domain')
        
def crawl_site(url: str = 'request_input', max_seen_urls: int = 10, max_known_urls: int = 100000) -> list:
    
    """
    Crawls website's internal pages. Returns any links found as a list.
    
    Parameters
    ---------- 
    url : str 
        URL of the website to crawl.
    max_seen_urls : int 
        the number of pages the crawler is allowed to see before stopping.
    max_known_urls : int 
        the number of URLs the crawler is allowed to find before stopping.

    Returns
    -------
    result : list 
        a list of URLs found.
    """
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting URL if needed (e.g., adding missing HTTPS prefix)
    url = correct_url(url)

    # Starting crawl
    to_visit, known_urls = focused_crawler(url, max_seen_urls=max_seen_urls, max_known_urls=max_known_urls)
    
    
    # Formatting results
    to_visit, known_urls = list(to_visit), sorted(known_urls)
    visit_set = set(to_visit)
    known_set = set(known_urls)
    
    # Combining set of URLs visited with URLs found
    total_urls = list(visit_set.union(known_set))
    
    return total_urls

def correct_link_errors(url: str, source_domain = None) -> str:
    
    """
    Checks for errors in a link and corrects them. Returns a corrected link as a string.
    
    Parameters
    ---------- 
    url : str 
        URL to correct.
    source_domain : str 
        the domain or URL where the link was found.

    Returns
    -------
    url : str 
        a corrected URL.
    """
    
    # Source domain defaults to None. Changing this to an empty string for type handling.
    if source_domain == None:
        source_domain = ''
    
    # Ensuring source_domain is a domain, not a full URL
    source_domain = get_domain(source_domain)
    
    # Checking if domain is valid and retrieving domain if so
    domain_check = get_domain(url)
    
    # If domain check fails
    if (domain_check == '') or (domain_check == None) or ('.' not in domain_check):
        
        # Checking if URL is bad. If true, cleaning URL
        if check_bad_url(url) == True:

            url = url.strip().strip('/').strip()
            url = clean_url(url)
            url = scrub_url(url)
            
            # Checking if URL is still bad. If true, appending source domain to start of URL
            if check_bad_url(url) == True:
                url = source_domain + '/' + url
            
            # Correcting URL if needed (e.g., adding missing HTTPS prefix)
            url = correct_url(url)
    
    return url

def correct_seed_errors(url: str) -> str:
    
    """Checks for and corrects errors in a URL to be used as a crawler seed. Returns a corrected link as a string.
    
    Parameters
    ---------- 
    url : str 
        URL to correct.

    Returns
    -------
    url : str 
        a corrected URL."""
    
    # Checking for URL's domain
    domain_check = get_domain(url)
    
    # Proceeding if domain check fails 
    if (domain_check == '') or (domain_check == None) or ('.' not in domain_check):
        
        # Proceeding if URL is a bad link 
        if check_bad_url(url) == True:
            
            # If URL is bad, applying corrections
            url = url.strip().strip('/').strip()
            url = clean_url(url)
            url = scrub_url(url)
            url = correct_url(url)
    
    return url

def select_crawled_links(
                        iteration: int,
                         source_domain: str, 
                         link_elements, 
                         urls: queue.PriorityQueue, 
                         visited_urls: list, 
                         ignore_urls: list, 
                         ignore_domains: list,
                         excluded_url_terms: list
                        ) -> tuple:
    
    """
    Selects links to crawl from set of crawler scraper results. To be used by web crawler. Returns a tuple containing links and URLs.
    
    Parameters
    ---------- 
    iteration : int 
        how many iterations the crawler engine has run.
    source_domain : str 
        domain of URL currently being crawled.
    link_elements : list
        iterable containing URL's links as HTML elements.
    urls : queue 
        ordered queue of URLs to be crawled.
    visited_urls : list 
        list of URLs already visited.
    ignore_urls : list 
        list of URLs to ignore.
    ignore_domains : 
        list list of domains to ignore.
    excluded_url_terms : list 
        list of strings; link will be ignored if it contains any string in list.

    Returns
    -------
    result : tuple 
        a tuple containing the updated URLs queue and any new links found.
    """
    
    # Initialising links list
    links = []
    
    # Iterating through link lements to extract links
    for link_element in link_elements:
        
        # Cleaning link elements
        url = link_element
        url = url.strip('/').strip('.').strip().lower()
        
        try:
            url = correct_link_errors(url = url, source_domain = source_domain)
        except:
            pass

        try:
            domain = get_domain(url)
        except:
            domain = ''
        
        # Appending link to list of links found
        links.append(url)
        
        # Checking if the URL does not include an excluded term
        exclude_test = False
        for term in excluded_url_terms:
            term = term.lower()
            url_check = url.lower()
                
            if (term in url_check) == True:
                exclude_test = True
        
        # If the URL does not an excluded term, selects link to be added to queue
        if exclude_test != True:
            
            # Proceeds if the URL is not included in the ignore list
            if (
                (url != None) 
                and (url not in ignore_urls) 
                and (
                    (domain != '') 
                    and (domain not in ignore_domains)
                    )

            ):
                # Creating variations on URL to check if already visited
                no_http = url.replace('https://', '').replace('http://', '')
                www_added = 'www.'+ no_http
                www_https_added = 'https://' + www_added
                www_removed = no_http.replace('www', '')
                www_removed_https_added = 'https://' + www_removed
                
                # Proceeds if URL or a variation on it has not been visited already
                if (
                    (url not in visited_urls) 
                    and (no_http not in visited_urls)
                    and (www_added not in visited_urls)
                    and (www_https_added not in visited_urls)
                    and (www_removed not in visited_urls)
                    and (www_removed_https_added not in visited_urls)
                    and (url not in [item[1] for item in urls.queue])
                ):
                    
                    # Setting default priority score as the current crawler's iteration number. 
                    # This queues them in order of crawl depth.
                    priority_score = iteration
                    
                    # Incrementing priority score to queue links in order of discovery.
                    # Increment is very small to ensure 
                    # that the depth-first ranking of discovered links is not overriden.
                    priority_score += 0.001
                    
                    # Prioritising shorter urls. Uses the normalised string length of the url, 
                    # where length of 0 -> 0 and an infinite length -> 0.1
                    
                    url_len = len(url)
                    normalised_len = map_inf_to_1(url_len) / 10
                    priority_score += normalised_len
                    
                    # Adding link to URLs queue with assigned priority score
                    urls.put((priority_score, url))

    return (urls, links)

def excluded_term_test(current_url: str, excluded_url_terms: list, case_sensitive: bool) -> bool:
    
    """
    Checks if URL contains terms to be excluded. To be used by web crawler.
    
    Parameters
    ---------- 
    current_url : str 
        link to check.
    excluded_url_terms : list 
        list of strings to check for.
    case_sensitive : bool 
        whether to be case sensitive when checking for terms.
    
    Returns
    -------
    result : bool 
        True if the URL contains a term in excluded_url_terms list.
    """
    
    # Initialising output variable; defaulting to False
    excluded_term_test = False
    
    # Iterating through excluded terms to check in URL
    for term in excluded_url_terms:
            
            # If instructed to not be case sensitive, turning all strings to lowercase
            if case_sensitive == False:
                term = term.lower()
                url_check = str(current_url.lower())
            
            # Returning True if term is found
            if (term in url_check) == True:
                return True
        
def required_keywords_test(text, required_keywords, case_sensitive):
    
    """
    Checks if text contains a set of required keywords. Returns True if it does. To be used by web crawler.
    
    Parameters
    ---------- 
    text : str 
        text to check.
    required_keywords : list 
        list of strings to check for.
    case_sensitive : bool 
        whether to be case sensitive when checking for terms.
    
    Returns
    -------
    result : bool 
        True if the text contains a term in required_keywords list.
    """
    
    # Initialising output variable; defaulting to True
    result = True
    
    # Iterating through required terms to check in text
    for required_word in required_keywords:
            
            # Checking for None types; changing to empty string to avoid errors
            if text == None:
                    text = ''
            
            # If instructed to not be case sensitive, turning all strings to lowercase
            if case_sensitive == False:
                required_word = required_word.lower()
                text = text.lower()
            
            # Returning False if term is *not* found
            if required_word not in text:
                result = False
    
    return result

def excluded_keywords_test(text, excluded_keywords, case_sensitive):
    
    """
    Checks if text contains a set of excluded keywords. Returns True if it does not. To be used by web crawler.
    
    Parameters
    ---------- 
    text : str 
        text to check.
    excluded_keywords : list 
        list of strings to check for.
    case_sensitive : bool 
        whether to be case sensitive when checking for terms.
    
    Returns
    -------
    result : bool 
        True if the text contains a term in excluded_keywords list.
    """
    
    # Initialising output variable; defaulting to True
    result = True
    
    # Iterating through excluded terms to check in text
    for excluded_word in excluded_keywords:
            
            # Checking for None types; changing to empty string to avoid errors
            if text == None:
                    text = ''
            
            # If instructed to not be case sensitive, turning all strings to lowercase
            if case_sensitive == False:
                excluded_word = excluded_word.lower()
                text = text.lower()
            
            # Returning False if term is found
            if excluded_word in text:
                result = False
    
    return result

def crawler_engine(
                    urls,
                    required_keywords, 
                    excluded_keywords,
                    excluded_url_terms,
                    case_sensitive,
                    visit_limit, 
                    ignore_urls, 
                    ignore_domains,
                    be_polite, 
                    full
                ):
    
    """
    Core functionality for web crawler. Takes inputted URLs, scrapes them, and returns a dictionary of results.
    
    Iterates through queue of URLS; runs checks to see if should proceed with scraping; 
    if so, scrapes them and adds links found to queue.
    
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
    visit_limit : int 
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
    visited_urls = []
    output_dict = {}
    iteration = 1
    
    # until all pages have been visited
    
    while not urls.empty():
        
        # Checking if crawler has visited the maximum number of URls. If so, halts.
        if len(visited_urls) > visit_limit:
            print('\nLimit reached')
            break
        
        # Getting the URL to visit from the queue
        _, current_url = urls.get()
        
        # Cleaning URL
        current_url = current_url.strip('/').strip('.').strip()
        
        # Checking if URL has been visited. If True, skips.
        if current_url in visited_urls:
            continue
        
        # Checking if URL includes an excluded term. If True, skips URL
        if excluded_term_test(current_url, excluded_url_terms, case_sensitive) == True:
            continue
        
        # Checking if URL is bad. If True, tries to correct it.
        if check_bad_url('current_url') == True:
            current_url = correct_seed_errors(current_url)
        
        # If be_polite is True, checks if crawler has permission to crawl/scrape URL
        if be_polite == True:
            try:
                # If the crawler does not have permission, skips URL
                if check_crawl_permission(current_url) == False:
                    continue
            except:
                pass
        
        # Initialising result variable
        crawl_res = None
        
        # Trying to scrape URL
        try:
            
            # Scraping URL and retrieving links
            crawl_res = crawler_scraper(current_url, full)
            scraped_links = crawl_res[2]
            
            # Appending results to result dictionary
            output_dict[current_url] = crawl_res[1]
            
            # Retrieving domain
            domain = get_domain(current_url)
        
        # If scrape fails, sets current_url to None
        except:
            # Appending results to result dictionary
            output_dict[current_url] = crawl_res
            continue
        
        # Extracting raw text from site scrape result
        if type(crawl_res) == tuple:
            text = crawl_res[1]['raw_text']
        
        elif crawl_res == None:
            text = ''
        
        
        if (
            # Skips if the scraped data does not contain required keywords
            (required_keywords_test(text, required_keywords, case_sensitive) == False) 
            # Skips if the scraped data contains excluded keywords
            or (excluded_keywords_test(text, excluded_keywords, case_sensitive) == False)
        ):
            continue
        
        # Adding current URL to list of URLs already visited
        visited_urls.append(current_url)
        
        # Incrementing iteration count
        iteration += 1
        
        # Selecting links to crawl from URL scrape result
        links_res = select_crawled_links(
                            iteration = iteration,
                            source_domain = domain,
                            link_elements = scraped_links, 
                            urls = urls, 
                            visited_urls = visited_urls,
                            ignore_urls = ignore_urls, 
                            ignore_domains = ignore_domains,
                            excluded_url_terms = excluded_url_terms
                            )
            
        urls = links_res[0]
        current_links = list(set(links_res[1]))
        
        # Appending results to result dictionary
        output_dict[current_url]['domain'] = domain
        output_dict[current_url]['links'] = current_links
        
        # Pausing crawler for a random interval to reduce server loads and avoid being blocked
        time.sleep(random.uniform(0.25, 1)) 
        
        # Displaying status to user
        print(f'\nSite visited: {current_url}')
        print(f'Visited count: {len(visited_urls)}')

    return output_dict

def seed_str_to_list(seed_urls: str) -> list:
    
    """
    Takes a string of seed URLs in list format and returns a list object. To be used by web crawler.
    
    Parameters
    ---------- 
    seed_urls : str 
        string containing seed URLs.
    
    Returns
    -------
    seed_urls : list 
        List of seed urls.
    """
    
    obj_type = type(seed_urls)
    
    # Checking type. If list, no action taken
    if (obj_type == list) or (obj_type == set) or (obj_type == tuple):
        seed_urls = list(seed_urls)
    
    # If string, cleaning data and splitting into list
    elif obj_type == str:
        
        seed_urls = seed_urls.replace('[', '').replace(']', '').replace('"', '').replace("'", '')
        
        if (type(seed_urls) == str) and (', ' in seed_urls):
            seed_urls = seed_urls.split(', ')

        if (type(seed_urls) == str) and (',' in seed_urls):
            seed_urls = seed_urls.split(',')

        if (type(seed_urls) == str) and (';' in seed_urls):
            seed_urls = seed_urls.split(';')

        if type(seed_urls) == str:
            seed_urls = [seed_urls]
    
    # Raising an error if conversion has failed
    if type(seed_urls) != list:
        raise TypeError(f'seed_urls must be a string, list, set, or tuple, not {obj_type}')
    
    return seed_urls

def clean_seed_urls(seed_urls: list) -> list:
    
    """
    Cleans list of seed URLs. To be used by web crawler.
    
    Parameters
    ---------- 
    seed_urls : list 
        list containing seed URLs.
    
    Returns
    -------
    seed_urls : list 
        List of cleaned seed urls.
    """
    
    # If input is string, set, or tuple, converting to list
    seed_urls = seed_str_to_list(seed_urls)
    
    # Initialising output variable
    cleaned_seeds = []
    
    # Iterating through seeds and cleaning
    for seed in seed_urls:
        seed = seed.strip()
        seed = correct_seed_errors(seed)
        seed = correct_url(seed)
        cleaned_seeds.append(seed)
    
    return cleaned_seeds

def crawler(
            seed_urls: str = 'request_input',
            visit_limit: int = 5, 
            excluded_url_terms: str = 'default',
            required_keywords: bool = None, 
            excluded_keywords: bool = None, 
            case_sensitive: bool = False,
            ignore_urls: list = None, 
            ignore_domains: list = 'default',
            be_polite: bool = True,
            full: bool = True,
            output_as: str = 'dataframe'
            ):
    
    """
    General purpose web crawler. Crawls from a single URL or list of URLs.
    
    The crawler iterates through queue of URLS; runs checks to see if should proceed with scraping; 
    if so, scrapes them and adds links found to queue.
    
    Parameters
    ---------- 
    seed_urls : str or list 
        one or more URLs from which to crawl.
    visit_limit : int 
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
    result : object 
        an object containing the results of a crawl.
    """
    
    # See https://www.zenrows.com/blog/web-crawler-python#transitioning-to-a-real-world-web-crawler

    # Requesting seed URLS from user input if none provided
    if seed_urls == 'request_input':
        seed_urls = input('Start from: ')
    
    # If seed_urls are not in a Python list object, converting them to one
    if type(seed_urls) != list:
        seed_urls = seed_str_to_list(seed_urls)
    
    # Cleaning seed URLs to avoid errors
    seed_urls = clean_seed_urls(seed_urls)
    
    # If ignore_domains set to default, loads a preset list of domains to ignore
    if ignore_domains == 'default':
        ignore_domains = [
                        'ad.doubleclick.net'
                        ]
    # If no ignore_urls given, sets an empty list
    if ignore_urls == None:
        ignore_urls = []
    
    # If excluded_url_terms set to default, loads a preset terms of domains to exclude
    if excluded_url_terms == 'default':
        excluded_url_terms = [
                            'advertise',
                            'doubleclick',
                            'advertising',
                            'cloudflare',
                            'holidays',
                            'app.',
                            'ctdonate',
                            'sitemap',
                            'jobs.',
                            'policies.google',
                            'adsense'
                            ]
        
    # If no required_keywords given, sets an empty list
    if required_keywords == None:
        required_keywords = []
    
    # If no excluded_keywords given, sets an empty list
    if excluded_keywords == None:
        excluded_keywords = []
    
    # Storing the URLs discovered to visit in a specific order
    urls = queue.PriorityQueue()
    
    # Queing seeds with highest priority
    for seed in seed_urls:
        urls.put((0.0001, seed))
    
    # Running crawler engine
    output = crawler_engine(
                    urls = urls,
                    visit_limit = visit_limit, 
                    excluded_url_terms = excluded_url_terms,
                    required_keywords = required_keywords, 
                    excluded_keywords = excluded_keywords, 
                    case_sensitive = case_sensitive,
                    ignore_urls = ignore_urls, 
                    ignore_domains = ignore_domains,
                    be_polite = be_polite,
                    full = full
                    )
    
    # Printing end status for user
    print('\n\n----------------------\nCrawl complete\n----------------------')
    
    # If result is to be outputted as a dataframe, converting results dictionary to pandas.DataFrame
    if output_as == 'dataframe':
        
        if (len(output.keys()) > 0) and (list(output.values()) != [None]):
        
            df = pd.DataFrame().from_dict(output).T
            df.index.name = 'URL'
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
        
        else:
            
            df = pd.DataFrame(columns = ['title',
                                             'author',
                                             'hostname',
                                             'date',
                                             'fingerprint',
                                             'id',
                                             'license',
                                             'comments',
                                             'raw_text',
                                             'text',
                                             'language',
                                             'image',
                                             'pagetype',
                                             'source',
                                             'source-hostname',
                                             'excerpt',
                                             'categories',
                                             'tags',
                                             'html',
                                             'url',
                                             'description',
                                             'sitename',
                                             'body',
                                             'commentsbody',
                                             'links'
                                        ]
                                     )
        
        # Formatting dataframe
        df = df.astype(object)
        df = df.replace(np.nan, None).where(df.notnull(), None)
        output = df
    
    return output

def crawl_web(
            seed_urls: str = 'request_input',
            visit_limit: int = 5, 
            excluded_url_terms: str = 'default',
            required_keywords: bool = None, 
            excluded_keywords: bool = None, 
            case_sensitive: bool = False,
            ignore_urls: list = None, 
            ignore_domains: list = 'default',
            be_polite: bool = True,
            full: bool = True,
            output_as: str = 'dataframe'
            ):
    
    
    """
    Crawls internet from a single URL or list of URLs. Returns details like links found,  HTML scraped, and site metadata.
    
    Parameters
    ---------- 
    seed_urls : str or list 
        one or more URLs from which to crawl.
    visit_limit : int 
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
    result : object 
        an object containing the results of a crawl.
    """
    
    
    return crawler(
                    seed_urls = seed_urls,
                    visit_limit = visit_limit, 
                    excluded_url_terms = excluded_url_terms,
                    required_keywords = required_keywords, 
                    excluded_keywords = excluded_keywords, 
                    case_sensitive = case_sensitive,
                    ignore_urls = ignore_urls, 
                    ignore_domains = ignore_domains,
                    be_polite = be_polite,
                    full = full,
                    output_as = output_as
                    )

def crawl_from_search(
            query: str = 'request_input',
            visit_limit: int = 5, 
            excluded_url_terms: list = 'default',
            required_keywords: list = None, 
            excluded_keywords: list = None, 
            case_sensitive: bool= False,
            ignore_urls: list = None, 
            ignore_domains: list = 'default',
            be_polite: bool = True,
            full: bool = True,
            output_as: str = 'dataframe'
            ):
    
    """
    Crawls internet from a web search.
    
    Returns details like links found,  HTML scraped, and site metadata.
    
    Parameters
    ---------- 
    query : str 
        query to search.
    visit_limit : int 
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
    result : object 
        an object containing the results of a crawl.
    """
    
    # Launching web search and retrieving results
    df = scrape_google_search(query = query)
    
    # Initialising seed list for crawler
    urls = df['URL'].to_list()
    
    # Running web crawler
    return crawler(
                    seed_urls = urls,
                    visit_limit = visit_limit, 
                    excluded_url_terms = excluded_url_terms,
                    required_keywords = required_keywords, 
                    excluded_keywords = excluded_keywords, 
                    case_sensitive = case_sensitive,
                    ignore_urls = ignore_urls, 
                    ignore_domains = ignore_domains,
                    be_polite = be_polite,
                    full = full,
                    output_as = output_as
                    )

def crawl_google_scholar(
                            query = 'request_input',
                            page_limit = 20,
                            by_citations = True,
                            by_recommended = True,
                            crawl_depth = 3, 
                            crawl_limit = 100,
                            discovery_limit = 1000, 
                            select_keywords = None, 
                            exclude_keywords = None, 
                            case_sensitive = False, 
                            ):

    """
    Crawls from a Google Scholar search. Returns details like links found,  HTML scraped, and site metadata in a Pandas DataFrame.
    
    Parameters
    ---------- 
    query : str
        query to search Google Scholar. Defaults to requesting from user input.
    page_limit : int
        maximum number of Google Scholar results pages to scrape. Defaults to 20.
    by_citations : bool
        whether to crawl citations. Defaults to True.
    by_recommended : bool
        whether to crawl Google Scholar recommendation links. Defaults to True.
    crawl_depth : int
        the maximum crawl depth the crawler will reach before stopping. Defaults to 3.
    crawl_limit : int
        the maximum number of websites the crawler will visit before stopping. Defaults to 100.
    discovery_limit : int
        the maximum number of results the crawler will discover before stopping. Defaults to 1000.
    select_keywords : list
        list of keywords which sites must contain to be crawled.
    exclude_keywords : list
        list of keywords which sites must *not* contain to be crawled.
    case_sensitive : bool 
        whether or not to ignore string characters' case.
    
    Returns
    -------
    df : pandas.DataFrame 
        a Pandas DataFrame containing the results of a crawl.
    """

    if query == 'request_input':
        query = input('Search query: ')

    if type(query) != str:
        raise TypeError('Query must be a string')
    
    if select_keywords != None:
        keyword_selection = True
        
        if type(select_keywords) is not list:
            select_keywords = [select_keywords]
        
        if case_sensitive == False:
            select_keywords = list(np.char.lower(np.array(select_keywords)))
    
    else:
        keyword_selection = False
    
    if exclude_keywords != None:
        keyword_exclusion = True
        
        if type(exclude_keywords) is not list:
            exclude_keywords = [exclude_keywords]
        
        if case_sensitive == False:
            exclude_keywords = list(np.char.lower(np.array(exclude_keywords)))
        
    else:
        keyword_exclusion = False
    
    url_base = 'https://scholar.google.com/scholar?q='
    query = urllib.parse.quote_plus(query)
    url = url_base + query
    
    df = iterate_scholar_pages(scholar_page = url, page_limit = page_limit)
    
    if len(df.index) == 0:
        raise ValueError('Initial search did not return results. Google Scholar may have blocked your device or IP address. Please check this and retry.')
    
    
    seed_list = []
    if by_citations == True:
        seed_list = seed_list + df['cited_by'].to_list()
        
    if by_recommended == True:
        seed_list = seed_list + df['recommendations'].to_list()
        
    crawl_count = 0
    links_found = 0
    iteration = 0
    num_items_found = len(df.index)
    
    for i in range(0, crawl_depth):
        
        iteration += 1
        
        next_seed_list = []
        
        for link in seed_list:
            
            if (crawl_count > crawl_limit) or (num_items_found > discovery_limit):
                print('Limit reached')
                print('Total items found: '+str(num_items_found))
                return df
            
            if (
                (link != None)
                and ('scholar.google' in link)
                and (check_bad_url(url = link) == False)
                ):
                
                response_df = iterate_scholar_pages(link, page_limit = page_limit)
                
                
                if len(response_df.index) > 0:
                    
                    if keyword_selection == True:
                        
                        for keyword in select_keywords:
                            response_df = response_df[(response_df['title'].str.contains(keyword) == True) | (response_df['extract'].str.contains(keyword) == True)]
                              

                    if keyword_exclusion == True:
                        
                        for keyword in exclude_keywords:
                            response_df = response_df[(response_df['title'].str.contains(keyword) == False) & (response_df['extract'].str.contains(keyword) == False)]
                              

                    if by_citations == True:
                        next_seed_list = next_seed_list + response_df['cited_by'].to_list()

                    if by_recommended == True:
                        next_seed_list = next_seed_list + response_df['recommendations'].to_list()
                
                
                    num_items_found = num_items_found + len(response_df.index)
                
                
                df = pd.concat([df, response_df])
                df = df.drop_duplicates('Link')
                df = df.reset_index().drop('index', axis=1)
                
                crawl_count += 1
                print('Crawl depth: '+str(iteration))
                print('Links crawled: '+str(crawl_count))
                print('Items found: '+str(num_items_found))
                print('- - - - - - - \n')
                
        seed_list = list(set(next_seed_list))
    
    
    print('Crawl complete')
    print('Total items found: '+str(num_items_found))
    
    df = df.drop_duplicates('Link')
    df = df.reset_index().drop('index', axis=1)
    
    return df

def network_from_crawl_result(crawl_df: pd.DataFrame) -> Graph:
    
    """
    Takes web crawler output and returns a network object.
    
    Crawled URLs become vertices; links between URLS become directed edges.
    
    Parameters
    ---------- 
    crawl_df : pandas.DataFrame 
        web crawler output to be converted to network.
    
    Returns
    -------
    network : igraph.Graph 
        a network representing the results of a web crawl.
    """
    
    # Retrieving list of links
    links_list = crawl_df.index.to_list()
    
    # Counting links
    links_n = len(links_list)
    
    # Initialising network object
    network = Graph(
                    n = links_n, 
                    directed = True, 
                    vertex_attrs = {
                                    'name': links_list,
                                    'type': 'website'
                                    }
                    )
    
    # Adding site data as vertices attributes
    for column in crawl_df.columns:
        network.vs[column] = crawl_df[column].to_list()
    
    # Adding edges by iterating through URLs (i.e. vertices)
    for source in network.vs:
        
        # Retrieving vertex index and name
        source_index = source.index
        source_name = source['name']
        
        # Checking if vertex is in the original list of URls crawled
        if source_index >= links_n:
            
            # If not in list, vertex will not link to other vertices 
            # as no links will have been scraped
            target_urls = []
        
        else:
            # Retrieving scraped links
            target_urls = crawl_df.loc[source_name, 'links']
            
            # If the result is a string, transforms to list to avoid errors
            if type(target_urls) == str:
                target_urls = [target_urls]
            
            # If the result is None, transforms to empty list to list to avoid errors
            if target_urls == None:
                target_urls = []
        
        # Iterating through each link to identify targets
        for url in target_urls:
            
            # If target URL not in network, adding vertex to network with URL as name
            if url not in network.vs['name']:
                network.add_vertex(name = url, type = 'website')
            
            # Finding vertex index of target URL
            target_index = network.vs.find(name = url)
            
            # Adding edge between source vertex and target vertex
            network.add_edge(source, 
                           target_index, 
                            type = 'link'
                          )
            
    return network

def network_from_crawl(seed_urls: str = 'request_input',
                            visit_limit: int = 5, 
                            excluded_url_terms: list = 'default',
                            required_keywords: list = None, 
                            excluded_keywords: list = None, 
                            case_sensitive: bool = False,
                            ignore_urls: list = None, 
                            ignore_domains: list = 'default',
                            be_polite: bool = True,
                            full: bool = True,
                            output_as: str = 'dataframe'
                            ) -> Graph:
    
    """
    Runs web crawl from one or more seed URLS and generates a directed network from the results.
    
    Crawled URLs become vertices; links between URLS become directed edges.
    
    Parameters
    ---------- 
    seed_urls : str or list 
        one or more URLs from which to crawl.
    visit_limit : int 
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
    
    Returns
    -------
    network : igraph.Graph 
        a network representing the results of a web crawl.
    """
    
    crawl_result = crawl_web(
            seed_urls = seed_urls,
            visit_limit = visit_limit, 
            excluded_url_terms = excluded_url_terms,
            required_keywords = required_keywords, 
            excluded_keywords = excluded_keywords, 
            case_sensitive = case_sensitive,
            ignore_urls = ignore_urls, 
            ignore_domains = ignore_domains,
            be_polite = be_polite,
            full = full,
            output_as = 'dataframe'
            )
    
    network = network_from_crawl_result(crawl_result)
    
    return network

def compare_sites(crawl_df: pd.DataFrame) -> pd.DataFrame:
    
    """
    Runs comparative analysis on web crawler results.  Compares every combination of the sites provided. 
    
    WARNING: can be slow if crawl is large.
    
    Parameters
    ---------- 
    crawl_df : pandas.DataFrame 
        web crawler output to be analysed.
    
    Returns
    -------
    output_df : pandas.DataFrame 
        dataframe of comparisons.
    
    Notes
    -----
        * title_distance : float Levenshtein distance between titles (0 if identical).
        * author_distance : float Levenshtein distance between author names (0 if identical).
        * hostname_distance : float Levenshtein distance between hostnames (0 if identical).
        * same_domain : bool whether sites share the same domain (True/False).
        * domain_distance : float Levenshtein distance between domains (0 if identical).
        * shared_links : set links shared by both sites.
        * date_difference (timedelta): time difference between site date entries.
        * same_language : bool whether sites share the same language (True/False).
        * domain_distance : float Levenshtein distance between domains (0 if identical).
        * fingerprint_distance : float Levenshtein distance between site fingerprints (0 if identical).
        * id_distance : float Levenshtein distance between site IDs (0 if identical).
        * license_distance : float Levenshtein distance between licenses (0 if identical).
        * comments_distance : float Levenshtein distance between comments (0 if identical).
        * raw_text_distance : float Levenshtein distance between raw texts (0 if identical).
        * raw_text_cosine : float Cosine similarity by word count of raw texts (1 if identical).
        * text_distance : float Levenshtein distance between parsed texts (0 if identical).
        * text_cosine : float Cosine similarity by word count of parsed texts (1 if identical).
        * source_distance : float Levenshtein distance between source names (0 if identical).
        * source-hostname_distance : float Levenshtein distance between source-hostname names (0 if identical).
        * source_distance : float Levenshtein distance between source code (0 if identical).
        * source_distance : float Levenshtein distance between source code (0 if identical).
        * excerpt_distance : float Levenshtein distance between excerpts (0 if identical).
        * excerpt_cosine : float Cosine similarity by word count of excerpts (1 if identical).
        * shared_categories : set category tags shared by both sites.
        * shared_tags : set tags shared by both sites.
        * html_distance : float Levenshtein distance between HTML source codes (0 if identical).
        * description_distance : float Levenshtein distance between descriptions (0 if identical).
        * description_cosine : float Cosine similarity by word count of descriptions (1 if identical).
        * sitename_distance : float Levenshtein distance between sitenames (0 if identical).
    """
    
    # Retrieving list of URLs to compare
    links_list = crawl_df.index.to_list()
    
    # Initialising output dataframe
    output_df = pd.DataFrame(index = list(itertools.combinations(links_list, 2)), columns = [
                'title_distance',  # Levenshtein distance (if not None)
                 'author_distance', # Levenshtein distance (if not None)
                 'hostname_distance',  # Levenshtein distance(if not None)
                'same_domain', # True/False
                 'domain_distance', # Levenshtein distance (if not None)
                 'shared_links', # Set intersection
                'shared_links_count', # Size of shared_links
                 'date_difference',  # Time delta (if not None)
                 'same_language', # True/False
                 'fingerprint_distance',  # Levenshtein distance (if not None)
                 'id_distance',  # Levenshtein distance (if not None)
                 'license_distance', # Levenshtein distance (if not None)
                 'comments_distance', # Levenshtein distance (if not None)
                 'raw_text_distance', # Levenshtein distance (if not None)
                'raw_text_cosine', # Cosine distance (if not None)
                 'text_distance', # Levenshtein distance (if not None)
                'text_cosine',  # Cosine distance (if not None)
                 'source_distance', # Levenshtein distance (if not None)
                 'source-hostname_distance',  # Levenshtein distance (if not None)
                 'excerpt_distance',  # Levenshtein distance (if not None)
                'excerpt_cosine', # Cosine distance (if not None)
                 'shared_categories', # Set intersection
                 'shared_tags', # Set intersection
                 'html_distance',   # Levenshtein distance (if not None)
                 'description_distance',    # Levenshtein distance (if not None)
                'description_cosine', # Cosine distance (if not None)
                 'sitename_distance'    # Levenshtein distance (if not None)
                ]
                            )
    
    # Iterating through columns
    for column in output_df.columns:
        
        # For columns requiring levenshtein distance, sets measure to 'lev' and records source column
        if '_distance' in column:
            measure = 'lev'
            source_column = column.split('_distance')[0]
        
        # For columns requiring cosine similarity, sets measure to 'cosine' and records source column
        if '_cosine' in column:
            measure = 'cosine'
            source_column = column.split('_cosine')[0]
        
        # For columns requiring set intersection, sets measure to 'intersection' and records source column
        if 'shared_' in column:
            measure = 'intersection'
            source_column = column.split('shared_')[1]
        
        # If column is 'shared_links_count', sets measure to 'shared_links_count' and records source column
        if column == 'shared_links_count':
            measure = 'shared_links_count'
            source_column = 'links'
        
        # If column is to check for identical data, sets measure to 'are_identical' and records source column
        if 'same_' in column:
            measure = 'are_identical'
            source_column = column.split('same_')[1]
        
        # For columns requiring timedelta, sets measure to 'cosine' and records source column
        if column == 'date_difference':
            measure = 'timedelta'
            source_column = 'date'
        
        # Ignores column names that are not in the dataframe
        if source_column not in crawl_df.columns:
            continue
        
        # Filling in output columns with required datatypes ahead of analysis
        output_df[column] = source_column
        
        # Iterating through URL combinations
        for i in output_df.index:
            
            # Retrieving URLs from index tuple
            url_1 = i[0]
            url_2 = i[1]
            
            # Retrieving crawl results for URLs
            url_1_data = crawl_df.loc[url_1, source_column]
            url_2_data = crawl_df.loc[url_2, source_column]
            
            # Does not run analysis if either crawl result is None
            if (url_1_data == None) or (url_2_data == None):
                result = None
            
            else:
                # Running levenshtein distance analysis if required
                if measure == 'lev':
                    result = lev(url_1_data, url_2_data)
                    
                # Running cosine similarity analysis if required
                if measure == 'cosine':
                    result = cosine_sim([url_1_data, url_2_data])
                
                # Running set intersection analysis if required
                if measure == 'intersection':
                    set_1 = set(url_1_data)
                    set_2 = set(url_2_data)
                    result = set_1.intersection(set_2)
                
                # Checking if data is identical if required
                if measure == 'are_identical':
                    result = url_1_data == url_2_data
                
                # Calculating time difference if required
                if measure == 'timedelta':
                    result = abs(url_1_data - url_2_data)
                
                # Calculating size of set intersection if required
                if measure == 'shared_links_count':
                    result = len(output_df.at[i, 'shared_links'])
            
            # Appending result to output dataframe
            output_df.at[i, column] = result
                

    return output_df

def site_similarities_from_crawl(
                                    seed_urls: str = 'request_input',
                                    measure: str = 'html_distance',
                                    visit_limit: int = 5, 
                                    excluded_url_terms: list = 'default',
                                    required_keywords: list = None, 
                                    excluded_keywords: list = None, 
                                    case_sensitive: bool = False,
                                    ignore_urls: list = None, 
                                    ignore_domains: list = 'default',
                                    be_polite: bool = True,
                                    full: bool = True
                                ) -> pd.DataFrame:
    
    """
    Runs web crawl from seed URL or URls, then runs a comparative analysis on results.  Compares every combination of site crawled. 
    
    WARNING: can be slow if crawl is large.
    
    Parameters
    ---------- 
    seed_urls : str or list 
        one or more URLs from which to crawl.
    visit_limit : int 
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
    
    Returns
    -------
    output_df : pandas.DataFrame
        dataframe of comparisons.
    
    Notes
    -----
        * title_distance : float Levenshtein distance between titles (0 if identical).
        * author_distance : float Levenshtein distance between author names (0 if identical).
        * hostname_distance : float Levenshtein distance between hostnames (0 if identical).
        * same_domain : bool whether sites share the same domain (True/False).
        * domain_distance : float Levenshtein distance between domains (0 if identical).
        * shared_links : set links shared by both sites.
        * date_difference (timedelta): time difference between site date entries.
        * same_language : bool whether sites share the same language (True/False).
        * domain_distance : float Levenshtein distance between domains (0 if identical).
        * fingerprint_distance : float Levenshtein distance between site fingerprints (0 if identical).
        * id_distance : float Levenshtein distance between site IDs (0 if identical).
        * license_distance : float Levenshtein distance between licenses (0 if identical).
        * comments_distance : float Levenshtein distance between comments (0 if identical).
        * raw_text_distance : float Levenshtein distance between raw texts (0 if identical).
        * raw_text_cosine : float Cosine similarity by word count of raw texts (1 if identical).
        * text_distance : float Levenshtein distance between parsed texts (0 if identical).
        * text_cosine : float Cosine similarity by word count of parsed texts (1 if identical).
        * source_distance : float Levenshtein distance between source names (0 if identical).
        * source-hostname_distance : float Levenshtein distance between source-hostname names (0 if identical).
        * source_distance : float Levenshtein distance between source code (0 if identical).
        * source_distance : float Levenshtein distance between source code (0 if identical).
        * excerpt_distance : float Levenshtein distance between excerpts (0 if identical).
        * excerpt_cosine : float Cosine similarity by word count of excerpts (1 if identical).
        * shared_categories : set category tags shared by both sites.
        * shared_tags : set tags shared by both sites.
        * html_distance : float Levenshtein distance between HTML source codes (0 if identical).
        * description_distance : float Levenshtein distance between descriptions (0 if identical).
        * description_cosine : float Cosine similarity by word count of descriptions (1 if identical).
        * sitename_distance : float Levenshtein distance between sitenames (0 if identical).
    """
    
    crawl_result = crawler(
                                    seed_urls = seed_urls,
                                    visit_limit = visit_limit, 
                                    excluded_url_terms = excluded_url_terms,
                                    required_keywords = required_keywords, 
                                    excluded_keywords = excluded_keywords, 
                                    case_sensitive = case_sensitive,
                                    ignore_urls = ignore_urls, 
                                    ignore_domains = ignore_domains,
                                    be_polite = be_polite,
                                    full = full,
                                    output_as = 'dataframe'
                                )
    
    comparisons_df = compare_sites(crawl_result)
    
    return comparisons_df

def site_similarity_network(comparisons_df: pd.DataFrame, measure: str = 'html_distance') -> Graph:
    
    """
    Takes site comparison result and returns a weighted network.
    
    Crawled URLs become vertices; similarity measures between URLs become undirected edges.
    Edge weights correspond to a chosen similarity measure.
    
    Parameters
    ---------- 
    comparisons_df : pandas.DataFrame 
        site comparison result to be converted to network.
    measure : str 
        similarity measure to use as edge weight.
    
    Returns
    -------
    network : igraph.Graph 
        a weighted, undirected, complete network representing the site comparisons.
    
    Notes
    -----
    Options for 'measure' (defaults to 'html_distance'):
        * title_distance : float Levenshtein distance between titles (0 if identical).
        * author_distance : float Levenshtein distance between author names (0 if identical).
        * hostname_distance : float Levenshtein distance between hostnames (0 if identical).
        * same_domain : bool whether sites share the same domain (True/False).
        * domain_distance : float Levenshtein distance between domains (0 if identical).
        * shared_links : set links shared by both sites.
        * date_difference (timedelta): time difference between site date entries.
        * same_language : bool whether sites share the same language (True/False).
        * domain_distance : float Levenshtein distance between domains (0 if identical).
        * fingerprint_distance : float Levenshtein distance between site fingerprints (0 if identical).
        * id_distance : float Levenshtein distance between site IDs (0 if identical).
        * license_distance : float Levenshtein distance between licenses (0 if identical).
        * comments_distance : float Levenshtein distance between comments (0 if identical).
        * raw_text_distance : float Levenshtein distance between raw texts (0 if identical).
        * raw_text_cosine : float Cosine similarity by word count of raw texts (1 if identical).
        * text_distance : float Levenshtein distance between parsed texts (0 if identical).
        * text_cosine : float Cosine similarity by word count of parsed texts (1 if identical).
        * source_distance : float Levenshtein distance between source names (0 if identical).
        * source-hostname_distance : float Levenshtein distance between source-hostname names (0 if identical).
        * source_distance : float Levenshtein distance between source code (0 if identical).
        * source_distance : float Levenshtein distance between source code (0 if identical).
        * excerpt_distance : float Levenshtein distance between excerpts (0 if identical).
        * excerpt_cosine : float Cosine similarity by word count of excerpts (1 if identical).
        * shared_categories : set category tags shared by both sites.
        * shared_tags : set tags shared by both sites.
        * html_distance : float Levenshtein distance between HTML source codes (0 if identical).
        * description_distance : float Levenshtein distance between descriptions (0 if identical).
        * description_cosine : float Cosine similarity by word count of descriptions (1 if identical).
        * sitename_distance : float Levenshtein distance between sitenames (0 if identical).
    """
    
    # Retrieving pairs of URLs from comparisons result
    tuples = comparisons_df.index.to_list()
    
    # Reformatting dataframe.
    comparisons_df = comparisons_df.reset_index()
    
    # Building tuple of URLs
    urls = ()
    for pair in tuples:
        urls = urls + pair
    
    # Converting tuple of tuples into a list of URls with repeats removed
    urls = list(set(urls))
    
    # Getting number of URLs
    vs_n = len(urls)
    
    # Initialising full network
    network = Graph.Full(n=vs_n, directed= False)
    
    # Assigning URL metadata as vertex attributes
    network.vs['name'] = urls
    network.vs['type'] = 'website'
    
    
    # Iterating through edges
    for e in network.es:
        
        # Retrieving edge's source vertex ID and name
        source_id = e.source
        source_name = network.vs[source_id]['name']
        
        # Retrieving edge's source vertex ID and name
        target_id = e.target
        target_name = network.vs[target_id]['name']
        
        # Creating tuple of names to use to mask dataframe
        tuple_1 = (source_name, target_name)
        tuple_2 = (target_name, source_name)
        
        # Masking dataframe to extract data from selected column
        comparisons_masked = comparisons_df[(comparisons_df['index'] == tuple_1) | (comparisons_df['index'] == tuple_2)].reset_index()
        weight = comparisons_masked[measure][0]
        if weight == None:
            weight = 'N/A'
        e['weight'] = weight
        
        # Assigning comparison results as edge attributes
        for column in comparisons_masked.columns:
            e[column] = comparisons_masked[column][0]
        
    return network
    
def similarity_network_from_crawl_result(crawl_df: pd.DataFrame, measure: str = 'html_distance') -> Graph:
    
    """
    Takes web crawl result, compares sites found, and returns a weighted network representing those sites' similarities.
    
    Crawled URLs become vertices; similarity measures between URLs become undirected edges.
    Edge weights correspond to a chosen similarity measure.
    
    Parameters
    ---------- 
    crawl_df : pandas.DataFrame
        web crawl result.
    measure : str
        similarity measure to use as edge weight.
    
    Returns
    -------
    network : igraph.Graph 
        a weighted, undirected, complete network representing the site comparisons.
    
    Notes
    -----
    Options for 'measure' (defaults to 'html_distance'):
        * title_distance : float Levenshtein distance between titles (0 if identical).
        * author_distance : float Levenshtein distance between author names (0 if identical).
        * hostname_distance : float Levenshtein distance between hostnames (0 if identical).
        * same_domain : bool whether sites share the same domain (True/False).
        * domain_distance : float Levenshtein distance between domains (0 if identical).
        * shared_links : set links shared by both sites.
        * date_difference (timedelta): time difference between site date entries.
        * same_language : bool whether sites share the same language (True/False).
        * domain_distance : float Levenshtein distance between domains (0 if identical).
        * fingerprint_distance : float Levenshtein distance between site fingerprints (0 if identical).
        * id_distance : float Levenshtein distance between site IDs (0 if identical).
        * license_distance : float Levenshtein distance between licenses (0 if identical).
        * comments_distance : float Levenshtein distance between comments (0 if identical).
        * raw_text_distance : float Levenshtein distance between raw texts (0 if identical).
        * raw_text_cosine : float Cosine similarity by word count of raw texts (1 if identical).
        * text_distance : float Levenshtein distance between parsed texts (0 if identical).
        * text_cosine : float Cosine similarity by word count of parsed texts (1 if identical).
        * source_distance : float Levenshtein distance between source names (0 if identical).
        * source-hostname_distance : float Levenshtein distance between source-hostname names (0 if identical).
        * source_distance : float Levenshtein distance between source code (0 if identical).
        * source_distance : float Levenshtein distance between source code (0 if identical).
        * excerpt_distance : float Levenshtein distance between excerpts (0 if identical).
        * excerpt_cosine : float Cosine similarity by word count of excerpts (1 if identical).
        * shared_categories : set category tags shared by both sites.
        * shared_tags : set tags shared by both sites.
        * html_distance : float Levenshtein distance between HTML source codes (0 if identical).
        * description_distance : float Levenshtein distance between descriptions (0 if identical).
        * description_cosine : float Cosine similarity by word count of descriptions (1 if identical).
        * sitename_distance : float Levenshtein distance between sitenames (0 if identical).
    """
    
    # Running site comparisons
    comparisons_df = compare_sites(crawl_df)
    
    # Generating network
    network = site_similarity_network(comparisons_df, measure = measure)
    
    return network

def similarity_network_from_crawl(
                                seed_urls: str = 'request_input',
                                measure: str = 'html_distance',
                                visit_limit: int = 5, 
                                excluded_url_terms: list = 'default',
                                required_keywords: list = None, 
                                excluded_keywords: list= None, 
                                case_sensitive: bool = False,
                                ignore_urls: list = None, 
                                ignore_domains: list = 'default',
                                be_polite: bool = True,
                                full: bool = True
                                ) -> Graph:
    
    """
    Runs web crawl, compares sites found, and returns a weighted network representing those sites' similarities.
    
    Crawled URLs become vertices; similarity measures between URLs become undirected edges.
    Edge weights correspond to a chosen similarity measure.
    
    Parameters
    ---------- 
    seed_urls : str or list 
        one or more URLs from which to crawl.
    measure : str 
        similarity measure to use as edge weight.
    visit_limit : int 
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
    
    Returns
    -------
    network : igraph.Graph 
        a weighted, undirected, complete network representing the site comparisons.
    
    Notes
    -----
    Options for 'measure' (defaults to 'html_distance'):
        * title_distance : float Levenshtein distance between titles (0 if identical).
        * author_distance : float Levenshtein distance between author names (0 if identical).
        * hostname_distance : float Levenshtein distance between hostnames (0 if identical).
        * same_domain : bool whether sites share the same domain (True/False).
        * domain_distance : float Levenshtein distance between domains (0 if identical).
        * shared_links : set links shared by both sites.
        * date_difference (timedelta): time difference between site date entries.
        * same_language : bool whether sites share the same language (True/False).
        * domain_distance : float Levenshtein distance between domains (0 if identical).
        * fingerprint_distance : float Levenshtein distance between site fingerprints (0 if identical).
        * id_distance : float Levenshtein distance between site IDs (0 if identical).
        * license_distance : float Levenshtein distance between licenses (0 if identical).
        * comments_distance : float Levenshtein distance between comments (0 if identical).
        * raw_text_distance : float Levenshtein distance between raw texts (0 if identical).
        * raw_text_cosine : float Cosine similarity by word count of raw texts (1 if identical).
        * text_distance : float Levenshtein distance between parsed texts (0 if identical).
        * text_cosine : float Cosine similarity by word count of parsed texts (1 if identical).
        * source_distance : float Levenshtein distance between source names (0 if identical).
        * source-hostname_distance : float Levenshtein distance between source-hostname names (0 if identical).
        * source_distance : float Levenshtein distance between source code (0 if identical).
        * source_distance : float Levenshtein distance between source code (0 if identical).
        * excerpt_distance : float Levenshtein distance between excerpts (0 if identical).
        * excerpt_cosine : float Cosine similarity by word count of excerpts (1 if identical).
        * shared_categories : set category tags shared by both sites.
        * shared_tags : set tags shared by both sites.
        * html_distance : float Levenshtein distance between HTML source codes (0 if identical).
        * description_distance : float Levenshtein distance between descriptions (0 if identical).
        * description_cosine : float Cosine similarity by word count of descriptions (1 if identical).
        * sitename_distance : float Levenshtein distance between sitenames (0 if identical).
    """
    
    # Running web crawl
    crawl_result = crawler(
                            seed_urls = seed_urls,
                            visit_limit = visit_limit, 
                            excluded_url_terms = excluded_url_terms,
                            required_keywords = required_keywords, 
                            excluded_keywords = excluded_keywords, 
                            case_sensitive = case_sensitive,
                            ignore_urls = ignore_urls, 
                            ignore_domains = ignore_domains,
                            be_polite = be_polite,
                            full = full,
                            output_as = 'dataframe')
        
    
    # Running site comparisons and generating network
    network = similarity_network_from_crawl_result(crawl_result, measure = measure)
    
    return network
    
def network_from_search_crawl(
                            query: str = 'request_input',
                            visit_limit: int = 5, 
                            excluded_url_terms: list = 'default',
                            required_keywords: list = None, 
                            excluded_keywords: list = None, 
                            case_sensitive: bool = False,
                            ignore_urls: list = None, 
                            ignore_domains: list = 'default',
                            be_polite: bool = True,
                            full: bool = True,
                            output_as: str = 'dataframe'
                            ) -> Graph:
    
    """
    Runs web crawl from search and generates a directed network from the results.
    
    Crawled URLs become vertices; links between URLS become directed edges.
    
    Parameters
    ---------- 
    query : str 
        query to search.
    visit_limit : int 
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
    
    Returns
    -------
    network : igraph.Graph 
        a network representing the results of a web crawl.
    """
    
    crawl_result = crawl_from_search(
            query = query,
            visit_limit = visit_limit, 
            excluded_url_terms = excluded_url_terms,
            required_keywords = required_keywords, 
            excluded_keywords = excluded_keywords, 
            case_sensitive = case_sensitive,
            ignore_urls = ignore_urls, 
            ignore_domains = ignore_domains,
            be_polite = be_polite,
            full = full,
            output_as = 'dataframe'
            )
    
    network = network_from_crawl_result(crawl_result)
    
    return network
    
def similarity_network_from_search_crawl(
                            query: str = 'request_input',
                            measure: str = 'html_distance',
                            visit_limit: int = 5, 
                            excluded_url_terms: list = 'default',
                            required_keywords: list = None, 
                            excluded_keywords: list = None, 
                            case_sensitive: bool = False,
                            ignore_urls: list = None, 
                            ignore_domains: list = 'default',
                            be_polite: bool = True,
                            full: bool = True,
                            output_as: str = 'dataframe'
                            ) -> Graph:
    
    """
    Runs web crawl from search, compares sites that are found, and generates an undirected network from the results.
    
    Crawled URLs become vertices; similarity measures between URLs become undirected edges.
    
    Parameters
    ---------- 
    query : str 
        query to search.
    measure : str 
        similarity measure to use as edge weight.
    visit_limit : int 
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
    ignore_domains : list list of domains to ignore.
    be_polite : bool 
        whether respect websites' permissions for crawlers.
    full : bool 
        whether to run a full scrape on each site. This takes longer.
    
    Returns
    -------
    network : igraph.Graph 
        a network representing the results of site comparison analysis.
        
    Notes
    -----
    Options for 'measure' (defaults to 'html_distance'):
        * title_distance : float Levenshtein distance between titles (0 if identical).
        * author_distance : float Levenshtein distance between author names (0 if identical).
        * hostname_distance : float Levenshtein distance between hostnames (0 if identical).
        * same_domain : bool whether sites share the same domain (True/False).
        * domain_distance : float Levenshtein distance between domains (0 if identical).
        * shared_links : set links shared by both sites.
        * date_difference (timedelta): time difference between site date entries.
        * same_language : bool whether sites share the same language (True/False).
        * domain_distance : float Levenshtein distance between domains (0 if identical).
        * fingerprint_distance : float Levenshtein distance between site fingerprints (0 if identical).
        * id_distance : float Levenshtein distance between site IDs (0 if identical).
        * license_distance : float Levenshtein distance between licenses (0 if identical).
        * comments_distance : float Levenshtein distance between comments (0 if identical).
        * raw_text_distance : float Levenshtein distance between raw texts (0 if identical).
        * raw_text_cosine : float Cosine similarity by word count of raw texts (1 if identical).
        * text_distance : float Levenshtein distance between parsed texts (0 if identical).
        * text_cosine : float Cosine similarity by word count of parsed texts (1 if identical).
        * source_distance : float Levenshtein distance between source names (0 if identical).
        * source-hostname_distance : float Levenshtein distance between source-hostname names (0 if identical).
        * source_distance : float Levenshtein distance between source code (0 if identical).
        * source_distance : float Levenshtein distance between source code (0 if identical).
        * excerpt_distance : float Levenshtein distance between excerpts (0 if identical).
        * excerpt_cosine : float Cosine similarity by word count of excerpts (1 if identical).
        * shared_categories : set category tags shared by both sites.
        * shared_tags : set tags shared by both sites.
        * html_distance : float Levenshtein distance between HTML source codes (0 if identical).
        * description_distance : float Levenshtein distance between descriptions (0 if identical).
        * description_cosine : float Cosine similarity by word count of descriptions (1 if identical).
        * sitename_distance : float Levenshtein distance between sitenames (0 if identical).
    """
    
    crawl_result = crawl_from_search(
            query = query,
            visit_limit = visit_limit, 
            excluded_url_terms = excluded_url_terms,
            required_keywords = required_keywords, 
            excluded_keywords = excluded_keywords, 
            case_sensitive = case_sensitive,
            ignore_urls = ignore_urls, 
            ignore_domains = ignore_domains,
            be_polite = be_polite,
            full = full,
            output_as = 'dataframe'
            )
    
    network = similarity_network_from_crawl_result(crawl_result = crawl_result, measure = measure)
    
    return network