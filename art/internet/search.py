"""Functions for launching internet searches."""

from .webanalysis import open_url, open_url_source

from typing import List, Dict, Tuple
import webbrowser
from urllib.parse import quote
import copy

def search_web(
                query: str = 'request_input',
                search_engine: str = 'Google',
                view_source: bool = False
              ):
    
    """
    Launches web search in the default web browser using user's query.
    
    Parameters
    ----------
    query : str
        query to search. Defaults to requesting from user input.
    search_engine : str
        name of search engine to use. Defaults to 'Google'.
    view_source : bool
        whether to open the page source code of the search result.
    """
    
    # Requesting query from user input if none given
    if query == 'request_input':
        query = input('Search: ')
    
    # Reformatting query
    query = quote(query)
    
    # Creating base for URL depending on search engine specified
    if search_engine == 'Google':
            url_base = 'https://www.google.com/search?hl=en&q='
        
    else:

        if search_engine == 'DuckDuckGo':
            url_base = 'https://duckduckgo.com/?q='

        if search_engine == 'Bing':
            url_base = 'https://www.bing.com/search?q='
    
    # Creating search URL
    url = url_base + query
    
    # If view_source is True, opens the URL's source code
    if view_source == True:
        open_url_source(url = url)
    
    else:
        open_url(url = url)
         
def multi_search_web(iteration_terms: list = 'request_input',
                        query: str = 'request_input',
                        search_engine: str = 'Google',
                        view_source: bool = False
                    ):
    
    """
    Launches multiple web searches by iterating on a query through a list of terms.
    
    Parameters
    ----------
    iteration_terms : list, set, or tuple
        iterable of strings to insert into the search query.
    query : str
        query to search. The function replaces '___' with the iteration term. Defaults to requesting from user input.
    search_engine : str
        name of search engine to use. Defaults to 'Google'.
    view_source : bool
        whether to open the page source code of the search result.
    """
    
    # Requesting iteration terms from user input if none given
    if iteration_terms == 'request_input':
        iteration_terms = input('Type search terms to iterate separated by ",": ')
        iteration_terms = iteration_terms.split(',')
        iteration_terms = [i.strip() for i in iteration_terms]
    
    # Requesting query from user input if none given
    if query == 'request_input':
        query = input('Type search query with ___ in place of iteration terms: ')
    
    # Splitting query to input iteration terms
    query_split = query.split('___')
    
    # If only 1 iteration term is provided, runs a normal search
    if len(query_split) == 1:
        return search_web(query = query, 
                   search_engine = search_engine, 
                   view_source = view_source)
    
    print('Search query: ' + query_split[0] + str(iteration_terms) + query_split[1])
    
    
    # Iterating through terms, replacing '___' in query with each term
    for i in iteration_terms:
        
        query_iteration = query_split[0] + str(i) + query_split[1]
        search_web(query = query_iteration, 
                   search_engine = search_engine, 
                   view_source = view_source)

def search_website(query: str = 'request_input', url = 'request_input', view_source = False):
    
    """
    Launches a website-specific Google search for an inputted query and URL.
    
    Parameters
    ----------
    query : str
        query to search. Defaults to requesting from user input.
    url : str
        URL for website to search within.
    view_source : bool
        whether to open the page source code of the search result.
    """
    
    # Requesting query from user input if none given
    if query == 'request_input':
        query = input('Search for: ')
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Formatting query to focus on site
    site_query = 'site:' + url
    query = site_query + ' ' + query
    
    # Opening web search
    search_web(query, search_engine = 'Google', view_source = view_source)
    
def search_social_media(query: str = 'request_input', platform: str = 'Twitter', view_source = False):
    
    """
    Launches a Google search focused on specified social media platform for inputted query.
    
    Parameters
    ----------
    query : str
        query to search. Defaults to requesting from user input.
    platform : str
        name of the social media platform to search. Defaults to 'Twitter'.
    view_source : bool
        whether to open the page source code of the search result.
        
    Notes
    -----
    Options for 'platform':
        * Twitter
        * Facebook
        * Instagram
        * TikTok
        * Threads
        * LinkedIn
        * Reddit
        * 4Chan
    """
    
    # Requesting query from user input if none given
    if query == 'request_input':
        query = input(f'Search {platform} for: ')
    
    # Selecting platform and formatting query
    platform = platform.lower().strip()
    
    if platform == 'twitter':
        site_query = 'site:twitter.com'
    
    if platform == 'facebook':
        site_query = 'site:facebook.com'
    
    if platform == 'instagram':
        site_query = 'site:instagram.com'
    
    if platform == 'tiktok':
        site_query = 'site:tiktok.com'
    
    if platform == 'threads':
        site_query = 'site:threads.net'
        
    if platform == 'linkedin':
        site_query = 'site:linkedin.com'
    
    if platform == 'reddit':
        site_query = 'site:reddit.com'
        
    if platform == '4chan':
        site_query = 'site:4chan.org'
    
    query = site_query + ' ' + query
    
    # Running search
    search_web(query, search_engine = 'Google', view_source = view_source)
        
def search_twitter(query: str = 'request_input'):
    
    """
    Launches a Google search focused Twitter for inputted query.
    """
    
    search_social_media(query = query, platform = 'Twitter')

def search_images(query: str = 'request_input',
               search_engine: str = 'Google Images'
              ):
    
    """
    Launches an image search using the default web browser.
    
    Parameters
    ----------
    query : str
        query to search. Defaults to requesting from user input.
    search_engine : str
        name of image search engine to use. Defaults to 'Google Images'.
    """
    
    # Requesting query from user input if none given
    if query == 'request_input':
        query = input(f'Search: ')
    
    # Reformatting query
    query = quote(query)
    
    # Creating base for URL depending on search engine specified
    if search_engine == 'Google Images':
            url_base = 'https://www.google.com/search?tbm=isch&q='
        
    else:

        if search_engine == 'DuckDuckGo':
            url_base = 'https://duckduckgo.com/?q=i!'

        if search_engine == 'Bing':
            url_base = 'https://www.bing.com/images/search?q='
    
    # Creating search URL
    url = url_base + query
    
    # Opening search
    return webbrowser.open(url)
        
def reverse_image_search(url = 'request_input',
               search_engine: str = 'Google Images'
              ):
    
    """
    Launches a reverse image search for a URL using the default web browser.
    
    Parameters
    ----------
    url : str
        URL to search. Defaults to requesting from user input.
    search_engine : str
        name of image search engine to use. Defaults to 'Google Images'.
    """
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Creating base for URL depending on search engine specified
    if search_engine == 'Google Images':
        url_base = 'https://lens.google.com/uploadbyurl?url='
        
    else:

        if search_engine == 'Google Lens':
            url_base = 'https://lens.google.com/uploadbyurl?url='
        
        if search_engine == 'Bing':
            url_base = 'https://www.bing.com/images/search?view=detailv2&iss=sbi&form=SBIIRP&sbisrc=UrlPaste&q=imgurl:'
        
        if search_engine == 'TinEye':
            url_base = 'https://tineye.com/search?url='
    
    # Creating URL for search
    url = url_base + url
    
    # Opening search in browser
    return webbrowser.open(url)