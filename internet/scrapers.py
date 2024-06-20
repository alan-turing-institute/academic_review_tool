"""Functions for scraping web data"""

from .webanalysis import correct_url, get_domain, open_url
from ..utils.cleaners import join_list_by_colon, split_str_by_colon

from typing import List
import json
import requests

import numpy as np
import pandas as pd

import cloudscraper

from trafilatura import fetch_url, extract, extract_metadata

import urllib
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

from courlan import normalize_url, clean_url, scrub_url

from selenium import webdriver
from selenium.webdriver.common.by import By


headers = {'User-Agent': 'Mozilla/5.0 (X11; Windows; Windows x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36'}

results_cols = [
                            'id',
                            'title',
                            'authors',
                            'year',
                            'source',
                            'publisher',
                            'type',
                            'keywords',
                            'abstract',
                            'description',
                            'extract',
                            'full_text',
                            'citations',
                            'citation_links',
                            'cited_by',
                            'recommendations',
                            'repository',
                            'doi',
                            'isbn',
                            'other_ids',
                            'link',
                            'author_links'
                                ]

def get_final_url(url):

    global headers
    req = Request(url=url, headers=headers)

    resp = urlopen(req)
    return resp.geturl()

def bs_find(tag, content, soup):
    return ('soup.find(attrs={"'+tag+'":"'+content + '"})', soup.find(attrs={tag:content}))

def bs_find_all(tag, content, soup):
    return ('soup.find_all(attrs={"'+tag+'":"'+content + '"})', soup.find_all(attrs={tag:content}))

def bs_name_content(content_tag, soup):
    return ('soup.find(attrs={"name":"'+content_tag + '"}).attrs["content"]', soup.find(attrs={'name':content_tag}).attrs['content'])

def get_url_source(url = 'request_input'):
    
    """Returns the HTTP response for the provided URL. 

    Parameters
    ---------- 
    url : str
        URL of the page to scrape.

    Returns
    -------
    response : object
        HTTP response object from cloudscraper.
    """
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting errors in URL (e.g. missing HTTPS prefix)
    url = correct_url(url)
   
    # Trying to scrape site using CloudScraper to avoid Cloudflare protection
    try:
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url)
        return response
    
    # Handling errors
    except:
        raise ValueError('Scraper failed')

def url_to_soup(url = 'request_input'):
    
    if url == 'request_input':
        url = input('URL: ')
    
    response = get_url_source(url)
    
    return BeautifulSoup(response.text)

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

def correct_link_errors(url: str, source_domain: str = None) -> str:
    
    """Checks for errors in a link and corrects them. Returns a corrected link as a string.
    
    Parameters
    ---------- 
    url : str 
        URL to correct.
    source_domain : str 
        the domain or URL where the link was found.

    Returns
    -------
    url : str 
        a corrected URL."""
    
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

def scrape_url_html(url = 'request_input') -> str:
    
    """Scrapes raw HTML code from the provided URL. 

    Parameters
    ---------- 
    url : str
        URL of the page to scrape.

    Returns
    -------
    HTML : str
        the returned HTML as a string."""
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting errors in URL (e.g. missing HTTPS prefix)
    url = correct_url(url = url)
    
    # Retrieving HTTP response
    response = get_url_source(url = url)
    
    # Extracting HTML
    result = response.text
    
    return result

def scrape_url_metadata(url = 'request_input') -> dict:
    
    """Scrapes metadata from the provided URL. 

    Parameters
    ---------- 
    url : str 
        URL of the page to scrape.

    Returns
    -------
    metadata : dict
        the URL's metadata as a dictionary."""
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting errors in URL (e.g. missing HTTPS prefix)
    url = correct_url(url = url)
    
    # Using trafilatura to fetch site data
    downloaded = fetch_url(url = url)
    
    # Creating empty result variable to avoid errors
    result = None
    
    # Extracting metadata and assigning to result variable
    if downloaded != None:
        result = extract_metadata(downloaded)
        
        if result != None:
            
            # Converting result to dictionary
            result = result.as_dict()
            
        else:
            result = None
        
    return result

def scrape_url_rawtext(url = 'request_input') -> str:
    
    """Scrapes raw text from the provided URL. 

    Parameters
    ---------- 
    url : str 
        URL of the page to scrape.

    Returns
    -------
    rawtext : str
        the URL's raw text as a string."""
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting errors in URL (e.g. missing HTTPS prefix)
    url = correct_url(url = url)
    
    # Using trafilatura to fetch site data
    downloaded = fetch_url(url = url)
    
    if downloaded != None:
        
        # Extracting raw text and assigning to result variable
        result = extract(downloaded, include_tables=True, include_comments=True, include_images=True, include_links=True)
    
    return result

def scrape_url_xml(url = 'request_input') -> str:
    
    """Scrapes data from the provided URL and returns in XML format. 

    Parameters
    ---------- 
    url : str 
        URL of the page to scrape.

    Returns
    -------
    xml : str
        the URL's data as an XML-formatted string."""
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting errors in URL (e.g. missing HTTPS prefix)
    url = correct_url(url = url)
    
    # Using trafilatura to fetch site data
    downloaded = fetch_url(url)
    if downloaded != None:
        
        # Extracting data and assigning to result variable
        result = extract(downloaded, output_format="xml", include_tables=True, include_comments=True, include_images=True, include_links=True)
    
    return result

def scrape_url_json(url = 'request_input') -> str:
    
    """Scrapes data from the provided URL and returns in JSON format. 

    Parameters
    ---------- 
    url : str 
        URL of the page to scrape.

    Returns
    -------
    json : str
        the URL's data as a JSON-formatted string."""
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting errors in URL (e.g. missing HTTPS prefix)
    url = correct_url(url = url)
    
    # Using trafilatura to fetch site data
    downloaded = fetch_url(url)
    if downloaded != None:
        
        # Extracting data and assigning to result variable
        result = extract(downloaded, output_format="json", include_tables=True, include_comments=True, include_images=True, include_links=True)
    else:
        result = ''
        
    return result

def scrape_url_csv(url = 'request_input'):
    
    """Scrapes data from the provided URL and returns in CSV format. 

    Parameters
    ---------- 
    url : str 
        URL of the page to scrape.

    Returns
    -------
    csv : str
        the URL's data as a CSV-formatted string."""
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting errors in URL (e.g. missing HTTPS prefix)
    url = correct_url(url = url)
    
    # Using trafilatura to fetch site data
    downloaded = fetch_url(url)
    if downloaded != None:
        
        # Extracting data and assigning to result variable
        result = extract(downloaded, output_format="csv", include_tables=True, include_comments=True, include_images=True, include_links=True)
    
    return result

def scrape_url_to_dict(url = 'request_input') -> str:
    
    """Scrapes data from provided URL and returns as a dictionary. 

    Parameters
    ---------- 
    url : str 
        URL of the page to scrape.

    Returns
    -------
    result : dict 
        the URL's data as a dictionary."""
    
    # Retrieving site data as a JSON-formatted string
    result_json = scrape_url_json(url = url)
    
    # Avoiding errors if no result retrieved
    if result_json == None:
        result = {}
    
    else:
        # Converting JSON to dictionary
        result = json.loads(result_json)
    
    return result
    
def scrape_url_links(url = 'request_input') -> List[str]:
    
    """Scrapes links from provided URL and returns as a list. 

    Parameters
    ---------- 
    url : str 
        URL of the page to scrape.

    Returns
    -------
    links : list 
        the URL's links as a list."""
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Scraping data
    scraped_data = scrape_url_html(url = url)
    
    # Making HTML soup
    soup = BeautifulSoup(scraped_data, "html.parser")
    
    # Selecting dividers
    href_select = soup.select("a")  
    
    # Extracting links as list and applying corrections 
    links = [correct_link_errors(source_domain = url, url = i['href']) for i in href_select if 'href' in i.attrs]
    
    return links
 
def scrape_url(url = 'request_input', parse_pdf = True, output: str = 'dict'):
    
    """Scrapes data from URL. Returns any HTML code, text, links, and metatdata found. 
    
    Defaults to returning a dictionary.
    
    Parameters
    ---------- 
    url : str 
        URL of the page to scrape.
    parse_pdf : bool
        whether to detect PDFs and parse them using PDF parser.

    Returns
    -------
    result : object
        the result in a user-selected format.
    """
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
        
    # Cleaning URL
    url = url.strip("'").strip('"').strip()
    
    # Running scrapers depending on the format specified
    
    output = output.lower().strip()
    
    if output == 'html':
        result = scrape_url_html(url = url)
    
    if (output == 'raw') or (output == 'rawtext') or (output == 'raw text'):
        result = scrape_url_rawtext(url = url)
    
    if output == 'xml':
        result = scrape_url_xml(url = url)
    
    if output == 'json':
        result = scrape_url_json(url = url)
    
    if output == 'csv':
        result = scrape_url_csv(url = url)
    
    # If the output format selected is dictionary, scrapes all data available
    if output == 'dict':
        
        # Running main scraper
        result = scrape_url_to_dict(url = url)
        
        # Appending html to output dict
        result['html'] = scrape_url_html(url = url)
        
        # Extracting links
        current_url = url
        soup = BeautifulSoup(result['html'], "html.parser")
        href_select = soup.select("a")
        links = [correct_link_errors(source_domain = current_url, url = i['href']) for i in href_select if 'href' in i.attrs]
        result['links'] = links
        
        # If parse_pdf is selected, check if URL is PDF and parse
        
        if parse_pdf == True:
            
            if url.endswith('.pdf') == True:
                
                # Running PDF downloader and parser
                pdf_parsed = read_pdf_url(url = url)
                
                # Appending result
                result['title'] = pdf_parsed['title']
                result['author'] = pdf_parsed['authors']
                result['raw_text'] = pdf_parsed['raw']
                result['text'] = pdf_parsed['full_text']
                result['date'] = pdf_parsed['date']
                result['links'] = pdf_parsed['links']
                result['format'] = 'PDF'
                result['type'] = 'document'
        
        # Scraping URL metadata using trafilatura
        metadata = scrape_url_metadata(url = url)
        if metadata != None:
            for key in metadata.keys():
                if key not in result.keys():
                    result[key] = metadata[key]
        
        # Appending URL
        result['url'] = url
        
    return result

def scrape_urls_list(urls: list, parse_pdf = True, output: str = 'dataframe'):
    
    """Scrapes list of URLs. Returns any HTML code, text, links, and metatdata found. 
    
    Defaults to returning a dataframe.
    
    Parameters
    ---------- 
    url : str 
        URL of the page to scrape.
    parse_pdf : bool
        whether to detect PDFs and parse them using PDF parser.

    Returns
    -------
    result : object 
        the result in a user-selected format."""
    
    # Initialising dictionary for results
    output_dict = {}
    
    # Iterating through links and scraping
    for link in urls:
        output_dict[link] = scrape_url(url = link, parse_pdf = parse_pdf, output = 'dict')
    
    # If selected output is dataframe, converting to dataframe
    if output == 'dataframe':
        output = pd.DataFrame.from_dict(output_dict).T
    
    else:
        output = output_dict
    
    return output

def scrape_dynamic_page(url = 'request_input') -> dict:
    
    """Scrapes dynamic webpages using provided URL. Returns a dictionary of data. Uses Selenium.
    
    Parameters
    ---------- 
    url : str 
        URL of the page to scrape.

    Returns
    -------
    result : dict 
        the result as a dictionary."""
    
    # Initialising dictionary for results
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting errors in URL (e.g. missing HTTPS prefix)
    url = correct_url(url)
    
    # Initialising Selenium webdriver
    driver = webdriver.Chrome()
    
    # Retrieving data
    driver.get(url)
    
    # Selecting CSS elements
    items = driver.find_elements(By.CSS_SELECTOR, ".grid a[data-testid='link']")
    
    # Iterating through results, retrieving attributes, and adding to output dictionary
    output_dict = {}
    index = 0
    
    for item in items:
        
        name = item.accessible_name
        if name == None:
            try:
                name = item.get_attribute('name')
            except:
                name = None
        
        if name == None:
            name = index
            index += 1
        
        # Retrieving text
        text = item.text
        output_dict[name] = text
    
    return output_dict

def scrape_google_search(query: str = 'request_input'):

    """Fetches and parses Google Search results. Offers an alternative to using Google's API. 
    
    Requires user to manually copy and paste Google search source code to avoid Google's CAPCHA system.
    
    Parameters
    ----------
    query : str
        query to search. Defaults to requesting from user input.
    """
    
    print('This function will open your web browser. When prompted, copy and paste the code that appears into the space provided\n')
    
    # Launching web search
    search_web(
                query = query,
                search_engine = 'Google',
                view_source = True
                )
    
    # Requesting search source code from user
    html = input('Search page code: ')
    
    # Parsing result
    output = parse_google_result(html)
    
    return output

def crawler_scraper(current_url: str, full: bool) -> tuple:
    
    """Scraper used by web crawler. Returns result as a tuple. 
    
    Parameters
    ---------- 
    current_url : str 
        URL of the page to scrape.
    full : bool 
        whether to run complete scrape.

    Returns
    -------
    result : tuple 
        the result as a tuple containing BeautifulSoup object, the scraped data, and links."""
    
    # If full is selected, runs a complete scrape using tools
    if full == True:
        
        # Tries to scrape URL using standard scraper function
        try:
            scraped_data = scrape_url(current_url)
        
        # If scraper fails, runs cleaners on URL and tries to run it again
        except:
            current_url = clean_url(current_url)
            current_url = scrub_url(current_url)
            current_url = normalize_url(current_url)
            current_url = correct_url(current_url)

            try:
                scraped_data = scrape_url(current_url)

            except:
                
                # If standard scraper fails twice, tries to use cloudscraper's basic scraper
                try:
                    scraper = cloudscraper.create_scraper()
                    res = scraper.get(current_url)
                    scraped_data['html'] = res.content
                    
                except:
                    scraped_data['html'] = ''
    
    # If full is not selected, runs cloudscraper's basic scraper
    else:
        
        try:
            
            scraper = cloudscraper.create_scraper()
            res = scraper.get(current_url)
            scraped_data['html'] = res.content
            
        except:
            scraped_data['html'] = ''
        
    # Making HTML soup
    soup = BeautifulSoup(scraped_data['html'], "html.parser")
    
    # Selecting dividers
    href_select = soup.select("a")  
    
    # Extracting links
    links = [correct_link_errors(source_domain = current_url, url = i['href']) for i in href_select if 'href' in i.attrs]          
    
    # Returning results as tuple
    return (soup, scraped_data, links)

def scrape_frontiers(url):

    if ('frontiersin.org' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for a Frontiers webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    soup = BeautifulSoup(res,'lxml')

    meta = soup.find_all('meta')

    doi = soup.find(attrs = {'name':"citation_doi"}).attrs['content']
    keywords = soup.find(attrs = {'name':"citation_keywords"}).attrs['content'].split('; ')
    item_type = 'article'
    link = soup.find(attrs = {'property':"og:url"}).attrs['content']
    date = soup.find(attrs={'name':"citation_publication_date"}).attrs['content']
    source = soup.find(attrs = {'name':"citation_journal_title"}).attrs['content']
    publisher = soup.find(attrs = {'name':"citation_publisher"}).attrs['content']
    repository = 'Frontiers'
    title = soup.find(attrs = {'name':"citation_title"}).attrs['content']
    description = soup.find(attrs = {'name':"description"}).attrs['content']
    abstract = soup.find(attrs={'name':"citation_abstract"}).attrs['content']
    
    alist = soup.find_all(attrs={'name':"citation_author"})
    authors = []
    for name in alist:
        authors.append(name.attrs['content'])
    authors
    
    paras = []
    refs = []

    soup.find_all('p')

    for i in soup.find_all('p'):

        if ('<p class="References' in str(i)):
            refs.append(i.text.strip())

        if (
            ('<span' not in str(i)) 
            and ('<p class="References' not in str(i))
            ):
            paras.append(i.text.replace('\n', '').replace('\r', '').replace('  ', '').replace('   ', '').strip())

    full_text = ' \n\n '.join(paras)
    
    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)

    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'source'] = source
    result.loc[0, 'publisher'] = publisher
    result.loc[0, 'type'] = item_type
    result.loc[0, 'keywords'] = keywords
    result.loc[0, 'abstract'] = abstract
    result.loc[0, 'repository'] = repository
    result.loc[0, 'doi'] = doi
    result.loc[0, 'link'] = link
    result.loc[0, 'full_text'] = full_text
    result.loc[0, 'citations'] = refs
    
    return result

def scrape_arxiv(url):
    
    if ('arxiv.org' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for a Arxiv webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    soup = BeautifulSoup(res,'lxml', features="xml")

    meta = soup.find_all('meta')

    doi = soup.find(attrs = {'class':"tablecell arxivdoi"}).a.attrs['href']
    keywords = soup.find(attrs = {'class':"tablecell subjects"}).text.replace('\n', '').split('; ')
    item_type = 'article'
    link = soup.find(attrs = {'property':"og:url"}).attrs['content']
    date = soup.find(attrs = {'class': "dateline"}).text.replace('\n', '').replace('  ', '').replace('[', '').replace(']', '').replace('Submitted on ', '')
    source = 'arxiv'
    publisher = 'arxiv'
    repository = 'arxiv'
    title = []
    # descriptions = []
    abstract = []
    authors = []


    for i in meta:

        string = str(i)
        
        if 'property="og:site_name"/>' in string:
            string = string.replace('<meta content="', '').replace('" ', '').replace('property="og:site_name"/>', '')
            domain = string

        if 'property="og:title"/>' in string:
            string = string.replace('<meta content="', '').replace('property="og:title"/>', '').replace('" ', '')
            title.append(string)

        if 'property="og:description"/>' in string:
            string = string.replace('<meta content="', '').replace('property="og:description"/>', '').replace('" ', '')
            string = string
            abstract.append(string)


    try:
        title = title[0]
    except:
        None
    
    try:
        abstract = abstract[0]
    except:
        None

    string_list = soup.find(attrs={'class':'authors'}).find_all('a')
    authors = []
    for item in string_list:
        string = str(item).replace('<', ' <').replace('>', '> ').replace('</a> ', '').replace(' <a href="https://arxiv.org/search/cs?searchtype=author&amp;query=', '').strip()
        string = string.split('> ')[1]
        authors.append(string)

    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)

    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'source'] = source
    result.loc[0, 'publisher'] = publisher
    result.loc[0, 'type'] = item_type
    result.loc[0, 'keywords'] = keywords
    result.loc[0, 'abstract'] = abstract
    result.loc[0, 'repository'] = repository
    result.loc[0, 'doi'] = doi
    result.loc[0, 'link'] = link
    
    return result

def scrape_springer(url):
    
    if ('springer' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for a Springer webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    soup = BeautifulSoup(res,'lxml')

    meta = soup.find_all('meta')

    doi = []
    keywords = []
    item_type = item_type= []
    link = []
    date = []
    source = []
    publisher = []
    titles = []
    descriptions = []
    authors = []


    for i in meta:

        string = str(i)

        if 'name="citation_doi"/>' in string:
            string = string.replace('<meta content="', '').replace('name="citation_doi"/>', '').replace('" ', '')
            doi.append(string)

        if 'keyword' in string:
            string = string.replace('<meta content="', '').replace('property="og:type"/>', '').replace('" ', '')
            keywords.append(string)

        if 'property="og:type"/>' in string:
            string = string.replace('<meta content="', '').replace('property="og:type"/>', '').replace('" ', '').lower()
            item_type.append(string)

        if 'name="citation_fulltext_html_url"/>' in string:
            string = string.replace('<meta content="', '').replace('name="citation_fulltext_html_url"/>', '').replace('" ', '')
            link.append(string)

        if 'name="citation_publication_date"/>' in string:
            string = string.replace('<meta content="', '').replace('name="citation_publication_date"/>', '').replace('" ', '')
            date.append(string)

        if 'name="citation_inbook_title"' in string:
            string = string.replace('<meta content="', '').replace('name="citation_inbook_title"/>', '').replace('" ', '')
            source.append(string)

        if 'name="citation_publisher"/>' in string:
            string = string.replace('<meta content="', '').replace('name="citation_publisher"/>', '').replace('" ', '')
            publisher.append(string)

        if 'name="citation_title"/>' in string:
            string = string.replace('<meta content="', '').replace('name="citation_title"/>', '').replace('" ', '')
            titles.append(string)

        if 'name="description"/>' in string:
            string = string.replace('<meta content="', '').replace('name="description"/>', '').replace('" ', '')
            string = string
            descriptions.append(string)

        if '"citation_author"' in string:
            string = string.replace('<meta content="', '').replace('name="citation_author"/>', '').replace('" ', '')
            string = string
            authors.append(string)

    try:
        doi = doi[0]
    except:
        None

    try:
        keywords = keywords[0]
    except:
        None

    try:
        item_type = item_type[0]
    except:
        None

    try:
        link = link[0]
    except:
        None

    try:
        date = date[0]
    except:
        None

    try:
        source = source[0]
    except:
        None

    try:
        publisher = publisher[0]
    except:
        None


    try:
        title = titles[0]
    except:
        None

    notes = soup.find_all('section')[1].find_all('p')

    refs = []
    citation_links = []

    for i in notes:

        string = str(i)
        string = string.replace('<p>', '').replace('</p>', '').replace('[', '').replace(']', '')
        refs.append(string)

        try:
            citation_links.append(i.select('a')[0].attrs['href'])

        except:
            None

    sections = soup.find_all('section')
    abstract = str(sections[0].find_all('p')).replace('<p>', '').replace('</p>', '').replace('[', '').replace(']', '')
    
    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)
    
    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'source'] = source
    result.loc[0, 'publisher'] = publisher
    result.loc[0, 'type'] = item_type
    result.loc[0, 'keywords'] = keywords
    result.loc[0, 'abstract'] = abstract
    result.loc[0, 'citations'] = refs
    result.loc[0, 'citation_links'] = citation_links
    result.loc[0, 'repository'] = 'Springer'
    result.loc[0, 'doi'] = doi
    result.loc[0, 'link'] = link
    
    return result

def scrape_nature(url = 'request_input'):
    
    if url == 'request_input':
        url = input('URL: ')
    
    if type(url) !=str:
        raise TypeError('URL must be a string')
    
    if ('nature.com' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for a Nature webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    soup = BeautifulSoup(res, 'lxml')

    all_scripts = soup.find_all('script')
    for i in all_scripts:
        if 'type="application/ld+json">{"mainEntity"' in str(i):
            script_json = str(i).replace('<script type="application/ld+json">', '').replace('</script>', '')

    main = json.loads(script_json)['mainEntity']

    title = main['headline']
    abstract = main['description']
    date = main['datePublished']
    doi = main['sameAs']
    keywords = main['keywords']
    source = main['isPartOf']['name']
    publisher = main['publisher']['name']
    item_type = 'article'

    authors = []
    for i in main['author']:
        authors.append(i['name'])

    for i in soup.find_all('link'):
        string = str(i)
        if 'canonical' in string:
            link = string.replace('<link href="', '').replace('rel="canonical"/>', '').replace('" ', '')

    meta = soup.find_all('meta')

    citations = []
    citations_links = []
    for i in meta:
        if 'name="citation_reference"/>' in str(i):
            citation = i['content'].split('; ')
            cit_dict = {}
            for i in range(0, len(citation)):
                if '=' in citation[i]:
                    pair = citation[i].split('=')
                    cit_dict[pair[0]] = pair[1]
                    if pair[0] == 'citation_doi':
                        citations_links.append(pair[1])
                else:
                    cit_dict['item_title'] = citation[i].replace('\n', '').replace('  ', '').replace('   ', '').strip()
            
            citations.append(cit_dict)
    
    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)
         
    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'source'] = source
    result.loc[0, 'publisher'] = publisher
    result.loc[0, 'type'] = item_type
    result.loc[0, 'keywords'] = keywords
    result.loc[0, 'abstract'] = abstract
    result.loc[0, 'citations'] = citations
    result.loc[0, 'citation_links'] = citations_links
    result.loc[0, 'repository'] = 'Nature'
    result.loc[0, 'doi'] = doi
    result.loc[0, 'link'] = link
    
    return result

def scrape_ieee(url):

    if type(url) !=str:
        raise TypeError('URL must be a string')
    
    if ('ieee.org' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for an IEEE webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    soup = BeautifulSoup(res, 'lxml')

    meta = soup.find_all('meta')

    item_type = 'article'
    link = []
    date = []
    source = []
    publisher = 'IEEE'
    titles = []
    descriptions = []
    abstract = []
    authors = []


    for i in meta:

        string = str(i)

        if 'name="parsely-link"' in string:
            string = string.replace('<meta content="', '').replace('name="parsely-link"/>', '').replace('" ', '')
            link.append(string)

        if 'property="parsely-date"/>' in string:
            string = string.replace('<meta content="', '').replace('name="citation_publication_date"/>', '').replace('" ', '')
            date.append(string)

        if 'name="parsely-section"/>' in string:
            string = string.replace('<meta content="', '').replace('name="parsely-section"/>', '').replace('" ', '')
            source.append(string)

        if 'property="og:title"/>' in string:
            string = string.replace('<meta content="', '').replace('property="og:title"/>', '').replace('" ', '')
            titles.append(string)

        if 'id="meta-description"' in string:
            string = string.replace('<meta content="', '').replace('id="meta-description" name="Description"/>', '').replace('" ', '')
            string = string
            descriptions.append(string)
        
        if 'property="og:description"/>' in string:
            string = string.replace('<meta content="', '').replace('property="og:description"/>', '').replace('" ', '')
            string = string
            abstract.append(string)

        if 'name="parsely-author"/>' in string:
            string = string.replace('<meta content="', '').replace('name="parsely-author"/>', '').replace('" ', '')
            string = string
            authors.append(string)

    
    try:
        link = link[0]
    except:
        None

    try:
        date = date[0]
    except:
        None

    try:
        source = source[0]
    except:
        None

    try:
        descriptions = descriptions[0]
    except:
        None     
        
    try:
        abstract = abstract[0]
    except:
        None

    try:
        title = titles[0]
    except:
        None

    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)

    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'source'] = source
    result.loc[0, 'publisher'] = publisher
    result.loc[0, 'type'] = item_type
    result.loc[0, 'description'] = descriptions
    result.loc[0, 'abstract'] = abstract
    result.loc[0, 'repository'] = 'IEEE'
    result.loc[0, 'link'] = link
    
    return result

def scrape_pubmed(url):

    if type(url) !=str:
        raise TypeError('URL must be a string')
    
    if ('pubmed.ncbi.' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for a PubMed webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    soup = BeautifulSoup(res, 'lxml')

    meta = soup.find_all('meta')

    doi = soup.find(attrs={'name':'citation_doi'}).attrs['content']
    item_type = 'article'
    link = soup.find(attrs={'rel':'canonical'}).attrs['href']
    date = soup.find(attrs={'name':'citation_date'}).attrs['content']
    source = soup.find(attrs={'name':'citation_journal_title'}).attrs['content']
    publisher = soup.find(attrs={'name':'citation_publisher'}).attrs['content']
    repository = 'PubMed'
    title = soup.find(attrs={'name':'citation_title'}).attrs['content']
    description = soup.find(attrs={'name':'description'}).attrs['content']
    abstract = soup.find(attrs={'class':"abstract-content selected"}).text.replace('\n', '').replace('  ', '').replace('   ', '')
    authors = soup.find(attrs={'name':'citation_authors'}).attrs['content'].split(';')
    
    citations = []
    citation_links = []
    refs = soup.find_all(attrs={'class':'references-and-notes-list'})
    for citation in refs:
        citations.append(citation.text.replace('\n', '').replace('   ', '').replace('  ', '').replace(' .', '.').strip())
        citation_links.append(citation.li.a.attrs['href'])
    
    for i in soup.find_all('p'):
        if 'keywords' in str(i).lower():
            keywords = i.text.replace('\n', '').replace('  ', '').replace('   ', '').replace('Keywords:', '').replace('.', '').split('; ')
        else:
            keywords = []
    
    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)

    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'source'] = source
    result.loc[0, 'publisher'] = publisher
    result.loc[0, 'type'] = item_type
    result.at[0, 'keywords'] = keywords
    result.loc[0, 'abstract'] = abstract
    result.loc[0, 'description'] = description
    result.loc[0, 'repository'] = repository
    result.loc[0, 'doi'] = doi
    result.loc[0, 'link'] = link
    result.loc[0, 'citations'] = citations
    result.loc[0, 'citation_links'] = citation_links
    
    return result

def scrape_pmc(url):

    if type(url) !=str:
        raise TypeError('URL must be a string')
    
    if ('gov/pmc' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for a PubMedCentral webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    soup = BeautifulSoup(res, 'lxml')

    doi = soup.find(attrs={'name':'citation_doi'}).attrs['content']
    item_type = soup.find(attrs={'property':'og:type'}).attrs['content']
    link = soup.find(attrs={'rel':'canonical'}).attrs['href']
    date = soup.find(attrs={'name':'citation_publication_date'}).attrs['content']
    source = soup.find(attrs={'name':'citation_journal_title'}).attrs['content']
    publisher = soup.find(attrs={'name':'DC.Publisher'}).attrs['content']
    repository = 'PubMedCentral'
    title = soup.find(attrs={'name':'citation_title'}).attrs['content']
    description = soup.find(attrs={'property':'og:description'}).attrs['content']
    keywords = soup.find(attrs={'class':"kwd-text"}).text
    abstract = soup.find(attrs={'class':"p p-first-last"}).text
   
    authors = []
    for author in soup.find_all(attrs={'name':'citation_author'}):
        authors.append(author.attrs['content'])
    
    citations = []
    citation_links = []
    refs = soup.find(attrs={'class':"ref-list-sec"})
    for i in refs:
        citations.append(i.text.replace('\n', '').replace('..', '.').replace('  ', '').replace('   ', '').replace('[PubMed]', '').replace('[Google Scholar]', '').replace('[PMC free article]', ''))

    for i in refs.find_all(attrs={'target':"_blank"}):
        citation_links.append(i.attrs['href'])
    
    full_text = []
    for i in soup.find_all('p'):
        if 'class="p p-' in str(i):
            full_text.append(i.text.replace('\n', '').replace('  ', '').replace('   ', ''))

    full_text = '\n\n'.join(full_text)
    
    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)

    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'source'] = source
    result.loc[0, 'publisher'] = publisher
    result.loc[0, 'type'] = item_type
    result.loc[0, 'keywords'] = keywords
    result.loc[0, 'abstract'] = abstract
    result.loc[0, 'description'] = description
    result.loc[0, 'full_text'] = full_text
    result.loc[0, 'repository'] = repository
    result.loc[0, 'doi'] = doi
    result.loc[0, 'link'] = link
    result.loc[0, 'citations'] = citations
    result.loc[0, 'citation_links'] = citation_links

    return result

def scrape_ssrn(url = 'request_input'):

    if url == 'request_input':
        url = input('URL: ')
    
    if type(url) !=str:
        raise TypeError('URL must be a string')
    
    if ('ssrn.com' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for an SSRN webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    soup = BeautifulSoup(res, 'lxml')

    try:
        doi = soup.find(attrs={'name':'citation_doi'}).text
    except:
        doi = None

    item_type = 'article'

    try:
        link = soup.find(attrs={'rel':'canonical'}).attrs['href']
    except:
        link = None
    
    date = soup.find(attrs={'name':'citation_publication_date'}).attrs['content']
    source = soup.find(attrs={'class': 'btn-link'}).text
    publisher = 'SSRN'
    repository = 'SSRN'
    title = soup.find(attrs={'name': 'citation_title'}).attrs['content']
    description = soup.find(attrs={'name': 'description'}).attrs['content']
    keywords = soup.find(attrs={'name': 'citation_keywords'}).attrs['content']
    abstract = soup.find(attrs={'class':'abstract-text'}).p.text
   
    authors = []
    for author in soup.find_all(attrs={'name':'citation_author'}):
        authors.append(author.attrs['content'])
    
    
    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)

    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'source'] = source
    result.loc[0, 'publisher'] = publisher
    result.loc[0, 'type'] = item_type
    result.loc[0, 'keywords'] = keywords
    result.loc[0, 'abstract'] = abstract
    result.loc[0, 'description'] = description
    result.loc[0, 'repository'] = repository
    result.loc[0, 'doi'] = doi
    result.loc[0, 'link'] = link
    
    return result

def scrape_heinonline(url):

    if type(url) !=str:
        raise TypeError('URL must be a string')
    
    if ('heinonline.org' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for a HeinOnline webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    soup = BeautifulSoup(res, 'lxml')
    
    item_type = 'article'
    repository = 'HeinOnline'
    title = soup.find(attrs={'class':'cite_title'}).text.strip()
    description = soup.find(attrs={'name': 'description'}).attrs['content'].replace('\r', '').replace('\n', '').replace('  ', '').replace('   ', '').strip()
    keywords = soup.find(attrs={'name': 'keywords'}).attrs['content'].replace('\r', '').replace('\n', '').replace('  ', '').replace('   ', '').strip()
   
    author_res = soup.find(attrs={'class':"Z3988"}).attrs['title'].replace('ctx_ver', ' ').replace('rft', ' ').replace('=', ' ').replace('%', ' ').replace('&', ' ').strip()

    for i in author_res.split('_'):
            if '.au' in i:
                auths_to_clean = i

    auths_to_clean = auths_to_clean.split('.')

    authors = []
    for auth in auths_to_clean:
        if 'au ' in auth:
            authors.append(auth.replace('au','').replace('  ','').strip())
    
    date_res = soup.find(attrs={'class':"Z3988"}).attrs['title'].replace('ctx_ver', ' ').replace('rft', ' ').replace('=', ' ').replace('%', ' ').replace('&', ' ').split('.')

    for i in date_res:
        if 'date' in i:
            date = i.replace('date', '').strip()
    
    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)

    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'type'] = item_type
    result.loc[0, 'keywords'] = keywords
    result.loc[0, 'description'] = description
    result.loc[0, 'repository'] = repository
    result.loc[0, 'link'] = url
    
    return result

def scrape_mdpi(url):

    if type(url) !=str:
        raise TypeError('URL must be a string')
    
    if ('mdpi.com' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for a MDPI webpage')
        
    try:
        res = scrape_url(url = url, parse_pdf = False, output = 'html')
        soup = BeautifulSoup(res, 'lxml')
    
        doi = soup.find(attrs={'name':'citation_doi'}).text
        item_type = 'article'
        link = url
        date = soup.find(attrs={'name':'citation_publication_date'}).attrs['content']
        source = soup.find(attrs={'name': 'dc.source'}).attrs['content']
        publisher = soup.find(attrs={'name': 'dc.publisher'}).attrs['content']
        repository = 'PNAS'
        title = soup.find(attrs={'name': 'title'}).attrs['content']
        description = soup.find(attrs={'name': 'description'}).attrs['content']
        abstract = soup.find(attrs={'name': 'description'}).attrs['content']

        authors = []
        for author in soup.find_all(attrs={'name':'dc.creator'}):
            authors.append(author.attrs['content'])

        keywords = []
        for term in soup.find_all(attrs={'name':'dc.subject'}):
            keywords.append(term.attrs['content'])


        global results_cols
        result = pd.DataFrame(columns = results_cols, dtype=object)

        result.loc[0, 'title'] = title
        result.loc[0, 'authors'] = authors
        result.loc[0, 'year'] = date
        result.loc[0, 'source'] = source
        result.loc[0, 'publisher'] = publisher
        result.loc[0, 'type'] = item_type
        result.loc[0, 'keywords'] = keywords
        result.loc[0, 'abstract'] = abstract
        result.loc[0, 'description'] = description
        result.loc[0, 'repository'] = repository
        result.loc[0, 'doi'] = doi
        result.loc[0, 'link'] = link

        return result

    except:
        None

def scrape_acm(url = 'request_input'):

    if url == 'request_input':
        url = input('URL: ')
    
    if type(url) !=str:
        raise TypeError('URL must be a string')
    
    if ('acm.org' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for an ACM webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    soup = BeautifulSoup(res, 'lxml')
    
    item_type = 'article'
    link = soup.find(attrs={'property':'og:url'}).attrs['content']
    doi = link.replace('https://dl.acm.org/', '')
    date = soup.find(attrs={'class':"rlist article-chapter-history-list"}).text.replace('Published: ', '')
    source = soup.find(attrs={'class':"epub-section__title"}).text
    publisher = 'ACM'
    repository = 'ACM'
    title = soup.find(attrs={'property':'og:title'}).attrs['content']
    abstract = soup.find(attrs={'class':"abstractSection abstractInFull"}).p.text.strip()
    
    keywords = []
    try:
        chart_terms = soup.find(attrs={'class':"rlist organizational-chart"}).find_all('a')
        
        for i in chart_terms:
            keywords.append(i.text)
    except:
        None

    authors = []
    try:
        names_txt = soup.find_all(attrs={'class':'author-name'})
        for i in names_txt:
            authors.append(i.attrs['title'])
    except:
        None
    
    citations = []
    try:
        refs_txt = soup.find_all(attrs={'class':"references__note"})
        citation_links = []
        for i in refs_txt:
            citations.append(i.text.replace('Google Scholar', '').replace('Cross Ref', '').replace('Digital Library', ''))
            citation_links.append(i.find(attrs={'class':'references__suffix'}).a.attrs['href'])
    except:
        None
    
    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)

    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'source'] = source
    result.loc[0, 'publisher'] = publisher
    result.loc[0, 'type'] = item_type
    result.loc[0, 'keywords'] = keywords
    result.loc[0, 'abstract'] = abstract
    result.loc[0, 'repository'] = repository
    result.loc[0, 'doi'] = doi
    result.loc[0, 'link'] = link
    result.loc[0, 'citations'] = citations
    result.loc[0, 'citation_links'] = citation_links
    
    return result

def parse_muse_from_source(source = 'request_input', link = None):
    
    if source == 'request_input':
        source = input('HTML: ')
    
    if type(source) != str:
        raise TypeError('Source must be a string')

    soup = BeautifulSoup(source,'lxml')
    
    try:
        authors_html = soup.find_all(attrs={'name':'citation_author'})
        authors = [i.attrs['content'] for i in authors_html]
    except:
        authors = None
        
    try:
        item_type = soup.find(attrs={'class':'type'}).text
    except:
        item_type = None
    
    try:
        link = soup.find(attrs={'name':'citation_fulltext_html_url'}).attrs['content']
    except:
        link = link
    
    try:
        doi = soup.find(attrs={'name':'citation_doi'}).attrs['content']
    except:
        doi = None
    
    try:
        date = soup.find(attrs={'name':'citation_year'}).attrs['content']
    except:
        date = None
    
    try:
        source = soup.find(attrs={'name':"citation_journal_title"}).attrs['content']
    except:
        source = None
    
    try:
        publisher = soup.find(attrs={'name':'citation_publisher'}).attrs['content']
    except:
        publisher = None
    
    try:
        title = soup.find(attrs={'name':'citation_title'}).attrs['content']
    except:
        title = None
        
    try:
        abstract = soup.find(attrs={'class':"abstract"}).text.replace('Abstract:', '').replace('\nAbstract', '').replace('   ', ' ').replace('  ', ' ').strip('\n').strip()
    except:
        abstract = None
        
    repository = 'Project MUSE'
    
    refs_html = soup.find_all(attrs={'name':'citation_reference'})
    try:
        refs_list = [i.attrs['content'] for i in refs_html]
    except:
        refs_list = []
        
    citations = []

    for i in refs_list:
        
        ref_dict = {'authors': []}
        ref_split = i.split('; ')
        
        for entry in ref_split:
            pair = entry.split('=')
            
            if pair[0] == 'citation_author':
                ref_dict['authors'].append(pair[1])
            else:
                ref_dict[pair[0]] = pair[1]

        citation = ''

        if 'authors' in ref_dict.keys():
            citation = citation + ', '.join(ref_dict['authors']) + ' '

        if 'citation_year' in ref_dict.keys():
            citation = citation + '(' + ref_dict['citation_year'] + '),'

        if 'citation_title' in ref_dict.keys():
            citation = citation + ' ' + ref_dict['citation_title'] + '.'

        if 'citation_journal_title' in ref_dict.keys():
            citation = (citation + ' ' 
                        + ref_dict['citation_journal_title']
                       )

        if 'citation_volume' in ref_dict.keys():
            citation = (citation + ' ' 
                        + ref_dict['citation_journal_title']
                       )

        if 'citation_issue' in ref_dict.keys():
            citation = (citation + '(' + ref_dict['citation_issue']  + ').' )

        if 'citation_firstpage' in ref_dict.keys():
            citation = (citation + ' pp.' + ref_dict['citation_firstpage'])

        if 'citation_lastpage' in ref_dict.keys():
            citation = (citation + '-' + ref_dict['citation_lastpage'] + '.')

        if 'citation_publisher' in ref_dict.keys():
            citation = (citation + ' ' + ref_dict['citation_publisher'] + '.')

        citation = citation.strip()
        citations.append(citation)
    
    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)

    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'source'] = source
    result.loc[0, 'publisher'] = publisher
    result.loc[0, 'type'] = item_type
    result.loc[0, 'abstract'] = abstract
    result.loc[0, 'repository'] = repository
    result.loc[0, 'doi'] = doi
    result.loc[0, 'link'] = link
    result.loc[0, 'citations'] = citations
    
    return result

def scrape_muse(url = 'request_input'):
    
    if url == 'request_input':
        url = input('URL: ')
    
    if type(url) !=str:
        raise TypeError('URL must be a string')
    
    if ('muse.jhu.edu' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for a Project MUSE webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    
    result = parse_muse_from_source(source = res, link = url)
    
    return result

def parse_proquest_from_source(source, link = None):

    if type(source) !=str:
        raise TypeError('Source must be a string')

    soup = BeautifulSoup(source,'lxml')
    
    try:
        for i in soup.find_all(attrs={'type':'text/javascript'}):
            if 'pi(' in str(i):
                result = i.text.replace('require(["t5/core/pageinit"], function(pi) ', '').replace(' });', '}').strip('{ ').strip('}')

        result = result.replace('pi(', '').replace('["', '').split('],')
        metadata_txt = [i for i in result if 'DocViewAnalytics' in i]
        meta_json = metadata_txt[0].replace("components/docview/DocViewAnalytics", "").strip(',"')
        metadata = json.loads(meta_json)
    
    except:
        metadata = {
            'primaryAuthor': None, 
            'author2': None, 
            'author3: None,': None, 
            'containsCitedBy': None, 
            'documentLanguage': None, 
            'doi': None, 
            'fulltextAccess': None, 
            'goID': None, 
            'issn': None, 
            'languageOfPublication': None, 
            'pageCount': None, 
            'objectType': None, 
            'isOpenAccess': None, 
            'pubPlace': None, 
            'pubId': None, 
            'pubTitle': None, 
            'year': None, 
            'recordType': None, 
            'searchId': None, 
            'searchQueryTerms': None, 
            'sourceType': None, 
            'subjectIndex1': None, 
            'subjectIndex2': None, 
            'subjectIndex3': None, 
            'subjectIndex4': None, 
            'displayFormatString': None
                }
    
    doi = metadata['doi']
    item_type = metadata['recordType']
    link = link
    date = metadata['year']
    source = metadata['pubTitle']
    repository = 'ProQuest'
    title = soup.find(attrs={'id':'documentTitle'}).text

    auths_txt = soup.find(attrs={'class':"truncatedAuthor"}).text.replace('\xa0', '').replace('\\u', ' ').replace('\n', '')
    authors = auths_txt.split('.')[0].split(';')
    
    try:
        abstract = soup.find(attrs={'class':"abstractContainer"}).text.replace('\nTranslate', '').replace('Abstract. ', '').replace('Abstract', '').replace('ABSTRACT. ', '').replace('ABSTRACT', '').replace('You have requested "on-the-fly" machine translation of selected content from our databases. This functionality is provided solely for your convenience and is in no way intended to replace human translation. Show full disclaimerNeither ProQuest nor its licensors make any representations or warranties with respect to the translations. The translations are automatically generated "AS IS" and "AS AVAILABLE" and are not retained in our systems. PROQUEST AND ITS LICENSORS SPECIFICALLY DISCLAIM ANY AND ALL EXPRESS OR IMPLIED WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY WARRANTIES FOR AVAILABILITY, ACCURACY, TIMELINESS, COMPLETENESS, NON-INFRINGMENT, MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE. Your use of the translations is subject to all use restrictions contained in your Electronic Products License Agreement and by using the translation functionality you agree to forgo any and all claims against ProQuest or its licensors for your use of the translation functionality and any output derived there from. Hide full disclaimer\n\nLonger documents can take a while to translate. Rather than keep you waiting, we have only translated the first few paragraphs. Click the button below if you want to translate the rest of the document.\nTranslate All', '')
    except:
        abstract = None
        
    try:
        full_text_list = soup.find(attrs={'id':"fullTextZone"}).find_all('p')
        full_text_list = [i.text for i in full_text_list]

        full_text = '\n'.join(full_text_list).split('\nReferences\n')
        main_body = full_text[0]
        
        try:
            citations = full_text[1].split('\n')
            citation_links = full_text[1].split(' ')
            citation_links = [i for i in citation_links if (('http' in i) or ('www.') in i)]
        
        except:
            citations = None
            citation_links = None
        
    except:
        main_body = None
        citations = None
        citation_links = None
    
    if (main_body == None) or (abstract == None):
        try:
            main_text = soup.find(attrs={'role':'main'}).find_all('div')
            res_list = [i.find_all('p') for i in main_text if 'ABS' in str(i)]

            try:
                    res_text = [i.text for i in res_list[0]]
                    full_text = '\n'.join(res_text).replace('\nTranslate\n', '')
                    full_text = full_text.split('\nShow less\n')[0].replace('\n\n','').replace('Show less','')
                    split_text = full_text.split('\n\xa0\n')
                    abstract = split_text[0]

                    try:
                        main_body = split_text[1]
                    except:
                        main_body = None

            except:
                    abstract = None
                    main_body = None

        except:
            abstract = None
            main_body = None
    
    try:
        main_body = main_body.replace('ABSTRACT', '').replace('Abstract', '').strip().strip('.')
        abstract = abstract.replace('ABSTRACT', '').replace('Abstract', '').strip().strip('.')
    except:
        None
    
    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)

    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'source'] = source
    result.loc[0, 'type'] = item_type
    result.loc[0, 'abstract'] = abstract
    result.loc[0, 'full_text'] = main_body
    result.loc[0, 'repository'] = repository
    result.loc[0, 'citations'] = citations
    result.loc[0, 'citation_links'] = citation_links
    result.loc[0, 'doi'] = doi
    result.loc[0, 'link'] = link
    
    return result

def scrape_proquest(url = 'request_input'):
    
    if url == 'request_input':
        url = input('URL: ')
    
    if type(url) !=str:
        raise TypeError('URL must be a string')
    
    if ('proquest.com' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for a ProQuest webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    result = parse_proquest_from_source(source = res, link = url)
    
    return result

def parse_jstor_from_source(source = 'request_input', link = None):

    if source == 'request_input':
        source = input('Source code: ')
    
    if type(source) !=str:
        raise TypeError('Source must be a string')

    if (
        ('unusual traffic activity from your address' in source) 
        or ('unusual traffic activity from your device' in source) 
        or ('detected unusual traffic activity from your network' in source) 
        or ('complete this reCAPTCHA' in source) 
        or ('fill this reCAPTCHA' in source)
        or ('you making the requests and not a robot' in source)
        or ('confirm you are not a robot' in source)
        ):
        raise ValueError('The web browser seems to have been blocked by a bot detection system')
               
    
    soup = BeautifulSoup(source,'lxml')
    
    repository = 'JSTOR'
    
    try:
        res = soup.find_all(attrs={'type':'text/javascript'})
        meta_text = [i.text for i in res if 'itemTitle' in str(i)][0]
        meta_list = meta_text.replace('   ', '').replace('  ', '').replace('\n', '').split('{')
        meta_text = [i for i in meta_list if 'itemTitle' in i][0].split('}')[0].strip()
        meta_json = '{' + meta_text + '}'
        metadata = json.loads(meta_json)
        
    except:
        metadata = None
    
    try:
        entry_citation = soup.find(attrs={'name':'description'}).attrs['content']
    except:
        entry_citation = None
    
    if (metadata == None) and (entry_citation == None):
        return
    
    if entry_citation != None:
        
        description = entry_citation
        
        try:
            authors = entry_citation.split(metadata['itemTitle'])[0].strip().split(', ')
            authors = [i.strip(',') for i in authors]
        except:
            authors = None
        
        try:
            date_items = ' '.join([i for i in entry_citation.split(', ') if any(char.isdigit() for char in i)])
            date_items = date_items.replace('Vol. ', '').replace('No. ', '').replace('pp. ', '').replace('p. ', '').replace('-', ' ').replace('/', ' ').replace(')', '').replace('(', '').split(' ')
            date = [i for i in date_items if ((i.isdigit() == True) and (len(i) > 3))][0]
        except:
            date = None
            
    else:
        authors = None
        date = None
    
    try:
        link = soup.find(attrs={'rel':'canonical'}).attrs['href']
    except:
        link = link
    
    
    if metadata != None:
        
        try:
            item_type = metadata['itemType']
        except:
            item_type = None
    
        try:
            doi = metadata['objectDOI']
        except:
            doi = None
    
        try:
            source = metadata['contentName']
        except:
            source = None
    
        try:
            publisher = metadata['contentPublisher']
        except:
            publisher = None
    
        try:
            title = metadata['itemTitle']
        except:
            title = None
    
    else:
        item_type = None
        doi = None
        source = None
        publisher = None
        title = None
    
    global results_cols
    result = pd.DataFrame(columns = results_cols, dtype=object)

    result.loc[0, 'title'] = title
    result.loc[0, 'authors'] = authors
    result.loc[0, 'year'] = date
    result.loc[0, 'source'] = source
    result.loc[0, 'publisher'] = publisher
    result.loc[0, 'type'] = item_type
    result.loc[0, 'description'] = description
    result.loc[0, 'repository'] = repository
    result.loc[0, 'doi'] = doi
    result.loc[0, 'link'] = link
    
    return result

def scrape_jstor(url = 'request_input'):
    
    if url == 'request_input':
        url = input('URL: ')
    
    if type(url) !=str:
        raise TypeError('URL must be a string')
    
    if ('jstor.org' not in url) and ('doi.org' not in url):
        raise ValueError('URL must be for a JSTOR webpage')
    
    res = scrape_url(url = url, parse_pdf = False, output = 'html')
    result = parse_jstor_from_source(source = res, link = url)
    
    return result

def parse_google_scholar_source(source = 'request_input'):
    
    if source == 'request_input':
        source = '"""' + input('Source code: ') + '"""'
    
    if type(source) != str:
        raise TypeError('Query must be a string')
    
    soup=BeautifulSoup(source,'lxml')
    
    data_lids = soup.select('[data-lid]')
    
    global results_cols
    df = pd.DataFrame(columns = results_cols, dtype=object)

    for item in data_lids:

        index = len(df.index)
        
        try:
            df.loc[index, 'title'] = item.select('h3')[0].get_text()
            df.loc[index, 'link'] = item.select('a')[0]['href']
            df.loc[index, 'extract'] = item.select('.gs_rs')[0].get_text()
            
            author_list = []
            selected = item.select('a')
            
            for entry in selected:
                
                if 'related:' in entry['href']:
                    df.loc[index, 'recommendations'] = 'https://scholar.google.com/' + entry['href']
            
                if 'user=' in entry['href']:
                    df.loc[index, 'author_link'] = 'https://scholar.google.com' + entry['href']
                
                if 'cites=' in entry['href']:
                    df.loc[index, 'cited_by'] = 'https://scholar.google.com' + entry['href']
            
                entry_text = entry.get_text()
                
                if (
                    ('[' not in entry_text)
                    and (']' not in entry_text)
                    and (']' not in entry_text)
                    and ('.c' not in entry_text)
                    and ('.n' not in entry_text)
                    and (entry_text not in df.loc[index, 'title'])
                    and (entry_text != 'Save') 
                    and (entry_text != 'Cite') 
                    and ('Cited by' not in entry_text) 
                    and (entry_text != 'Related articles') 
                    and ('versions' not in entry_text)
                    and (entry_text != 'View as HTML')
                    and (entry_text != '\n')
                    and (entry_text != 'Library Search')
                    ):
                        author_list.append(entry_text)
            
            df.loc[index, 'authors'] = author_list
            
            if ((df.loc[index, 'title'] == np.nan) or (df.loc[index, 'title'] == None)) and ('books.google' in df.loc[index, 'link']):

                link = df.loc[index, 'link']
                gbooks_response = scrape_url(url = link)
                title=BeautifulSoup(gbooks_response.content,'html').select('title')[0].get_text()
                df.loc[index, 'title'] = title
            
                
            
        except Exception as e:
            print('')
    
    df = df.replace(np.nan, None)
    df['title'] = df['title'].str.replace('[PDF]', '').str.replace('[BOOK]', '').str.replace('[B]', '').str.replace('[HTML]', '')
    df['authors'] = df['authors'].apply(join_list_by_colon).str.replace('cached', '').str.replace('full View', '')
    df['authors'] = df['authors'].apply(split_str_by_colon)
    
    return df

def search_google_scholar(query = 'request_input', pages = 1, open_source = False):
    
    if query == None:
        query = input('Query: ')
    query = urllib.parse.quote_plus(query)
    
    url_base = 'https://scholar.google.com/scholar?start='
    if open_source == True:
          url_base = 'view-source:' + url_base

    for i in range(0, pages):
            url = url_base + str(i)+ '0' + '&q=' + query
            open_url(url)

def open_google_scholar_links(source = None):
    
    if source == None:
        source = '"""' + input('Source code: ') + '"""'

    res = parse_google_scholar_source(source = source)

    for i in res['Link'].values:
        open_url(i)

def scrape_google_scholar(url):
    
    if type(url) != str:
        raise TypeError('Query must be a string')
    
    headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10 _11_2) AppleWebkit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9'}
   
    try:
        response=requests.get(url,headers=headers) 
        source = response.content
    except:
        raise ValueError('The scraper encountered an error. Google Scholar may have blocked it.')
    
    try:
        return parse_google_scholar_source(source = source)
    except:
        raise ValueError('The scraper encountered an error. Google Scholar may have blocked it.')

def scrape_google_scholar_search(query):
    
    if type(query) != str:
        raise TypeError('Query must be a string')
    
    
    url_base = 'https://scholar.google.com/scholar?q='
    query = urllib.parse.quote_plus(query)
    url = url_base + query

    return scrape_google_scholar(url)

def iterate_scholar_pages(scholar_page, page_limit = 20):
    
    global results_cols
    df = pd.DataFrame(columns = results_cols, dtype=object)

    url_end = scholar_page.split('?')[-1]
    
    if 'start=' not in url_end:

        for i in range(0, page_limit):
            page = str(i)+'0'
            new_url = 'https://scholar.google.com/scholar?start=' + page + '&' + url_end
            df = pd.concat([df, scrape_google_scholar(new_url)])   
            

    else:
        for i in range(0, page_limit):
            page = str(i)+'0'
            url_split = scholar_page.split('start=')
            url_start = url_split[0]
            url_end = url_split[1][2:]

            new_url = url_start + 'start=' + str(page) + url_end
            df = pd.concat([df, scrape_google_scholar(new_url)])
    

    df = df.reset_index().drop('index', axis=1)
    
    return df

def scrape_doi(doi):

    if doi.startswith('www.doi.org/') ==  True:
        doi = doi.replace('www.doi.org/', 'https://doi.org/')

    if doi.startswith('https://doi.org/') == False:
        doi = 'https://doi.org/' + doi
    
    if check_bad_url(doi) == False:

        final_url = get_final_url(doi)

        return scrape_article(url=final_url)
    else:
        raise ValueError('Bad DOI code or URL. Please check.')

def scrape_article(url = 'request_input'):

    if url == 'request_input':
            url = input('URL: ')

    if type(url) != str:
        raise TypeError('URL must be a string.')

    global results_cols
    df = pd.DataFrame(columns = results_cols)

    if ('doi.org' in url):
        return scrape_doi(url)

    if ('frontiersin.org' in url):
        return scrape_frontiers(url)
    
    if ('arxiv.org' in url):
        return scrape_arxiv(url)
    
    if ('springer' in url):
        return scrape_springer(url)
    
    if ('nature.com' in url):
        return scrape_nature(url)
    
    if ('ieee.org' in url):
        return scrape_ieee(url)
    
    if ('pubmed.ncbi.' in url):
        return scrape_pubmed(url)
    
    if ('gov/pmc' in url):
        return scrape_pmc(url)
    
    if ('ssrn.com' in url):
        return scrape_ssrn(url)
    
    if ('heinonline.org' in url):
        return scrape_heinonline(url)
    
    if ('mdpi.com' in url):
        return scrape_mdpi(url)
    
    if ('acm.org' in url):
        return scrape_acm(url)
    
    if ('muse.jhu.edu' in url):
        return scrape_muse(url)
    
    if ('proquest.com' in url):
        return scrape_proquest(url)
    
    if ('jstor.org' in url):
        return scrape_jstor(url)
    
    if ('scholar.google.com' in url):
        return scrape_google_scholar(url)

    else:
        return df
    


# Automated scraping not functional for CUP, OUP, SAGE, T&F, Science, Wiley, SciDirect, or ResearchGate sites due to bot blockers. 
## See notebook for scripts for parsing CUP, OUP, SAGE, T&F, Science, Wiley, SciDirect, and ResearchGate