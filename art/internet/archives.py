"""Functions for interacting with web archives."""
 
from .webanalysis import correct_url

from typing import List, Dict, Tuple
from datetime import datetime, date, timedelta
import webbrowser
import copy

import wayback
from wayback import WaybackClient
from comcrawl import IndexClient

def search_archiveis(url = 'request_input'):
    
    """
    Opens an ArchiveIs search for a URL using the default web browser.
    """
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Creating base for search URL
    url = 'https://archive.is/' + url
    
    # Opening search in browser
    return webbrowser.open(url)

def search_internet_archive(
                            url: str = 'request_input', 
                           get: str = 'all', 
                           from_datetime = None, 
                           to_datetime = None, 
                           filter_field = None, 
                           output_metadata: bool = False
                            ):
    
    """
    Searches the Internet Archive for a URL.
    
    Parameters
    ----------
    url : str
        URL to search. Defaults to requesting from user input.
    get : str
        whether to return the first archive result or all results. Defaults to 'all'.
    from_datetime : str or None
        earliest date to search from. Defaults to None.
    to_datetime : str or None
        last date to search until. Defaults to None.
    filter_field : str
        the field to return.
    output_metadata : bool
        whether to output archive metdata.
    """
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Formatting datetimes if passed as arguments 
    if from_datetime != None:
    
        if '.' in from_datetime:
            from_datetime = datetime.strptime(from_datetime, '%d.%m.%Y')

        elif '/' in from_datetime:
            from_datetime = datetime.strptime(from_datetime, '%d/%m/%Y')

        elif '-' in from_datetime:
            from_datetime = datetime.strptime(from_datetime, '%d-%m-%Y')
        
        else:
            from_datetime = datetime.strptime(from_datetime, '%d%m%Y')
            
        from_datetime = from_datetime - timedelta(hours=12)

    if to_datetime != None:
    
        if '.' in to_datetime:
            to_datetime = datetime.strptime(to_datetime, '%d.%m.%Y')

        elif '/' in to_datetime:
            to_datetime = datetime.strptime(to_datetime, '%d/%m/%Y')

        elif '-' in to_datetime:
            to_datetime = datetime.strptime(to_datetime, '%d-%m-%Y')
        
        else:
            to_datetime = datetime.strptime(to_datetime, '%d%m%Y')
            
        to_datetime = to_datetime + timedelta(hours=12)
    
    # Initialising Wayback Machine client
    client = WaybackClient()
    
    # Retrieving results
    results = client.search(url,
                            from_date = from_datetime, 
                            to_date = to_datetime,
                            filter_field = None
                           )
    
    # Exiting if no results found
    if results == None:
        return print('No archives found')
    
    # Initialising metadata dictionary
    metadata = {}
    
    # If instructed to get the first result, retrieving metadata for first Wayback result
    if get == 'first':
        
        record = next(results)
        
        metadata = {
                'url': record[2],
                'date_time': record[1].strftime("%Y-%m-%d %H:%M:%S"),
                'file_type': record[3],
                'status_code': record[4],
                'raw_url': record[7],
                'view_url': record[8]}
        
        # Outputting metadata if instructed
        if output_metadata == True:
            print(metadata)
            
        return record
    
    # Otherwise, iterating through results and outputting all metadata
    else: 
        
        record_list = []
        
        for item in results:
            print(item)
            record_list.append(item)
            
            
            metadata[item[0]] = {
                'url': item[2],
                'date_time': item[1].strftime("%Y-%m-%d %H:%M:%S"),
                'file_type': item[3],
                'status_code': item[4],
                'raw_url': item[7],
                'view_url': item[8]}
    
        if output_metadata == True:
            print(metadata)
        
        # If instructed to get the last result, retrieving metadata for last Wayback result
        if get == 'latest':
            return record_list[-1]
            
        else:
            return record_list
    
        
    

def open_internet_archive(
                        url = 'request_input', 
                          precise_date = None, 
                          date_range = None, 
                          get: str = 'first', 
                          open_index: int = 0
                         ):
    
    """
    Opens Internet Archive search for a URL in default browser.
    
    Parameters
    ----------
    url : str
        URL of archive to open. Defaults to requesting from user input.
    precise_date : str, datetime, or None
        a specific date to retrieve archive results from. If None, this is ignored. Defaults to None.
    date_range : str or None
        range of archive dates to open.
    get : str
        whether to return the first archive result or all results. Defaults to 'all'.
    open_index : int
        index of result to open. Defaults to 0.
    """
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Correcting URL
    if 'http' not in url:
        url = 'https://' + url
    
    # Defining base URL for search
    url_base = 'https://web.archive.org/web/'
    
    # If provided with a precise date, formatting search URL to specify that date
    if precise_date != None:
        
        precise_date = precise_date.replace(' ', '').replace('--', '-').replace('.', '-').replace('/','-')
        date_time_obj = datetime.strptime(precise_date, '%d-%m-%Y')
        date_time_str = date_time_obj.strftime('%Y%m%d%%H%M%S')
        archive_url = url_base + date_time_str + '/' + url + '/'
        
        # Running search
        return webbrowser.open(archive_url)
    
    # If provided with a date range, formatting search URL to specify that range
    if date_range != None:
        
        dates = date_range.replace(' to ', '-').replace(' ', '').replace('--', '-')
        dates = dates.split('-')
        from_date = dates[0]
        to_date = dates[1]
    
    else: 
        from_date = None
        to_date = None
    
    # Running search
    result = search_internet_archive(url, get = get, from_datetime = from_date, to_datetime = to_date)
    
    # Correcting result type
    if ')/' in result[0]:
        result = [result]
    
    # Checking if result is None or invalid
    if (
        (result == None)
        or (bool(result) == False)
    ):
        return
    
    # If instructed, opening all archives in browser
    if open_index == 'all':
        for i in range(0, len(result)):
            archive_url = result[i][8]
            webbrowser.open(archive_url)
   
    # Else, opening specified archive as instructed
    else:
        archive_url = result[open_index][8]
        return webbrowser.open(archive_url)
    
    

def search_common_crawl(url = 'request_input', threads: int = 1):
    
    """
    Searches the Common Crawl for a URL.
    
    Parameters
    ----------
    url : str
        URL to search. Defaults to requesting from user input.
    threads : int
        how many threads to use.
    """
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Adding URL prefixes and domain if necessary
    url = correct_url(url)
    
    # Initialising CC client
    client = IndexClient()

    # Searching CC
    client.search(url, threads = threads)
    
    # Downloading result
    client.download(threads = threads)
    
    # Storing result
    result = client.results
    
    return result


def search_cc_index(url: str, index_name: str = 'CC-MAIN-2023-40'):
    
    """
    Searches the Common Crawl's index for a URL.
    
    Parameters
    ----------
    url : str
        URL to search. Defaults to requesting from user input.
    index_name : str
        the Common Crawl index you want to query.
        *** Replace this with the latest index name
    """
    
    # Defining the URL of the Common Crawl Index server
    CC_INDEX_SERVER = 'http://index.commoncrawl.org/'
    
    
    encoded_url = quote_plus(url)
    index_url = f'{CC_INDEX_SERVER}{index_name}-index?url={encoded_url}&output=json'
    response = requests.get(index_url)
    print("Response from CCI:", response.text)  # Output the response from the server
    
    if response.status_code == 200:
        records = response.text.strip().split('\n')
        return [json.loads(record) for record in records]
    else:
        return None


def fetch_page_from_cc(records: list):
    
    """
    Fetches Common Crawl records.
    """
    
    for record in records:
        offset, length = int(record['offset']), int(record['length'])
        prefix = record['filename'].split('/')[0]
        s3_url = f'https://data.commoncrawl.org/{record["filename"]}'
        response = requests.get(s3_url, headers={'Range': f'bytes={offset}-{offset+length-1}'})
        
        if response.status_code == 206:
            # Process the response content if necessary
            # For example, you can use warcio to parse the WARC record
            return response.content
        
        else:
            print(f"Failed to fetch data: {response.status_code}")
            return None

        
def fetch_common_crawl_record(url = 'request_input', index_name: str = 'CC-MAIN-2023-40'):
    
    """
    Fetches Common Crawl records for URL.
    
    Parameters
    ----------
    url : str
        url to fetch
    index_name : str
        the Common Crawl index you want to query
        *** Replace this with the latest index name
    """
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Search the index for the target URL
    records = search_cc_index(url, index_name)
    
    # Proceeding if records found
    if records:
        print(f"Found {len(records)} records for {url}")

        # Fetch the page content from the first record
        content = fetch_page_from_cc(records)
        if content:
            print(f"Successfully fetched content for {url}")
            # You can now process the 'content' variable as needed
    else:
        print(f"No records found for {url}")
        content = None
    
    return content