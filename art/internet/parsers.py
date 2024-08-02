"""Functions for parsing web data"""

from typing import List, Dict, Tuple
import copy
from bs4 import BeautifulSoup
import pandas as pd

def parse_google_result(html: str = 'request_input') -> pd.DataFrame:
    
    """
    Parses source code from Google search result page and returns a dataframe of results.
    """
    
    # Requesting source code from user input if none given
    if html == 'request_input':
        html = input('Source code: ')
    
    # Initialising output dataframe
    df = pd.DataFrame(columns = ['URL', 'Site', 'Title', 'Heading', 'Inner text', 'All text'])
    
    # Making an HTML soup 
    soup = BeautifulSoup(html, "html.parser")
    
    # Selecting section dividers
    a = soup.select('a')

    # Iterating through dividers
    for i in a:
        
        # Checking for valid links
        if ('href' in i.attrs) and (i['href'] != '#') and (i['href'] != ''):
            
            # If link is present, retrieving link
            url = i['href']
            
            # Correcting URL if it seems to be an internal link without a domain
            if url[0] == '/':
                url = correct_link_errors(source_domain = 'https://www.google.com', url = url)
            
            # Ignoring Google service pages
            if (
                ('/support.google.com' in url) 
                or ('webhp?' in url)
                or ('/account.google.com' in url)
                or ('/policies.google.com' in url)
                ):
                continue
            
            # Initialising data variables
            title = None
            site_name = None
            heading_text = None
            inner_text = None
            all_text = None
            
            # Selecting title using h3 divider
            h3 = i.find('h3')
            if h3 != None:
                title = h3.text
            
            # If available, adding GTRloc class data to title
            gtr_lock = i.find(attrs={'class':'GTRloc'})
            if gtr_lock != None:
                gtr_text = gtr_lock.find('span').text
                if gtr_text != None:
                    title = gtr_text + ': ' + title
            
            # Selecting heading using role class if object is 'heading'
            heading = i.find(attrs={'role':'heading'})
            if heading != None:
                heading_text = heading.text
            
            # Selecting inner text of result using role class if object is 'text'
            role_text = i.find(attrs={'role':'text'})
            if  role_text != None:
                inner_text = role_text.text
            
            # Extracting all text
            all_text = i.text
            
            # Selecting spans dividers
            spans = i.select('span')
            
            # Iterating through spans to extract additional and missing data
            for item in spans:

                span_text = item.text
                str_length = len(span_text)
                if all_text[:str_length] == span_text:
                    
                    # Adding title from span if none found so far
                    if title == None:
                        title = span_text
                
                # Adding site name
                if 'data-dtld' in item.attrs:
                    site_name = item['data-dtld']

            # Adding title from all text if none found so far
            if title == None:
                title = all_text
            
            # Adding site name as URL domain if none found so far
            if site_name == None:
                site_name = get_domain(url)
            
            # Appending result to dataframe
            index_pos = len(df.index)
            df.loc[index_pos] = [url, site_name, title, heading_text, inner_text, all_text]
        
    
    return df