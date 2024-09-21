"""Functions for url, domain, website, IP and WhoIs analysis"""

from typing import List
import copy
import re
import ipaddress
import webbrowser
from courlan import check_url
from urllib.parse import quote


def is_url(url: str) -> bool:

    """
    Checks if a string is a correctly formatted URL. Returns True if yes; False if no.
    """
    
    url_regex = re.compile(r'https?://(?:www\.)?[a-zA-Z0-9./]+')
    return bool(url_regex.match(url))

def domain_splitter(web_address: str) -> str:
    
    """
    Takes URL and returns its domain.
    """
    
    # Checking type; converting lists, sets, and tuples to strings
    if (
        ((type(web_address) == list)
        or (type(web_address) == set)
        or (type(web_address) == tuple)) 
        and (len(web_address) > 0)
        ):
            web_address = list(web_address)[0]
    
    if type(web_address) != str:
        raise TypeError('Domain_splitter requires a string')
    
    # Removing URL prefixes
    web_address = web_address.replace('https://', '').replace('https=://', '').replace('www.', '')
    
    # Splitting URL into substrings
    split = web_address.split('/')
    
    # Taking the first substring; removing unwanted leading and trailing characters
    domain = split[0].strip('/').strip('.').strip()
    
    return domain

def is_domain(domain_name: str = 'request_input') -> bool:
    
    """
    Checks if string is a valid domain format.
    """
    
    # Requesting domain name from user input if none given
    if domain_name == 'request_input':
        domain_name = input('Domain name to check: ')
    
    # If list is inputted, converts to string
    if (type(domain_name) == list) and (len(domain_name) > 0):
        domain_name = domain_name[0]
    
    # If set or tuple is inputted, converts to string
    if (((type(domain_name) == set)
        or (type(domain_name) == tuple))
        and (len(domain_name) > 0)):
        domain_name = list(domain_name)[0]
    
    # Raising errors if input is not string, list, or set
    if type(domain_name) != str:
        raise TypeError('Domain name to check must be a string')
    
    # Checking the overall length of the domain name; should not exceed 253 characters
    if len(domain_name) > 253:
        return False

    # Regular expression pattern to validate domain name:
    #   - (?!-) ensures that the label doesn't start with a hyphen
    #   - [A-Za-z0-9-]{1,63} ensures that the label contains 1 to 63 alphanumeric characters or hyphens
    #   - (?<!-) ensures that the label doesn't end with a hyphen
    #   - \. matches a dot between labels
    #   - [A-Za-z]{2,} matches a two or more character top-level domain (TLD)
    pattern = r'^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,}$'

    # Checking if the domain name matches the pattern
    try:
        if re.match(pattern, domain_name):
            return True
        else:
            return False
    except:
        return False

def is_ip_address(string: str = 'request_input') -> bool:
    
    """
    Checks if string is a valid IP address.
    """
    
    # Requesting string from user input if none given
    if string == 'request_input':
        string = input('String to check: ')
    
    # Converting lists, sets, and tuples to strings; else raising TypeError
    if ((type(string) == list)
        or (type(string) == set)
        or (type(string) == tuple)
        ) and (len(string) > 0):
        string = list(string)[0]
    
    if type(string) != str:
        raise TypeError('IP address to check must be a string')
    
    # Cleaning string
    string = string.strip().strip('/').strip('\\').strip('#').strip('-').strip()
    
    # Checking if string is an IP address
    try:
        ipaddress.ip_address(string)
        return True
    
    except:
        return False

def is_registered_domain(domain_name: str) -> bool:
    
    """
    Returns a boolean indicating whether a given domain name is registered.
    """
    
    try:
        w = whois.whois(domain_name)
    except Exception:
        return False
    else:
        return bool(w.domain_name)

def correct_url(url: str) -> str:
    
    """
    If URL lacks an HTTPS prefix, adds one.
    """
    
    if (url.startswith('https://') == False) and (url.startswith('http://') == False):
        url = 'https://' + url
    
    return url

def get_domain(url: str) -> str:
    
    """
    Returns a URL's domain.
    """
    
    # Using check_url() to extract domain
    try:
        domain = check_url(url)[1]
        
        # If check_url() does not find a domain, falls back to domain_splitter()
        if domain == None:
            domain = domain_splitter(url)
    
    # If check_url() raises an error, falls back to domain_splitter()
    except:
        domain = domain_splitter(url)
            
    return domain
            
## Functions for opening links and addresses

def open_url(url = 'request_input'):
    
    """
    Opens URL in the default web browser.
    """
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Opening browser
    return webbrowser.open(url)

def open_doi(doi = 'request_input'):

    """
    Opens DOI in the default web browser.
    """

    if doi == 'request_input':
        doi = input('DOI: ')
    
    url = 'https://doi.org/' + doi
    
    return webbrowser.open(url)

def open_urls_list(urls: list):
    
    """
    Opens list of URLs in the default web browser.
    """
    
    # Iterating through URLs list
    for link in urls:
        open_url(link)

def open_url_source(url = 'request_input'):
    
    """
    Opens URL's source code in the default web browser.
    """
    
    # Requesting URL from user input if none given
    if url == 'request_input':
        url = input('URL: ')
    
    # Editing URL to view source
    url = 'view-source:' + url
    
    # Opening browser
    open_url(url)

def regex_check_then_open_url(url: str):
    
        """
        Checks if string is a web address or file path. If true, opens using default web browser.
        """
    
    # Regex objects to check type

        www = re.compile(r'www\.', re.IGNORECASE)
        https = re.compile(r'https://', re.IGNORECASE)
        http = re.compile(r'http://', re.IGNORECASE)
        file_address = re.compile(r'file://', re.IGNORECASE)
        win_drive = re.compile(r'[A-Za-z]:\\', re.IGNORECASE)
        unc = re.compile(r'\\\\', re.IGNORECASE)
        rel = re.compile(r'\.\\', re.IGNORECASE)
        mac = re.compile(r'~/', re.IGNORECASE)
        mac_user = re.compile(r'/Users', re.IGNORECASE)
        unix_par = re.compile(r'\.\.', re.IGNORECASE)
        unix_home = re.compile(r'~', re.IGNORECASE)

         # Checking for matches
        www_match = www.search(url)
        https_match = https.search(url)
        http_match = http.search(url)
        file_address_match = file_address.search(url)
        win_drive_match = win_drive.search(url)
        unc_match = unc.search(url)
        rel_match = rel.search(url)
        mac_match = mac.search(url)
        mac_user_match = mac_user.search(url)
        unix_p_match = unix_par.search(url)
        unix_h_match = unix_home.search(url)
        
        # Checking if string is a file path; if true, opens in web browser
        if (
                (file_address_match != None) == True
                or ((win_drive_match != None) == True)
                or ((unc_match != None) == True)
                or ((rel_match != None) == True)
                or ((mac_match != None) == True)
                or ((mac_user_match != None) == True)
                or ((unix_p_match != None) == True)
                or ((unix_h_match != None) == True)
                ):
                is_filepath = 'True'
                url = 'file://' + url
                return webbrowser.open(url, new=1)
        
        # Checking if string is a web address; if true, opens in web browser
        if (
                ((www_match != None) == True)
                and (https_match == None)
                and (http_match == None)
                ):
                is_webaddress = 'True'
                url = 'https://' + url
                return webbrowser.open(url)

        # Checking if string is a web address; if true, opens in web browser
        if (((https_match != None) == True)
                or ((http_match != None) == True)
                ):
                is_webaddress = 'True'
                return webbrowser.open(url)
        
        # If checks have failed, searches for string on Google
        if ((
            www_match == None
            and https_match == None
            and http_match == None
            and file_address_match == None
            and win_drive_match == None
            and unc_match == None
            and rel_match == None
            and mac_match == None
            and mac_user_match == None
            and unix_p_match == None
            and unix_h_match == None)
            and (',' or ', ') not in url
            ):
                query = quote(url)
                url_base = 'https://www.google.com/search?hl=en&q='
                url = url_base + query
                return webbrowser.open(url)

def url_to_valid_attr_name(url: str) -> str:
    
    """
    Converts URL to a string which can be used as a Python object attribute name.
    """
    
    name = url.replace('https://', '').replace('http://', '').replace('www.', '')
    name = name.replace('.', '_').replace('~', '_').replace('/', '_').replace('&', '_').replace('#', '_').replace('@', '_at_')
    name = name.replace('(', '').replace(')', '').replace('{', '').replace('}', '').replace('%', '').replace('!', '').replace('?', '').replace('*', '').replace(':', '')
    name = name.replace('<', '').replace('>', '').replace('-', '').replace('+', '_').replace('=', '_')
    name = name.replace('"', '').replace("'", "")
    name = name.strip().lower()
    
    return name