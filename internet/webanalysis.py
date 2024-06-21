"""Functions for url, domain, website, IP and WhoIs analysis"""

from typing import List
import copy
import re
import ipaddress
import socket
import webbrowser
import numpy as np
import pandas as pd
from courlan import check_url
from urllib.parse import quote


def is_url(url: str) -> bool:
    
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

def get_my_ip() -> str:
    
    """
    Returns user's IP address.
    """
    
    # Trying to retrieve IP address using socket's gethostbyname() method
    try:
        hostname=socket.gethostname()
        IPAddr=socket.gethostbyname(hostname)
    
    
    except:
        
        # If socket's gethostbyname() method raises an error, trying to use socket's getfqdn() method
        try:
            IPAddr = socket.gethostbyname(socket.getfqdn()) 
            
        except:
            
            # If socket's getfqdn() method raises an error, trying to use socket's connect() and getsockname() methods
            
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            IPAddr = s.getsockname()[0]
            s.close()
    
    return IPAddr

def get_my_ip_geocode():
    
    """
    Returns the geocode associated with user's IP address.
    """
    
    return geocoder.ip('me')

def get_ip_geocode(ip_address: str = 'request_input'):
    
    """
    Returns the geocode associated with an IP address.
    """
    
    # Requesting IP address from user input if none given
    if ip_address == 'request_input':
        ip_address = input('IP address: ')

    return geocoder.ip(ip_address)

def get_ip_coordinates(ip_address: str = 'request_input') -> str:
    
    """
    Returns the coordinates associated with an IP address.
    """
    
    # Requesting IP address from user input if none given
    if ip_address == 'request_input':
        ip_address = input('IP address: ')
    
    # Retrieving geocode
    g = geocoder.ip(ip_address)
    
    # Retrieving coordinates
    coords = str(g.latlng)
    
    return coords

def get_my_ip_coordinates() -> str:
    
    """
    Returns the coordinates associated with user's IP address.
    """
    
    return get_ip_coordinates(ip_address = 'me')

def get_ip_physical_location(ip_address: str = 'request_input') -> str:

    """
    Returns the address associated with an IP address.
    """
    
    # Requesting IP address from user input if none given
    if ip_address == 'request_input':
        ip_address = input('IP address: ')
    
    # Retrieving address associated with IP's coordinates using Geopy
    address = str(geocoder.ip(ip_address).address)
    
    return address

def get_my_ip_physical_location() -> str:
    
    """
    Returns the address associated with user's IP address.
    """
    
    # Retrieving geocode associated with user's IP
    location = get_my_ip_geocode()
    
    # Retrieving address
    address = str(location.address)
    
    return address

def lookup_ip_coordinates(ip_address: str = 'request_input', site: str = 'Google Maps'):
    
    """
    Searches for coordinates associated with an IP address on a chosen mapping platform.
    
    Parameters
    ----------
    ip_address : str
        IP address to look up.
    site : str
        name of mapping platform to use. Defaults to 'Google Maps'.
    """
    
    # Requesting IP address from user input if none given
    if ip_address == 'request_input':
        ip_address = input('IP address: ')
    
    # Retrieving coordinates
    coordinates = get_ip_coordinates(ip_address = ip_address)
    latitude = coordinates[0]
    longitude = coordinates[1]
    
    # Searching for coordinates on mapping platform
    try:
        return lookup_coordinates(latitude = latitude, longitude = longitude, site = site)
    except:
        raise ValueError('Lookup failed. Please check the IP address provided.')


def domain_from_ip(ip_address: str) -> pd.Series:
    
    """
    Returns domain address associated with IP address.
    """
    
    # Cleaning IP address
    ip_address = ip_address.strip().strip('/').strip()
    
    # Checking if string is a valid IP address
    if is_ip_address(ip_address) == True:
        
        # Trying to retrieve domain
        try:
            result = socket.gethostbyaddr(ip_address)
        except socket.herror:
            return "No domain details found"
        
        # Creating Pandas series for result
        output_series = pd.Series(result, index = ['domain_name', 'aliases', 'ip_address'])

        return output_series
    
    else:
        raise ValueError('Address given is not a valid IP address')

def ip_from_domain(domain: str) -> str:
    
    """
    Returns IP address associated with domain address.
    """
    
    # Checking if string is a valid domain address
    if is_domain(domain) == False:
        try:
            domain = domain_splitter(domain)
        except:
            pass
                
        if is_domain(domain) == False:
            domain = None
    
    # Trying to retrieve IP address associated with domain
    try:
        result = str(socket.gethostbyname(domain))
        
    except socket.herror:
        return "No domain details found"
    
    return result


            
            

class WhoisResult:
    
    """
    This is a class to store WhoIs result data.
    
    Parameters
    ----------
    domain : str
        domain to run WhoIs search on.
    ip_address : str
        IP address to run WhoIs search on.
    """
    
    def __init__(self, domain: str = None, ip_address: str = None):
        
        """
        Initialises WhoisResult object.
        
        Parameters
        ----------
        domain : str
            domain to run WhoIs search on.
        ip_address : str
            IP address to run WhoIs search on.
        """
        
        # Creating results dataframe and assigning as attribute
        self.all_results = pd.DataFrame(columns = ['Metadata'], dtype = object)
        self.all_results.index.name = 'Category'
        
        # Cleaning domain if given
        if domain != None:
            if is_domain(domain) == False:
                try:
                    domain = domain_splitter(domain)
                except:
                    pass
                
                if is_domain(domain) == False:
                    domain = None
        
        # Cleaning IP address if given
        if ip_address != None:
            ip_address = ip_address.strip().strip('/').strip()
            if is_ip_address(ip_address) == False:
                ip_address = None
        
        # If a domain is given but no IP address, tries to retrieve associated IP address
        if (domain != None) and (ip_address == None):
            try:
                ip_address = ip_from_domain(domain)
            except:
                ip_address = None
        
        # If an IP address is given but no domain, tries to retrieve associated domain
        if (domain == None) and (ip_address != None):
            try:
                domain = domain_from_ip(ip_address)['domain_name']
            except:
                domain = None
        
        # Assigning domain and IP address as attribute
        self.domain = domain
        self.ip_address = ip_address
        
        # Running domain WhoIs lookup
        try:
            self.domain_whois(domain = domain, ip_address = ip_address)
        except:
            pass
        
        # Running IP WhoIs lookup
        try:
            self.ip_whois(domain = domain, ip_address = ip_address)
        except:
            pass
        
        # If IP WhoIs search has been successful, assigns RDAP result as an attribute
        try:
            self.RDAP_obj = self.IPWhois_obj.lookup_rdap(depth=1)
        except:
            self.RDAP_obj = None
        
        # If IP WhoIs search has been successful, assigns RDAP result as an attribute
        try:
            self.results_dict = self.RDAP_obj
        except:
            self.results_dict = None
        
        # If WhoIs search has been successful, assigns rdap_res result to results dataframe
        try:
            rdap_res = pd.Series(self.RDAP_obj)
            df = pd.DataFrame(rdap_res, columns = ['Metadata'])
            df.index.name = 'Category'
            self.all_results = pd.concat([df, self.all_results])
        except:
            pass
        
        # If WhoIs search has been successful, assigns nir result as an attribute
        try:
            self.nir = self.all_results.loc['nir', 'Metadata']
        except:
            self.nir = None
        
        # If WhoIs search has been successful, assigns asn result as an attribute
        try:
            self.asn_registry = self.all_results.loc['asn_registry', 'Metadata']
        except:
            self.asn_registry = None
        
        # If WhoIs search has been successful, assigns asn result as an attribute
        try:
            self.asn = self.all_results.loc['asn', 'Metadata']
        except:
            self.asn = None
        
        # If WhoIs search has been successful, assigns asn_cidr result as an attribute
        try:
            self.asn_cidr = self.all_results.loc['asn_cidr', 'Metadata']
        except:
            self.asn_cidr = None
        
        # If WhoIs search has been successful, assigns asn country code result as an attribute
        try:
            self.asn_country_code = self.all_results.loc['asn_country_code', 'Metadata']
        except:
            self.asn_country_code = None
        
         # If WhoIs search has been successful, assigns asn date country code result as an attribute
        try:
            self.asn_date = self.all_results.loc['asn_date', 'Metadata']
        except:
            self.asn_date = None
        
         # If WhoIs search has been successful, assigns asn description code result as an attribute
        try:
            self.asn_description = self.all_results.loc['asn_description', 'Metadata']
        except:
            self.asn_description = None
        
         # If WhoIs search has been successful, assigns query result as an attribute
        try:
            self.query = self.all_results.loc['query', 'Metadata']
        except:
            self.query = None
        
        # If WhoIs search has been successful, assigns entities result as an attribute
        try:
            self.entities = self.all_results.loc['entities', 'Metadata']
        except:
            self.entities = None
        
        # If WhoIs search has been successful, assigns raw result as an attribute
        try:
            self.raw = self.all_results.loc['raw', 'Metadata']
        except:
            self.raw = None
        
        # If WhoIs search has been successful, assigns network result as an attribute
        try:
            self.network = pd.DataFrame(pd.Series(self.all_results.loc['network', 'Metadata']), columns = ['Metadata'])
            self.network.index.name = 'Category'
        except:
            self.network = None
        
        # If WhoIs search has been successful, parses network events result and assigns as an attribute
        try:
            
            self.network_events = pd.DataFrame(columns = ['action', 'timestamp', 'actor'])
            events = self.network.loc['events', 'Metadata']
            
            for i in events:
                df = pd.DataFrame(pd.Series(i)).T
                self.network_events = pd.concat([self.network_events, df])
            
            self.network_events = self.network_events.reset_index().drop('index', axis=1)
            self.all_results.at['network_events', 'Metadata'] = events
            
        except:
            self.network_events = None
        
        # If WhoIs search has been successful, parses network notices result and assigns as an attribute
        try:
            
            self.network_notices = pd.DataFrame(columns = ['title', 'description', 'links'])
            notices = self.network.loc['notices', 'Metadata']
            
            for i in notices:
                df = pd.DataFrame(pd.Series(i)).T
                self.network_notices = pd.concat([self.network_notices, df])
            
            self.network_notices = self.network_notices.reset_index().drop('index', axis=1)
            self.all_results.at['network_notices', 'Metadata'] = notices

        except:
            self.network_notices = None
        
        # If WhoIs search has been successful, parses objects result and assigns as an attribute
        try:
            objects_series = pd.Series(self.all_results.loc['objects', 'Metadata'])
            self.objects = pd.DataFrame(dtype = object)
            
            for i in objects_series:
                obj_df = pd.DataFrame(pd.Series(i)).T
                self.objects = pd.concat([self.objects, obj_df]).reset_index().drop('index', axis=1)
            
        except:
            self.objects = None
        
        # If WhoIs search has been successful, parses contacts result and assigns as an attribute
        try:
            contacts_series = self.objects['contact']
    
            self.contacts = pd.DataFrame(dtype = object)
            
            for i in contacts_series:
                contact_df = pd.DataFrame(pd.Series(i)).T
                try:
                    contact_df.loc[0, 'address'] = contact_df.loc[0, 'address'][0]['value']
                except:
                    None
                
                try:
                    contact_df.loc[0, 'phone'] = contact_df.loc[0, 'phone'][0]['value']
                except:
                    None
                
                try:
                    contact_df.loc[0, 'email'] = contact_df.loc[0, 'email'][0]['value']
                except:
                    None
                
                self.contacts = pd.concat([self.contacts, contact_df]).reset_index().drop('index', axis=1)
                self.all_results.at['contacts', 'Metadata'] = contacts_series.to_list()
        except:
            self.contacts = None
        
        # Cleaning up results dataframe
        self.all_results = self.all_results.replace(np.nan, None)
        self.all_results = self.all_results.reset_index().drop_duplicates(subset = 'Category').set_index('Category')
                        
            
    def domain_whois(self, domain: str, ip_address: str):
    
        """
        Performs a WhoIs lookup on a domain.
        """

        # Checking if domain address given is valid; trying to retrieve domain if not
        if domain != None:
                if is_domain(domain) == False:
                    try:
                        domain = domain_splitter(domain)
                    except:
                        pass

                    if is_domain(domain) == False:
                        domain = None

        # Checking if IP address given is valid
        if ip_address != None:
            if is_ip_address(ip_address) == False:
                ip_address = None

        # If only an IP address has been given, tries to retrieve associated domain address
        if (domain == None) and (ip_address != None):
            try:
                domain = domain_from_ip(ip_address)['domain_name']
            except:
                domain = None
                return

            # Using recursion, runs a WhoIs lookup on the domain that is found
            domain_whois(self, domain = domain, ip_address = ip_address)

        # Running WhoIs lookup on the domain given
        if domain != None:

                # Trying to run WhoIs search on domain
                try:
                    # Creating dataframe for output and inputting WhoIs result
                    df = pd.DataFrame(pd.Series(whois.whois(domain)), columns = ['Metadata'])
                    df.index.name = 'Category'
                    self.all_results = pd.concat([self.all_results, df])
                    self.all_results.loc['domain', 'Metadata'] = domain

                except:
                    pass

                # Trying to retrieve associated IP address if none given
                try:
                    if ip_address == None:
                        try:
                            ip_address = ip_from_domain(domain)
                        except:
                            ip_address = None

                        self.ip_address = ip_address
                        self.all_results.loc['ip_address', 'Metadata'] = ip_address

                    else:
                        pass

                except:
                    pass
    
    
    def ip_whois(self, domain: str, ip_address: str):

        """
        Performs a WhoIs lookup on an IP address.
        """

        # Checking if domain address given is valid; trying to retrieve domain if not
        if domain != None:
                if is_domain(domain) == False:
                    try:
                        domain = domain_splitter(domain)
                    except:
                        pass

                    if is_domain(domain) == False:
                        domain = None

        # Checking if IP address given is valid
        if ip_address != None:

            ip_address = ip_address.strip().strip('/').strip()
            if is_ip_address(ip_address) == False:
                ip_address = None

        # If only a domain has been given, tries to retrieve associated IP address
        if (domain != None) and (ip_address == None):
            try:
                ip_address = ip_from_domain(domain)
                self.ip_address = ip_address

                # Using recursion, runs a WhoIs lookup on the IP address that is found
                ip_whois(self, domain = domain, ip_address = ip_address)
            except:
                pass

        # Running WhoIs lookup on the IP address given
        if ip_address != None:
                
                # Retrieving domain if none already given/found
                if domain == None:
                    try:
                        domain = domain_from_ip(ip_address)['domain_name']
                    except:
                        domain = None
                    self.domain = domain
                
                # Running domain WhoIs on domain
                try:
                    domain_whois(self, domain = domain, ip_address = ip_address)
                except:
                    pass
                
                # Formatting results as pandas.DataFrame
                try:
                    domain_reverse_search = pd.DataFrame(domain_from_ip(ip_address), columns = ['Metadata'])
                    domain_reverse_search.index.name = 'Category'
                    self.all_results = pd.concat([self.all_results, domain_reverse_search])
                except:
                    pass
                
                # Running WhoIs lookup on IP address
                try:
                    self.IPWhois_obj = IPWhois(ip_address)
                except:
                    pass
                
                # Retrieving geocode associated with IP address using Geopy
                try:
                    self.ip_geocode = get_ip_geocode(ip_address)
                    self.all_results.at['ip_geocode', 'Metadata'] = self.ip_geocode
                except:
                    pass
                
                # Retrieving coordinates associated with IP address using Geopy
                try:
                    self.ip_coordinates = get_ip_coordinates(ip_address)
                    self.all_results.at['ip_coordinates', 'Metadata'] = self.ip_coordinates
                except:
                    pass
                
                # Retrieving location associated with IP address using Geopy
                try:
                    self.ip_location = get_ip_physical_location(ip_address)
                    self.all_results.at['ip_location', 'Metadata'] = self.ip_location
                except:
                    pass
                
                # Assigning results to WhoisResult object
                try:
                    self.all_results.at['ip_address', 'Metadata'] = ip_address
                except:
                    None

        else:
            self.IPWhois_obj = None



    def __repr__(self):
        
            """Controls how WhoIsResult objects are represented in string form."""
        
            return str(self.all_results)

    def contents(self):
            
            """Returns a list of object contents."""
            
            return list(self.__dict__.keys())

    def copy(self):
            
            """Creates a copy of object."""
            
            return copy.deepcopy(self)
    
def domain_whois(domain: str = 'request_input'):
    
    """
    Performs a WhoIs lookup on a domain address.
    """
    
    # Requesting domain address from user input if none given
    if domain == 'request_input':
        domain = input('Domain: ')
    
    # Raising error if domain given is not a string
    if type(domain) != str:
        raise TypeError('Domain must be a string')
    
    # Checking if domain is valid; if not, tries to extract domain
    if is_domain(domain) == False:
        domain = domain_splitter(domain)
    
     # Re-checking if domain is valid; if true, creates WhoisResult object
    if is_domain(domain) == True:
        return WhoisResult(domain = domain)
    
    else:
        return None

def domains_whois(domains_list: List[str]):
    
    """
    Performs a WhoIs lookup on a list or set of domain addresses.
    """
    
    # Creating output dataframe
    output_df = pd.DataFrame(dtype=object)
    
    # Iterating through domains
    for domain in domains_list:
        
        if domain == None:
            df = pd.DataFrame()
            
        else:
            # If domain is valid, running WhoIs lookup using WhoisResult class
            if is_domain(domain) == True:
                try:
                    df = domain_whois(domain = domain).all_results.T
                except:
                    continue
            else:
                # If domain is not valid, trying to retrieve domain
                try:
                    domain = domain_splitter(domain)
                except:
                    continue
                
                # If domain is valid, running WhoIs lookup using WhoisResult class
                if is_domain(domain) == True:
                    try:
                        df = domain_whois(domain = domain).all_results.T
                    except:
                        continue
        
        # Concatanating results dataframes
        output_df = pd.concat([output_df, df])
    
    # Cleaning and reformatting output dataframe
    output_df = output_df.replace(np.nan, None).reset_index().drop('index', axis=1)
    output_df = output_df.set_index('domain')
    output_df.index.rename('domain')
    output_df.columns.name = None
    
    return output_df
        

def ip_whois(ip_address: str = 'request_input'):
    
    """
    Performs a WhoIs lookup on an IP address.
    """
    
    # Requesting domain address from user input if none given
    if ip_address == 'request_input':
        ip_address = input('IP address: ')
    
    # Cleaning IP address
    ip_address = ip_address.strip().strip('/').strip()
    
    # Checking if IP address is valid; if true, running WhoIs lookup using WhoisResult class
    if is_ip_address(ip_address) == True:
        return WhoisResult(ip_address = ip_address)
    
    else:
        return None

def ips_whois(ip_addresses = List[str]):
    
    """
    Performs a WhoIs lookup on a list of IP addresses.
    """
    
    # Creating output dataframe
    output_df = pd.DataFrame(dtype=object)
    
    # Iterating through IP addresses
    for ip in ip_addresses:
        
        if ip == None:
            df = pd.DataFrame()
        
        # If IP address is inputted, tries to run a WhoIs lookup using WhoisResult class
        else:
        
            try:
                df = ip_whois(ip_address = ip).all_results.T
            except:
                df = pd.DataFrame()
        
        # Concatanating results dataframes
        output_df = pd.concat([df, output_df])
    
    # Cleaning and reformatting output dataframe
    output_df = output_df.replace(np.nan, None).reset_index().drop('index', axis=1)
    output_df = output_df.set_index('ip_address')
    output_df.index.rename('ip_address')
    output_df.columns.name = None
    
    return output_df


def lookup_whois(string: str = 'request_input'):
    
    """
    Performs a WhoIs lookup on an inputted domain or IP address.
    """
    
    # Requesting string from user input if none given
    if string == 'request_input':
        string = input('Search query: ')
    
    # Raising error if input is not a string object
    if type(string) != str:
        raise TypeError('Search query must be a string')
    
    # Making a copy of string
    original_string = copy.deepcopy(string)
    
    # Cleaning string
    string = string.strip().strip('/').strip('\\').strip('#').strip('.').strip('-').strip()
    
    # If string is an IP address, runs IP WhoIs lookup
    if is_ip_address(string) == True:
        try:
            return ip_whois(string)
        except:
            return
    
    # If string is not an IP address and is not a valid domain, tries to retrieve domain
    if is_domain(string) == False:
        string = domain_splitter(string)
    
    # If string is a domain, runs domain WhoIs lookup
    if is_domain(string) == True:
        try:
            return domain_whois(domain = string)
        except:
            return
    
    else:
        raise ValueError(f'Search query "{original_string}" contains neither a valid domain name nor a valid IP address')
    
    
    

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