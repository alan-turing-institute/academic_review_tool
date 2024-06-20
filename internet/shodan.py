"""Functions for searching and analysing Shodan"""

from shodan import Shodan


def set_api_key(key: str = 'request_input') -> str:
    
    """
    Initialises Shodan API key as variable local environment.
    
    Parameters
    ----------
    key : str
        Shodan API key. Defaults to requesting from user input.
    
    Returns
    -------
    key : str
        the inputted API key as a variable in the global environment.
    """
    
    # Requesting API key from user if none given
    if key  == 'request_input':
        key = input('Shodan API key: ')
    
    # Initialising key in global environment
    global SHODAN_API_KEY
    SHODAN_API_KEY = key
    
    return SHODAN_API_KEY


def init_api(api_key: str = None) -> Shodan:
    
    """
    Initialises Shodan API using an API key.
    
    Parameters
    ----------
    api_key : str or None
        Shodan API key. If None, searches for key in environment; if none set, sets key.
    
    Returns
    -------
    result : Shodan
        a Shodan API instance.
    """
    
    # Checking if an API key has been provided
    if api_key  == None:
        
        if 'SHODAN_API_KEY' not in globals().keys():
            # Requesting API key from user
            set_api_key()
        
        # Retrieving API key from globals
        global SHODAN_API_KEY
        api_key = SHODAN_API_KEY
    
    # Initialising Shodan API object as a global variable
    global SHODAN_API
    SHODAN_API = Shodan(api_key)
    
    return SHODAN_API


def search_ip(ip: str = 'request_input'):
    
    """
    Searches Shodan API for an IP address.
    """
    
    # Checking if Shodan API key has been set
    if 'SHODAN_API_KEY' not in globals().keys():
        # If not, requesting API key from user
        set_api_key()
    
    # Retrieving API key from globals
    global SHODAN_API_KEY
    key = SHODAN_API_KEY
    
    # If no API session initialised, initialising Shodan API as global variable
    if 'SHODAN_API' not in globals().keys():
        init_api(api_key = key)
    
    # Retrieving Shodan API object
    global SHODAN_API
    api = SHODAN_API
    
    # Requesting IP address from user if none provided
    if ip  == 'request_input':
        ip = input('IP address: ')
    
    # Searching Shodan for IP address
    ipinfo = api.host(ip)
    
    return ipinfo
