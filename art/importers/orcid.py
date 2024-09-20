"""Functions to interface with the ORCID API"""

from ..utils.basics import results_cols
from ..utils.cleaners import is_int
from ..internet.webanalysis import is_url

from time import sleep

from requests import RequestException
from pathlib import Path

import pyorcid # type: ignore
from pyorcid import OrcidAuthentication, Orcid, OrcidSearch # type: ignore

import pandas as pd
import numpy as np

client_id = 'APP-105SVMUPF0B5U042'
client_secret = 'c7455d6f-f546-4d01-af46-2c314a4a8f46'
orcid_auth = OrcidAuthentication(client_id=client_id, client_secret=client_secret)

public_access_token = orcid_auth.get_public_access_token()

def lookup_orcid(orcid_id = 'request_input'):

    """
    Looks up an ORCID ID and returns a Pandas DataFrame of potential matches.

    Parameters
    ----------
    orcid_id : str
        an ORCID ID to look up. Defaults to requesting from user input.
    
    Results
    -------
    df : pandas.DataFrame
        a Pandas DataFrame of potential matches in the ORCID database.
    """

    if orcid_id == 'request_input':
        orcid_id = input('ORCID ID: ')
    
    orcid_id = str(orcid_id).replace('https://', '').replace('http://', '').replace('orcid.org/', '')

    try:
        orcid = pyorcid.OrcidScrapper(orcid_id=orcid_id)

        result = orcid.record_summary()
        df = pd.DataFrame.from_dict(result, orient='index').T
        df.columns = df.columns.str.lower()
    except:
        print(f'ORCiD lookup for {orcid_id} failed.')
        df = pd.DataFrame(columns = ['orcid id', 'last modified', 'name', 'family name', 'credit name', 'other names', 'biography', 'emails', 'research tags (keywords)', 'employment', 'distinctions', 'fundings', 'works'],
                          dtype=object)

    return df

def get_author(orcid_id = 'request_input'):

    """
    Retrieves an ORCID account using an ORCID ID. Returns a Pyorcid Orcid object.
    """

    if orcid_id == 'request_input':
        orcid_id = input('ORCID ID: ')
    
    global public_access_token
    orcid = Orcid(orcid_id=orcid_id, orcid_access_token=public_access_token, state = "public")

    return orcid

def get_author_works(orcid_id = 'request_input', output = 'dataframe'):

    """
    Retrieves data on an ORCID profile's listed works.

    Parameters
    ----------
    orcid_id : str
        an ORCID ID to look up. Defaults to requesting from user input.
    output : str
        the type of object to return. Defaults to 'dataframe' (a Pandas DataFrame).
    
    Returns
    -------
    works_list : pandas.DataFrame or list or tuple
        an object containing data on the ORCID profile's listed works.
    """

    if orcid_id == 'request_input':
        orcid_id = input('ORCID ID: ')
    
    orcid = get_author(orcid_id=orcid_id)
    works_tuple = orcid.works()
    if len(works_tuple) > 0:
        works_list = works_tuple[0]
    else:
        works_list = []

    if (output == tuple) or (output == 'tuple'):
        return works_tuple

    if (output == list) or (output == 'list'):
        return works_list
    
    if output == 'dataframe':
        return pd.DataFrame(works_list)

def search(query: str = 'request_input', start: int = 0, limit: int = 1000, output: str = 'dataframe'):

    """
        Searches for author records using the Orcid API.

        Parameters
        ----------
        query : str
            query to search. Allows for keywords and Boolean logic.
        start : int
            index position of first result to return. Defaults to 0.
        limit : int
            the maximum number of results returned. Defaults to 1000.
        output : str
            the type of object to return. Defaults to 'dataframe' (a Pandas DataFrame).

        Returns
        -------
        results_list : pandas.DataFrame or list or tuple
            an object containing search results.
    """

    if query == 'request_input':
        query = input('Search query: ')

    global public_access_token
    
    orcidSearch = OrcidSearch(orcid_access_token=public_access_token)
    results = orcidSearch.search(query=query, start=start, rows=limit)

    error_msg = ''
    if 'response-code' in results.keys():
        error_msg = f'{error_msg}Response code: {results["response-code"]}.'
    
    if 'error-code' in results.keys():
        error_msg = f'{error_msg} Error code: {results["error-code"]}.'
    
    if 'developer-message' in results.keys():
        error_msg = f'{error_msg} {results["developer-message"]}.'

    if 'more-info' in results.keys():
        error_msg = f'{error_msg} For more info, see: {results["more-info"]}.'

    if len(error_msg) > 0:
        raise ValueError(f'{error_msg}')

    if 'num-found' in results.keys():
        num_found = results['num-found']
        print(f'{num_found} results found') # type: ignore

    if 'expanded-result' in results.keys():
        results_list = results['expanded-result']
    else:
        results_list = []

    if (output == dict) or (output.lower().strip() == 'dict'):
        return results

    if (output == list) or (output.lower().strip() == 'list'):
        return results_list
    
    if (output == pd.DataFrame) or (output.lower().strip() == 'dataframe'):
        return pd.DataFrame(results_list)

def save_summary(self: Orcid, file_name: str = 'request_input', file_path: str = 'request_input'):

    if file_name == 'request_input':
        file_name = input('File name: ')
    
    file_name = file_name.strip('.md')
    
    if file_path == 'request_input':
        file_path = input('File path: ')
    
    path_obj = Path(file_path)

    if path_obj.exists() == False:
        raise ValueError('File path {file_path} does not exist')

    new_addr = file_path + '/' + file_name + '.md'

    self.generate_markdown_file(output_file=new_addr)

Orcid.save_summary = save_summary