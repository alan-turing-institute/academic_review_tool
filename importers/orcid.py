"""Functions to interface with the ORCID API"""

from ..utils.basics import results_cols
from ..utils.cleaners import is_int
from ..internet.webanalysis import is_url

from time import sleep

from requests import RequestException
import pyorcid # type: ignore
from pyorcid import OrcidAuthentication, Orcid # type: ignore

import pandas as pd
import numpy as np

client_id = 'APP-105SVMUPF0B5U042'
client_secret = 'c7455d6f-f546-4d01-af46-2c314a4a8f46'
orcid_auth = OrcidAuthentication(client_id=client_id, client_secret=client_secret)

public_access_token = orcid_auth.get_public_access_token()

def lookup_orcid(orcid_id = 'request_input'):

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

    if orcid_id == 'request_input':
        orcid_id = input('ORCID ID: ')
    
    global public_access_token
    orcid = Orcid(orcid_id=orcid_id, orcid_access_token=public_access_token, state = "public")

    return orcid

def get_author_works(orcid_id = 'request_input', output = 'dataframe'):

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