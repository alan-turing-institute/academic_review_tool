"""Functions to interface with the ORCID API"""

from ..utils.basics import results_cols
from ..utils.cleaners import is_int
from ..internet.webanalysis import is_url

from time import sleep

from requests import RequestException
import pyorcid # type: ignore
import pandas as pd
import numpy as np

client_id = 'APP-105SVMUPF0B5U042'
client_secret = 'c7455d6f-f546-4d01-af46-2c314a4a8f46'
public_access_token = '00afb570-0432-4435-a8ca-00ef1063606c'

def lookup_orcid(orcid_id = 'request_input'):

    if orcid_id == 'request_input':
        orcid_id = input('ORCID ID: ')
    
    try:
        orcid = pyorcid.OrcidScrapper(orcid_id=orcid_id)

        result = orcid.record_summary()
        df = pd.DataFrame.from_dict(result, orient='index').T
        df.columns = df.columns.str.lower()
    except:
        df = pd.DataFrame(columns = ['orcid id', 'last modified', 'name', 'family name', 'credit name', 'other names', 'biography', 'emails', 'research tags (keywords)', 'employment', 'distinctions', 'fundings', 'works'],
                          dtype=object)

    return df