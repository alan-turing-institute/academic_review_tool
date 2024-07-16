from ..utils.basics import results_cols

import pandas as pd
import numpy as np

api_key = 'e015290bc75d27a1814cde5c468523e7'

import pybliometrics # type: ignore
    
pybliometrics.scopus.create_config(keys = [api_key])
