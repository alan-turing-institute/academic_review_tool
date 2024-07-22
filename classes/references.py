from ..utils.basics import results_cols
from ..importers.crossref import references_to_df
from .results import Results

import pandas as pd
import numpy as np


class References(Results):

    """
    This is a References object. It is a modified Pandas Dataframe object designed to store References relating to an entry.
    
    Parameters
    ----------
    
    
    Attributes
    ----------
    """

    def __init__(self, dataframe = None, index = []):
        
        """
        Initialises References instance.
        
        Parameters
        ----------
        """

        
        # Inheriting methods and attributes from References class
        super().__init__()
            
        self.replace(np.nan, None)
    
    def __repr__(self):

        return f'References object containing {len(self)} items'

    def from_dataframe(dataframe): # type: ignore
        
        dataframe = dataframe.copy(deep=True).reset_index().drop('index', axis=1)
        results_table = References(index = dataframe.index)
        results_table.columns = results_table.columns.astype(str).str.lower().str.replace(' ', '_')
        dataframe.columns = dataframe.columns.astype(str).str.lower().str.replace(' ', '_')

        for c in dataframe.columns:
            results_table[c] = dataframe[c]
        
        return results_table



def is_formatted_reference(item):

    if type(item) == References:
        return True
    else:
        return False


def extract_references(references_data, add_work_ids = True, update_from_doi = False):

    refs = References()

    if type(references_data) == References:
        refs = references_data
        refs.refs_count = None

    
    if (type(references_data) != References) and (type(references_data) != str) and (type(references_data) != list) and (type(references_data) != dict):
        refs.refs_count = None
        return refs

    if (type(references_data) == list) and (len(references_data) > 0) and (type(references_data[0]) == dict):
        
        if 'count' in references_data[0].keys():
            count = references_data[0]['count']
        else:
            count = None

        df = references_to_df(references_data, update_from_doi = update_from_doi)
        df.replace({np.nan: None})
        refs = References.from_dataframe(df) # type: ignore
        refs.refs_count = count

        if 'db' in refs.columns:
            refs = refs.drop('db', axis=1)
        
        if 'count' in refs.columns:
            refs = refs.drop('count', axis=1)
        
        refs = refs.dropna(axis=0, how='all').reset_index().drop('index', axis=1)
        refs = References.from_dataframe(dataframe=refs) # type: ignore

        if add_work_ids == True:
            refs.generate_work_ids() # type: ignore
    
    if (type(references_data) == list) and (len(references_data) > 0) and (type(references_data[0]) == str):
        df = pd.DataFrame(columns=results_cols, dtype=object)
        df['link'] = pd.Series(references_data, dtype=object)
        df.replace({np.nan: None})

        refs = References.from_dataframe(df) # type: ignore
        if add_work_ids == True:
            refs.generate_work_ids()

        refs.refs_count = None


    return refs

