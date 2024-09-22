"""Functions to load and parse JSTOR database files"""

from ..utils.basics import results_cols

import webbrowser
from pathlib import Path

import pandas as pd
import numpy as np

def access_jstor_database():

    """
    Opens the JSTOR Constellate website in the default web browser.
    """

    return webbrowser.open('https://constellate.org/builder')

def import_metadata(file_path = 'request_input',
                        #   clean_results = True
                          ):
    
    """
        Reads a metadata CSV file outputted by JSTOR's Constellate portal and returns a Pandas DataFrame.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.

        Returns
        -------
        output_df : pandas.DataFrame
            a Pandas DataFrame containing JSTOR metadata.
    """

    if file_path == 'request_input':
        file_path = input('File path: ')

    import_df = pd.read_csv(file_path, dtype=object).replace(np.nan, None)
    import_df = import_df.rename(columns=
                                        {'id': 'other_ids',
                                         'isPartOf': 'source',
                                        'datePublished': 'date',
                                        'docType': 'type',
                                        'provider': 'repository',
                                        'url': 'link',
                                        'placeOfPublication': 'publisher_location',
                                        'creator': 'authors',
                                        'keyphrase': 'keywords'
                                            })
    
    import_df = import_df.dropna(axis='columns', how='all')

    global results_cols

    to_drop = []
    for c in import_df.columns:
        if c not in results_cols:
            to_drop.append(c)
    
    output_df = import_df.drop(labels=to_drop, axis='columns')
    output_df['authors'] = output_df['authors'].str.lower().str.split(';')
    output_df['authors_data'] = output_df['authors'].copy(deep=True)
    output_df['keywords'] = output_df['keywords'].str.lower().str.split(';')

    output_df = output_df.replace(np.nan, None).replace('untitled', None)
    
    
    # if clean_results == True:
        
    #     cleaning_list = ['book review:',
    #                     '\[',
    #                     'references',
    #                      'index'
    #                     ]

    #     for item in cleaning_list:
    #         instances = output_df[output_df['title'].str.contains(item) == True].index.to_list()
    #         for i in instances:
    #             output_df.loc[i, 'title'] = None
    
    return output_df

def import_full(file_path = 'request_input', 
                # clean_results = True
                ):
    
    """
        Reads a JSON file outputted by JSTOR's Constellate portal and returns a Pandas DataFrame.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.

        Returns
        -------
        output_df : pandas.DataFrame
            a Pandas DataFrame containing JSTOR data.
    """

    if file_path == 'request_input':
        file_path = input('File path: ')
    
    import_df = pd.read_json(file_path, lines=True).replace(np.nan, None)
    import_df = import_df.rename(columns=
                                        {'id': 'other_ids',
                                         'isPartOf': 'source',
                                        'datePublished': 'date',
                                        'docType': 'type',
                                        'provider': 'repository',
                                        'url': 'link',
                                        'placeOfPublication': 'publisher_location',
                                        'creator': 'authors',
                                        'keyphrase': 'keywords',
                                        'fullText': 'full_text'
                                            })
    
    import_df = import_df.dropna(axis='columns', how='all')

    global results_cols

    to_drop = []
    for c in import_df.columns:
        if c not in results_cols:
            to_drop.append(c)
    
    output_df = import_df.drop(labels=to_drop, axis='columns')
    output_df['authors'] = output_df['authors'].str.lower().str.split(';')
    output_df['authors_data'] = output_df['authors'].copy(deep=True)
    output_df['keywords'] = output_df['keywords'].str.lower().str.split(';')

    output_df = output_df.replace(np.nan, None).replace('untitled', None)
    
    
    # if clean_results == True:
        
    #     cleaning_list = ['book review:',
    #                     '\[',
    #                     'references',
    #                      'index'
    #                     ]

    #     for item in cleaning_list:
    #         instances = output_df[output_df['title'].str.contains(item) == True].index.to_list()
    #         for i in instances:
    #             output_df.loc[i, 'title'] = None
    
    return output_df

def import_jstor(file_path = 'request_input') -> pd.DataFrame:

    """
        Reads a file outputted by JSTOR's Constellate portal and returns as a Pandas DataFrame.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.

        Returns
        -------
        result : pandas.DataFrame.
            a Pandas DataFrame containing JSTOR data.
        
        Notes
        -----
        Can read:
            * .csv
            * .json
    """

    if file_path == 'request_input':
        file_path = input('File path: ')
    
    path_obj = Path(file_path)
    suffix = path_obj.suffix.strip('.')

    if (suffix != 'csv') and (suffix != 'json'):
        raise TypeError('File must be a CSV or JSON')

    if suffix == 'csv':
        result = import_metadata(file_path=file_path)
    
    if suffix == 'json':
        result = import_full(file_path=file_path)
    
    return result