"""Functions to load and parse JSTOR database files"""

from ..utils.basics import results_cols

import webbrowser

import pandas as pd
import numpy as np

def access_jstor_database():
    return webbrowser.open('https://constellate.org/builder')

def import_jstor_metadata(file_path = 'request_input', clean_results = True):
    
    if file_path == 'request_input':
        file_path = input('File path: ')

    df = pd.read_csv(file_path, dtype=str).replace(np.nan, None)

    global results_cols
    output_df = pd.DataFrame(columns=results_cols)
    
    output_df['title'] = df['title'].str.lower()
    output_df['link'] = df['id']
    output_df['source'] = df['isPartOf']
    output_df['date'] = df['publicationdate']
    output_df['doi'] = df['doi']
    output_df['type'] = df['docType']
    output_df['repository'] = df['provider']
    output_df['authors'] = df['creator'].str.lower().str.split(';')
    output_df['publisher'] = df['publisher']
    output_df['keywords'] = df['keyphrase'].str.lower().str.split(';')

    output_df = output_df.replace(np.nan, None).replace('untitled', None)
    
    
    if clean_results == True:
        
        cleaning_list = ['book review:',
                        '\[',
                        'references',
                         'index'
                        ]

        for item in cleaning_list:
            instances = output_df[output_df['title'].str.contains(item) == True].index.to_list()
            for i in instances:
                output_df.loc[i, 'title'] = None
    
    return output_df

def import_jstor_full(file_path = 'request_input', clean_results = True):
    
    if file_path == 'request_input':
        file_path = input('File path: ')
    
    df = pd.read_json(file_path, lines=True).replace(np.nan, None)

    global results_cols
    output_df = pd.DataFrame(columns=results_cols)
    
    output_df['title'] = df['title'].str.lower()
    output_df['link'] = df['id']
    output_df['source'] = df['isPartOf']
    output_df['date'] = df['datePublished']
    output_df['doi'] = df['doi']
    output_df['other_ids'] = df['id']
    output_df['type'] = df['docType']
    output_df['repository'] = df['provider']
    output_df['authors'] = df['creator']
    output_df['publisher'] = df['publisher']
    output_df['keywords'] = df['keyphrase']
    output_df['full_text'] = df['fullText'].astype('str').str.lower()
    output_df['abstract'] = df['abstract'].astype('str').str.lower()

    output_df = output_df.replace(np.nan, None).replace('untitled', None)
    
    
    if clean_results == True:
        
        cleaning_list = ['book review:',
                        '\[',
                        'references',
                         'index'
                        ]

        for item in cleaning_list:
            instances = output_df[output_df['title'].str.contains(item) == True].index.to_list()
            for i in instances:
                output_df.loc[i, 'title'] = None
    
    return output_df
    