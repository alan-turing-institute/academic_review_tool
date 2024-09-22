from ..utils.basics import results_cols

import pybtex # type: ignore
import pandas as pd
import numpy as np

def import_bibtex(file_path = 'request_input'):

    """
    Reads Bibtex bibliography (.bib) file and returns as a Pandas DataFrame.

    Parameters
    ----------
    file_path : str
        directory address for .bib file to read. Defaults to requesting from user input.
    
    Returns
    -------
    df : pandas.DataFrame
        a Pandas DataFrame of the bibliographic data contained in the Bibtex file.
    """

    if file_path == 'request_input':
        file_path = input('File path: ')

    bib = pybtex.database.parse_file(file_path)

    df = pd.DataFrame(columns=results_cols, dtype=object)

    bib_keys = list(bib.entries.keys())

    index = 0
    for k in bib_keys:

        entry = bib.entries.get(k)

        fields = dict(entry.fields)
        field_keys = fields.keys()

        df.loc[index, 'type'] = entry.type

        if 'title' in field_keys:
            df.loc[index, 'title'] = fields['title']
        
        if 'year' in field_keys:
            df.loc[index, 'date'] = fields['year']
        
        if 'url' in field_keys:
            df.loc[index, 'link'] = fields['url']
        
        if 'doi' in field_keys:
            df.loc[index, 'doi'] = fields['doi']

        if 'language' in field_keys:
            df.loc[index, 'language'] = fields['language']
        
        if 'journal' in field_keys:
            df.loc[index, 'source'] = fields['journal']
        
        if 'book' in field_keys:
            df.loc[index, 'source'] = fields['book']
        
        if 'booktitle' in field_keys:
            df.loc[index, 'source'] = fields['booktitle']
        
        if 'publisher' in field_keys:
            df.loc[index, 'publisher'] = fields['publisher']
        
        if 'pmid' in field_keys:
            df.loc[index, 'pubmed_id'] = fields['pmid']

        if 'issn' in field_keys:
            df.loc[index, 'issn'] = fields['issn']
        
        if 'isbn' in field_keys:
            df.loc[index, 'isbn'] = fields['isbn']
        
        if 'keywords' in field_keys:
            df.at[index, 'keywords'] = fields['keywords'].split(',')
        
        persons = dict(entry.persons)
        persons_keys = persons.keys()

        if 'author' in persons_keys:
            auths = persons['author']
            auths_list = []

            for a in auths:
                first = ''.join(a.first_names)
                middle = ' '.join(a.middle_names)
                last = ' '.join(a.last_names)

                full_name = first + ' ' + middle + ' ' + last
                full_name = full_name.strip()
                auths_list.append(full_name)

            df.at[index, 'authors'] = auths_list
            df.at[index, 'authors_data'] = auths_list
        
        index += 1


    return df