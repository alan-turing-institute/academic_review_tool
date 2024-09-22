from ..utils.basics import results_cols
from ..utils.cleaners import strip_list_str, deduplicate
from ..importers.pdf import read_pdf_to_table
from ..importers.jstor import import_jstor
from ..importers.bibtex import import_bibtex
from ..importers.crossref import lookup_doi, lookup_dois
from ..datasets import stopwords

from .entities import Entity, Entities
from .funders import Funder, Funders, format_funders

from pathlib import Path

import pandas as pd
import numpy as np
from nltk.tokenize import word_tokenize # type: ignore
from pybtex.database import BibliographyData, Entry #type: ignore

def generate_work_id(work_data: pd.Series):
    
        """
            Takes a Pandas Series containing details about a published work and returns a unique identifier code (work ID).

            Parameters
            ----------
            work_data : pandas.Series
                a series containing data on a published work.

            Returns
            -------
            work_id : str
                a work ID.
        """

        work_data = work_data.copy(deep=True).dropna()

        work_id = 'W:'
        
        if 'authors' in work_data.index:
            auths_type = type(work_data['authors'])
            auths_type_str = str(auths_type)

            if auths_type == list:
                work_data['authors'] = pd.Series(work_data['authors'],  dtype=object).sort_values().to_list()
            
            else:
                if '.Authors' in auths_type_str:
                    work_data['authors'] = work_data['authors'].summary['full_name'].sort_values().to_list()
            

        work_data = work_data.astype(str).str.lower()

        if 'authors' in work_data.index:
            authors = work_data['authors']
        else:
            authors = ''
        
        if 'title' in work_data.index:
            title = work_data['title']
        else:
            title = ''
        
        if 'date' in work_data.index:
            date = work_data['date']
        else:
            date = ''

        if (authors != None) and (authors != '') and (authors != 'None') and (authors != [])and (authors != '[]'):
            authors_str = authors.lower().strip().replace('[','').replace(']','').replace("'", "").replace('"', '')
            authors_list = authors_str.split(',')
            authors_list = [i.strip() for i in authors_list]
            
            if len(authors_list) > 0:
                authors_sorted = (pd.Series(authors_list, dtype=object).sort_values())
                first_author = str(authors_sorted[0])
                first_author = first_author.split(' ')[-1].split(' ')[-1]
            else:
                first_author = ''
            
            work_id = work_id + '-' + first_author

        if (title != None) and (title != '') and (title != 'None'):
            title = title.strip().lower()
            title_words = list(word_tokenize(title))
            title_first2 = '-'.join(title_words[:2])
            if len(title_words) > 3:
                title_last = title_words[-1]
            else:
                title_last = ''
            title_shortened = title_first2 + '-' + title_last
            title_shortened = title_shortened[:15] # capping at 15 characters to avoid overly long UIDs
            work_id = work_id + '-' + title_shortened
        
        if (date != None) and (date != '') and (date != 'None'):
            work_id = work_id + '-' + str(date)

        uid = ''

        if 'doi' in work_data.index:
            uid = work_data['doi']

            if (uid == None) or (uid == 'None') or (uid == ''):
                if 'isbn' in work_data.index:
                    uid = work_data['isbn']

                if (uid == None) or (uid == 'None') or (uid == ''):
                    if 'issn' in work_data.index:
                        uid = work_data['issn']

                    if (uid == None) or (uid == 'None') or (uid == ''):
                        if 'link' in work_data.index:
                            uid = work_data['link'][:15] # keeping URLs short as these can produce very long IDs

                        if (uid == None) or (uid == 'None') or (uid == ''):
                            uid = ''
        
        uid_shortened = uid.replace('https://', '').replace('http://', '').replace('www.', '').replace('doi.org.','').replace('scholar.google.com/','')[:23] # DOIs are 23 characters long

        work_id = work_id + '-' + uid_shortened
        work_id = work_id.replace('W:-', 'W:').replace("'s", '').replace('\r', '').replace('\n', '').replace("'", "").replace('"', '').replace('(','').replace(')','').replace('`','').replace('.', '').replace('’','').replace('--', '-').replace('W:-', 'W:').strip('-')
        work_id = work_id

        return work_id

class Results(pd.DataFrame):

    """
    This is a Results DataFrame. It is a modified Pandas Dataframe object designed to store the results of an academic review.
    
    Parameters
    ----------
    dataframe : pandas.DataFrame
        a Pandas DataFrame to convert to a Results DataFrame. Defaults to None.
    index : list
        list of indices for Results DataFrame. Defaults to an empty list.

    Attributes
    ----------
    T : pandas.DataFrame
    _AXIS_LEN : int
    _AXIS_ORDERS : list
    _AXIS_TO_AXIS_NUMBER : dict
    _HANDLED_TYPES : tuple
    __annotations__ : dict
    __array_priority__ : int
    _attrs : dict
    _constructor : type
    _constructor_sliced : type
    _hidden_attrs : frozenset
    _info_axis : pandas.Index
    _info_axis_name : str
    _info_axis_number : int
    _internal_names : set
    _is_homogeneous_type : bool
    _is_mixed_type : bool
    _is_view : bool
    _item_cache : dict
    _metadata : list
    _series : dict
    _stat_axis : pandas.Index
    _stat_axis_name : str
    _stat_axis_number : int
    _typ : str
    _values : numpy.ndarray
    attrs : dict
    axes : list
    columns : pandas.Index
    dtypes : pandas.Series
    empty : bool
    flags : pandas.Flags
    index : pandas.Index
    ndim : int
    shape : tuple
    size : numpy.int64
    values : numpy.ndarray

    Columns
    -------
    * **work_id**: a unique identifier assigned to each result.
    * **title**: the result's title.
    * **authors**: any authors associated with the result.
    * **date**: any date(s) or year(s) associated with the result.
    * **source**: the name of the journal, conference, book, website, or other publication in which the result is contained (if any).
    * **type**: result type (e.g. article, chapter, book, website).
    * **editors**:  any authors associated with the result.
    * **publisher**: the name of the result's publisher (if any).
    * **publisher_location**: any locations or addresses associated with the result's publisher.
    * **funder**:  any funders associated with the result.
    * **keywords**:  any keywords associated with the result.
    * **abstract**: the result's abstract (if available).
    * **description**: the result's abstract (if available).
    * **extract**: the result's extract (if available).
    * **full_text**: the result's full text (if available).
    * **access_type**: the result's access type (e.g. open access, restricted access)
    * **authors_data**: unformatted data on any authors associated with the result.
    * **author_count**: the number of authors associated with the result.
    * **author_affiliations**: any affiliations associated with the result's authors.
    * **editors_data**: unformatted data on any editors associated with the result.
    * **citations**: any citations/references/links associated with the result.
    * **citation_count**: the number of citations/references/links associated with the result.
    * **citations_data**: unformatted data on any citations/references/links associated with the result.
    * **cited_by**: a list of publications that cite/reference/link to the result.
    * **cited_by_count**: the number of publications that cite/reference/link to the result.
    * **cited_by_data**: unformatted data on publications that cite/reference/link to the result.
    * **recommendations**: data on recommended publications associated with the result.
    * **crossref_score**: a bibliometric score assigned by CrossRef (if available).
    * **repository**: the repository from which the result was retrieved (if available).
    * **language**: the language(s) of the result.
    * **doi**: the Digital Object Identifier (DOI) assigned to the result.
    * **isbn**: the International Standard Book Number (ISBN) assigned to the result.
    * **issn**: the International Standard Serial Number (ISSN) assigned to the result or its source.
    * **pii**: any Publisher Item Identifiers (PII) assigned to the result.
    * **scopus_id**: the Scopus identifier assigned to the result.
    * **wos_id**: the Web of Science (WoS) identifier assigned to the result.
    * **pubmed_id**: the PubMed Identifier (PMID) assigned to the result.
    * **other_ids**: any other identifiers assigned to the result.
    * **link**: a URL or other link to the result.
    """

    def __init__(self, dataframe = None, index = []):
        
        """
        Initialises Results instance.
        
        Parameters
        ----------
        dataframe : pandas.DataFrame
            a Pandas DataFrame to convert to a Results DataFrame. Defaults to None.
        index : list
            list of indices for Results DataFrame. Defaults to an empty list.
        """

        if dataframe is None:

            global results_cols

            # Inheriting methods and attributes from Pandas.DataFrame class
            super().__init__(dtype=object, columns = results_cols, index = index # type: ignore
                            ) 
            
            self.replace(np.nan, None)
        
        else:
            df = dataframe
            if type(dataframe) == pd.DataFrame:
                self = Results.from_dataframe(dataframe = df)

    def drop_empty_rows(self):

        """
        Drops rows that contain no data.
        """


        ignore_cols = ['work_id', 'authors', 'funder', 'citations']

        df = self.dropna(axis=0, how='all')
        drop_cols = [c for c in df.columns if c not in ignore_cols]
        df = df.dropna(axis=0, how='all', subset=drop_cols).reset_index().drop('index', axis=1)

        results = Results.from_dataframe(dataframe=df, drop_duplicates=False) # type: ignore

        self.__dict__.update(results.__dict__)

        return self

    def remove_duplicates(self, drop_empty_rows = True, use_api = False):

        """
        Removes duplicate results.

        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to True.
        use_api : bool
            whether to update the results data using all available APIs. Defaults to False.
        
        Returns
        -------
        self : Results
            a Results object.
        """

        if drop_empty_rows == True:
            self.drop_empty_rows()

        self['doi'] = self['doi'].str.replace('https://', '', regex = False).str.replace('http://', '', regex = False).str.replace('dx.', '', regex = False).str.replace('doi.org/', '', regex = False).str.replace('doi/', '', regex = False)

        df = deduplicate(self)
        results = Results.from_dataframe(dataframe = df, drop_duplicates=False)

        if use_api == True:
            results.update_from_dois()
        
        results.update_work_ids()
        df2 = results.drop_duplicates(subset='work_id').reset_index().drop('index',axis=1)

        results2 = Results.from_dataframe(dataframe=df2, drop_duplicates=False) # type: ignore
        
        self.__dict__.update(results2.__dict__)

        return self

    def get(self, work_id: str):

        """
        Retrieves result using a work ID.
        """

        indexes = self[self['work_id'] == work_id].index.to_list()
        if len(indexes) > 0:
            index = indexes[0]
            return self.loc[index]
        else:
            raise KeyError('work_id not found')

    def add_pdf(self, path = 'request_input'):
        
        """
        Reads a PDF file from a file path or URL and adds its data to the Results DataFrame.

        Parameters
        ----------
        path : str
            a filepath or URL that directs to a PDF file. Defaults to requesting from user input.
        """

        if path == 'request_input':
            path = input('Path to PDF (URL or filepath): ')

        table = read_pdf_to_table(path)
        table = table.replace(np.nan, None).astype(object)

        series = table.loc[0]
        work_id = generate_work_id(series) # type: ignore
        series['work_id'] = work_id

        index = len(self)
        self.loc[index] = series

        return self
    
    def add_row(self, data: pd.Series, drop_duplicates: bool = True):

        """
        Adds a Pandas Series to the Results DataFrame as a new row.

        Parameters
        ----------
        data : pandas.Series
            a row to add.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to True.
        """

        if type(data) != pd.Series:
            raise TypeError(f'Results must be a Pandas.Series, not {type(data)}')

        data.index = data.index.astype(str).str.lower().str.replace(' ', '_')
        if len(data) != len(self.columns):
            for c in data.index:
                if c not in self.columns:
                    self[c] = pd.Series(dtype=object)

        index = len(self)

        work_id = generate_work_id(data)
        # work_id = self.get_unique_id(work_id, index)
        data['work_id'] = work_id

        
        self.loc[index] = data

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=False)
        
    def get_unique_id(self, work_id, index):

        """
        Checks whether work ID is used more than once in the Results DataFrame. If yes, returns a unique ID.
        """

        if (type(work_id) == str) and (work_id != ''):
            try:
                work_id = str(work_id.split('#')[0])
                if work_id in self['work_id'].to_list():
                
                    df = self.copy(deep=True).astype(str)
                    df['work_id'] = df['work_id'].dropna()
                    masked = df[df['work_id'].str.contains(work_id)]
                    masked_indexes = masked.index.to_list()
                    if index not in masked_indexes:
                        id_count = len(masked) # type: ignore
                        work_id = work_id + f'#{id_count + 1}'
            except:
                pass
        return work_id

    def add_dataframe(self, dataframe: pd.DataFrame, drop_empty_rows = True, drop_duplicates = False, update_work_ids = True):
        
        """
        Adds a Pandas DataFrame to the Results DataFrame.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            a Pandas DataFrame to add to the Results DataFrame.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to True.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        update_work_ids : bool
            whether to update results entries' work IDs. Defaults to True.
        """

        if (type(dataframe) != pd.DataFrame) and (type(dataframe) != pd.Series):
            raise TypeError(f'Results must be a Pandas.Series or Pandas.DataFrame, not {type(dataframe)}')

        dataframe = dataframe.reset_index().drop('index', axis=1)
        dataframe.columns = dataframe.columns.astype(str).str.lower().str.replace(' ', '_')

        if (self.columns.to_list()) != (dataframe.columns.to_list()):
            for c in dataframe.columns:
                if c not in self.columns:
                    self[c] = pd.Series(dtype=object)
        
        self_copy = self.copy(deep=True)
        concat_df = pd.concat([self_copy, dataframe])
        concat_df = concat_df.reset_index().drop('index', axis=1)

        new_results = Results()

        for c in concat_df.columns:
            new_results[c] = concat_df[c]
        
        self.__dict__.update(new_results.__dict__)

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if update_work_ids == True:
            self.update_work_ids(drop_duplicates=False)

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

    def add_doi(self, doi: str = 'request_input', drop_empty_rows = True, drop_duplicates = False, timeout: int = 60):

        """
        Looks up a DOI using the CrossRef API and adds to the Results DataFrame.

        Parameters
        ----------
        doi : str
            DOI to look up. Defaults to requesting from user input.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        """


        df = lookup_doi(doi=doi, timeout=timeout)
        self.add_dataframe(dataframe=df)

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=False)
        
        if drop_empty_rows == True:
            self.drop_empty_rows()

    def add_dois(self, dois_list: list = [], drop_empty_rows = True, rate_limit: float = 0.05, timeout = 60):

        """
        Looks up a list of DOIs using the CrossRef API and adds to Results DataFrame.

        Parameters
        ----------
        dois_list : list
            list of DOIs to look up. Defaults to an empty list.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        """

        df = lookup_dois(dois_list=dois_list, rate_limit=rate_limit, timeout=timeout)
        self.add_dataframe(dataframe=df, drop_empty_rows = drop_empty_rows)

    def correct_dois(self, drop_duplicates = False):

        """
        Checks all entries in Results DataFrame for correctly formatted DOIs. If none is found, checks whether URL contains a valid DOI and, if so, uses this. Additionally, strips existing DOIs of unnecessary strings.

        Parameters
        ----------
        drop_duplicates : bool
            whether to remove duplicated rows.
        """

        no_doi = self[self['doi'].isna()]
        has_link = no_doi[~no_doi['link'].isna()]
        doi_in_link = has_link[has_link['link'].str.contains('doi.org')]

        for i in doi_in_link.index:
            link = str(doi_in_link.loc[i, 'link'])
            doi = link.replace('http://', '').replace('https://', '').replace('www.', '').replace('dx.', '').replace('doi.org/', '').strip('/').strip()
            self.loc[i, 'doi'] = doi
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=False)

    def generate_work_ids(self):

        """
        Assigns a unique identifier (work ID) for each published work in the Results DataFrame.
        """

        for i in self.index:
            work_id = generate_work_id(self.loc[i])
            self.loc[i, 'work_id'] = work_id

    def update_work_ids(self, drop_duplicates = False):

        """
        Checks each published work in the Results DataFrame to ensure the work ID is up-to-date. If not, generates and assigns a new work ID.

        Parameters
        ----------
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to True.
        """

        for i in self.index:
            work_id = generate_work_id(self.loc[i])
            if self.loc[i, 'work_id'] != work_id:
                # work_id = self.get_unique_id(work_id, i)
                self.loc[i, 'work_id'] = work_id
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=False)

    def update_from_doi(self, index, drop_empty_rows = True, drop_duplicates = False, timeout: int = 60):
        
        """
        Updates a given result using the CrossRef API if it has a DOI associated.

        Parameters
        ----------
        index : int or str
            row index position for the result to update.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        """

        try:
            old_series = self.loc[index]
            doi = old_series['doi']

            if (type(doi) != None) and (type(doi) != '') and (type(doi) != 'None'):
                new_series = lookup_doi(doi=doi, timeout=timeout).loc[0]

                for i in new_series.index:
                    if i in old_series.index:
                        old_val = old_series[i]
                        new_val = new_series[i]
                        if (new_val != None) and (new_val != np.nan) and (new_val != '') and (len(str(new_val)) > len(str(old_val))):
                            old_series[i] = new_val
                    
                    else:
                        old_series[i] = new_val
        except:
            pass

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=False)
        
        if drop_empty_rows == True:
            self.drop_empty_rows()
        
    def update_from_dois(self, drop_empty_rows = True, drop_duplicates = False, timeout: int = 60):

        """
        Updates results that have DOIs associated using the CrossRef API.

        Parameters
        ----------
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        """

        self.correct_dois(drop_duplicates=False)

        for i in self.index:
            self.update_from_doi(index = i, drop_duplicates = False, timeout=timeout)
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=False)
        
        if drop_empty_rows == True:
            self.drop_empty_rows()

    def __add__(self, results_obj):

        """
        Defines addition behaviour for Results DataFrames. Returns a left-wise inner join between the two DataFrames. 
        """

        left = self.copy(deep=True)
        right = results_obj.copy(deep=True)
        right.columns = right.columns.astype(str).str.lower().str.replace(' ', '_')

        if (left.columns.to_list()) != (right.columns.to_list()):
            for c in left.columns:
                if c not in right.columns:
                    right[c] = pd.Series(dtype=object)
            
            for c in right.columns:
                if c not in left.columns:
                    left[c] = pd.Series(dtype=object)
        
        index = len(left)
        for i in right.index:
            left.loc[index] = right.loc[i]
            index += 1
        
        return left

    def to_dataframe(self):

        """
        Converts Results object to a Pandas DataFrame.

        Returns
        -------
        dataframe : pandas.DataFrame
            the Results object converted to a Pandas DataFrame.
        """

        return self.copy(deep=True)
    
    def from_dataframe(dataframe, drop_empty_rows = False, drop_duplicates = False): # type: ignore
        
        """
        Converts a Pandas DataFrame to a Results object.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            a Pandas DataFrame to convert to a Results object.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        
        Returns
        -------
        results_table : Results
            a Results object.
        """

        dataframe = dataframe.copy(deep=True).reset_index().drop('index', axis=1)
        results_table = Results(index = dataframe.index)
        results_table.columns = results_table.columns.astype(str).str.lower().str.replace(' ', '_')
        dataframe.columns = dataframe.columns.astype(str).str.lower().str.replace(' ', '_')

        for c in dataframe.columns:
            results_table[c] = dataframe[c]

        if drop_duplicates == True:
            results_table.remove_duplicates(drop_empty_rows=False)
        
        if drop_empty_rows == True:
            results_table.drop_empty_rows()

        return results_table

    def to_pybtex(self):

        """
        Converts the Results DataFrame to a Pybtex BibliographyData object.

        Returns
        -------
        bib_data : pybtex.BibliographyData
            a Pybtex BibliographyData object.
        """

        res_dict = {}

        for i in self.index:
            
            row = self.loc[i].copy(deep=True).dropna()

            if 'type' in row.index:
                entry_type = row['type']
            else:
                entry_type = 'misc'
            
            if 'title' in row.index:
                title = str(row['title'])
            else:
                title = ''
            
            if 'authors' in row.index:
                authors = row['authors']
            else:
                authors = ''
            
            if ('__dict__' in authors.__dir__()) and ('summary' in authors.__dict__.keys()):
                if (type(authors.summary) == pd.DataFrame) and ('full_name' in authors.summary.columns):
                    authors_str = ', '.join(authors.summary['full_name'].to_list())
                else:
                    authors_str = ''
            
            if type(authors) == list:
                authors_str = ', '.join(authors)
            else:
                authors_str = ''
            
            if 'date' in row.index:
                year = str(row['date'])
            else:
                year = ''
            
            if 'keywords' in row.index:
                keywords = ', '.join(row['keywords'])
            else:
                keywords = ''

            if 'doi' in row.index:
                doi = str(row['doi'])
            else:
                doi = ''
            
            if 'publisher' in row.index:
                publisher = str(row['publisher'])
            else:
                publisher = ''
            
            if 'link' in row.index:
                link = str(row['link'])
            else:
                link = ''
            
            if 'isbn' in row.index:
                isbn = str(row['isbn'])
            else:
                isbn = ''

            if 'work_id' in row.index:
                key = row['work_id']
            else:
                key = str(title).lower() + '_' + str(authors_str)[:10].lower() + '_' + str(year).lower()

            entry_list = []

            if (authors_str is not None) and (authors_str != '') and (type(authors_str) == str):
                authors_tuple = ('author', authors_str)
                entry_list.append(authors_tuple)
            
            if (title is not None) and (title != '') and (type(title) == str):
                title_tuple = ('title', title)
                entry_list.append(title_tuple)
            
            if (year is not None) and (year != '') and (type(year) == str):
                year_tuple = ('year', year)
                entry_list.append(year_tuple)
            
            if (doi is not None) and (doi != '') and (type(doi) == str):
                doi_tuple = ('doi', doi)
                entry_list.append(doi_tuple)
            
            if (publisher is not None) and (publisher != '') and (type(publisher) == str):
                publisher_tuple = ('publisher', publisher)
                entry_list.append(publisher_tuple)
            
            if (link is not None) and (link != '') and (type(link) == str):
                link_tuple = ('url', link)
                entry_list.append(link_tuple)
            
            if (isbn is not None) and (isbn != '') and (type(isbn) == str):
                isbn_tuple = ('isbn', isbn)
                entry_list.append(isbn_tuple)
            
            if (keywords is not None) and (keywords != '') and (type(keywords) == str):
                keywords_tuple = ('keywords', keywords)
                entry_list.append(keywords_tuple)
            
            if 'article' in entry_type:

                entry_type = 'article'

                if 'source' in row.index:
                    journal = row['source']
                    if (journal is not None) and (journal != '') and (type(journal) == str):
                        journal_tuple = ('journal', journal)
                        entry_list.append(journal_tuple)

            if ('book' in entry_type) and ('chapter' in entry_type):

                entry_type = 'incollection'

                if 'source' in row.index:
                    booktitle = row['source']
                    if (booktitle is not None) and (booktitle != '') and (type(booktitle) == str):
                        booktitle_tuple = ('journal', booktitle)
                        entry_list.append(booktitle_tuple)
            
            if 'book' in entry_type:
                entry_type = 'book'

            entry = Entry(entry_type, entry_list)
            res_dict[key] = entry
        
        bib_data = BibliographyData(res_dict)

        return bib_data

    def to_bibtex(self):

        """
        Converts the Results DataFrame to a Bibtex-formatted (.bib) bibliography string.

        Returns
        -------
        bib_data : str
            the Results DataFrame in Bibtex (.bib) bibliography file formatting.
        """

        bib_data = self.to_pybtex()
        return bib_data.to_string('bibtex')
    
    def to_yaml(self):

        """
        Converts the Results DataFrame to a YAML-formatted (.yaml) bibliography string.

        Returns
        -------
        bib_data : str
            the Results DataFrame in YAML (.yaml) bibliography file formatting.
        """

        bib_data = self.to_pybtex()
        return bib_data.to_string('yaml')
    
    def export_bibtex(self, file_name = 'request_input', folder_path = 'request_input'):

        """
        Exports the Results DataFrame as a Bibtex-formatted (.bib) bibliography file.

        Parameters
        ----------
        file_name : str
            name of file to create. Defaults to requesting from user input.
        folder_path : str
            location to create file. Defaults to requesting from user input.
        """

        if file_name == 'request_input':
            file_name = input('File name: ')
        
        if folder_path == 'request_input':
            folder_path = input('Folder path: ')
        
        if Path(folder_path).exists() == False:
            raise ValueError('Folder does not exist')

        bib = self.to_bibtex()

        filepath = folder_path + '/' + file_name + '.bib'

        bib_bytes = bytes(bib, "utf-8").decode("unicode_escape")

        with open(filepath, 'w') as file:
            file.write(bib_bytes)
    
    def export_yaml(self, file_name = 'request_input', folder_path = 'request_input'):

        """
        Exports the Results DataFrame as a YAML-formatted (.yaml) bibliography file.

        Parameters
        ----------
        file_name : str
            name of file to create. Defaults to requesting from user input.
        folder_path : str
            location to create file. Defaults to requesting from user input.
        """

        if file_name == 'request_input':
            file_name = input('File name: ')
        
        if folder_path == 'request_input':
            folder_path = input('Folder path: ')
        
        if Path(folder_path).exists() == False:
            raise ValueError('Folder does not exist')

        yaml = self.to_yaml()

        filepath = folder_path + '/' + file_name + '.yaml'

        yaml_bytes = bytes(yaml, "utf-8").decode("unicode_escape")

        with open(filepath, 'w') as file:
            file.write(yaml_bytes)
        
    def clear_rows(self):

        """
        Deletes all rows.

        Returns
        -------
        self : Results
            a blank Results DataFrame.
        """

        results = Results()
        self.__dict__.update(results.__dict__)

        return self

    def import_bibtex(self, file_path = 'request_input'):

        """
        Reads a Bibtex (.bib) bibliography file and adds its data to the Results DataFrame.

        Parameters
        ----------
        file_path : str
            location of the Bibtex (.bib) bibliography file to read.
        """

        df = import_bibtex(file_path = file_path)
        self.add_dataframe(dataframe=df, drop_duplicates=False, drop_empty_rows=False)
    
    def from_bibtex(file_path = 'request_input'):

        """
        Reads a Bibtex (.bib) bibliography file and returns as a Results DataFrame.

        Parameters
        ----------
        file_path : str
            location of the Bibtex (.bib) bibliography file to read.
        
        Returns
        -------
        results : Results
            a Results DataFrame.
        """

        results = Results()
        results.import_bibtex(file_path=file_path)

        return results

    def import_excel(self, file_path = 'request_input', sheet_name = None):

        """
        Reads an Excel (.xlsx) file and adds its data to the Results DataFrame.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        sheet_name : str
            optional: name of Excel sheet to read.

        Returns
        -------
        self : Results
            a Results DataFrame.
        """

        if file_path == 'request_input':
            file_path = input('File path: ')

        if sheet_name == None:
            sheet_name = 0
        
        excel_import = pd.read_excel(file_path, sheet_name = sheet_name, header = 0, index_col = 0).replace({np.nan: None, 'none': None})
        
        self.add_dataframe(excel_import)

        cols = ['authors', 'keywords']
        for col in cols:
            for i in self.index:
                items = self.loc[i, col]
                
                try:
                    self.at[i, col] = strip_list_str(items) # type: ignore
                    
                except:
                    pass

        return self

    def from_excel(file_path = 'request_input', sheet_name = None): # type: ignore

        """
        Reads an Excel (.xlsx) file and returns as a Results DataFrame.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        sheet_name : str
            optional: name of Excel sheet to read.

        Returns
        -------
        results_table : Results
            a Results DataFrame.
        """

        results_table = Results()
        results_table = results_table.import_excel(file_path, sheet_name).replace(np.nan, None) # type: ignore
        results_table.format_authors() # type: ignore


        return results_table

    def import_csv(self, file_path = 'request_input'):

            """
            Reads a CSV (.csv) file and adds its data to the Results DataFrame.

            Parameters
            ----------
            file_path : str
                directory path of file to import. Defaults to requesting from user input.

            Returns
            -------
            self : Results
                a Results object.
            """

            if file_path == 'request_input':
                file_path = input('File path: ')
                
            csv_import = pd.read_csv(file_path, header = 0, index_col = 0).replace({np.nan: None, 'none': None})
            self.add_dataframe(csv_import)

            cols = ['authors', 'keywords']
            for col in cols:
                for i in self.index:
                    items = self.loc[i, col]
                    
                    try:
                        self.at[i, col] = strip_list_str(items) # type: ignore
                        
                    except:
                        pass

            return self
    
    def from_csv(file_path = 'request_input'): # type: ignore

        """
        Reads a CSV (.csv) file and returns as a Results DataFrame.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.

        Returns
        -------
        results_table : Results
            a Results object.
        """

        results_table = Results()
        results_table.import_csv(file_path).replace(np.nan, None) # type: ignore

        return results_table

    def import_json(self, file_path = 'request_input'):

        """
        Reads a JSON (.json) file and adds its data to the Results DataFrame.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.

        Returns
        -------
        self : Results
            a Results object.
        """

        if file_path == 'request_input':
                file_path = input('File path: ')

        json_import = pd.read_json(file_path)
        self.add_dataframe(json_import)

        cols = ['authors', 'keywords']
        for col in cols:
                for i in self.index:
                    items = self.loc[i, col]
                    
                    try:
                        self.at[i, col] = strip_list_str(items) # type: ignore
                        
                    except:
                        pass

        self = self.replace(np.nan, None)

        return self
    
    def from_json(file_path = 'request_input'): # type: ignore

        """
        Reads a JSON (.json) file and returns as a Results DataFrame.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.

        Returns
        -------
        results_table : Results
            a Results object.
        """

        results_table = Results()
        results_table.import_json(file_path).replace(np.nan, None) # type: ignore
        
        return results_table
    
    def import_file(self, file_path = 'request_input', sheet_name = None):

        """
        Reads a file, determines its file type, and adds its data to the Results object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        sheet_name : str
            optional: name of an Excel sheet to read (if one exists).

        Notes
        -----
        Can read:
            * .xlsx
            * .csv
            * .json
            * .bib
        """

        if file_path == 'request_input':
            file_path = input('File path: ')
        
        path_obj = Path(file_path)
        suffix = path_obj.suffix

        if path_obj.exists() == True:

            if suffix.strip('.') == 'xlsx':
                return self.import_excel(file_path, sheet_name)
            
            if suffix.strip('.') == 'csv':
                return self.import_csv(file_path)
            
            if suffix.strip('.') == 'json':
                return self.import_json(file_path)

            if suffix.strip('.') == 'bib':
                return self.import_bibtex(file_path)
        
        else:
            raise ValueError('File does not exist')
    
    def from_file(file_path = 'request_input', sheet_name = None): # type: ignore

        """
        Reads a file, determines its file type, and returns its data as a Results object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        sheet_name : str
            optional: name of an Excel sheet to read (if one exists).

        Notes
        -----
        Can read:
            * .xlsx
            * .csv
            * .json
            * .bib
        """

        if file_path == 'request_input':
            file_path = input('File path: ')
        
        results_table = Results()

        path_obj = Path(file_path)  # type: ignore
        suffix = path_obj.suffix

        if path_obj.exists() == True:

            if suffix.strip('.') == 'xlsx':
                return results_table.import_excel(file_path, sheet_name)  # type: ignore
            
            if suffix.strip('.') == 'csv':
                return results_table.import_csv(file_path)  # type: ignore
            
            if suffix.strip('.') == 'json':
                return results_table.import_json(file_path) # type: ignore
        
        else:
            raise ValueError('File does not exist')

    def import_jstor(self, file_path = 'request_input', drop_empty_rows = False, drop_duplicates = False, update_work_ids = True):

        """
        Reads a file outputted by JSTOR's Constellate portal and adds its data to the Results DataFrame.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        update_work_ids : bool
            whether to add and/or update work IDs. Defaults to True.
        
        Notes
        -----
        Can read:
            * .csv
            * .json
        """

        df = import_jstor(file_path = file_path)
        self.add_dataframe(dataframe=df, drop_empty_rows = drop_empty_rows, drop_duplicates = drop_duplicates, update_work_ids = update_work_ids)

    def from_jstor(self, file_path = 'request_input', drop_empty_rows = False, drop_duplicates = False, update_work_ids = True):

        """
        Reads a file outputted by JSTOR's Constellate portal and returns as a Results DataFrame.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        update_work_ids : bool
            whether to add and/or update work IDs. Defaults to True.
        
        Returns
        -------
        results : Results
            a Results object.   

        Notes
        -----
        Can read:
            * .csv
            * .json
        """

        results = Results()
        results.import_jstor(file_path = file_path, drop_empty_rows = drop_empty_rows, drop_duplicates = drop_duplicates, update_work_ids = update_work_ids)
        
        return results

    def search_field(self, field = 'request_input', any_kwds = 'request_input', all_kwds = None, not_kwds = None, case_sensitive = False, output = 'Results'):
        
        """
        Searches a given field in the Results DataFrame for a string.

        Parameters
        ----------
        field : str
            name of field to search. Defaults to requesting from user input.
        any_kwds : str or list
            one or more keywords to search for. Returns results where *any* matches are found. Defaults to requesting from user input.
        all_kwds : str or list
            one or more keywords to search for. Returns results where *all* matches are found. Defaults to None.
        not_kwds : str or list
            one or more keywords to search for. Returns results where *no* matches are found. Defaults to None.
        case_sensitive : bool
            whether to pay attention to the case of string data. Defaults to False.
        output : str
            the type of object to return. Defaults to Results.

        Returns
        -------
        output : Results or pandas.DataFrame
            search results.
        """

        if field == 'request_input':
            field = input('Field: ')
        
        if any_kwds == 'request_input':
            any_kwds = input('Any keywords: ')
            any_kwds = any_kwds.strip().split(',')
            any_kwds = [i.strip() for i in any_kwds]

        contains_df = pd.DataFrame()
        not_kwds_df= pd.DataFrame()
        
        df = self.copy(deep=True)
        
        df[field] =  df[field].astype('str')

        if case_sensitive == False:
            df[field] = df[field].str.lower()
        
        if any_kwds != None:
                
            if (type(any_kwds) != list) and (type(any_kwds) != str):
                raise TypeError('"any_kwds" must be a string or list')

            if type(any_kwds) == str:
                any_kwds = [any_kwds]

            if case_sensitive == False:
                any_kwds = pd.Series(any_kwds).str.lower().to_list()

            for item in any_kwds:

                rows = df[df[field].str.contains(item) == True]
                contains_df = pd.concat([contains_df, rows])
                
        if not_kwds != None:

                if type(not_kwds) == str:
                    not_kwds = [not_kwds]

                if type(not_kwds) != list:
                    raise TypeError('"not_kwds" must be a string or list')

                if case_sensitive == False:
                    not_kwds = pd.Series(not_kwds).str.lower().to_list()

                for item in not_kwds:
                    
                        rows = df[df[field].str.contains(item) == False]
                        not_kwds = pd.concat([not_kwds, rows]) # type: ignore
        
        
        combined_df = pd.concat([contains_df, not_kwds_df])
        
        if all_kwds != None:
                
            if (type(all_kwds) != list) and (type(all_kwds) != str):
                raise TypeError('"all_kwds" must be a string or list')

            if type(all_kwds) == str:
                any_kwds = any_kwds.strip().split(',') # type: ignore
                any_kwds = [i.strip() for i in any_kwds]

            if case_sensitive == False:
                all_kwds = pd.Series(all_kwds).str.lower().to_list()
            
            for item in all_kwds:
                    
                combined_df = combined_df[combined_df[field].str.contains(item)]
        
        if output.lower() == 'results':

            masked = self.loc[combined_df.index]
            return masked

        else:
            if (output.lower() == 'dataframe') or (output.lower() == 'pandas.dataframe') or (output.lower() == 'pd.dataframe'):
                return combined_df

    def search(self, fields = 'all', any_kwds = 'request_input', all_kwds = None, not_kwds = None, case_sensitive = False, output = 'Results'):

        """
        Searches for a string throughout the Results DataFrame.

        Parameters
        ----------
        fields : str or list
            names of one or fields to search. Defaults to 'all'.
        any_kwds : str or list
            one or more keywords to search for. Returns results where *any* matches are found. Defaults to requesting from user input.
        all_kwds : str or list
            one or more keywords to search for. Returns results where *all* matches are found. Defaults to None.
        not_kwds : str or list
            one or more keywords to search for. Returns results where *no* matches are found. Defaults to None.
        case_sensitive : bool
            whether to pay attention to the case of string data. Defaults to False.
        output : str
            the class of object to output. Defaults to Results.

        Returns
        -------
        output : Results or pandas.DataFrame
            search results.
        """

        if any_kwds == 'request_input':
            any_kwds = input('Any keywords: ')
            any_kwds = any_kwds.strip().split(',')
            any_kwds = [i.strip() for i in any_kwds]
    
        if all_kwds != None:
                
            if (type(all_kwds) != list) and (type(all_kwds) != str):
                raise TypeError('"all_kwds" must be a string or list')

            if type(all_kwds) == str:
                any_kwds = any_kwds.strip().split(',') # type: ignore
                any_kwds = [i.strip() for i in any_kwds]

            if case_sensitive == False:
                all_kwds = pd.Series(all_kwds).str.lower().to_list()

        global results_cols

        output_df = pd.DataFrame(columns = results_cols, dtype=object)

        if fields == 'all':
            
            output_df = pd.DataFrame(columns = self.columns.to_list(), dtype=object)
            
            for col in self.columns:
                rows = self.search_field(field = col, any_kwds = any_kwds, not_kwds = not_kwds, case_sensitive = case_sensitive, output = 'pandas.dataframe') # type: ignore
                output_df = pd.concat([output_df, rows])
            
            output_df = output_df[~output_df.index.duplicated(keep = 'first') == True]
            
            if all_kwds != None:
                
                for item in all_kwds:
                    for i in output_df.index:
                        if True not in (output_df.loc[i].str.contains(item) == True).to_list():
                            output_df = output_df.drop(i)
    
        else:
            
            if (type(fields) == str) or ((type(fields) == list) and (len(fields) == 1)):

                output_df = self.search_field(field = fields, any_kwds = any_kwds, not_kwds = not_kwds, case_sensitive = case_sensitive) # type: ignore
                output_df = output_df[~output_df.index.duplicated(keep = 'first') == True] # type: ignore
                
                if all_kwds != None:
                    
                    for item in all_kwds:
                        
                        for i in output_df.index:
                            if True not in (output_df.loc[i].str.contains(item) == True).to_list():
                                output_df = output_df.drop(i)
        
        if output.lower() == 'results':
            masked = self.loc[output_df.index]
            return masked

        else:
            if (output.lower() == 'dataframe') or (output.lower() == 'pandas.dataframe') or (output.lower() == 'pd.dataframe'):
                return output_df
    
    def get_keywords(self):
        
        """
        Returns a Pandas Series containing all keywords associated with results in the Results DataFrame.
        """

        output = []

        for i in self['keywords']:
            if type(i) == str:
                i = strip_list_str(i)  # type: ignore
            
            if type(i) == list:
                output = output + i

        output = pd.Series(output).dropna()
        output = output.astype(str)
        output = output.str.strip().str.lower()
        output = output.drop(output[output.values == 'none'].index).reset_index().drop('index', axis=1)[0] # type: ignore
        
        return output
    
    def get_keywords_list(self):

        """
        Returns a list containing all keywords associated with results in the Results DataFrame.
        """

        return self.get_keywords().to_list()
    
    def get_keywords_set(self):

        """
        Returns a set containing all unique keywords associated with results in the Results DataFrame.
        """

        return set(self.get_keywords_list())
    
    def keyword_frequencies(self):

        """
        Returns a Pandas Series containing the frequencies of all keywords associated with results in the Results DataFrame.
        """

        return self.get_keywords().value_counts()

    def keyword_stats(self):

        """
        Returns a Pandas Series containing summary statistics for the frequency of keywords associated with results in the Results DataFrame.
        """

        return self.keyword_frequencies().describe()
    
    def get_titles_words(self, ignore_stopwords = True):

        """
        Returns a list containing all words used in all titles across the Results DataFrame.

        Parameters
        ----------
        ignore_stopwords : bool
            whether to remove stopwords from the list. Uses the 'all' dataset from the art.datasets.stopwords.stopwords dictionary. Defaults to True.

        Returns
        -------
        output : list
            a list containing all words used in all titles across the Results DataFrame.
        """

        df = self.copy(deep=True)
        df['title'] = df['title'].astype(str).str.lower().str.strip()

        output = []

        for i in df['title']:
            title = i.strip().replace("'", "").replace("`", "").replace(':', ' ').replace('-', ' ')
            words = list(word_tokenize(title))
            output = output + words
        
    
        if ignore_stopwords == True:
            
            global stopwords
            output = [i for i in output if i not in stopwords['all']]
        
        return output
    
    def get_titles_words_set(self, ignore_stopwords = True):

        """
        Returns a set containing unique words used in titles across the Results DataFrame.

        Parameters
        ----------
        ignore_stopwords : bool
            whether to remove stopwords from the set. Uses the 'all' dataset from the art.datasets.stopwords.stopwords dictionary. Defaults to True.

        Returns
        -------
        output : set
            a set containing unique words used in titles across the Results DataFrame.
        """

        return set(self.get_titles_words(ignore_stopwords = ignore_stopwords))
    
    def title_word_frequencies(self, ignore_stopwords = True):

        """
        Returns a Pandas Series containing the frequencies of words used in titles across the Results DataFrame.

        Parameters
        ----------
        ignore_stopwords : bool
            whether to ignore stopwords. Uses the 'all' dataset from the art.datasets.stopwords.stopwords dictionary. Defaults to True.

        Returns
        -------
        frequencies : pandas.Series
            a Pandas Series containing the frequencies of words used in titles across the Results DataFrame.
        """

        return pd.Series(self.get_titles_words(ignore_stopwords = ignore_stopwords)).value_counts()
                                               
    def title_words_stats(self, ignore_stopwords = True):

        """
        Returns a Pandas Series containing summary statistics for the frequency of words used in titles across the Results DataFrame.

        Parameters
        ----------
        ignore_stopwords : bool
            whether to ignore stopwords. Uses the 'all' dataset from the art.datasets.stopwords.stopwords dictionary. Defaults to True.

        Returns
        -------
        frequencies : pandas.Series
            a Pandas Series containing summary statistics for the frequency of words used in titles across the Results DataFrame.
        """

        return self.title_word_frequencies(ignore_stopwords = ignore_stopwords).describe()
    
    def drop_containing_keywords(self, keywords: list):
        
        """
        Removes all rows which contain any of the inputted keywords.

        Parameters
        ----------
        keywords : list
            a list of keywords to search for.
        """

        if type(keywords) == str:
            keywords = [keywords]

        results = self.search(any_kwds = keywords).index # type: ignore
        return self.drop(index = results, axis=0).reset_index().drop('index', axis=1)

    def filter_by_keyword_frequency(self, cutoff = 3):

        """
        Filters the Results DataFrame to show only results which contain keywords that meet a frequency cutoff. 

        Parameters
        ----------
        cutoff : int
            a frequency cutoff for keywords.
        
        Returns
        -------
        output : Results or pandas.DataFrame
            the filtered DataFrame.
        """

        keywords_freq = self.keyword_frequencies()

        frequent_kws = keywords_freq[keywords_freq.values > cutoff] # type: ignore
        frequent_kws = list(frequent_kws.index)
        
        output = pd.DataFrame(dtype=object)
        for i in frequent_kws:
            df = self.search(any_kwds = i).copy(deep=True) # type: ignore
            output = pd.concat([output, df])

        output = output.drop_duplicates('title')
        output = output.reset_index().drop('index', axis=1)

        print(f'Keywords: {frequent_kws}')

        return self.loc[output.index]
    
    def has_citations_data(self):

        """
        Returns all Results entries which contain citations data.
        """

        return self[~self['citations_data'].isna()]

    def has(self, column):

        """
        Returns all Results entries which contain data in the inputted column.

        Parameters
        ----------
        column : str
            name of column to filter on.
        
        Returns
        -------
        self : Results
            the masked Results DataFrame.
        """

        return self[~self[column].isna()]
    
    def contains(self, query: str = 'request_input', ignore_case: bool = True) -> bool:

        """
        Checks if the Results DataFrame contains an inputted string. Returns True if yes; else, returns False.

        Parameters
        ----------
        query : str
            a string to check for. Defaults to requesting from user input.
        ignore_case : bool
            whether to ignore the case of string data. Defaults to True.
        
        Returns
        -------
        result : bool
            whether the Results DataFrame contains the string.
        """

        if query == 'request_input':
            query = input('Search query').strip()

        query = str(query).strip()

        all_str = self.copy(deep=True).astype(str)
        
        if ignore_case == True:
            query = query.lower()
            

        cols = all_str.columns

        for c in cols:

            if ignore_case == True:
                all_str[c] = all_str[c].str.lower()

            if c == 'work_id':
                res = all_str[all_str[c].str.contains(query)]
                if len(res) > 0:
                    return True
            
            if c == 'title':
                res = all_str[all_str[c] == query]
                if len(res) > 0:
                    return True
            
            if c == 'date':
                res = all_str[all_str[c].str.contains(query)]
                if len(res) > 0:
                    return True
            
            if c == 'source':
                res = all_str[all_str[c].str.contains(query)]
                if len(res) > 0:
                    return True
            
            if c == 'publisher':
                res = all_str[all_str[c].str.contains(query)]
                if len(res) > 0:
                    return True
            
            if c == 'funder':
                res = all_str[all_str[c].str.contains(query)]
                if len(res) > 0:
                    return True
            
            if c == 'keywords':
                res = all_str[all_str[c].str.contains(query)]
                if len(res) > 0:
                    return True
            
            if c == 'doi':
                res = all_str[all_str[c].str.contains(query)]
                if len(res) > 0:
                    return True
            
            if c == 'isbn':
                res = all_str[all_str[c] == query]
                if len(res) > 0:
                    return True
            
            if c == 'issn':
                res = all_str[all_str[c] == query]
                if len(res) > 0:
                    return True
            
            if c == 'link':
                res = all_str[all_str[c] == query]
                if len(res) > 0:
                    return True

        return False

    def mask_affiliations(self, query: str = 'request_input', ignore_case: bool = True):

        """
        Filters the Results DataFrame for entries which contain an inputted string in their affiliations data.

        Parameters
        ----------
        query : str
            a string to search for. Defaults to requesting from user input.
        ignore_case : bool
            whether to ignore the case of string data. Defaults to True.

        Returns
        -------
        output : Results or pandas.DataFrame
            the filtered DataFrame.
        """

        if query == 'request_input':
            query = input('Search query: ').strip()

        query = str(query).strip()

        def affil_masker(authors):
            if 'mask_entities' in authors.__dir__():
                masked = authors.mask_entities(column='affiliations', query=query, ignore_case=ignore_case)
                res = len(masked) > 0
                
            else:
                res = False

            return res

        
        masked = self[self['authors'].apply(affil_masker)]

        return masked

    def mask_entities(self, column, query: str = 'request_input', ignore_case: bool = True):

        """
        Filters the Results DataFrame for entries which contain an inputted string in their authors or funders data.

        Parameters
        ----------
        query : str
            a string to search for. Defaults to requesting from user input.
        ignore_case : bool
            whether to ignore the case of string data. Defaults to True.

        Returns
        -------
        output : Results or pandas.DataFrame
            the filtered DataFrame.
        """

        if query == 'request_input':
            query = input('Search query').strip()

        query = str(query).strip()

        def entity_masker(entities):
            
            if 'contains' in entities.__dir__():
                res = entities.contains(query=query, ignore_case=ignore_case)
                
            else:
                res = False

            return res
        
        masked = self[self[column].apply(entity_masker)]

        return masked

    def format_funders(self, use_api: bool = False):

        """
        Formats all funders data as Funders objects.
        """

        try:
            funders = self['funder'].apply(format_funders) # type: ignore
        except:
            funders = self['funder']

        self['funder'] = funders

Entity.publications = Results() # type: ignore
Funder.publications = Results() # type: ignore


