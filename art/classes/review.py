from ..utils.basics import Iterator, results_cols
from ..utils.cleaners import deduplicate
from ..exporters.general_exporters import obj_to_folder, art_class_to_folder

from ..importers.pdf import read_pdf_to_table
from ..importers.crossref import search_works, lookup_doi, lookup_dois, lookup_journal, lookup_journals, search_journals, get_journal_entries, search_journal_entries, lookup_funder, lookup_funders, search_funders, get_funder_works, search_funder_works
from ..importers.crossref import query_builder as crossref_query_builder
from ..importers.scopus import query_builder as scopus_query_builder, search as search_scopus, lookup as lookup_scopus
# from ..importers.wos import search as search_wos, query_builder as wos_query_builder
from ..importers.search import search as api_search

from ..internet.scrapers import scrape_article, scrape_doi, scrape_google_scholar, scrape_google_scholar_search
from ..networks.network_functions import generate_coauthors_network, generate_citations_network, generate_funders_network, generate_author_works_network, generate_funder_works_network, generate_author_affils_network, generate_cocitation_network, generate_bibcoupling_network

from .properties import Properties
from .affiliations import Affiliation, Affiliations, format_affiliations
from .funders import Funders, format_funders
from .results import Results, Funder, generate_work_id
from .references import References, is_formatted_reference, format_references
from .activitylog import ActivityLog
from .authors import Author, Authors, format_authors as orig_format_authors
from .networks import Network, Networks
from .citation_crawler import citation_crawler, academic_scraper

import copy
import pickle
from pathlib import Path

import pandas as pd
import numpy as np

from igraph import Graph # type: ignore


def add_pdf(self, path = 'request_input'):
        
        """
        Imports a PDF file using a filepath and adds to the Results dataframe.
        
        Parameters
        ----------
        path : str
            filepath for PDF. Requests input if none passed explicitly.
        
        Returns
        -------
        self : Results
            a Results object.
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

        self.format_authors()

        return self

Results.add_pdf = add_pdf # type: ignore

def add_row(self, data):

        """
        Adds inputted data as a row to Results dataframe.
        
        Parameters
        ----------
        data : pandas.Series
            data to add to Results.
        
        Returns
        -------
        self : Results
            a Results object.
        """

        if type(data) != pd.Series:
            raise TypeError(f'Results must be a pandas.Series, not {type(data)}')

        data.index = data.index.astype(str).str.lower().str.replace(' ', '_')
        if len(data) != len(self.columns):
            for c in data.index:
                if c not in self.columns:
                    self[c] = pd.Series(dtype=object)

        index = len(self)

        work_id = generate_work_id(data)
        work_id = self.get_unique_id(work_id, index)
        data['work_id'] = work_id
        
        self.loc[index] = data
        self.format_authors()

Results.add_row = add_row # type: ignore

def add_dataframe(self,  dataframe: pd.DataFrame, drop_duplicates = False, drop_empty_rows = False, update_work_ids = True, format_authors = False):
        
        """
        Merges inputted dataframe with Results dataframe.
        
        Parameters
        ----------
        dataframe : pandas.DataFrame
            dataframe to add to Results.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_work_ids : bool
            whether to update results work ID's.
        format_authors : bool
            whether to format author data.
        
        Returns
        -------
        self : Results
            a Results object.
        """

        if (type(dataframe) != pd.DataFrame) and (type(dataframe) != pd.Series):
            raise TypeError(f'Results must be a pandas.Series or pandas.DataFrame, not {type(dataframe)}')

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

        if format_authors == True:
            self.format_authors()

Results.add_dataframe = add_dataframe # type: ignore

def has_formatted_citations(self):
        
        """
        Returns all results entries which contain properly formatted citations.
        """

        return self[self['citations'].apply(is_formatted_reference)]

Results.has_formatted_citations = has_formatted_citations # type: ignore

def lacks_formatted_citations(self):
        
        """
        Returns all results entries which lack properly formatted citations.
        """

        return self[~self['citations'].apply(is_formatted_reference)]

Results.lacks_formatted_citations = lacks_formatted_citations # type: ignore

def format_citations(self, add_work_ids = False, update_from_doi = False, verbose = True):
        
        """
        Formats all results entries' citations data as References objects.
        
        Parameters
        ----------
        add_work_ids : bool
            whether to add new work ID's to results entries.
        update_from_doi : bool
            whether to update results data from DOI's.
        verbose : bool
            whether to print dialogue during formatting.
        """

        self['citations'] = self['citations'].replace({np.nan: None})
        self['citations_data'] = self['citations_data'].replace({np.nan: None})

        if len(self[self['citations_data'].isna()]) == len(self['citations_data']):
            self['citations_data'] = self['citations'].copy(deep=True)

        unformatted = self.lacks_formatted_citations()
        length = len(unformatted)
        if length > 0:
            
            if verbose == True:
                if length == 1:
                    intro_message = '\nFormatting 1 set of citations...'
                else:
                    intro_message = f'\nFormatting {length} sets of citations...'
                
                print(intro_message)

            indices = unformatted.index
            processing_count = 0
            for i in indices:
                refs = format_references(self.loc[i, 'citations_data'], add_work_ids = add_work_ids, update_from_doi = update_from_doi)
                refs_count  = None

                if 'refs_count' in refs.__dict__.keys():
                    refs_count  = refs.refs_count
                
                if refs_count is None:
                    try:
                        refs_count = len(refs) # type: ignore
                    except:
                        refs_count = 0

                processing_count = processing_count + refs_count
                self.at[i, 'citations'] = refs
                self.at[i, 'citation_count'] = refs_count
            
            if verbose == True:
                if processing_count == 1:
                    outro_message = '1 citation formatted\n'
                else:
                    outro_message = f'{processing_count} citations formatted\n'
                print(outro_message)

Results.format_citations = format_citations # type: ignore

def format_authors(self):

        """
        Formats all results entries' authors data as Authors objects.
        """

        if len(self[self['authors_data'].isna()]) < len(self['authors_data']):
            authors_data = self['authors_data']
        
        else:
            authors_data = self['authors']

        new_series = authors_data.apply(orig_format_authors) # type: ignore

        self['authors'] = new_series

        return self['authors']

Results.format_authors = format_authors # type: ignore
    
def add_citations_to_results(self, add_work_ids = False, update_from_doi = False, drop_duplicates = False, drop_empty_rows = True):

        """
        Formats all results entries' citations and adds them to the Results object.
        
        Parameters
        ----------
        add_work_ids : bool
            whether to add work ID's to newly added results entries. Defaults to False.
        update_from_doi : bool
            whether to update results data from DOI's. Defaults to False.
        drop_empty_rows : bool
            whether to remove duplicate rows. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        
        Returns
        -------
        self : Results
            a Results object.
        """

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates()

        unformatted = self.lacks_formatted_citations()
        if len(unformatted) > 0:
            self.format_citations(add_work_ids = add_work_ids, update_from_doi = update_from_doi)

        citations = self['citations'].to_list()
        existing_ids = set(self['work_id'].to_list())
        
        for i in citations:

            if (type(i) == References) or (type(i) == Results) or (type(i) == pd.DataFrame):
                df = i.copy(deep=True)

                new_ids = set(df['work_id'].to_list())
                diff_len = len(new_ids.difference(existing_ids))

                if diff_len > 0:
                    self.add_dataframe(dataframe=df, drop_empty_rows = False)
        
        self.drop_empty_rows()
        self.update_work_ids()
        self.format_authors()


        return self

Results.add_citations_to_results = add_citations_to_results # type: ignore

class Review:
    
    """
    This is a Review object. It stores data from academic reviews.
    
    Parameters
    ----------
    review_name : str 
        the Review's name. Defaults to requesting from user input.
    file_location : str
        file location associated with Review.
    file_type : str
        file type associated with Review file(s).
    
    Attributes
    ----------
    properties : Properties
        metadata associated with this Review object.
    results : Results
        data on publications.
    authors : Authors
        data on authors.
    funders : Funders
        data on funders.
    affiliations : Affiliations
        data on authors' affiliate organisations.
    networks : Networks
        network objects derived from Review data.
    activity_log : ActivityLog
        metadata logging changes to the Review, including: additions, deletions, crawling, and searches.
    """

    results = Results()
    activity_log = ActivityLog()
    description = ''

    def __init__(self, review_name = None, file_location = None):
        
        """
        Initialises a Review instance.
        
        Parameters
        ----------
        review_name : str 
            the Review's name. Defaults to requesting from user input.
        file_location : str
            file location associated with Review.
        """
        
        if review_name == 'request_input':
            review_name = input('Review name: ')

        self.properties = Properties(review_name = review_name, file_location = file_location)
        self.results = Results()
        self.authors = Authors()
        self.funders = Funders()
        self.affiliations = Affiliations()
        self.activity_log = ActivityLog()
        self.description = ''
        self.networks = Networks()
        self.format()
        self.update_properties()
    
    def update_properties(self):
        
        """
        Updates Review's properties.
        
        Updates
        -------
            * review_id
            * case_count
            * size
            * last_changed
        """
        
        self.properties.review_id = id(self) # type: ignore
        self.properties.size = str(self.__sizeof__()) + ' bytes' # type: ignore
        self.properties.update_last_changed()
    
    def __repr__(self):
        
        """
        Defines how Review objects are represented in string form.
        """
        
        output = f'\n\n{"-"*(13+len(self.properties.review_name))}\nReview name: {self.properties.review_name}\n{"-"*(13+len(self.properties.review_name))}\n\nProperties:\n-----------\n{self.properties}\n\nDescription:\n------------\n\n{self.description}\n\nResults:\n--------\n\n{self.results}\n\nAuthors:\n--------\n\n{self.authors.summary.head(10)}\n\nFunders:\n--------\n\n{self.funders.summary.head(10)}\n\n'
        
        return output
            
    def __iter__(self):
        
        """
        Implements iteration functionality for Review objects.
        """
        
        return Iterator(self)
    
    def __getitem__(self, key):
        
        """
        Retrieves Review contents or results using an index/key.

        Returns
        -------
        item : object
            item associated with the inputted key.
        """
        
        if key in self.__dict__.keys():
            return self.__dict__[key]

        if key in self.results['work_id'].to_list():
            return self.results.get(key)
        
        if key in self.authors.all.keys():
            return self.authors[key]
        
        if key in self.results.columns.to_list():
            return self.results[key]
        
        if key in self.authors.summary.columns.to_list():
            return self.authors.summary[key]

    def __setitem__(self, index, data):
        
        """
        Adds entry to results using an index position. Specifying a column position is optional.
        """
        
        self.results.loc[index] = data
        
        self.update_properties()
        
    def __delitem__(self, index):
        
        """
        Deletes entry from results using its index.
        """
        
        self.results = self.results.drop(index)
        self.update_properties()
    
    def contents(self):
        
        """
        Returns the Review's attributes as a list.

        Returns
        -------
        contents : list
            the names of the Review object's attributes.
        """
        
        return self.__dict__.keys()
    
    def __len__(self):

        """
        Returns the number of entries in the Results table.

        Returns
        -------
        result : int
            the number of results entries contained in the Results dataframe.
        """    
        
        return len(self.results)
    
    def count_results(self):
        
        """
        Returns the number of entries in the Results table.

        Returns
        -------
        result : int
            the number of results entries contained in the Results dataframe.
        """
        
        return len(self.results)
        
    def to_list(self):
        
        """
        Returns the Review as a list.

        Returns
        -------
        result : list
            Review object formatted as a list.
        """
        
        return [i for i in self]
    
    def to_dict(self):
        
        """
        Returns the Review as a dictionary.  Excludes the Review's 'properties' attribute.

        Returns
        -------
        output_dict : dict
            Review object formatted as a dictionary.
        """
        
        output_dict = {}
        for index in self.__dict__.keys():
            output_dict[index] = self.__dict__[index]
        
        return output_dict
    
    def to_bibtex(self):
        
        """
        Returns an object containing Results data in bibtex format.
        """

        return self.results.to_bibtex()
    
    def to_yaml(self):

        """
        Returns an object containing Results data in .yaml format.
        """

        return self.results.to_yaml()
    
    def export_bibtex(self, file_name = 'request_input', folder_path= 'request_input'):

        """
        Exports Results data as a .bib file.
        
        Parameters
        ----------
        file_name : str
            name for export file. Defaults to requesting from user input.
        folder_path : str
            directory path for folder to export to. Defaults to requesting from user input.
        """

        return self.results.export_bibtex(file_name=file_name, folder_path=folder_path)

    def export_yaml(self, file_name = 'request_input', folder_path= 'request_input'):

        """
        Exports Results data as a .yaml file.
        
        Parameters
        ----------
        file_name : str
            name for export file. Defaults to requesting from user input.
        folder_path : str
            directory path for folder to export to. Defaults to requesting from user input.
        """

        return self.results.export_yaml(file_name=file_name, folder_path=folder_path)

    def copy(self):
        
        """
        Returns the a copy of the Review object.
        """
        
        return copy.deepcopy(self)
    
    def get_result(self, index_position, column_position = None):
        
        """
        Returns a result when given its index position, with an option for specifying column position. 
        Equivalent to pandas.DataFrame.loc[...]

        Parameters
        ----------
        index_position : int
            index position of result entry to return from Results dataframe.
        column_position : object
            name of column of datapoint to return from Results dataframe.
        
        Returns
        -------
        result : object
            the selected object.
        """
        
        if column_position == None:
            return self.results.loc[index_position]
        else:
            return self.results.loc[index_position, column_position]
    
    def get_affiliations_dict(self):

        """
        Returns the all affiliations associated with Authors objects as a dictionary.

        Returns
        -------
        result : Affiliations
            Affiliations associated with Authors objects.
        """

        return self.authors.affiliations()

    def get_name_str(self):
        
        """
        Returns the Review's variable name as a string. 
        
        Notes
        -----
            * Searches global environment dictionary for objects sharing Review's ID. Returns key if found.
            * If none found, searches local environment dictionary for objects sharing Review's ID. Returns key if found.
        """
        
        for name in globals():
            if id(globals()[name]) == id(self):
                return name
        
        for name in locals():
            if id(locals()[name]) == id(self):
                return name
    
    def add_pdf(self, path = 'request_input', update_formatting: bool = True):
        
        """
        Reads a PDF and adds its data to the Results dataframe.

        Parameters
        ----------
        path : str
            file path for PDF to read. Defaults to request from user input.
        update_formatting : bool
            whether to format the added data (e.g., citations, authors, funders, and affiliations)
        
        Returns
        -------
        self : Review
            a Review object.
        """

        old_res_len = len(self.results)
        self.results.add_pdf(path) # type: ignore
        new_res_len = len(self.results)
        res_diff = new_res_len - old_res_len

        changes = {'results': res_diff}
        self.activity_log.add_activity(type='data import', activity='added PDF data to results', location = ['results'], changes_dict=changes)
        
        if update_formatting == True:
            self.format()

        self.update_properties()

        return self

    def varstr(self):
        
        """
        Returns the Review's name as a string. Defaults to using its variable name; falls back to using its name property.
        
        Returns
        -------
        string : str
            the Review's name as a string.

        Notes
        -----
            * Searches global environment dictionary for objects sharing Review's ID. Returns key if found.
            * If none found, searches local environment dictionary for objects sharing Review's ID. Returns key if found.
            * If none found, returns Review's name property.
            * If name property is None, 'none', or 'self', returns an empty string.
        """
        
        try:
            string = self.get_name_str()
        except:
            string = None
        
        if (string == None) or (string == 'self'):
            
            try:
                string = self.properties.review_name
            except:
                string = ''
                
        if (string == None) or (string == 'self'):
            string = ''
            
        return string
    
    def to_dataframe(self, attribute: str = 'results'):

        """
        Returns one of the Review's datasets as a Pandas DataFrame. Defaults to returning the Results dataset.
        
        Parameters
        ----------
        attribute : str
            the name of the Review dataset to return.

        Returns
        -------
        df : pandas.DataFrame
            the dataset as a pandas dataframe.
        """

        df = pd.DataFrame(dtype=object)

        if attribute.lower() == 'results':
            df = self.results.to_dataframe() # type: ignore
        
        if attribute.lower() == 'authors':
            df = self.authors.to_dataframe() # type: ignore
        
        if attribute.lower() == 'funders':
            df = self.funders.to_dataframe() # type: ignore
        
        if (attribute.lower() == 'affiliations') or (attribute.lower() == 'affils'):
            df = self.affiliations.to_dataframe() # type: ignore

        return df

    def from_dataframe(dataframe: pd.DataFrame): # type: ignore
        
        """
        Creates a Review object from a Pandas DataFrame.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            a Pandas DataFrame to use.

        Returns
        -------
        review : Review
            a Review object.
        """

        review = Review()
        review.results = Results.from_dataframe(dataframe) # type: ignore
        review.format() # type: ignore

        return review

    def format_funders(self):

        """
        Formats results entries' funders data into Funders objects and stores in Review's Funders attribute.
        """

        self.results.format_funders() # type: ignore

        funders_data = self.results['funder'].to_list()

        for i in funders_data:

            if type(i) == Funders:
                self.funders.merge(i)
                continue
            
            if type(i) == Funder:
                self.funders.add_funder(funder=i)
                continue
            
            if (type(i) == str) and (len(i) > 0):
                i = i.split(',')

            if (type(i) == list) and (len(i) > 0):

                if type(i[0]) == Funder:
                    self.funders.add_funders_list(funders_list=i)
                    continue

                if type(i[0]) == str:
                    funders = Funders()
                    for f in i:
                        funder = Funder(name=f.strip())
                        funders.add_funder(funder)
                    self.funders.merge(funders)
                    continue

    def format_affiliations(self):

        """
        Formats authors' affiliations data as Affiliations objects and stores in Review's Affiliations attribute.
        """

        self.authors.format_affiliations()

        affils_data = self.authors.summary['affiliations'].to_list()

        for i in affils_data:

            if type(i) == Affiliations:
                self.affiliations.merge(i)
                continue
            
            if type(i) == Affiliation:
                self.affiliations.add_affiliation(affiliation=i)
                continue
            
            if (type(i) == str) and (len(i) > 0):
                i = i.split(',')

            if (type(i) == list) and (len(i) > 0):

                if type(i[0]) == Affiliation:
                    affils = Affiliations()
                    for a in i:
                        affils.add_affiliation(affiliation=a) # type: ignore
                    self.affiliations.merge(affiliations=affils)
                    continue

                if type(i[0]) == str:
                    affils = Affiliations()
                    for a in i:
                        affiliation = Affiliation(name=a.strip())
                        affils.add_affiliation(affiliation)
                    self.affiliations.merge(affiliations=affils)
                    continue

    def format_citations(self, add_work_ids = False, update_from_doi = False, verbose=True):

        """
        Formats results entries' citations data into References objects.

        Parameters
        ----------
        add_work_ids : bool
            whether to add work ID's to References entries.
        """

        self.results.format_citations(add_work_ids = add_work_ids, update_from_doi=update_from_doi, verbose=verbose) # type: ignore

    def format_authors(self, drop_duplicates = False, drop_empty_rows=True):

        """
        Formats results entries' authors data into Authors objects and stores in Review's Authors attribute.

        Parameters
        ----------
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        """

        self.results.format_authors() # type: ignore

        authors_data = self.results['authors'].to_list()

        for i in authors_data:

            if type(i) == Authors:
                self.authors.merge(i)
          
            if (type(i) == str) and (len(i) > 0):
                i = i.split(',')

            if (type(i) == list) and (len(i) > 0):
                auths = Authors()
                for a in i:
                    auth = Author(full_name=a.strip())
                    auths.add_author(auth)
                self.authors.merge(auths)
        
        self.authors.sync(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
    
    def update_author_attrs(self, ignore_case: bool = True, drop_duplicates = False, drop_empty_rows=True):

        """
        Formats authors entries, identifies their publications, and stores these.

        Parameters
        ----------
        ignore_case : bool
            whether to ignore the case of string data.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        """

        self.authors.sync(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        auths_data = self.authors.summary[['author_id', 'orcid', 'google_scholar', 'crossref', 'scopus', 'full_name']]
        auths_data = auths_data.dropna(axis=1, how='all')

        global results_cols

        for i in auths_data.index:
            
            author_id = auths_data.loc[i, 'author_id']
            author_pubs = pd.DataFrame(columns=results_cols, dtype=object)

            for c in auths_data.columns:

                datapoint = auths_data.loc[i, c]

                if (datapoint != None) and (datapoint != '') and (datapoint != 'None'):

                    data_matches = self.results.mask_entities(column = 'authors', query=datapoint, ignore_case=ignore_case) # type: ignore
                    
                    match_ids = set(data_matches['work_id'])
                    current_ids = set(author_pubs['work_id'])
                    diff = match_ids.difference(current_ids)
                    
                    if (len(author_pubs) == 0) or (len(diff) > 0):
                        author_pubs = pd.concat([author_pubs, data_matches])
            
            deduplicated = author_pubs.copy(deep=True)
            deduplicated_indexes = deduplicated.drop_duplicates(subset='work_id').index.to_list()
            if len(deduplicated_indexes) > 0:
                author_pubs_deduplicated = author_pubs.loc[deduplicated_indexes]
            else:
                author_pubs_deduplicated = author_pubs

            # deduplicated2 = author_pubs_deduplicated.copy(deep=True).astype(str).drop_duplicates(subset=['title', 'doi'], ignore_index = True).index
            # author_pubs_deduplicated2 = author_pubs.loc[deduplicated2]

            # deduplicated3 = author_pubs_deduplicated2.copy(deep=True).astype(str).drop_duplicates(subset=['title', 'doi'], ignore_index = True).index
            # author_pubs_deduplicated3 = author_pubs.loc[deduplicated3]

            results = Results.from_dataframe(author_pubs_deduplicated) # type: ignore
            self.authors.all[author_id].publications = results
            
            pubs_dict = {}
            for work in results.index:
                key = results.loc[work, 'work_id']
                pubs_dict[key] = results.loc[work, 'title']
            
            if 'publications' not in self.authors.summary.columns:
                self.authors.summary['publications'] = pd.Series(dtype=object)

            self.authors.summary.at[i, 'publications'] = pubs_dict
            self.authors.all[author_id].affiliations = self.authors.all[author_id].details.loc[0, 'affiliations']
        
    def update_funder_attrs(self, ignore_case: bool = True):

        """
        Formats funders entries, identifies their publications, and stores these.

        Parameters
        ----------
        ignore_case : bool
            whether to ignore the case of string data.
        """

        self.funders.sync_all()

        f_data = self.funders.summary[['funder_id', 'uri', 'crossref_id', 'website','name']]
        f_data = f_data.dropna(axis=1, how='all')

        global results_cols

        for i in f_data.index:
            
            f_id = f_data.loc[i, 'funder_id']
            f_pubs = pd.DataFrame(columns=results_cols, dtype=object)

            for c in f_data.columns:

                datapoint = f_data.loc[i, c]

                if (datapoint != None) and (datapoint != '') and (datapoint != 'None'):

                    data_matches = self.results.mask_entities(column = 'funder', query=datapoint, ignore_case=ignore_case) # type: ignore
                    
                    match_ids = set(data_matches['work_id'])
                    current_ids = set(f_pubs['work_id'])
                    diff = match_ids.difference(current_ids)
                    
                    if (len(f_pubs) == 0) or (len(diff) > 0):
                        f_pubs = pd.concat([f_pubs, data_matches])
            
            deduplicated = f_pubs.copy(deep=True)
            deduplicated_indexes = deduplicated.drop_duplicates(subset='work_id').index.to_list()

            if len(deduplicated_indexes) > 0:
                f_pubs_deduplicated = f_pubs.loc[deduplicated_indexes]
            else:
                f_pubs_deduplicated = f_pubs

            # deduplicated2 = f_pubs_deduplicated.copy(deep=True).astype(str).drop_duplicates(subset=['title', 'doi'], ignore_index = True).index
            # f_pubs_deduplicated2 = f_pubs.loc[deduplicated2]

            # deduplicated3 = f_pubs_deduplicated2.copy(deep=True).astype(str).drop_duplicates(subset=['title', 'doi'], ignore_index = True).index
            # f_pubs_deduplicated3 = f_pubs.loc[deduplicated3]

            results = Results.from_dataframe(f_pubs_deduplicated) # type: ignore
            
            pubs_dict = {}
            for work in results.index:
                key = results.loc[work, 'work_id']
                pubs_dict[key] = results.loc[work, 'title']

            self.funders.all[f_id].publications = results
            if 'publications' not in self.funders.summary.columns:
                self.funders.summary['publications'] = pd.Series(dtype=object)
            self.funders.summary.at[i, 'publications'] = pubs_dict

    def update_affiliation_attrs(self, update_authors: bool = True, ignore_case: bool = True):
        
        """
        Formats affiliations entries, identifies their publications, and stores these.

        Parameters
        ----------
        ignore_case : bool
            whether to ignore the case of string data.
        """

        if update_authors == True:
            self.update_author_attrs(ignore_case=ignore_case)

        affils_data = self.affiliations.summary[['affiliation_id', 'name', 'uri', 'website']]
        affils_data = affils_data.dropna(axis=1, how='all')

        global results_cols

        for i in affils_data.index:

            affil_id = affils_data.loc[i, 'affiliation_id']
            if (affil_id == None) or (affil_id == '') or (affil_id == 'None'):
                affil_id = ''
            affil_id = str(affil_id)
            
            affil_auths = self.authors.mask_entities(column='affiliations', query=affil_id, ignore_case=ignore_case)

            if (affil_id != None) and (affil_id != '') and (affil_id != 'None'):
                self.affiliations.all[affil_id].authors = affil_auths

            affil_pubs = pd.DataFrame(columns=results_cols, dtype=object)
            for c in affils_data.columns:
                affil_info = affils_data.loc[i, c]

                if (affil_info != None) and (affil_info != '') and (affil_info != 'None'):
                    affil_info = str(affil_info)
                
                    masked_pubs = self.results.mask_affiliations(query=affil_info, ignore_case=ignore_case).copy(deep=True) # type: ignore

                    match_ids = set(masked_pubs['work_id'])
                    current_ids = set(affil_pubs['work_id'])
                    diff = match_ids.difference(current_ids)
                        
                    if (len(affil_pubs) == 0) or (len(diff) > 0):
                        affil_pubs = pd.concat([affil_pubs, masked_pubs])
                    
            deduplicated = affil_pubs.copy(deep=True)
            deduplicated_indexes = deduplicated.drop_duplicates(subset='work_id').index.to_list()

            if len(deduplicated_indexes) > 0:
                        affil_pubs_deduplicated = affil_pubs.loc[deduplicated_indexes]
            else:
                        affil_pubs_deduplicated = affil_pubs

            results = Results.from_dataframe(affil_pubs_deduplicated) # type: ignore

            pubs_dict = {}
            for work in results.index:
                key = results.loc[work, 'work_id']
                pubs_dict[key] = results.loc[work, 'title']

            self.affiliations.all[affil_id].publications = results
            
            if 'publications' not in self.affiliations.summary.columns:
                self.affiliations.summary['publications'] = pd.Series(dtype=object)
            self.affiliations.summary.at[i, 'publications'] = pubs_dict
                
    def update_entity_attrs(self, ignore_case: bool = True):
        
        """
        Formats authors, funders, and affiliations entries; identifies their publications; and stores these

        Parameters
        ----------
        ignore_case : bool
            whether to ignore the case of string data.
        """

        self.update_author_attrs(ignore_case=ignore_case)
        self.update_affiliation_attrs(update_authors=False, ignore_case=ignore_case)
        self.update_funder_attrs(ignore_case=ignore_case)

    def get_coauthors(self, format: bool = True, update_attrs: bool = True, ignore_case: bool = True, add_to_authors: bool = True, drop_duplicates = False, drop_empty_rows=True):
        
        """
        Returns a dictionary of co-authors.

        Parameters
        ----------
        format : bool
            whether to format results, authors, funders, and affiliations data.
        update_attrs : bool
            whether to update author attributes.
        ignore_case : bool
            whether to ignore the case of string data.
        add_to_authors : bool
            whether to store the dict of co-authors in the Review's Authors attribute.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.

        Returns
        -------
        output : dict
            a dictionary containing co-authors. 
                * Keys: author IDs
                * Values: co-authors
        """

        if format == True:
            self.format(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        if update_attrs == True:
            self.update_author_attrs(ignore_case=ignore_case, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        auth_ids = self.authors.all.keys()
        output = {}

        cols = Authors().summary.columns.to_list()

        for a in auth_ids:

            auth_pubs = self.results.mask_entities(column = 'authors', query=a, ignore_case=ignore_case) # type: ignore
            
            all_coauthors = pd.DataFrame(columns=cols, dtype=object)
            all_coauthors['frequency'] = pd.Series(dtype=object)

            for i in auth_pubs.index:

                work_coauthors = auth_pubs.loc[i, 'authors']
                if type(work_coauthors) == Authors:
                    work_coauthors = work_coauthors.summary
                if (type(work_coauthors) == pd.DataFrame) or (type(work_coauthors) == pd.Series):
                    all_coauthors = pd.concat([all_coauthors, work_coauthors])
            
            all_coauthors = all_coauthors.reset_index().drop('index', axis=1)
            
            a_entries = all_coauthors[(all_coauthors['author_id'].str.contains(a)) | (all_coauthors['orcid'].str.contains(a)) | (all_coauthors['google_scholar'].str.contains(a)) | (all_coauthors['crossref'].str.contains(a))].index.to_list()
            all_coauthors = all_coauthors.drop(labels = a_entries, axis=0)

            coauthor_counts = all_coauthors['author_id'].value_counts().sort_index().reset_index().drop('index', axis=1)
            
            all_coauthors_str = all_coauthors.copy(deep=True).astype(str)
            deduplicated_index = all_coauthors_str.drop_duplicates().index.to_list()
            all_coauthors = all_coauthors.loc[deduplicated_index]
            
            all_coauthors = all_coauthors.sort_values('author_id').reset_index().drop('index', axis=1)
            all_coauthors['frequency'] = coauthor_counts

            output[a] = all_coauthors

            if add_to_authors == True:
                self.authors.all[a].coauthors = all_coauthors

        return output

    def get_cofunders(self, format: bool = True, update_attrs: bool = True, ignore_case: bool = True, add_to_funders: bool = True):

        """
        Returns a dictionary of co-funders.

        Parameters
        ----------
        format : bool
            whether to format results, authors, funders, and affiliations data.
        update_attrs : bool
            whether to update funder attributes.
        ignore_case : bool
            whether to ignore the case of string data.
        add_to_funders : bool
            whether to store the dict of co-funders in the Review's Funders attribute.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.

        Returns
        -------
        output : dict
            a dictionary containing co-funders. 
                * Keys: funder IDs
                * Values: co-funders
        """

        if format == True:
            self.format()
        
        if update_attrs == True:
            self.update_funder_attrs(ignore_case=ignore_case)
        
        f_ids = self.funders.all.keys()
        output = {}

        cols = Funders().summary.columns.to_list()

        for f in f_ids:

            f_pubs = self.results.mask_entities(column = 'funder', query=f, ignore_case=ignore_case) # type: ignore
            
            all_cofunders = pd.DataFrame(columns=cols, dtype=object)
            all_cofunders['frequency'] = pd.Series(dtype=object)

            for i in f_pubs.index:

                work_cofunders = f_pubs.loc[i, 'funder']
                if type(work_cofunders) == Funders:
                    work_cofunders = work_cofunders.summary
                if (type(work_cofunders) == pd.DataFrame) or (type(work_cofunders) == pd.Series):
                    all_cofunders = pd.concat([all_cofunders, work_cofunders])
            
            all_cofunders = all_cofunders.reset_index().drop('index', axis=1)
            
            f_entries = all_cofunders[(all_cofunders['funder_id'].str.contains(f)) | (all_cofunders['uri'].str.contains(f)) | (all_cofunders['crossref_id'].str.contains(f))].index.to_list()
            all_cofunders = all_cofunders.drop(labels = f_entries, axis=0)

            cofunder_counts = all_cofunders['funder_id'].value_counts().sort_index().reset_index().drop('index', axis=1)
            
            all_cofunders_str = all_cofunders.copy(deep=True).astype(str)
            deduplicated_index = all_cofunders_str.drop_duplicates().index.to_list()
            all_cofunders = all_cofunders.loc[deduplicated_index]
            
            all_cofunders = all_cofunders.sort_values('funder_id').reset_index().drop('index', axis=1)
            all_cofunders['frequency'] = cofunder_counts

            output[f] = all_cofunders

            if add_to_funders == True:
                self.funders.all[f].cofunders = all_cofunders

        return output
 
    def remove_duplicates(self, drop_empty_rows=True, use_api=False):

        """
        Removes duplicate data entries from results, authors, funders, and affiliations datasets.

        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        use_api : bool
            whether to update data using CrossRef, Orcid, and other APIs.
        """

        orig_res_len = len(self.results)
        self.results.remove_duplicates(drop_empty_rows=drop_empty_rows, update_from_api=use_api) # type: ignore
        new_res_len = len(self.results)
        res_diff = new_res_len - orig_res_len

        orig_auths_len = len(self.authors.summary)
        self.authors.remove_duplicates(drop_empty_rows=drop_empty_rows, sync=True)
        new_auths_len = len(self.authors.summary)
        auths_diff = new_auths_len - orig_auths_len

        orig_funders_len = len(self.funders.summary)
        self.funders.remove_duplicates(drop_empty_rows=drop_empty_rows, sync=True)
        new_funders_len = len(self.funders.summary)
        funders_diff = new_funders_len - orig_funders_len

        orig_affils_len = len(self.affiliations.summary)
        self.affiliations.remove_duplicates(drop_empty_rows=drop_empty_rows, sync=True)
        new_affils_len = len(self.affiliations.summary)
        affils_diff = new_affils_len - orig_affils_len

        changes = {'results': res_diff,
                   'authors': auths_diff,
                   'funders': funders_diff,
                   'affiliations': affils_diff}

        self.activity_log.add_activity(type='data cleaning', activity='deduplication', location = ['results', 'authors', 'funders', 'affiliations'], changes_dict=changes)


    def format(self, update_entities = False, drop_duplicates = False, drop_empty_rows=True, verbose=False):

        """
        Parses and formats all datasets (i.e., results, authors, funders and affiliations).

        Parameters
        ----------
        update_attrs : bool
            whether to update entity attributes.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        verbose : bool
            whether to print formatting dialogue.
        """

        self.format_funders()
        self.format_citations(verbose=verbose)
        self.format_authors(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        self.format_affiliations()

        if update_entities == True:
            self.update_entity_attrs()
        
        if drop_empty_rows == True:

            orig_res_len = len(self.results)
            self.results.drop_empty_rows() # type: ignore
            new_res_len = len(self.results)
            res_diff = new_res_len - orig_res_len

            orig_auths_len = len(self.authors.summary)
            self.authors.drop_empty_rows() # type: ignore
            new_auths_len = len(self.authors.summary)
            auths_diff = new_auths_len - orig_auths_len

            changes = {'results': res_diff,
                   'authors': auths_diff}

            self.activity_log.add_activity(type='data cleaning', activity='removed empty rows', location=list(changes.keys()), changes_dict=changes)

        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

    def add_citations_to_results(self, update_formatting: bool = True, drop_duplicates = False, drop_empty_rows = True):
        
        """
        Formats all results entries' citations and adds them to the Review's Results attribute.
        
        Parameters
        ----------
        update_formatting : bool
            whether to format results, authors, funders, and affiliations data.
        drop_empty_rows : bool
            whether to remove duplicate rows. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        """

        self.results.add_citations_to_results(drop_duplicates = drop_duplicates, drop_empty_rows = drop_empty_rows) # type: ignore

        if drop_empty_rows == True:

            orig_res_len = len(self.results)
            self.results.drop_empty_rows() # type: ignore
            new_res_len = len(self.results)
            res_diff = new_res_len - orig_res_len

            orig_auths_len = len(self.authors.summary)
            self.authors.drop_empty_rows() # type: ignore
            new_auths_len = len(self.authors.summary)
            auths_diff = new_auths_len - orig_auths_len

            changes = {'results': res_diff,
                   'authors': auths_diff}

            self.activity_log.add_activity(type='data cleaning', activity='removed empty rows', location=list(changes.keys()), changes_dict=changes)
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        if update_formatting == True:
            self.format(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

    def update_from_orcid(self, update_formatting: bool = True, drop_duplicates = False, drop_empty_rows=True):

        """
        Updates Authors data using the Orcid API.

        Parameters
        ----------
        update_formatting : bool
            whether to format results, authors, funders, and affiliations data.
        drop_empty_rows : bool
            whether to remove duplicate rows. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        """

        orcid_len = len(self.authors.has_orcid())

        old_auths_len = len(self.authors.summary)
        self.authors.update_from_orcid(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        new_auths_len = len(self.authors.summary)
        len_diff = new_auths_len - old_auths_len

        changes = {'authors': {'orcid_updated': orcid_len, 'count': len_diff}}
        self.activity_log.add_activity(type='API retrieval', activity='updated authors from ORCID', location = ['authors'], changes_dict = changes)
        

        if update_formatting == True:
            self.format()
        
    def add_dataframe(self, dataframe: pd.DataFrame, drop_empty_rows = False, drop_duplicates = False, update_formatting: bool = True):

        """
        Merges inputted dataframe with Review's Results dataset.
        
        Parameters
        ----------
        dataframe : pandas.DataFrame
            dataframe to add to Results.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_work_ids : bool
            whether to update results entries' work ID's.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        
        Returns
        -------
        self : Review
            a Review object.
        """

        orig_len = len(self.results)
        self.results.add_dataframe(dataframe=dataframe, drop_empty_rows=drop_empty_rows, drop_duplicates=drop_duplicates) # type: ignore
        new_len = len(self.results)
        len_diff = new_len - orig_len

        changes = {'results': len_diff}
        self.activity_log.add_activity(type='data merge', activity='added dataframe to results', location=['results'], changes_dict=changes)

        if drop_empty_rows == True:
            
            orig_res_len = len(self.results)
            self.results.drop_empty_rows() # type: ignore
            new_res_len = len(self.results)
            res_diff = new_res_len - orig_res_len

            changes = {'results': res_diff}

            self.activity_log.add_activity(type='data cleaning', activity='removed empty rows', location=['results'], changes_dict=changes)
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        if update_formatting == True:
            self.format(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        return self

    def import_bibtex(self, file_path = 'request_input', drop_empty_rows = False, drop_duplicates = False, update_formatting: bool = False, update_entities = False):
        
        """
        Reads a Bibtex (.bib) bibliography file and adds its data to Review object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.
        """

        if file_path == 'request_input':
            file_path = input('File path: ')

        orig_len = len(self.results)
        self.results.import_bibtex(file_path=file_path)
        new_len = len(self.results)
        len_diff = new_len - orig_len

        changes = {'results': len_diff}
        self.activity_log.add_activity(type='data import', activity='imported .bib file to results', location=['results'], changes_dict=changes)
        
        self.properties.file_location = file_path
        self.properties.update_file_type()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        if update_formatting == True:
            self.format(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        if update_entities == True: 
            self.update_entity_attrs()
    
    def from_bibtex(file_path = 'request_input', drop_empty_rows = False, drop_duplicates = False, update_formatting: bool = False, update_entities = False):

        """
        Reads .bib file and returns a Review object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.

        Returns
        -------
        review : Review
            a Review object.
        """

        if file_path == 'request_input':
            file_path = input('File path: ')

        review = Review(file_location=file_path)
        review.import_bibtex(file_path=file_path, drop_empty_rows = drop_empty_rows, drop_duplicates = drop_duplicates, update_formatting=update_formatting, update_entities=update_entities)

        return review

    def import_excel(self, file_path = 'request_input', sheet_name = None, update_formatting: bool = True, update_entities = False, drop_empty_rows = False, drop_duplicates = False):
        
        """
        Reads an Excel (.xlsx) file and adds its data to the Review object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        sheet_name : str
            optional: name of Excel sheet to read.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.

        Returns
        -------
        self : Review
            a Review object.
        """

        if file_path == 'request_input':
            file_path = input('File path: ')

        orig_len = len(self.results)
        self.results.import_excel(file_path, sheet_name) # type: ignore
        new_len = len(self.results)
        len_diff = new_len - orig_len

        changes = {'results': len_diff}
        self.activity_log.add_activity(type='data import', activity='imported Excel file to results', location=['results'], changes_dict=changes)

        if drop_empty_rows == True:

            orig_res_len = len(self.results)
            self.results.drop_empty_rows() # type: ignore
            new_res_len = len(self.results)
            res_diff = new_res_len - orig_res_len

            orig_auths_len = len(self.authors.summary)
            self.authors.drop_empty_rows() # type: ignore
            new_auths_len = len(self.authors.summary)
            auths_diff = new_auths_len - orig_auths_len

            changes = {'results': res_diff,
                   'authors': auths_diff}

            self.activity_log.add_activity(type='data cleaning', activity='removed empty rows', location=list(changes.keys()), changes_dict=changes)
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        if update_formatting == True:
            self.format(update_entities=update_entities, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        self.properties.file_location = file_path
        self.properties.update_file_type()

        return self
    
    def from_excel(file_path = 'request_input', sheet_name = None, update_formatting: bool = True, update_entities = False, drop_empty_rows = False, drop_duplicates = False): # type: ignore
        
        """
        Reads an Excel (.xlsx) file and returns as a Review object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        sheet_name : str
            optional: name of Excel sheet to read.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_entities : bool
            whether to update entity attributes.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.

        Returns
        -------
        review : Review
            a Review object.
        """

        if file_path == 'request_input':
            file_path = input('File path: ')

        review = Review(file_location=file_path)
        review.results = Results.from_excel(file_path, sheet_name) # type: ignore

        if update_formatting == True:
            review.format(update_entities=update_entities, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        review.activity_log.add_activity(type='data import', activity='created Review from imported Excel file', location=['results', 'authors', 'funders', 'affiliations'])
        
        return review

    def import_csv(self, file_path = 'request_input', update_formatting: bool = True, update_entities = False, drop_empty_rows = False, drop_duplicates = False):
        
        """
        Reads a CSV (.csv) file and adds its data to the Review object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.

        Returns
        -------
        self : Review
            a Review object.
        """

        if file_path == 'request_input':
            file_path = input('File path: ')

        orig_len = len(self.results)
        self.results.import_csv(file_path) # type: ignore
        new_len = len(self.results)
        len_diff = new_len - orig_len

        changes = {'results': len_diff}
        self.activity_log.add_activity(type='data import', activity='imported CSV file to results', location=['results'], changes_dict=changes)

        if drop_empty_rows == True:

            orig_res_len = len(self.results)
            self.results.drop_empty_rows() # type: ignore
            new_res_len = len(self.results)
            res_diff =  new_res_len - orig_res_len

            orig_auths_len = len(self.authors.summary)
            self.authors.drop_empty_rows() # type: ignore
            new_auths_len = len(self.authors.summary)
            auths_diff = new_auths_len - orig_auths_len

            changes = {'results': res_diff,
                   'authors': auths_diff}

            self.activity_log.add_activity(type='data cleaning', activity='removed empty rows', location=list(changes.keys()), changes_dict=changes)
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        if update_formatting == True:
            self.format(update_entities=update_entities, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        self.properties.file_location = file_path
        self.properties.update_file_type()

        return self
    
    def from_csv(file_path = 'request_input', update_formatting: bool = True, update_entities = False, drop_empty_rows = False, drop_duplicates = False): # type: ignore

        """
        Reads a CSV (.csv) file and returns as a Review object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.

        Returns
        -------
        review : Review
            a Review object.
        """

        if file_path == 'request_input':
            file_path = input('File path: ')

        review = Review(file_location=file_path)
        review.results = Results.from_csv(file_path) # type: ignore

        if update_formatting == True:
            review.format(update_entities=update_entities, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        review.activity_log.add_activity(type='data import', activity='created Review from imported CSV file', location=['results', 'authors', 'funders', 'affiliations'])

        return review

    def import_json(self, file_path = 'request_input', update_formatting: bool = True):

        """
        Reads a JSON (.json) file and adds its data to the Review object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.

        Returns
        -------
        self : Review
            a Review object.
        """

        if file_path == 'request_input':
            file_path = input('File path: ')

        self.results.import_json(file_path) # type: ignore

        if update_formatting == True:
            self.format()
        
        self.properties.file_location = file_path
        self.properties.update_file_type()
        self.update_properties()

        return self
    
    def from_json(file_path = 'request_input'): # type: ignore

        """
        Reads a JSON (.json) file and returns as a Review object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.

        Returns
        -------
        review : Review
            a Review object.
        """

        if file_path == 'request_input':
            file_path = input('File path: ')

        review = Review(file_location=file_path)
        review.import_json(file_path = file_path) # type: ignore

        return review
    
    def import_file(self, file_path = 'request_input', sheet_name = None, update_formatting: bool = True, update_entities = False, drop_empty_rows = False, drop_duplicates = False):
        
        """
        Reads a file, determines its file type, and adds its data to the Review object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        sheet_name : str
            optional: name of an Excel sheet to read.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.

        Notes
        -----
        Can read:
            * .xlsx
            * .csv
            * .json
            * .bib
            * .yaml
            * .txt
            * .review
        """

        if file_path == 'request_input':
            file_path = input('File path: ')

        self.results.import_file(file_path, sheet_name) # type: ignore

        if update_formatting == True:
            self.format(update_entities=update_entities, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        if drop_empty_rows == True:
            self.results.drop_empty_rows() # type: ignore
            self.authors.drop_empty_rows() # type: ignore
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        self.properties.file_location = file_path
        self.properties.update_file_type()
        self.update_properties()

    def from_file(file_path = 'request_input', sheet_name = None, update_formatting: bool = True, update_entities = False, drop_empty_rows = False, drop_duplicates = False): # type: ignore
        
        """
        Reads a file, determines its file type, and returns its data as a Review object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        sheet_name : str
            optional: name of an Excel sheet to read.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.
        
        Returns
        -------
        review : Review
            a Review object.

        Notes
        -----
        Can read:
            * .xlsx
            * .csv
            * .json
            * .bib
            * .yaml
            * .txt
            * .review
        """

        if file_path == 'request_input':
            file_path = input('File path: ')

        review = Review(file_location=file_path)
        review.results = Results.from_file(file_path, sheet_name) # type: ignore
        review.format(update_entities=update_entities, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows) # type: ignore

        return review

    def import_jstor(self, file_path = 'request_input', drop_empty_rows = False, drop_duplicates = False, update_work_ids = True, format_citations=True, format_authors = True, format_funders = True, format_affiliations=True):
        
        """
        Reads a file outputted by JSTOR's Constellate portal and adds its data to the Review object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.

        Returns
        -------
        review : Review
            a Review object.    
        
        Notes
        -----
        Can read:
            * .csv
            * .json
        """

        if file_path == 'request_input':
            file_path = input('File path: ')

        old_len = len(self.results)
        self.results.import_jstor(file_path = file_path, drop_empty_rows=drop_empty_rows,drop_duplicates=drop_duplicates, update_work_ids=update_work_ids) # type: ignore
        new_len = len(self.results)

        len_diff = new_len - old_len
        changes = {'results': len_diff}
        self.activity_log.add_activity(type='data import', activity='imported JSTOR .json file to results', location=['results'], changes_dict=changes)

        if format_citations == True:
            self.format_citations()

        if format_authors == True:
            self.format_authors(drop_empty_rows=drop_empty_rows, drop_duplicates=drop_duplicates)
        
        if format_funders == True:
            self.format_funders()
        
        if format_affiliations == True:
            self.format_affiliations()

        self.properties.file_location = file_path
        self.properties.update_file_type()

    def from_jstor(file_path: str = 'request_input', drop_empty_rows = False, drop_duplicates = False, update_work_ids = True, format_citations=True, format_authors = True, format_funders = True, format_affiliations=True): # type: ignore

        """
        Reads a file outputted by JSTOR's Constellate portal and returns its data as a Review object.

        Parameters
        ----------
        file_path : str
            directory path of file to import. Defaults to requesting from user input.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.

        Notes
        -----
        Can read:
            * .csv
            * .json
        """

        if file_path == 'request_input':
            file_path = input('File path: ')
        
        review = Review(file_location=file_path)
        review.import_jstor(file_path=file_path, drop_empty_rows=drop_empty_rows, drop_duplicates=drop_duplicates, update_work_ids=update_work_ids, format_citations=format_citations, format_authors = format_authors, format_funders = format_funders, format_affiliations=format_affiliations)

        return review

    def search_field(self, field = 'request_input', any_kwds = 'request_input', all_kwds = None, not_kwds = None, case_sensitive = False, output = 'Results'):
        
        """
        Searches a given field in the Results dataset for a string.

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
        
        return self.results.search_field(field = field, any_kwds = any_kwds, all_kwds = all_kwds, not_kwds = not_kwds, case_sensitive = case_sensitive, output = output) # type: ignore

    def search(self, any_kwds = 'request_input', all_kwds = None, not_kwds = None, fields = 'all', case_sensitive = False):

        """
        Searches for a string throughout Review.

        Parameters
        ----------
        any_kwds : str or list
            one or more keywords to search for. Returns results where *any* matches are found. Defaults to requesting from user input.
        all_kwds : str or list
            one or more keywords to search for. Returns results where *all* matches are found. Defaults to None.
        not_kwds : str or list
            one or more keywords to search for. Returns results where *no* matches are found. Defaults to None.
        fields : str or list
            names of one or fields to search. Defaults to 'all'.
        case_sensitive : bool
            whether to pay attention to the case of string data. Defaults to False.

        Returns
        -------
        output : pandas.DataFrame
            search results.
        """

        combined_query = str(any_kwds)
        if all_kwds is not None:
            combined_query = combined_query + str(all_kwds)

        combined_query = combined_query.replace(']','').replace('[','').replace('{','').replace('}','')

        results_search = self.results.search(fields = fields, any_kwds = any_kwds, all_kwds = all_kwds, not_kwds = not_kwds, case_sensitive = case_sensitive) # type: ignore
        results_search = results_search.copy(deep=True).rename(columns={'work_id':'id', 'title': 'name/title'}) # type: ignore
        results_search['type'] = 'work'
        
        authors_search = self.authors.search(query = combined_query)
        authors_search = authors_search.copy(deep=True).rename(columns={'author_id':'id', 'full_name': 'name/title'})
        authors_search['type'] = 'author'

        funders_search = self.funders.search(query = combined_query)
        funders_search = funders_search.copy(deep=True).rename(columns={'funder_id':'id', 'name': 'name/title'})
        funders_search['type'] = 'funder'

        affils_search = self.affiliations.search(query=combined_query)
        affils_search = affils_search.copy(deep=True).rename(columns={'affiliation_id':'id', 'name': 'name/title'})
        affils_search['type'] = 'affiliation'

        output = pd.concat([results_search, authors_search, funders_search, affils_search]).reset_index().drop('index', axis=1)
        
        old_cols = output.columns.to_list()
        new_cols = ['id', 'type', 'name/title']
        remaining_cols = [c for c in old_cols if c not in new_cols]
        cols = new_cols + remaining_cols
        output = output[cols]

        return output

    def export_folder(self, folder_name = 'request_input', folder_address = 'request_input', export_str_as = 'txt', export_dict_as = 'json', export_pandas_as = 'csv', export_network_as = 'graphML'):
        
        """
        Exports Review's contents to a folder.
        
        Parameters
        ----------
        folder_name : str 
            name of folder to create. Defaults to requesting from user input.
        folder_address : str 
            directory address to create folder in. defaults to requesting for user input.
        export_str_as : str 
            file type for exporting string objects. Defaults to 'txt'.
        export_dict_as : str 
            file type for exporting dictionary objects. Defaults to 'json'.
        export_pandas_as : str 
            file type for exporting Pandas objects. Defaults to 'csv'.
        export_network_as : str 
            file type for exporting network objects. Defaults to 'graphML'.

        Options
        -------
        export_str_as:
            * txt or .txt (Default)
        export_dict_as:
            * json or .json (Default)
            * txt or .txt
        export_pandas_as:
            * csv or .csv (Default)
            * xlsx or .xlsx or Excel
        export_network_as:
            * graphML or .graphML (Default)
            * gml or .gml
            * leda
            * lgl
            * ncol
            * pajek
            * kumu (i.e., formatted .json)
        """
        
        if folder_name == 'request_input':
            folder_name = input('Folder name: ')
        
        if folder_name.endswith('_Review') == False:
            folder_name = folder_name + '_Review'
        
        art_class_to_folder(self, folder_name = folder_name, folder_address = folder_address, export_str_as = export_str_as, export_dict_as = export_dict_as, export_pandas_as = export_pandas_as, export_network_as = export_network_as)

    def export_txt(self, new_file = True, file_name: str = 'request_input', folder_address:str = 'request_input'):
        
        """
        Exports the Review to a pickled .txt file.
        
        Parameters
        ----------
        file_name : str
            name of file to create. Defaults to requesting from user input.
        file_address : str
            directory address to create file in. defaults to requesting for user input.
        """
        
        if new_file == True:
            
            if file_name == 'request_input':
                file_name = input('File name: ')
            
            if folder_address == 'request_input':
                folder_address = input('Folder address: ')
                file_address = folder_address + '/' + file_name
            
        if new_file == False:
            
            if folder_address == 'request_input':
                folder_address = input('File path: ')
            
            file_address = str(folder_address)
        
        if str(file_address).endswith('.txt') == False:
            file_address = str(file_address) + str('.txt')

        with open(file_address, 'wb') as f:
            pickle.dump(self, f) 
    
    def export_review(self, new_file = True, file_name: str = 'request_input', folder_address:str = 'request_input'):
        
        """
        Exports the Review to a pickled text file with a custom suffix (.review).
        
        Parameters
        ----------
        file_name : str
            name of file to create. Defaults to requesting from user input.
        file_address : str
            directory address to create file in. defaults to requesting for user input.
        """
        
        if new_file == True:
            
            if file_name == 'request_input':
                file_name = input('File name: ')
            
            if folder_address == 'request_input':
                folder_address = input('Folder address: ')
            
            file_address = folder_address + '/' + file_name
            
        if new_file == False:
            
            if folder_address == 'request_input':
                folder_address = input('File path: ')
            
            file_address = str(folder_address)
        
        if str(file_address).endswith('.review') == False:
            file_address = str(file_address) + str('.review')
        

        with open(file_address, 'wb') as f:
            pickle.dump(self, f)


    def save_as(self,
                filetype = 'review',
                file_name = 'request_input', 
                      folder_address: str = 'request_input', 
                      export_str_as: str = 'txt', 
                      export_dict_as: str = 'json', 
                      export_pandas_as: str = 'csv', 
                      export_network_as: str = 'graphML'):
        
        """
        Saves the Review to a new file with an inputted name at a specified location.
        
        Parameters
        ----------
        filetype : str
            type of file to save. Defaults to 'review'.
        file_name : str
            name of file to create. Defaults to requesting from user input.
        folder_address : str 
            directory address of folder to create file in. defaults to requesting from user input.
        export_str_as : str 
            file type for exporting string objects. Defaults to 'txt'.
        export_dict_as : str 
            file type for exporting dictionary objects. Defaults to 'json'.
        export_pandas_as : str 
            file type for exporting Pandas objects. Defaults to 'csv'.
        export_network_as : str 
            file type for exporting network objects. Defaults to 'graphML'.

        Options
        -------
        filetype:
            * txt or
        export_str_as:
            * txt or .txt (Default)
        export_dict_as:
            * json or .json (Default)
            * txt or .txt
        export_pandas_as:
            * csv or .csv (Default)
            * xlsx or .xlsx or Excel
        export_network_as:
            * graphML or .graphML (Default)
            * gml or .gml
            * leda
            * lgl
            * ncol
            * pajek
            * kumu (i.e., formatted .json)
        """

        if file_name == 'request_input':
            file_name = input('File name: ')
        
        if folder_address == 'request_input':
            folder_address = input('Folder path: ')

        if (filetype == 'review') or (filetype == '.review'):

            full_path = folder_address + '/' + file_name + '.review'
            path_obj = Path(full_path)
            
            if path_obj.exists() == True:
                warning_res = input(f'Warning: a file named {file_name}.review already exists in this location. Do you want to overwrite it? [Y]/N: ')

                if (warning_res.lower() == 'n') or (warning_res.lower() == 'no'):
                    return

            self.export_review(new_file=True, file_name=file_name, folder_address=folder_address)
        
        if (filetype == 'txt') or (filetype == '.txt'):

            full_path = folder_address + '/' + file_name + '.txt'
            path_obj = Path(full_path)
            
            if path_obj.exists() == True:
                warning_res = input(f'Warning: a file named {file_name}.txt already exists in this location. Do you want to overwrite it? [Y]/N: ')

                if (warning_res.lower() == 'n') or (warning_res.lower() == 'no'):
                    return
                
            self.export_txt(new_file=True, file_name=file_name, folder_address=folder_address)
        
        if (filetype == 'bib') or (filetype=='.bib'):

            full_path = folder_address + '/' + file_name + '.bib'
            path_obj = Path(full_path)
            
            if path_obj.exists() == True:
                warning_res = input(f'Warning: a file named {file_name}.txt already exists in this location. Do you want to overwrite it? [Y]/N: ')

                if (warning_res.lower() == 'n') or (warning_res.lower() == 'no'):
                    return
                
            self.export_bibtex(file_name=file_name, folder_path=folder_address)
        
        if (filetype == 'yaml') or (filetype=='.yaml'):

            full_path = folder_address + '/' + file_name + '.yaml'
            path_obj = Path(full_path)
            
            if path_obj.exists() == True:
                warning_res = input(f'Warning: a file named {file_name}.txt already exists in this location. Do you want to overwrite it? [Y]/N: ')

                if (warning_res.lower() == 'n') or (warning_res.lower() == 'no'):
                    return
                
            self.export_yaml(file_name=file_name, folder_path=folder_address)

        if filetype == 'folder':

            full_path = folder_address + '/' + file_name
            path_obj = Path(full_path)
            
            if path_obj.exists() == True:
                warning_res = input(f'Warning: a folder named {file_name} already exists in this location. Do you want to overwrite it? [Y]/N: ')

                if (warning_res.lower() == 'n') or (warning_res.lower() == 'no'):
                    return

            self.export_folder(folder_name=file_name, folder_address=folder_address, export_str_as=export_str_as, export_dict_as=export_dict_as, export_pandas_as=export_pandas_as, export_network_as=export_network_as)

    def save(self, 
             export_str_as: str = 'txt', 
                      export_dict_as: str = 'json', 
                      export_pandas_as: str = 'csv', 
                      export_network_as: str = 'graphML'):
        
        """
        Saves the Review to the filepath stored in its Properties attribute.
        
        Parameters
        ----------
        export_str_as : str 
            file type for exporting string objects. Defaults to 'txt'.
        export_dict_as : str 
            file type for exporting dictionary objects. Defaults to 'json'.
        export_pandas_as : str 
            file type for exporting Pandas objects. Defaults to 'csv'.
        export_network_as : str 
            file type for exporting network objects. Defaults to 'graphML'.

        Options
        -------
        export_str_as:
            * txt or .txt (Default)
        export_dict_as:
            * json or .json (Default)
            * txt or .txt
        export_pandas_as:
            * csv or .csv (Default)
            * xlsx or .xlsx or Excel
        export_network_as:
            * graphML or .graphML (Default)
            * gml or .gml
            * leda
            * lgl
            * ncol
            * pajek
            * kumu (i.e., formatted .json)
        """

        file_path = self.properties.file_location

        if (file_path is None) or (file_path == ''):

            file_name = input('File name: ')
            folder_address = input('Folder path: ')
            filetype = input('File type: ')

            self.save_as(filetype=filetype, file_name=file_name, folder_address=folder_address, export_str_as=export_str_as, export_dict_as=export_dict_as, export_pandas_as=export_pandas_as, export_network_as=export_network_as)
            return

        if (type(file_path) == str) or (type(file_path) == Path):

            file_type = Path(file_path).suffix

            if (file_type is not None) and (file_type !=''):

                if file_type == '.review':
                    self.export_review(new_file=False, file_name=None, folder_address=file_path)
                    return

                if file_type == '.txt':
                    self.export_txt(new_file=False, file_name=None, folder_address=file_path)
                    return
                
                if file_type == '.bib':

                    path_obj = Path(file_path)
                    file_name = path_obj.name
                    folder_path = str(path_obj.parent)

                    self.export_bibtex(file_name=file_name, folder_path=folder_path)
                    return
                
                if file_type == '.yaml':

                    path_obj = Path(file_path)
                    file_name = path_obj.name
                    folder_path = str(path_obj.parent)

                    self.export_yaml(file_name=file_name, folder_path=folder_path)
                    return




    def import_txt(self, file_path: str = 'request_input'):

        """
        Imports data from a pickled .txt file and adds to the Review object.

        Parameters
        ----------
        file_path : str
            directory path of .txt file to import.
        """

        if file_path == 'request_input':
            file_path = input('File address: ')
        
        with open(file_path, 'rb') as f:
            review = pickle.load(f)
        
        results = review.results.copy(deep=True)
        authors = review.authors
        funders = review.funders
        affils = review.affiliations

        self.results.add_dataframe(results)
        self.authors.merge(authors=authors)
        self.funders.merge(funders=funders)
        self.affiliations.merge(affiliations=affils)

        self.properties.file_location = file_path
        self.properties.update_file_type()


    def from_txt(file_path: str = 'request_input'): # type: ignore

        """
        Imports a Review from a pickled .txt file.

        Parameters
        ----------
        file_path : str
            directory path of .txt file to import.
        
        Returns
        -------
        review : Review
            a Review object.
        """

        if file_path == 'request_input':
            file_path = input('File address: ')
        
        with open(file_path, 'rb') as f:
            review = pickle.load(f)
        
        review.properties.file_location = file_path
        review.properties.update_file_type()

        return review

    def open(file_path: str = 'request_input'): # type: ignore

        """
        Imports a Review from a .review or .txt file.

        Parameters
        ----------
        file_path : str
            directory path of .txt file to import.
        
        Returns
        -------
        review : Review
            a Review object.
        """

        if file_path == 'request_input':
            file_path = input('File address: ')
        
        if (file_path.endswith('.txt')) or (file_path.endswith('.review')):
            with open(file_path, 'rb') as f:
                review = pickle.load(f)
                review.properties.file_location = file_path
                review.properties.update_file_type()
                return review

    

    def scrape_article(self, url = 'request_input'):
        
        """
        Scrapes article data from a given URL and adds to Results.

        Parameters
        ----------
        url : str
            url of article to scrape. Defaults to requesting from user input.
        
        Notes
        -----
        This function is capable of scraping:
            * Frontiers
            * ArXiv
            * Springer
            * Nature
            * IEEE
            * PubMed
            * PMC
            * SSRN
            * HeinOnline
            * MDPI
            * ACM
            * Project Muse
            * Proquest
            * JSTOR
            * Google Scholar
        """

        if url == 'request_input':
            url = input('URL: ')

        df = scrape_article(url)
        self.activity_log.add_activity(type='web scraping', activity=f'scraped {url} and added to results', location=['results'], url=url)
        self.results.add_dataframe(df) # type: ignore

    def scrape_doi(self, doi = 'request_input'):
        
        """
        Scrapes article data from a given DOI and adds to Results.

        Parameters
        ----------
        doi : str
            DOI of article to scrape. Defaults to requesting from user input.
        
        Notes
        -----
        This function is capable of scraping:
            * Frontiers
            * ArXiv
            * Springer
            * Nature
            * IEEE
            * PubMed
            * PMC
            * SSRN
            * HeinOnline
            * MDPI
            * ACM
            * Project Muse
            * Proquest
            * JSTOR
            * Google Scholar
        """

        if doi == 'request_input':
            doi = input('doi or URL: ')

        df = scrape_doi(doi)
        url = f'https://doi.org/{doi}'
        self.activity_log.add_activity(type='web scraping', activity=f'scraped {url} and added to results', location=['results'], url=url)
        self.results.add_dataframe(df) # type: ignore

    def scrape_google_scholar(self, url = 'request_input'):

        """
        Scrapes article data from a given Google Scholar page and adds to Results.

        Parameters
        ----------
        url : str
            url of Google Scholar page to scrape. Defaults to requesting from user input.
        """

        if url == 'request_input':
            url = input('URL: ')

        df = scrape_google_scholar(url)
        self.activity_log.add_activity(type='web scraping', activity='scraped Google Scholar and added to results', location=['results'], url=url)
        self.results.add_dataframe(df) # type: ignore
    
    def scrape_google_scholar_search(self, url = 'request_input'):

        """
        Scrapes article data from a given Google Scholar search and adds to Results. 

        Parameters
        ----------
        url : str
            url of Google Scholar search to scrape. Defaults to requesting from user input.
        """

        if url == 'request_input':
            url = input('URL: ')

        df = scrape_google_scholar_search(url)
        self.activity_log.add_activity(type='web scraping', activity='scraped Google Scholar search and added to results', location=['results'], url=url)
        self.results.add_dataframe(df) # type: ignore
    
    def scrape(self, url = 'request_input', add_to_results=True, drop_empty_rows = True, drop_duplicates = False):

        """
        Scrapes website data from a given URL. 

        Parameters
        ----------
        url : str
            url to scrape. Defaults to requesting from user input.
        add_to_results : bool
            whether to add scraped data to Results.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        """

        if url == 'request_input':
            url = input('URL: ')
        
        df = academic_scraper(url)

        if add_to_results == True:
            self.activity_log.add_activity(type='web scraping', activity='scraped URL and added to results', location=['results'], url=url)
            self.add_dataframe(df, drop_empty_rows=drop_empty_rows)
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)
        
        return df

    def search_crossref(self,
                bibliographic: str = None,  # type: ignore
                title: str = None, # type: ignore
                author: str = None, # type: ignore
                author_affiliation: str = None, # type: ignore
                editor: str = None, # type: ignore
                entry_type: str = None, # type: ignore
                published_date: str = None, # type: ignore
                doi: str = None, # type: ignore
                issn: str = None, # type: ignore
                publisher_name: str = None, # type: ignore
                funder_name = None, # type: ignore
                source: str = None, # type: ignore
                link: str = None, # type: ignore
                filter: dict = None, # type: ignore
                select: list = None, # type: ignore
                sample: int = None, # type: ignore
                limit: int = None, # type: ignore
                rate_limit: float = 0.05,
                timeout = 60,
                add_to_results = False
                ) -> pd.DataFrame:
        
        """
        Searches CrossRef API and returns the results as a Pandas DataFrame.

        Parameters
        ----------
        bibliographic : str
            a combined search. Searches for titles, abstracts, authors, publishers, dates etc. Defaults to None.
        title : str
            searches for titles containing string. Defaults to None.
        author : str
            searches for authors containing string. Defaults to None.
        author_affiliation : str
            searches for author affiliations containing string. Defaults to None.
        editor : str
            searches for editor names containing string. Defaults to None.
        entry_type : str
            searches for types of entries containing string. Defaults to None.
        published_date : str
            searches for matching publication dates. Defaults to None.
        doi : str
            searches for matching DOIs.
        issn : str
            searches for matching ISSNs.
        publisher_name : str
             searches for publisher names containing string. Defaults to None.
        funder_name : str
            searches for funder names containing string. Defaults to None.
        source : str
            searches for sources (e.g. journals, books) containing string. Defaults to None.
        link : str
            searches for entry links containing string. Defaults to None.
        sample : int
            optional: select which results to return.
        limit : int
            optional: set a limit to the number of results returned.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        add_to_results : bool
            whether to add search results to Review.
        filter : dict
        select : list
        
        Returns
        -------
        df : pandas.DataFrame
            results from CrossRef API search.
        """

        df = search_works(bibliographic = bibliographic,
                title = title,
                author = author,
                author_affiliation = author_affiliation,
                editor = editor,
                entry_type = entry_type,
                published_date = published_date,
                doi = doi,
                issn = issn,
                publisher_name = publisher_name,
                funder_name = funder_name,
                source = source,
                link = link,
                filter = filter,
                select = select,
                sample = sample,
                limit = limit,
                rate_limit = rate_limit,
                timeout = timeout)
        
        df['repository'] = 'crossref'

        if add_to_results == True:

            query = crossref_query_builder(bibliographic = bibliographic,
                title = title,
                author = author,
                author_affiliation = author_affiliation,
                editor = editor,
                entry_type = entry_type,
                published_date = published_date,
                doi = doi,
                issn = issn,
                publisher_name = publisher_name,
                funder_name = funder_name,
                source = source,
                link = link)

            self.activity_log.add_activity(type='API search', activity='searched Crossref and added to results', location=['results'], database='crossref', query=query)
            self.add_dataframe(dataframe=df) # type: ignore
            self.format()
        
        return df

    def search_scopus(self,
                    tile_abs_key_auth = None,
                    all_fields = None,
                    title = None,
                    year = None,
                    author = None,
                    author_identifier = None,
                    affiliation = None,
                    editor = None,
                    publisher = None,
                    funder = None,
                    abstract = None,
                    keywords = None,
                    doctype = None,
                    doi = None,
                    issn = None,
                    isbn = None,
                    pubmed_id = None,
                    source_title = None,
                    volume = None,
                    page = None,
                    issue = None,
                    language = None,
                    link = None,
                    references = None,
                    default_operator = 'AND',
                    refresh=False, 
                    view=None, 
                    verbose=False, 
                    download=True, 
                    integrity_fields=None, 
                    integrity_action='raise', 
                    subscriber=False,
                    add_to_results = False,
                    drop_empty_rows = False,
                    drop_duplicates = False,
                    format=False):
        
        """
        Searches Scopus API and returns the results as a Pandas DataFrame.

        Parameters
        ----------
        tile_abs_key_auth : str
            a combined search. Searches for titles, abstracts, keywords, and author names. Defaults to None.
        all_fields : str
            searches all fields. Defaults to None.
        title : str
            searches for titles containing string. Defaults to None.
        year : str
            searches for matching publication years. Defaults to None.
        author : str
            searches for authors containing string. Defaults to None.
        author_identifier : str
            searches for Scopus author IDs. Defaults to None.
        affiliation : str
            searches for author affiliations containing string. Defaults to None.
        editor : str
            searches for editor names containing string. Defaults to None.
        publisher : str
            searches for publisher names containing string. Defaults to None.
        funder : str
            searches for funder names containing string. Defaults to None.
        abstract : str
            searches for abstracts containing string. Defaults to None.
        keywords : str
            searches for matching keywords. Defaults to None.
        doctype : str
            searches for types of entries containing string. Defaults to None.
        doi : str
            searches for matching DOIs. Defaults to None.
        issn : str
            searches for matching ISSNs. Defaults to None.
        isbn : str
            searches for matching ISBNs. Defaults to None.
        pubmed_id : str
            searches for matching PubMed IDs (PMIDs). Defaults to None.
        source_title : str
            searches for source titles (e.g. journals, books) containing string. Defaults to None.
        volume : str
            searches for journal entries with matching volume numbers. Defaults to None.
        page : str
            searches for entries with matching page numbers. Defaults to None.
        issue : str
            searches for journal entries with matching issue numbers. Defaults to None.
        language : str
            searches for entries by language Defaults to None.
        link : str
            searches for entry links containing string. Defaults to None.
        references : str
            searches for entries with citations that contain matching strings. Defaults to None.
        default_operator : str
            the default Boolean operator to build the search. Defaults to 'AND'
        add_to_results : bool
            whether to add search results to Review.
        drop_duplicates : bool
            whether to remove duplicated rows when adding to results.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data when adding to results.
        format : bool
            whether to format results, authors, funders, and affiliations data when adding to results.
        refresh : bool 
        view : bool 
        verbose : bool 
        download : bool 
        integrity_fields : None
        integrity_action : str
        subscriber : bool
        
        Returns
        -------
        df : pandas.DataFrame
            results from Scopus API search.
        
        Options
        -------
        Options for default_operator:
            * 'AND'
            * 'AND NOT'
            * 'NOT'
            * 'OR'
        """

        df = search_scopus(tile_abs_key_auth = tile_abs_key_auth,
                            all_fields = all_fields,
                            title = title,
                            year = year,
                            author = author,
                            author_identifier = author_identifier,
                            affiliation = affiliation,
                            editor = editor,
                            publisher = publisher,
                            funder = funder,
                            abstract = abstract,
                            keywords = keywords,
                            doctype = doctype,
                            doi = doi,
                            issn = issn,
                            isbn = isbn,
                            pubmed_id = pubmed_id,
                            source_title = source_title,
                            volume = volume,
                            page = page,
                            issue = issue,
                            language = language,
                            link = link,
                            references = references,
                            default_operator = default_operator,
                           refresh=refresh, 
                           view=view, 
                           verbose=verbose, 
                           download=download, 
                           integrity_fields=integrity_fields, 
                           integrity_action=integrity_action,
                           subscriber=subscriber)
        
        

        for c in df.columns:
                if c not in self.results.columns:
                    df = df.drop(c, axis=1)
        
        df['repository'] = 'scopus'

        if add_to_results == True:

            query = scopus_query_builder(tile_abs_key_auth = tile_abs_key_auth,
                            all_fields = all_fields,
                            title = title,
                            year = year,
                            author = author,
                            author_identifier = author_identifier,
                            affiliation = affiliation,
                            editor = editor,
                            publisher = publisher,
                            funder = funder,
                            abstract = abstract,
                            keywords = keywords,
                            doctype = doctype,
                            doi = doi,
                            issn = issn,
                            isbn = isbn,
                            pubmed_id = pubmed_id,
                            source_title = source_title,
                            volume = volume,
                            page = page,
                            issue = issue,
                            language = language,
                            link = link,
                            references = references,
                            default_operator = default_operator)

            self.activity_log.add_activity(type='API search', activity='searched Scopus and added to results', location=['results'], database='scopus', query=query)
            self.add_dataframe(dataframe=df, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows, update_formatting=format) # type: ignore

        return df

    # def search_wos(self,
    #                all_fields = None,
    #         title = None,
    #         year = None,
    #         author = None,
    #         author_identifier = None,
    #         affiliation = None,
    #         doctype = None,
    #         doi = None,
    #         issn = None,
    #         isbn = None,
    #         pubmed_id = None,
    #         source_title = None,
    #         volume = None,
    #         page = None,
    #         issue = None,
    #         topics = None,
    #         default_operator = 'AND',
    #        database: str = 'WOK',
    #        limit: int = 10,
    #        page_limit: int = 1,
    #        sort_field: str = 'RS+D',
    #        modified_time_span = None,
    #        tc_modified_time_span = None,
    #        detail = None, 
    #        add_to_results = False,
    #        drop_duplicates = False,
    #        drop_empty_rows = False
    #        ):
        
    #     df = search_wos(
    #         all_fields = all_fields,
    #         title = title,
    #         year = year,
    #         author = author,
    #         author_identifier = author_identifier,
    #         affiliation = affiliation,
    #         doctype = doctype,
    #         doi = doi,
    #         issn = issn,
    #         isbn = isbn,
    #         pubmed_id = pubmed_id,
    #         source_title = source_title,
    #         volume = volume,
    #         page = page,
    #         issue = issue,
    #         topics = topics,
    #         default_operator = default_operator,
    #        database = database,
    #        limit = limit,
    #        page_limit = page_limit,
    #        sort_field = sort_field,
    #        modified_time_span = modified_time_span,
    #        tc_modified_time_span = tc_modified_time_span,
    #        detail = detail
    #        )
        
    #     for c in df.columns:
    #             if c not in self.results.columns:
    #                 df = df.drop(c, axis=1)

    #     if add_to_results == True:
            
    #         query = wos_query_builder(all_fields = all_fields,
    #                                     title = title,
    #                                     year = year,
    #                                     author = author,
    #                                     author_identifier = author_identifier,
    #                                     affiliation = affiliation,
    #                                     doctype = doctype,
    #                                     doi = doi,
    #                                     issn = issn,
    #                                     isbn = isbn,
    #                                     pubmed_id = pubmed_id,
    #                                     source_title = source_title,
    #                                     volume = volume,
    #                                     page = page,
    #                                     issue = issue,
    #                                     topics = topics,
    #                                     default_operator = default_operator)

    #         self.activity_log.add_activity(type='API search', activity='searched World of Science and added to results', location=['results'], database=database, query=query)
    #         self.results.add_dataframe(dataframe=df, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows) # type: ignore


    #     return df

    def lookup_doi(self, doi = 'request_input', timeout = 60):

        """
        Looks up DOI using the CrossRef API.

        Parameters
        ----------
        doi : str
            DOI to look up. Defaults to requesting from user input.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.

        Returns
        -------
        df : pandas.DataFrame
            results from DOI lookup on CrossRef API.
        """

        return lookup_doi(doi=doi, timeout=timeout)
    
    def lookup_scopus(self, 
                      uid = 'request_input',
                      refresh = False,
                      view = 'META',
                      id_type=None,
                      add_to_results=False,
                      drop_duplicates = False,
                      drop_empty_rows = False
                      ):

        """
        Looks up publication using the Scopus API.

        Parameters
        ----------
        uid : str
            Scopus ID, DOI, ISBN, ISSN, or Pubmed ID (PMID) to look up. Defaults to requesting from user input.
        refresh : bool
            whether to refresh the Scopus session.
        view : str
            sets the amount of detail returned. Defaults to 'META'.
        add_to_results : bool
            whether to add results to Review.
        drop_duplicates : bool
            whether to remove duplicated rows when adding to results.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data when adding to results.
        id_type : None


        Returns
        -------
        df : pandas.DataFrame
            results from publication lookup on Scopus API.
        """

        if uid == 'request_input':
            uid = input('ID: ')
        
        df = lookup_scopus(uid = uid, 
                            refresh=refresh,
                            view=view,
                            id_type=id_type
                            )
        
        for c in df.columns:
                if c not in self.results.columns:
                    df = df.drop(c, axis=1)
        
        df['repository'] = 'scopus'

        if add_to_results == True:
            self.results.add_dataframe(dataframe=df, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows) # type: ignore

        return df

    def add_doi(self, doi = 'request_input', timeout = 60, update_formatting: bool = True, update_entities = False, drop_empty_rows = False, drop_duplicates = False):
        
        """
        Looks up DOI using the CrossRef API and adds to Review's results dataset.

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
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.

        Returns
        -------
        self : Review
            a Review object.
        """

        old_len = len(self.results)
        self.results.add_doi(doi=doi, timeout=timeout) # type: ignore
        new_len = len(self.results)
        
        len_diff = new_len - old_len
        change = {'results': len_diff}
        self.activity_log.add_activity(type='API search', activity='searched Crossref for DOI entry and added to results', location=['results'], database='crossref', changes_dict=change)
        

        if update_formatting == True:
            self.format(update_entities=update_entities, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        if drop_empty_rows == True:

            orig_res_len = len(self.results)
            self.results.drop_empty_rows() # type: ignore
            new_res_len = len(self.results)
            res_diff =  new_res_len - orig_res_len

            orig_auths_len = len(self.authors.summary)
            self.authors.drop_empty_rows() # type: ignore
            new_auths_len = len(self.authors.summary)
            auths_diff = new_auths_len - orig_auths_len

            drop_changes = {'results': res_diff,
                   'authors': auths_diff}

            self.activity_log.add_activity(type='data cleaning', activity='removed empty rows', location=list(drop_changes.keys()), changes_dict=drop_changes)
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        return self

    def from_doi(doi: str = 'request_input', timeout = 60, update_formatting: bool = True, update_entities = False, drop_empty_rows = False, drop_duplicates = False): # type: ignore

        """
        Looks up DOI using the CrossRef API and returns as a Review object.

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
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.

        Returns
        -------
        review : Review
            a Review object.
        """

        review = Review()
        review.add_doi(doi = doi, timeout = timeout, update_formatting = update_formatting, update_entities=update_entities, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        return review

    def lookup_dois(self, dois_list: list = [], rate_limit: float = 0.05, timeout = 60):

        """
        Looks up a list of DOIs using the CrossRef API. Returns a Pandas DataFrame.

        Parameters
        ----------
        dois_list : list
            list of DOIs to look up. Defaults to an empty list.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.

        Returns
        -------
        result : pandas.DataFrame
            result of DOI lookups.
        """

        return lookup_dois(dois_list=dois_list, rate_limit=rate_limit, timeout=timeout)
    
    def add_dois(self, dois_list: list = [], rate_limit: float = 0.05, timeout = 60, update_formatting: bool = True, update_entities = False, drop_empty_rows = False, drop_duplicates = False):
        
        """
        Looks up a list of DOIs using the CrossRef API and adds to Review's results dataset.

        Parameters
        ----------
        dois_list : list
            list of DOIs to look up. Defaults to an empty list.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.

        Returns
        -------
        self : Review
            a Review object.
        """

        old_len = len(self.results)
        self.results.add_dois(dois_list=dois_list, rate_limit=rate_limit, timeout=timeout) # type: ignore
        new_len = len(self.results)

        len_diff = new_len - old_len
        change = {'results': len_diff}
        self.activity_log.add_activity(type='API search', activity='searched Crossref for list of DOI entries and added to results', location=['results'], database='crossref', changes_dict=change)
        

        if update_formatting == True:
            self.format(update_entities=update_entities, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        if drop_empty_rows == True:

            orig_res_len = len(self.results)
            self.results.drop_empty_rows() # type: ignore
            new_res_len = len(self.results)
            res_diff =  new_res_len - orig_res_len

            orig_auths_len = len(self.authors.summary)
            self.authors.drop_empty_rows() # type: ignore
            new_auths_len = len(self.authors.summary)
            auths_diff = new_auths_len - orig_auths_len

            drop_changes = {'results': res_diff,
                   'authors': auths_diff}

            self.activity_log.add_activity(type='data cleaning', activity='removed empty rows', location=list(drop_changes.keys()), changes_dict=drop_changes)
        
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        return self
    
    def from_dois(dois_list: list = [], rate_limit: float = 0.05, timeout = 60, update_formatting: bool = True, update_entities = False, drop_empty_rows = False, drop_duplicates = False): # type: ignore

        """
        Looks up a list of DOIs using the CrossRef API and returns as a Review object.

        Parameters
        ----------
        dois_list : list
            list of DOIs to look up. Defaults to an empty list.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.

        Returns
        -------
        review : Review
            a Review object.
        """

        review = Review()
        review.add_dois(dois_list = dois_list, rate_limit=rate_limit, timeout = timeout, update_formatting = update_formatting, update_entities=update_entities, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        return review

    def update_from_dois(self, timeout: int = 60, update_formatting: bool = True, update_entities = False, drop_empty_rows = False, drop_duplicates = False):
        
        """
        Updates results entries that have DOIs associated using the CrossRef API.

        Parameters
        ----------
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_formatting : bool
            whether to format author, funder, affiliations, and citations data.
        update_entities : bool
            whether to update entity attributes.
        
        Returns
        -------
        self : Review
            a Review object.
        """

        has_doi = len(self.results.has('doi')) # type: ignore
        self.results.update_from_dois(timeout=timeout) # type: ignore

        changes = {'results': has_doi}
        self.activity_log.add_activity(type='API retrieval', activity='updated results data from Crossref using DOIs', location = ['results'], changes_dict = changes)
        

        if update_formatting == True:
            self.format(update_entities=update_entities, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        if drop_empty_rows == True:

            orig_res_len = len(self.results)
            self.results.drop_empty_rows() # type: ignore
            new_res_len = len(self.results)
            res_diff =  new_res_len - orig_res_len

            orig_auths_len = len(self.authors.summary)
            self.authors.drop_empty_rows() # type: ignore
            new_auths_len = len(self.authors.summary)
            auths_diff = new_auths_len - orig_auths_len

            drop_changes = {'results': res_diff,
                   'authors': auths_diff}

            self.activity_log.add_activity(type='data cleaning', activity='removed empty rows', location=list(drop_changes.keys()), changes_dict=drop_changes)
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        return self

    def sync_apis(self, timeout: int = 60, update_entities = False, drop_empty_rows = False, drop_duplicates = False):

        """
        Updates data using all APIs:
        * CrossRef (for DOI)
        * Orcid

        Parameters
        ----------
        timeout : int
            how long in seconds to wait for results before raising an error. Defaults to 60 seconds.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        update_entities : bool
            whether to update entity attributes.
        
        Returns
        -------
        self : Review
            a Review object.
        """

        self.update_from_dois(timeout=timeout)
        self.update_from_orcid()
        self.format(update_entities=update_entities, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        return self

    def lookup_journal(self, issn = 'request_input', timeout = 60):

        """
        Looks up a journal by its ISSN using the CrossRef API. Returns a Pandas DataFrame.

        Parameters
        ----------
        issn : str
            ISSN to look up. Defaults to requesting from user input.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.

        Returns
        -------
        result : pandas.DataFrame
            journal records.
        """

        return lookup_journal(issn = issn, timeout = timeout)
    
    def lookup_journals(self, issns_list: list = [], rate_limit: float = 0.05, timeout: int = 60):

        """
        Looks up a list of journal ISSNs using the CrossRef API. Returns a Pandas DataFrame.

        Parameters
        ----------
        issns_list : str
            list of ISSNs to look up. Defaults to an empty list.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.

        Returns
        -------
        result : pandas.DataFrame
            journal records.
        """

        return lookup_journals(issns_list = issns_list, rate_limit = rate_limit, timeout = timeout)
    
    def search_journals(self, *args, limit: int = None, rate_limit: float = 0.05, timeout = 60): # type: ignore

        """
        Searches CrossRef API for journal records and returns the results as a Pandas DataFrame.

        Parameters
        ----------
        *args
            search fields.
        limit : int
            optional: set a limit to the number of results returned.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        
        Returns
        -------
        df : pandas.DataFrame
            results from CrossRef API search.
        """

        return search_journals(*args, limit = limit, rate_limit=rate_limit, timeout = timeout)
    
    def get_journal_entries(self,
                        issn = 'request_input',
                        filter: dict = None, # type: ignore
                        select: list = None, # type: ignore
                        sample: int = None, # type: ignore
                        limit: int = None, # type: ignore
                        rate_limit: float = 0.05,
                        timeout = 60):
        
        """
        Looks up a journal using the CrossRef API and returns associated entries as a Pandas DataFrame.

        Parameters
        ----------
        issn : str
            ISSN to look up. Defaults to requesting from user input.
        sample : int
            optional: select which results to return.
        limit : int
            optional: set a limit to the number of results returned.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        add_to_results : bool
            whether to add results to Review.
        filter : dict
        select : list

        Returns
        -------
        result : pandas.DataFrame
            journal entry records.
        """

        return get_journal_entries(issn = issn, filter = filter, select = select, sample = sample, limit = limit, rate_limit = rate_limit, timeout = timeout)
    
    def search_journal_entries(
                        self,
                        issn = 'request_input',
                        bibliographic: str = None, # type: ignore
                        title: str = None, # type: ignore
                        author: str = None, # type: ignore
                        author_affiliation: str = None, # type: ignore
                        editor: str = None, # type: ignore
                        entry_type: str = None, # type: ignore
                        published_date: str = None, # type: ignore
                        doi: str = None, # type: ignore
                        publisher_name: str = None, # type: ignore
                        funder_name: str = None, # type: ignore
                        source: str = None, # type: ignore
                        link: str = None, # type: ignore
                        filter: dict = None, # type: ignore
                        select: list = None, # type: ignore
                        sample: int = None, # type: ignore
                        limit: int = None, # type: ignore
                        rate_limit: float = 0.05,
                        timeout: int = 60,
                        add_to_results: bool = False) -> pd.DataFrame:
            
            """
            Searches for journal entries and articles associated with an ISSN using the CrossRef API.

            Parameters
            ----------
            issn : str
                ISSN to look up. Defaults to requesting from user input.
            bibliographic : str
                a combined search. Searches for titles, abstracts, authors, publishers, dates etc. Defaults to None.
            title : str
                searches for titles containing string. Defaults to None.
            author : str
                searches for authors containing string. Defaults to None.
            author_affiliation : str
                searches for author affiliations containing string. Defaults to None.
            editor : str
                searches for editor names containing string. Defaults to None.
            entry_type : str
                searches for types of entries containing string. Defaults to None.
            published_date : str
                searches for matching publication dates. Defaults to None.
            doi : str
                searches for matching DOIs.
            issn : str
                searches for matching ISSNs.
            publisher_name : str
                searches for publisher names containing string. Defaults to None.
            funder_name : str
                searches for funder names containing string. Defaults to None.
            source : str
                searches for sources (e.g. journals, books) containing string. Defaults to None.
            link : str
                searches for entry links containing string. Defaults to None.
            sample : int
                optional: select which results to return.
            limit : int
                optional: set a limit to the number of results returned.
            rate_limit : float
                time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
            timeout : int
                maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
            add_to_results : bool
                whether to add search results to Review.
            filter : dict
            select : list
            
            Returns
            -------
            df : pandas.DataFrame
                results from CrossRef API search.
            """

            df = search_journal_entries(issn = issn,
                                          bibliographic = bibliographic,
                                          title=title,
                                          author=author,
                                          author_affiliation=author_affiliation,
                                          editor=editor,
                                          entry_type=entry_type,
                                          published_date = published_date,
                                          doi = doi,
                                          publisher_name = publisher_name,
                                          funder_name = funder_name,
                                          source = source,
                                          link=link,
                                          filter=filter,
                                          select = select,
                                          sample=sample,
                                          limit=limit,
                                          rate_limit=rate_limit,
                                          timeout=timeout
                                          )
            
            if add_to_results == True:
                
                self.activity_log.add_activity(type='API search', activity='searched Crossref for journal publications using ISSN and added to results', location=['results'], query=issn)
        

                self.add_dataframe(dataframe=df) # type: ignore
                self.format()
        
            return df
    
    def lookup_funder(self, funder_id = 'request_input', timeout = 60):

        """
        Looks up a funder using the CrossRef API. Returns a Pandas DataFrame.

        Parameters
        ----------
        funder_id : str
            CrossRef Funder ID to look up. Defaults to requesting from user input.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.

        Returns
        -------
        result : pandas.DataFrame
            funder records.
        """

        return lookup_funder(funder_id = funder_id, timeout = timeout)
    
    def lookup_funders(self, funder_ids: list = [], rate_limit: float = 0.05, timeout = 60):

        """
        Looks up a list of funders using the CrossRef API. Returns a Pandas DataFrame.

        Parameters
        ----------
        funder_ids : list
            list of CrossRef Funder IDs to look up. Defaults to an empty list.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.

        Returns
        -------
        result : pandas.DataFrame
            funder records.
        """

        return lookup_funders(funder_ids=funder_ids, rate_limit=rate_limit, timeout = timeout)
    
    def search_funders(self, *args, limit: int = None, rate_limit: float = 0.05, timeout = 60): # type: ignore

        """
        Searches CrossRef API for funder records and returns the results as a Pandas DataFrame.

        Parameters
        ----------
        *args
            search fields.
        limit : int
            optional: set a limit to the number of results returned.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        
        Returns
        -------
        df : pandas.DataFrame
            results from CrossRef API search.
        """

        return search_funders(*args, limit=limit, rate_limit=rate_limit, timeout=timeout)
    
    def get_funder_works(self,
                        funder_id = 'request_input',
                        filter: dict = None, # type: ignore
                        select: list = None, # type: ignore
                        sample: int = None, # type: ignore
                        limit: int = None, # type: ignore
                        rate_limit: float = 0.05,
                        timeout: int = 60,
                        add_to_results: bool = False):
        
        """
        Looks up a funder using the CrossRef API and returns associated publications as a Pandas DataFrame.

        Parameters
        ----------
        funder_id : str
            CrossRef Funder ID to look up. Defaults to requesting from user input.
        sample : int
            optional: select which results to return.
        limit : int
            optional: set a limit to the number of results returned.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        add_to_results : bool
            whether to add results to Review.
        filter : dict
        select : list

        Returns
        -------
        result : pandas.DataFrame
            publication records.
        """

        df = get_funder_works(funder_id=funder_id, filter=filter, select=select, sample=sample, limit=limit, rate_limit=rate_limit, timeout=timeout)

        if add_to_results == True:
                self.results.add_dataframe(dataframe=df) # type: ignore
                self.format_authors()
        
        return df
    
    def search_funder_works(self,
                        funder_id = 'request_input',
                        bibliographic: str = None, # type: ignore
                        title: str = None, # type: ignore
                        author: str = None, # type: ignore
                        author_affiliation: str = None, # type: ignore
                        editor: str = None, # type: ignore
                        entry_type: str = None, # type: ignore
                        published_date: str = None, # type: ignore
                        doi: str = None, # type: ignore
                        publisher_name: str = None, # type: ignore
                        funder_name = None,
                        source: str = None, # type: ignore
                        link: str = None, # type: ignore
                        filter: dict = None, # type: ignore
                        select: list = None, # type: ignore
                        sample: int = None, # type: ignore
                        limit: int = None, # type: ignore
                        rate_limit: float = 0.05,
                        timeout: int = 60,
                        add_to_results: bool = False):

        """
        Searches for publications associated with a funder using the CrossRef API.

        Parameters
        ----------
        funder_id : str
            CrossRef Funder ID to look up. Defaults to requesting from user input.
        bibliographic : str
            a combined search. Searches for titles, abstracts, authors, publishers, dates etc. Defaults to None.
        title : str
            searches for titles containing string. Defaults to None.
        author : str
            searches for authors containing string. Defaults to None.
        author_affiliation : str
            searches for author affiliations containing string. Defaults to None.
        editor : str
            searches for editor names containing string. Defaults to None.
        entry_type : str
            searches for types of entries containing string. Defaults to None.
        published_date : str
            searches for matching publication dates. Defaults to None.
        doi : str
            searches for matching DOIs.
        issn : str
            searches for matching ISSNs.
        publisher_name : str
             searches for publisher names containing string. Defaults to None.
        funder_name : str
            searches for funder names containing string. Defaults to None.
        source : str
            searches for sources (e.g. journals, books) containing string. Defaults to None.
        link : str
            searches for entry links containing string. Defaults to None.
        sample : int
            optional: select which results to return.
        limit : int
            optional: set a limit to the number of results returned.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on CrossRef servers. Defaults to 0.05 seconds.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        add_to_results : bool
            whether to add search results to Review.
        filter : dict
        select : list
        
        Returns
        -------
        df : pandas.DataFrame
            results from CrossRef API search.
        """

        df = search_funder_works(
                                funder_id=funder_id,
                                bibliographic=bibliographic,
                                title=title,
                                author=author,
                                author_affiliation=author_affiliation,
                                editor=editor,
                                entry_type=entry_type,
                                published_date=published_date,
                                doi=doi,
                                publisher_name=publisher_name,
                                funder_name=funder_name,
                                source=source,
                                link=link,
                                filter=filter,
                                select=select,
                                sample=sample,
                                limit=limit,
                                rate_limit=rate_limit,
                                timeout=timeout)
            
        if add_to_results == True:
                
                self.activity_log.add_activity(type='API search', activity='searched Crossref for funder publications using ID and added to results', location=['results'], query=funder_id)
                self.add_dataframe(dataframe=df) # type: ignore
                self.format_authors()
        
        return df

    def search_orcid(self, query: str = 'request_input', add_to_authors: bool = True):

        """
        Searches for author records using the Orcid API.

        Parameters
        ----------
        query : str
            query to search. Allows for keywords and Boolean logic.
        add_to_authors : bool
            whether to add results to Review's authors dataset.
        
        Returns
        -------
        result : pandas.DataFrame
            search result.
        """

        if add_to_authors == True:
            self.activity_log.add_activity(type='API search', activity='searched ORCID for author and added to authors', location=['authors'], query=query)
                
        return self.authors.search_orcid(query=query, add_to_authors=add_to_authors)

    def api_search(self,
                    default_query = None,
                    all_fields = None,
                    title = None,
                    year = None,
                    author = None,
                    author_identifier = None,
                    entry_type: str = None, # type: ignore
                    affiliation = None,
                    editor = None,
                    publisher = None,
                    funder = None,
                    abstract = None,
                    keywords = None,
                    doi = None,
                    issn = None,
                    isbn = None,
                    pubmed_id = None,
                    source_title = None,
                    volume = None,
                    page = None,
                    issue = None,
                    language = None,
                    link = None,
                    references = None,
                    topics = None,
                    default_operator = 'AND',
                    limit_per_api: int = 20,
                    rate_limit: float = 0.05,
                    timeout = 60,
                    crossref = True,
                    scopus = True,
                    wos = False, 
                    add_to_results = False):
        
        """
        Searches multiple APIs and returns the results as a Pandas DataFrame. API options:
            * CrossRef
            * Scopus
            * Web of Science (WoS)

        Parameters
        ----------
        default_query : str
            a combined search. Searches for titles, abstracts, authors, publishers, dates etc. Defaults to None.
        all_fields : str
            Scopus only: searches all fields. Defaults to None.
        title : str
            searches for titles containing string. Defaults to None.
        year : str
            searches for matching publication years. Defaults to None.
        author : str
            searches for authors containing string. Defaults to None.
        author_identifier : str
            searches for API-specific author IDs (e.g. CrossRef, Scopus, WoS, Orcid). Defaults to None.
        entry_type : str
            searches for types of entries containing string. Defaults to None.
        affiliation : str
            searches for author affiliations containing string. Defaults to None.
        editor : str
            searches for editor names containing string. Defaults to None.
        publisher : str
             searches for publisher names containing string. Defaults to None.
        funder : str
            searches for funder names containing string. Defaults to None.
        abstract : str
            searches for abstracts containing string. Defaults to None.
        keywords : str
            searches for matching keywords. Defaults to None.
        doi : str
            searches for matching DOIs.
        issn : str
            searches for matching ISSNs.
        isbn : str
            searches for matching ISBNs. Defaults to None.
        pubmed_id : str
            searches for matching PubMed IDs (PMIDs). Defaults to None.
        source_title : str
            searches for sources with titles (e.g. journals, books) containing string. Defaults to None.
        volume : str
            searches for journal entries with matching volume numbers. Defaults to None.
        page : str
            searches for entries with matching page numbers. Defaults to None.
        issue : str
            searches for journal entries with matching issue numbers. Defaults to None.
        language : str
            searches for entries by language Defaults to None.
        link : str
            searches for entry links containing string. Defaults to None.
        references : str
            searches for entries with citations that contain matching strings. Defaults to None.
        topics : str
            searches for entries tagged with matching topic names and keywords. Defaults to None.
        default_operator : str
            the default Boolean operator to build searches. Defaults to 'AND'.
        limit_per_api : int
            sets limits for the number of results to return per API. Used to limit impact on API servers. Defaults to 20.
        rate_limit : float
            CrossRef only: time delay in seconds per result. Used to limit impact on API servers. Defaults to 0.05 seconds.
        timeout : int
            CrossRef only: maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        crossref : bool
            whether to search using the CrossRef API.
        scopus : bool
            whether to search using the Scopus API.
        wos : bool
            whether to search using the Web of Science (WoS) API.
        add_to_results : bool
            whether to add search results to Review.
        
        Returns
        -------
        df : pandas.DataFrame
            combined results from API searches.
        """

        df = api_search(default_query = default_query,
                    all_fields = all_fields,
                    title = title,
                    year = year,
                    author = author,
                    author_identifier = author_identifier,
                    entry_type = entry_type,
                    affiliation = affiliation,
                    editor = editor,
                    publisher = publisher,
                    funder = funder,
                    abstract = abstract,
                    keywords = keywords,
                    doi = doi,
                    issn = issn,
                    isbn = isbn,
                    pubmed_id = pubmed_id,
                    source_title = source_title,
                    volume = volume,
                    page = page,
                    issue = issue,
                    language = language,
                    link = link,
                    references = references,
                    topics = topics,
                    default_operator = default_operator,
                    limit_per_api = limit_per_api,
                    rate_limit = rate_limit,
                    timeout = timeout,
                    crossref = crossref,
                    scopus = scopus,
                    wos = wos)
        
        for c in df.columns:
            if c not in self.results.columns:
                    df = df.drop(c, axis=1)

        if add_to_results == True:

            apis = ''
            if crossref == True:
                apis = apis + 'Crossref, '
            if scopus == True:
                apis = apis + 'Scopus, '
            if wos == True:
                apis = apis + 'Web of Science'
            apis = apis.strip().strip(',').strip()

            self.activity_log.add_activity(type='API search', activity=f'searched {apis} for works and added to results', location=['results'], query=default_query)
                
            self.add_dataframe(dataframe=df) # type: ignore
            self.format()
        
        return df
        

    def crawl_stored_citations(self, max_depth=3, processing_limit=1000, format = True, update_from_doi = False):

        """
        Crawls outward from results' citations to identify new results *only using data already stored in the Review*.

        Parameters
        ----------
        max_depth : int
            the maximum crawl depth the crawler will reach before stopping. 
            Defaults to 3.
        processing_limit : int
            the maximum number of results the crawler will process before stopping. Defaults to 1000.
        format : bool
            whether to format results, authors, funders, and affiliations data. Defaults to True.
        update_from_doi : bool
            whether to use the CrossRef API to update entries that have DOIs associated.
        
        Notes
        -----
        Operational details:
            * crawl type: utilises a breadth-first crawl.
            * crawl depth: the number of iterations the crawler performs. For each iteration, all results from the previous iteration are loaded as seeds to crawl from.
            * operation: for each iteration, the crawler takes all citations in the current dataset and -- if they have not been crawled already -- adds any citations data they contain to the results.
        """

        iteration = 1
        processed_indexes = []
        original_len = len(self.results)

        while (iteration <= max_depth) and (len(processed_indexes) <= processing_limit):
            
            if (iteration > max_depth) or (len(processed_indexes) > processing_limit):
                break

            unformatted = self.results.lacks_formatted_citations() # type: ignore
            if len(unformatted) > 0:
                self.results.format_citations(add_work_ids = False, update_from_doi = update_from_doi) # type: ignore

            indexes = self.results.index
            to_process = pd.Series(list(set(indexes).difference(set(processed_indexes))), dtype=object).sort_values().to_list()

            if len(to_process) > 0:

                rows = self.results.loc[to_process]
                citations = rows['citations'].to_list()
                
                new_df = pd.DataFrame(dtype=object)

                process_iteration = 0

                for i in citations:
                    
                    if (type(i) == References) or (type(i) == Results) or (type(i) == pd.DataFrame):

                        res = i.copy(deep=True)
                        if len(res) > 0:
                            new_df = pd.concat([new_df, res])

                    process_iteration += 1

                    if (len(processed_indexes) + process_iteration) > processing_limit:
                        to_process = to_process[:process_iteration]
                        break

                new_df_asstr = new_df.copy(deep=True).astype(str)
                unique_indexes = new_df_asstr.drop_duplicates().index
                new_df = new_df.loc[unique_indexes]
                new_df = new_df.reset_index().drop('index', axis=1)
                self.results.add_dataframe(dataframe=new_df, update_work_ids = False, format_authors = False) # type: ignore

            processed_indexes = processed_indexes + to_process
            len_diff = len(self.results) - original_len
            print(f'Iteration {iteration} complete:\n    - Entries processed: {len(processed_indexes)}\n    - Results added: {len_diff}')
            
            iteration += 1
            
        final_len_diff = len(self.results) - original_len
        

        self.results.update_work_ids() # type: ignore
        df = self.results.drop_duplicates(subset=['work_id']).reset_index().drop('index', axis=1)
        self.results = Results.from_dataframe(df) # type: ignore


        self.activity_log.add_activity(type='citation crawl', activity=f'crawled stored citations and added to results', location=['results'])

        if format == True:
            self.format()
        
        print(f'Crawl complete:\n    - Entries processed: {len(processed_indexes)}\n    - Results added: {final_len_diff}\n')

        return self.results

    def crawl_citations(
                    self,
                    use_api: bool = True,
                    crawl_limit: int = 1000, 
                    depth_limit: int = 3,
                    be_polite: bool = True,
                    rate_limit: float = 0.05,
                    timeout: int = 60,
                    add_to_results = True,
                    drop_duplicates = False,
                    drop_empty_rows = True
                    ):
    
        """
        Crawls all Results entries' citations to find new results. Returns a Pandas DataFrame.
        
        The crawler iterates through queue of works; extracts their citations; runs checks to validate each reference;
        based on these, selects a source to retrieve data from: 
        * if has a valid doi: Crossref API.
        * if no valid doi: bespoke web scraping for specific academic websites.
        * else if a link is present: general web scraping.
        
        Parameters
        ----------
        use_api : bool
            whether to lookup entries and update their data using APIs. Required for the crawler to find new and add new data. Defaults to True.
        depth_limit : int
            the maximum crawl depth the crawler will reach before stopping. Defaults to 3.
        crawl_limit : int
            the maximum number of results the crawler will process before stopping. Defaults to 1000.
        be_polite : bool
            whether to respect websites' crawler permissions, as set out by their robots.txt files.
        rate_limit : float
            time delay in seconds per result. Used to limit impact on API servers. Defaults to 0.05 seconds.
        timeout : int
            how long in seconds to wait for results before raising an error. Defaults to 60 seconds.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.

        Returns
        -------
        result : pandas.DataFrame 
            the crawl results.
        """

        data = self.results
        
        self.format_citations()

        result = citation_crawler(
                    data = data,  # type: ignore
                    use_api = use_api,
                    crawl_limit = crawl_limit, 
                    depth_limit = depth_limit,
                    be_polite = be_polite,
                    rate_limit = rate_limit,
                    timeout = timeout
                    )
        
        if drop_duplicates == True:
            result = deduplicate(result)

        if add_to_results == True:

            df = result.drop(labels=0, axis=0).reset_index().drop('index', axis=1)
            self.activity_log.add_activity(type='citation crawl', activity=f'crawled citations using APIs and added to results', location=['results'])
            self.results.add_dataframe(df) # type: ignore
            self.format(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        return result

    def citations_dict(self) -> dict:
        
        """
        Returns a dictionary containing Results entries and their citations. 
            * Keys: work_id
            * Values: References object containing citations
        """

        output = {}

        for i in self.results.index:
            data = self.results.loc[i]
            work_id = data['work_id']
            citations = data['citations']

            if type(citations) == References:
                citations.update_work_ids()

            output[work_id] = citations

        return output

    def author_works_dict(self) -> dict:

        """
        Returns a dictionary containing Results entries and their associated authors. 
            * Keys: work_id
            * Values: authors data as a list or dictionary
        """

        output = {}

        for i in self.results.index:
            data = self.results.loc[i].copy(deep=True)
            work_id = data['work_id']
            auths = data['authors']

            if type(auths) == Authors:
                auths.update_author_ids()
                auths = auths.all

            output[work_id] = auths

        return output

    def author_affiliations_dict(self) -> dict:

        """
        Returns a dictionary containing Author entries and their associated affiliations. 
            * Keys: author_id
            * Values: affiliations data as a list or dictionary
        """

        output = {}

        auths = self.authors.summary.copy(deep=True)

        for i in auths.index:
            data = auths.loc[i]
            auth_id = data['author_id']
            affils = data['affiliations']

            if type(affils) == Affiliations:
                affils.update_ids()
                affils = affils.all

            output[auth_id] = affils

        return output

    def funder_works_dict(self) -> dict:

        """
        Returns a dictionary containing Results entries and their associated funders. 
            * Keys: work_id
            * Values: funders data as a list or dictionary
        """

        output = {}

        for i in self.results.index:
            data = self.results.loc[i].copy(deep=True)
            work_id = data['work_id']
            funders = data['funder']

            if type(funders) == Funders:
                funders.update_ids()
                funders = funders.summary

            output[work_id] = funders

        return output

    def coauthors_network(self, 
                                format: bool = True, 
                                update_attrs: bool = True,
                                drop_duplicates = False,
                                drop_empty_rows = True,
                                ignore_case: bool = True,
                                add_to_networks: bool = True
                                ) -> Network:

        """
        Generates a network representing co-authorship relationships.

        Parameters
        ----------
        format : bool
            whether to format results, authors, funders, and affiliations data.
        update_attrs : bool
            whether to update author attributes.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        ignore_case : bool
            whether to ignore the case of string data.
        add_to_networks : bool
            whether to store the network in the Review's Networks attribute.

        Returns
        -------
        network : Network
            a network representing co-authorship relationships.
        """

        if drop_empty_rows == True:
            self.authors.drop_empty_rows()
        
        if drop_duplicates == True:
            self.authors.remove_duplicates(drop_empty_rows=drop_empty_rows)

        co_auths = self.get_coauthors(format=format, update_attrs=update_attrs, ignore_case=ignore_case)

        g = generate_coauthors_network(coauthors_dict=co_auths)

        for v in g.vs:

            auth_id = v['name']

            if (auth_id is not None) and (auth_id != ''):

                auth_keys = list(self.authors.all.keys())

                if auth_id not in auth_keys:
                    keys_series = pd.Series(auth_keys)
                    keys_masked = keys_series[keys_series.str.contains(auth_id)]
                    if len(keys_masked) > 0:
                        auth_id = list(keys_masked)[0]

                        if auth_id in g.vs['name']:
                            v = g.vs.find(name = auth_id)
                        else:
                            v['name'] = auth_id
                    else:
                        continue

                auth_obj = self.authors.all[auth_id]
                if 'publications' in auth_obj.__dict__.keys():
                    pubs = auth_obj.publications
                else:
                    pubs = []
                if 'affiliations' in auth_obj.__dict__.keys():
                    affils = auth_obj.affiliations
                else:
                    affils = []
                details = auth_obj.details

                for c in details.columns:
                    v[c] = details.loc[0, c]
                
                v['publications'] = pubs
                v['affiliations'] = affils

        network = Network(graph = g)

        if add_to_networks == True:
            self.activity_log.add_activity(type='network generation', activity=f'generated coauthors network and added to networks', location=['networks'])
            self.networks.__dict__['coauthors'] = network
        
        return network

    def cofunders_network(self, 
                                format: bool = True, 
                                update_attrs: bool = True,
                                # drop_duplicates = False,
                                # drop_empty_rows = True,
                                ignore_case: bool = True,
                                add_to_networks: bool = True
                                ) -> Network:

        """
        Generates a network representing co-funder relationships.

        Parameters
        ----------
        format : bool
            whether to format results, authors, funders, and affiliations data.
        update_attrs : bool
            whether to update funder attributes.
        ignore_case : bool
            whether to ignore the case of string data.
        add_to_networks : bool
            whether to store the network in the Review's Networks attribute.

        Returns
        -------
        network : Network
            a network representing co-funder relationships.
        """

        co_funders = self.get_cofunders(format=format, update_attrs=update_attrs, ignore_case=ignore_case)

        g = generate_funders_network(funders_dict=co_funders)

        for v in g.vs:

            f_id = v['name']

            if (f_id is not None) and (f_id != ''):

                f_keys = list(self.funders.all.keys())
                
                if f_id not in f_keys:
                    keys_series = pd.Series(f_keys)
                    keys_masked = keys_series[keys_series.str.contains(f_id)]
                    if len(keys_masked) > 0:
                        f_id = list(keys_masked)[0]

                        if f_id in g.vs['name']:
                            v = g.vs.find(name = f_id)
                        else:
                            v['name'] = f_id
                    else:
                        continue

                funder_obj = self.funders.all[f_id]
                pubs = funder_obj.publications
                details = funder_obj.details

                for c in details.columns:
                        v[c] = details.loc[0, c]
                    
                v['publications'] = pubs


        network = Network(graph = g)

        if add_to_networks == True:

            self.activity_log.add_activity(type='network generation', activity=f'generated cofunders network and added to networks', location=['networks'])
            
            self.networks.__dict__['cofunders'] = network
        
        return network

    def citation_network(self, 
                                format: bool = True, 
                                update_attrs: bool = True,
                                drop_duplicates = False,
                                drop_empty_rows = False,
                                add_citations_to_results=True,
                                add_to_networks: bool = True
                                ) -> Network:
        
        """
        Generates a network representing citations between publications.

        Parameters
        ----------
        format : bool
            whether to format results, authors, funders, and affiliations data.
        update_attrs : bool
            whether to update author, funder, and affiliations attributes.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        ignore_case : bool
            whether to ignore the case of string data.
        add_to_networks : bool
            whether to store the network in the Review's Networks attribute.

        Returns
        -------
        network : Network
            a network representing citations.
        """

        if drop_empty_rows == True:
            self.results.drop_empty_rows() # type: ignore
        
        if drop_duplicates == True:
            self.results.remove_duplicates(drop_empty_rows=drop_empty_rows) # type: ignore

        if add_citations_to_results == True:
            self.add_citations_to_results(update_formatting = format, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        else:
            if format == True:
                self.format(update_entities=update_attrs, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        if update_attrs == True:
            self.update_entity_attrs()


        citations = self.citations_dict(strip_ids = True)

        graph = generate_citations_network(citations) # type: ignore

        for v in graph.vs:
            work_id = v['name']
            
            masked_index = self.results[self.results['work_id'].str.contains(work_id, regex=False)].index.to_list()
            if len(masked_index) > 0:
                work_index = masked_index[0]
                work_data = self.results.loc[work_index]

                for c in work_data.index:
                    v[c] = work_data[c]

        network = Network(graph)

        if add_to_networks == True:
            self.activity_log.add_activity(type='network generation', activity=f'generated citations network and added to networks', location=['networks'])
            self.networks.__dict__['citations'] = network
        
        return network

    def cocitation_network(self,
                           refresh_citations = False,
                           format: bool = True, 
                            update_attrs: bool = True,
                            drop_duplicates = False,
                            drop_empty_rows = False,
                            add_citations_to_results=True,
                            add_to_networks: bool = True):

        """
        Generates a network representing instances of co-citations between publications.

        Parameters
        ----------
        refresh_citations : bool
            whether to re-generate the underlying citations network. Defaults to False.
        format : bool
            whether to format results, authors, funders, and affiliations data.
        update_attrs : bool
            whether to update author, funder, and affiliations attributes.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        ignore_case : bool
            whether to ignore the case of string data.
        add_citations_to_results : bool
            whether to add Results entries' citations as Results entries. Defaults to True.
        add_to_networks : bool
            whether to store the network in the Review's Networks attribute.

        Returns
        -------
        network : Network
            a network representing co-citation relationships.
        """

        if refresh_citations == True:

            citation_network = self.citation_network(format=format,
                                                 update_attrs=update_attrs,
                                                 drop_duplicates=drop_duplicates,
                                                 drop_empty_rows=drop_empty_rows,
                                                add_citations_to_results=add_citations_to_results,
                                                 add_to_networks=add_to_networks)
        
        else:

            if 'citations' in self.networks.__dict__.keys():
                citation_network = self.networks['citations'] # type: ignore

            else:
                citation_network = self.citation_network(format=format,
                                                 update_attrs=update_attrs,
                                                 drop_duplicates=drop_duplicates,
                                                 drop_empty_rows=drop_empty_rows,
                                                add_citations_to_results=add_citations_to_results,
                                                 add_to_networks=add_to_networks)


        graph = generate_cocitation_network(citation_network)
        network = Network(graph)

        if add_to_networks == True:
            self.activity_log.add_activity(type='network generation', activity=f'generated co-citations network and added to networks', location=['networks'])
            self.networks.__dict__['cocitations'] = network
        
        return network

    def bibcoupling_network(self, 
                           refresh_citations = False,
                           format: bool = True, 
                            update_attrs: bool = True,
                            drop_duplicates = False,
                            drop_empty_rows = False,
                            add_citations_to_results=True,
                            add_to_networks: bool = True):
        
        """
        Generates a network representing bibliometric coupling between publications.

        Parameters
        ----------
        refresh_citations : bool
            whether to re-generate the underlying citations network. Defaults to False.
        format : bool
            whether to format results, authors, funders, and affiliations data.
        update_attrs : bool
            whether to update author, funder, and affiliations attributes.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        ignore_case : bool
            whether to ignore the case of string data.
        add_citations_to_results : bool
            whether to add Results entries' citations as Results entries. Defaults to True.
        add_to_networks : bool
            whether to store the network in the Review's Networks attribute.

        Returns
        -------
        network : Network
            a network representing bibliometric coupling.
        """

        if refresh_citations == True:

            citation_network = self.citation_network(format=format,
                                                 update_attrs=update_attrs,
                                                 drop_duplicates=drop_duplicates,
                                                 drop_empty_rows=drop_empty_rows,
                                                add_citations_to_results=add_citations_to_results,
                                                 add_to_networks=add_to_networks)
        
        else:

            if 'citations' in self.networks.__dict__.keys():
                citation_network = self.networks['citations'] # type: ignore

            else:
                citation_network = self.citation_network(format=format,
                                                 update_attrs=update_attrs,
                                                 drop_duplicates=drop_duplicates,
                                                 drop_empty_rows=drop_empty_rows,
                                                add_citations_to_results=add_citations_to_results,
                                                 add_to_networks=add_to_networks)
        
        graph = generate_bibcoupling_network(citation_network)
        network = Network(graph)

        if add_to_networks == True:
            self.activity_log.add_activity(type='network generation', activity=f'generated bibliometric coupling network and added to networks', location=['networks'])
            self.networks.__dict__['bibcoupling'] = network

        return network

    def author_works_network(self,
                                format: bool = True, 
                                update_attrs: bool = True,
                                drop_duplicates = False,
                                drop_empty_rows = True,
                                add_to_networks: bool = True
                                ) -> Network:
        
        """
        Generates a bipartite network representing relationships between authors and publications.

        Parameters
        ----------
        format : bool
            whether to format results, authors, funders, and affiliations data.
        update_attrs : bool
            whether to update author, funder, and affiliations attributes.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        add_to_networks : bool
            whether to store the network in the Review's Networks attribute.

        Returns
        -------
        network : Network
            a network representing relationships between authors and publications.
        """

        if drop_empty_rows == True:
            self.results.drop_empty_rows() # type: ignore
            self.authors.drop_empty_rows() # type: ignore
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        if format == True:
            self.format(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        if update_attrs == True:
            self.update_author_attrs()
        
        author_works_dict = self.author_works_dict()

        g = generate_author_works_network(author_works_dict)

        network = Network(graph=g)

        if add_to_networks == True:
            self.activity_log.add_activity(type='network generation', activity=f'generated author-works network and added to networks', location=['networks'])
            self.networks.__dict__['author_works'] = network

        return network

    def funder_works_network(self,
                                format: bool = True, 
                                update_attrs: bool = True,
                                drop_duplicates = False,
                                drop_empty_rows = True,
                                add_to_networks: bool = True
                                ) -> Network:
        
        """
        Generates a bipartite network representing relationships between funders and publications.

        Parameters
        ----------
        format : bool
            whether to format results, authors, funders, and affiliations data.
        update_attrs : bool
            whether to update author, funder, and affiliations attributes.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        add_to_networks : bool
            whether to store the network in the Review's Networks attribute.

        Returns
        -------
        network : Network
            a network representing relationships between funders and publications.
        """

        if drop_empty_rows == True:
            self.results.drop_empty_rows() # type: ignore
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        if format == True:
            self.format(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        if update_attrs == True:
            self.update_funder_attrs()
        
        author_works_dict = self.funder_works_dict()

        g = generate_funder_works_network(author_works_dict)

        network = Network(graph=g)

        if add_to_networks == True:
            self.activity_log.add_activity(type='network generation', activity=f'generated funder-works network and added to networks', location=['networks'])
            self.networks.__dict__['funder_works'] = network

        return network

    def author_affils_network(self,
                                format: bool = True, 
                                update_attrs: bool = True,
                                drop_duplicates = False,
                                drop_empty_rows = True,
                                add_to_networks: bool = True
                                ) -> Network:
        
        """
        Generates a bipartite network representing relationships between authors and affiliate organisations.

        Parameters
        ----------
        format : bool
            whether to format results, authors, funders, and affiliations data.
        update_attrs : bool
            whether to update author, funder, and affiliations attributes.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        add_to_networks : bool
            whether to store the network in the Review's Networks attribute.

        Returns
        -------
        network : Network
            a network representing relationships between authors and and affiliate organisations.
        """

        if drop_empty_rows == True:
            self.authors.drop_empty_rows()
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        if format == True:
            self.format(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        if update_attrs == True:
            self.update_affiliation_attrs(update_authors=True)
        
        d = self.author_affiliations_dict()
        g = generate_author_affils_network(d)
        network = Network(graph=g)

        if add_to_networks == True:
            self.activity_log.add_activity(type='network generation', activity=f'generated author-affiliations network and added to networks', location=['networks'])
            self.networks.__dict__['author_affiliations'] = network

        return network

    def entities_network(self,
                                format: bool = True, 
                                update_attrs: bool = True,
                                drop_duplicates = False,
                                drop_empty_rows = True,
                                add_to_networks: bool = True
                                ) -> Network:
        
        """
        Generates an n-partite network representing relationships between publications, authors, funders, and affiliate organisations.

        Parameters
        ----------
        format : bool
            whether to format results, authors, funders, and affiliations data.
        update_attrs : bool
            whether to update author, funder, and affiliations attributes.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        add_to_networks : bool
            whether to store the network in the Review's Networks attribute.

        Returns
        -------
        network : Network
            a network representing relationships between publications, authors, funders, and affiliate organisations.
        """

        if drop_empty_rows == True:
            self.results.drop_empty_rows() # type: ignore
            self.authors.drop_empty_rows() # type: ignore
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        if format == True:
            self.format(update_entities=update_attrs, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        if update_attrs == True:
            self.update_entity_attrs()
        
        author_works = self.author_works_network(format=False, update_attrs=False, add_to_networks=add_to_networks).to_igraph()
        del author_works.vs['type']
        author_works.vs['type'] = author_works.vs['category']
        del author_works.vs['category']

        funder_works = self.funder_works_network(format=False, update_attrs=False, add_to_networks=add_to_networks).to_igraph()
        del funder_works.vs['type']
        funder_works.vs['type'] = funder_works.vs['category']
        del funder_works.vs['category']

        author_affils = self.author_affils_network(format=False, update_attrs=False, add_to_networks=add_to_networks).to_igraph()
        del author_affils.vs['type']
        author_affils.vs['type'] = author_affils.vs['category']
        del author_affils.vs['category']

        g = Graph.union(author_works, [funder_works, author_affils])

        network = Network(graph=g)

        if add_to_networks == True:
            self.activity_log.add_activity(type='network generation', activity=f'generated entities network and added to networks', location=['networks'])
            self.networks.__dict__['all_entities'] = network
        
        return network

    def all_networks(self,
                                format: bool = True, 
                                update_attrs: bool = True,
                                drop_duplicates = False,
                                drop_empty_rows = True,
                                ignore_case: bool = True,
                                add_citations_to_results: bool = True,
                                add_to_networks: bool = True
                                ) -> Networks:
        
        """
        Generates all available networks:
            * Citations
            * Co-citations
            * Bibliometric coupling
            * Co-authors
            * Co-funders
            * Author-works (bipartite)
            * Funder-works (bipartite)
            * Author-affiliations (bipartite)
            * Works, authors, funders and affiliations (n-partite)


        Parameters
        ----------
        format : bool
            whether to format results, authors, funders, and affiliations data.
        update_attrs : bool
            whether to update author, funder, and affiliations attributes.
        drop_duplicates : bool
            whether to remove duplicated rows.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data.
        ignore_case : bool
            whether to ignore the case of string data.
        add_citations_to_results : bool
            whether to add Results entries' citations as Results entries. Defaults to True.
        add_to_networks : bool
            whether to store the network in the Review's Networks attribute.

        Returns
        -------
        networks : Networks
            a Networks object containing all available networks.
        """


        if drop_empty_rows == True:
            self.results.drop_empty_rows() # type: ignore
            self.authors.drop_empty_rows() # type: ignore
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows = drop_empty_rows)

        if format == True:
            self.format(update_entities=update_attrs, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        if update_attrs == True:
            self.update_entity_attrs()
        
        citations_network = self.citation_network(format=format, update_attrs=False, add_citations_to_results=add_citations_to_results, add_to_networks=add_to_networks)
        
        if update_attrs == True:
            self.update_entity_attrs()

        coauthors_network = self.coauthors_network(format=False, update_attrs=False, ignore_case=ignore_case, add_to_networks=add_to_networks)
        cofunders_network = self.cofunders_network(format=False, update_attrs=False, ignore_case=ignore_case, add_to_networks=add_to_networks)
        author_works_network = self.author_works_network(format=False, update_attrs=False, add_to_networks=add_to_networks)
        funder_works_network = self.funder_works_network(format=False, update_attrs=False, add_to_networks=add_to_networks)
        author_affils_network = self.author_affils_network(format=False, update_attrs=False, add_to_networks=add_to_networks)
        all_entities_network = self.entities_network(format=False, update_attrs=False, add_to_networks=add_to_networks)
        cocitation_network = self.cocitation_network(refresh_citations=False, format=False, update_attrs=False, add_citations_to_results=False, drop_duplicates=drop_duplicates, drop_empty_rows=False, add_to_networks=add_to_networks)
        bibcoupling_network = self.bibcoupling_network(refresh_citations=False, format=False, update_attrs=False, add_citations_to_results=False, drop_duplicates=False, drop_empty_rows=False, add_to_networks=add_to_networks)


        if add_to_networks == True:
            networks = self.networks
        
        else:
            networks = Networks()
            networks.__dict__['coauthors_network'] = coauthors_network
            networks.__dict__['cofunders_network'] = cofunders_network
            networks.__dict__['citations_network'] = citations_network
            networks.__dict__['author_works_network'] = author_works_network
            networks.__dict__['funder_works_network'] = funder_works_network
            networks.__dict__['author_affils_network'] = author_affils_network
            networks.__dict__['all_entities_network'] = all_entities_network
            networks.__dict__['cocitation'] = cocitation_network
            networks.__dict__['bibcoupling'] = bibcoupling_network

        return networks
