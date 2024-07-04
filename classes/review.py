from ..utils.basics import Iterator, results_cols
from ..exporters.general_exporters import obj_to_folder
from ..importers.pdf import read_pdf_to_table
from ..importers.crossref import search_works, lookup_doi, lookup_dois, lookup_journal, lookup_journals, search_journals, get_journal_entries, search_journal_entries, lookup_funder, lookup_funders, search_funders, get_funder_works, search_funder_works
from ..internet.scrapers import scrape_article, scrape_doi, scrape_google_scholar, scrape_google_scholar_search

from .properties import Properties
from .affiliations import Affiliation, Affiliations, format_affiliations
from .funders import Funders, format_funders
from .results import Results, Funder, generate_work_id
from .references import References, is_formatted_reference, extract_references
from .activitylog import ActivityLog
from .authors import Author, Authors, format_authors as orig_format_authors
from .citation_crawler import citation_crawler


import copy
import pickle

import pandas as pd
import numpy as np


def add_pdf(self, path = 'request_input'):
        
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

        if type(data) != pd.Series:
            raise TypeError(f'Results must be a Pandas.Series, not {type(data)}')

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

def add_dataframe(self, dataframe, update_work_ids = True, format_authors = True):
        
        if (type(dataframe) != pd.DataFrame) and (type(dataframe) != pd.Series):
            raise TypeError(f'Results must be a Pandas.Series or Pandas.DataFrame, not {type(dataframe)}')

        dataframe = dataframe.reset_index().drop('index', axis=1)
        dataframe.columns = dataframe.columns.astype(str).str.lower().str.replace(' ', '_')

        if (self.columns.to_list()) != (dataframe.columns.to_list()):
            for c in dataframe.columns:
                if c not in self.columns:
                    self[c] = pd.Series(dtype=object)

        
        index = len(self)
        for i in dataframe.index:
                self.loc[index] = dataframe.loc[i]

                if update_work_ids == True:
                    work_id = generate_work_id(dataframe.loc[i])
                    work_id = self.get_unique_id(work_id, i)
                    self.loc[index, 'work_id'] = work_id

                index += 1
        
        if format_authors == True:
            self.format_authors()

Results.add_dataframe = add_dataframe # type: ignore

def has_formatted_citations(self):
        return self[self['citations'].apply(is_formatted_reference)]

Results.has_formatted_citations = has_formatted_citations # type: ignore


def lacks_formatted_citations(self):
        return self[~self['citations'].apply(is_formatted_reference)]

Results.lacks_formatted_citations = lacks_formatted_citations # type: ignore


def format_citations(self, add_work_ids = False, update_from_doi = False):

        self['citations'] = self['citations'].replace({np.nan: None})
        self['citations_data'] = self['citations_data'].replace({np.nan: None})

        unformatted = self.lacks_formatted_citations()
        length = len(unformatted)
        if length > 0:

            if length == 1:
                intro_message = '\nFormatting 1 set of citations...'
            else:
                intro_message = f'\nFormatting {length} sets of citations...'
            print(intro_message)

            indices = unformatted.index
            processing_count = 0
            for i in indices:
                refs = extract_references(self.loc[i, 'citations_data'], add_work_ids = add_work_ids, update_from_doi = update_from_doi)
                refs_count = len(refs) # type: ignore
                processing_count = processing_count + refs_count
                self.at[i, 'citations'] = refs
            
            if processing_count == 1:
                outro_message = '1 citation formatted\n'
            else:
                outro_message = f'{processing_count} citations formatted\n'
            print(outro_message)

Results.format_citations = format_citations # type: ignore


def format_authors(self):

        self['authors'] = self['authors_data'].apply(orig_format_authors) # type: ignore
        return self['authors']

Results.format_authors = format_authors # type: ignore
    

def add_citations_to_results(self, add_work_ids = False, update_from_doi = False):

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
                    self.add_dataframe(dataframe=df)
        
        self.update_work_ids()
        self.format_authors()


        return self

Results.add_citations_to_results = add_citations_to_results # type: ignore

class Review:
    
    """
    This is a Review object. It stores the data from academic reviews.
    
    Parameters
    ----------
    review_name : str 
        the Review's name. Defaults to requesting from user input.
    file_location : str
        file location associated with Review.
    file_type : str
        file type associated with Review file(s).
    """

    results = Results()
    activity_log = ActivityLog()
    description = ''

    def __init__(self, review_name = 'request_input', file_location = None, file_type = None):
        
        """
        Initialises a Review instance.
        
        Parameters
        ----------
        review_name : str 
            the Review's name. Defaults to requesting from user input.
        file_location : str
            file location associated with Review.
        file_type : str
            file type associated with Review file(s).
        """
        
        if review_name == 'request_input':
            review_name = input('Review name: ')

        self.properties = Properties(review_name = review_name, file_location = file_location, file_type = file_type)
        self.results = Results()
        self.authors = Authors()
        self.funders = Funders()
        self.affiliations = Affiliations()
        self.activity_log = ActivityLog()
        self.description = ''
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
        Defines how Reviews are represented in string form.
        """
        
        output = f'\n\n{"-"*(13+len(self.properties.review_name))}\nReview name: {self.properties.review_name}\n{"-"*(13+len(self.properties.review_name))}\n\nProperties:\n-----------\n{self.properties}\n\nDescription:\n------------\n\n{self.description}\n\nResults:\n--------\n\n{self.results}\n\nAuthors:\n--------\n\n{self.authors.all.head(10)}\n\nFunders:\n--------\n\n{self.funders.all.head(10)}\n\n'
        
        return output
            
    def __iter__(self):
        
        """
        Implements iteration functionality for Review objects.
        """
        
        return Iterator(self)
    
    def __getitem__(self, key):
        
        """
        Retrieves Review contents or results using an index/key.
        """
        
        if key in self.__dict__.keys():
            return self.__dict__[key]

        if key in self.results['work_id'].to_list():
            return self.results.get(key)
        
        if key in self.authors.details.keys():
            return self.authors[key]
        
        if key in self.results.columns.to_list():
            return self.results[key]
        
        if key in self.authors.all.columns.to_list():
            return self.authors.all[key]


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
        """
        
        return self.__dict__.keys()
    
    def __len__(self):

        return len(self.results)
    
    def count_results(self):
        
        """
        Returns the number of entries in the Results table.
        """
        
        return len(self.results)
        
    def to_list(self):
        
        """
        Returns the Review as a list.
        """
        
        return [i for i in self]
    
    def to_dict(self):
        
        """
        Returns the Review as a dictionary.  Excludes the Review's 'properties' attribute.
        """
        
        output_dict = {}
        for index in self.__dict__.keys():
            output_dict[index] = self.__dict__[index]
        
        return output_dict
    
    def copy(self):
        
        """
        Returns the a copy of the Review.
        """
        
        return copy.deepcopy(self)
    
    def get_result(self, row_position, column_position = None):
        
        """
        Returns a result when given its attribute name.
        """
        
        if column_position == None:
            return self.results.loc[row_position]
        else:
            return self.results.loc[row_position, column_position]
    
    def get_affiliations_dict(self):
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
        
        self.results.add_pdf(path) # type: ignore
        
        if update_formatting == True:
            self.format()

        self.update_properties()

    def varstr(self):
        
        """
        Returns the Review's name as a string. Defaults to using its variable name; falls back to using its name property.
        
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
    
    def to_dataframe(self):
        return self.results.to_dataframe() # type: ignore

    def from_dataframe(dataframe: pd.DataFrame): # type: ignore
        
        review = Review()
        review.results = Results.from_dataframe(dataframe) # type: ignore
        review.format() # type: ignore

        return review

    def format_funders(self):

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
        self.authors.format_affiliations()

        affils_data = self.authors.all['affiliations'].to_list()

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

    def format_citations(self):
        self.results.format_citations() # type: ignore

    def format_authors(self):

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
    
    def format(self):
        self.format_funders()
        self.format_citations()
        self.format_authors()
        self.format_affiliations()

    def update_author_publications(self, ignore_case: bool = True):

        self.authors.sync()

        auths_data = self.authors.all[['author_id', 'orcid', 'google_scholar', 'crossref', 'scopus', 'full_name']]
        auths_data = auths_data.dropna(axis=1, how='all')

        global results_cols

        for i in auths_data.index:
            author_id = auths_data.loc[i, 'author_id']
            author_pubs = pd.DataFrame(columns=results_cols, dtype=object)

            for c in auths_data.columns:

                datapoint = auths_data.loc[i, c]


                if (datapoint != None) and (datapoint != '') and (datapoint != 'None'):

                    data_matches = self.results.mask_entities(column = 'authors', query=datapoint, ignore_case=ignore_case) # type: ignore
                    author_pubs = pd.concat([author_pubs, data_matches])
                
            author_pubs = author_pubs.drop_duplicates().reset_index().drop('index', axis=1)

            self.authors.details[author_id].publications = author_pubs
        


    def add_citations_to_results(self, update_formatting: bool = True):
        self.results.add_citations_to_results() # type: ignore

        if update_formatting == True:
            self.format()


    def update_from_orcid(self, update_formatting: bool = True):
        self.authors.update_from_orcid()

        if update_formatting == True:
            self.format()

    def add_dataframe(self, dataframe: pd.DataFrame, update_formatting: bool = True):

        self.results.add_dataframe(dataframe=dataframe) # type: ignore

        if update_formatting == True:
            self.format()

    def import_excel(self, file_path = 'request_input', sheet_name = None, update_formatting: bool = True):
        self.update_properties()
        self.results.import_excel(file_path, sheet_name) # type: ignore

        if update_formatting == True:
            self.format()

        return self
    
    def from_excel(file_path = 'request_input', sheet_name = None): # type: ignore

        review = Review()
        review.results = Results.from_excel(file_path, sheet_name) # type: ignore
        review.format()
        
        return review

    def import_csv(self, file_path = 'request_input', update_formatting: bool = True):
        self.update_properties()
        self.results.import_csv(file_path) # type: ignore

        if update_formatting == True:
            self.format()

        return self
    
    def from_csv(file_path = 'request_input'): # type: ignore

        review = Review()
        review.results = Results.from_csv(file_path) # type: ignore
        review.format()

        return review

    def import_json(self, file_path = 'request_input', update_formatting: bool = True):
        self.update_properties()
        self.results.import_json(file_path) # type: ignore

        if update_formatting == True:
            self.format()

        return self
    
    def from_json(file_path = 'request_input'): # type: ignore

        review = Review()
        review.import_json(file_path = file_path) # type: ignore

        return review
    
    def import_file(self, file_path = 'request_input', sheet_name = None, update_formatting: bool = True):

        self.update_properties()
        self.results.import_file(file_path, sheet_name) # type: ignore

        if update_formatting == True:
            self.format()
    
    def from_file(file_path = 'request_input', sheet_name = None): # type: ignore
        
        review = Review()
        review.results = Results.from_file(file_path, sheet_name) # type: ignore
        review.format() # type: ignore

        return review

    def import_jstor_metadata(self, file_path = 'request_input', clean_results = True):
        self.results.import_jstor_metadata(file_path = file_path, clean_results = clean_results) # type: ignore
    
    def import_jstor_full(self, file_path = 'request_input', clean_results = True):
        self.results.import_jstor_full(file_path = file_path, clean_results = clean_results) # type: ignore

    def search_field(self, field = 'request_input', any_kwds = 'request_input', all_kwds = None, not_kwds = None, case_sensitive = False, output = 'Results'):
        return self.results.search_field(field = field, any_kwds = any_kwds, all_kwds = all_kwds, not_kwds = not_kwds, case_sensitive = case_sensitive, output = output) # type: ignore

    def search(self, any_kwds = 'request_input', all_kwds = None, not_kwds = None, fields = 'all', case_sensitive = False):

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


    def export_txt(self, file_name = 'request_input', file_address = 'request_input'):
        
        """
        Exports the Review to a .txt file.
        
        Parameters
        ----------
        file_name : str
            name of file to create. Defaults to using the object's variable name.
        file_address : str
            directory address to create file in. defaults to requesting for user input.
        """
        
        if file_name == 'request_input':
            file_name = input('File name: ')
            
        if file_address == 'request_input':
            file_address = input('File address: ')
            
        file_address = file_address + '/' + file_name

        if file_address.endswith('.Review') == False:
            file_address = file_address + '.Review'

        with open(file_address, 'wb') as f:
            pickle.dump(self, f) 
    
    def export_folder(self, folder_name = 'request_input', folder_address = 'request_input', export_str_as = 'txt', export_dict_as = 'json', export_pandas_as = 'csv', export_network_as = 'graphML'):
        
        """
        Exports Review's contents to a folder.
        
        Parameters
        ----------
        folder_name : str 
            name of folder to create. Defaults to using the object's variable name.
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
        """
        
        if folder_name == 'request_input':
            folder_name = input('Folder name: ')
        
        if folder_name.endswith('_Review') == False:
            folder_name = folder_name + '_Review'
        
        obj_to_folder(self, folder_name = folder_name, folder_address = folder_address, export_str_as = export_str_as, export_dict_as = export_dict_as, export_pandas_as = export_pandas_as, export_network_as = export_network_as)

    def scrape_article(self, url = 'request_input'):
        
        if url == 'request_input':
            url = input('URL: ')

        df = scrape_article(url)
        self.results.add_dataframe(df) # type: ignore

    def scrape_doi(self, doi = 'request_input'):
        
        if doi == 'request_input':
            doi = input('DOI or URL: ')

        df = scrape_doi(doi)
        self.results.add_dataframe(df) # type: ignore

    def scrape_google_scholar(self, url = 'request_input'):

        if url == 'request_input':
            url = input('URL: ')

        df = scrape_google_scholar(url)
        self.results.add_dataframe(df) # type: ignore
    
    def scrape_google_scholar_search(self, url = 'request_input'):

        if url == 'request_input':
            url = input('URL: ')

        df = scrape_google_scholar_search(url)
        self.results.add_dataframe(df) # type: ignore
    
    def search_crossref(self,
                bibliographic: str = None,  # type: ignore
                title: str = None, # type: ignore
                author: str = None, # type: ignore
                author_affiliation: str = None, # type: ignore
                editor: str = None, # type: ignore
                entry_type: str = None, # type: ignore
                published_date: str = None, # type: ignore
                DOI: str = None, # type: ignore
                ISSN: str = None, # type: ignore
                publisher_name: str = None, # type: ignore
                funder_name = None, # type: ignore
                source: str = None, # type: ignore
                link: str = None, # type: ignore
                filter: dict = None, # type: ignore
                select: list = None, # type: ignore
                sample: int = None, # type: ignore
                limit: int = None, # type: ignore
                rate_limit: float = 0.1,
                timeout = 60,
                add_to_results = False
                ) -> pd.DataFrame:
        
        df = search_works(bibliographic = bibliographic,
                title = title,
                author = author,
                author_affiliation = author_affiliation,
                editor = editor,
                entry_type = entry_type,
                published_date = published_date,
                DOI = DOI,
                ISSN = ISSN,
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
        
        if add_to_results == True:
            self.results.add_dataframe(dataframe=df) # type: ignore
            self.format_authors()
        
        return df

    def lookup_doi(self, doi = 'request_input', timeout = 60):
        return lookup_doi(doi=doi, timeout=timeout)
    
    def add_doi(self, doi = 'request_input', timeout = 60, update_formatting: bool = True):
            
        self.results.add_doi(doi=doi, timeout=timeout) # type: ignore

        if update_formatting == True:
            self.format()
        

    def lookup_dois(self, dois_list: list = [], rate_limit: float = 0.1, timeout = 60):
        return lookup_dois(dois_list=dois_list, rate_limit=rate_limit, timeout=timeout)
    
    def add_dois(self, dois_list: list = [], rate_limit: float = 0.1, timeout = 60, update_formatting: bool = True):
        self.results.add_dois(dois_list=dois_list, rate_limit=rate_limit, timeout=timeout) # type: ignore

        if update_formatting == True:
            self.format()
    
    def update_from_dois(self, timeout: int = 60, update_formatting: bool = True):
        self.results.update_from_dois(timeout=timeout) # type: ignore

        if update_formatting == True:
            self.format()

    def sync_apis(self, timeout: int = 60):

        self.update_from_dois(timeout=timeout)
        self.update_from_orcid()
        self.format()

    def lookup_journal(self, issn = 'request_input', timeout = 60):
        return lookup_journal(issn = issn, timeout = timeout)
    
    def lookup_journals(self, issns_list: list = [], rate_limit: float = 0.1, timeout: int = 60):
        return lookup_journals(issns_list = issns_list, rate_limit = rate_limit, timeout = timeout)
    
    def search_journals(self, *args, limit: int = None, rate_limit: float = 0.1, timeout = 60): # type: ignore
        return search_journals(*args, limit = limit, rate_limit=rate_limit, timeout = timeout)
    
    def get_journal_entries(self,
                        issn = 'request_input',
                        filter: dict = None, # type: ignore
                        select: list = None, # type: ignore
                        sample: int = None, # type: ignore
                        limit: int = None, # type: ignore
                        rate_limit: float = 0.1,
                        timeout = 60):
        
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
                        DOI: str = None, # type: ignore
                        publisher_name: str = None, # type: ignore
                        funder_name: str = None, # type: ignore
                        source: str = None, # type: ignore
                        link: str = None, # type: ignore
                        filter: dict = None, # type: ignore
                        select: list = None, # type: ignore
                        sample: int = None, # type: ignore
                        limit: int = None, # type: ignore
                        rate_limit: float = 0.1,
                        timeout: int = 60,
                        add_to_results: bool = False) -> pd.DataFrame:
        
            df = search_journal_entries(issn = issn,
                                          bibliographic = bibliographic,
                                          title=title,
                                          author=author,
                                          author_affiliation=author_affiliation,
                                          editor=editor,
                                          entry_type=entry_type,
                                          published_date = published_date,
                                          DOI = DOI,
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
                self.results.add_dataframe(dataframe=df) # type: ignore
                self.format_citations()
                self.format_authors()
        
            return df
    
    def lookup_funder(self, funder_id = 'request_input', timeout = 60):
        return lookup_funder(funder_id = funder_id, timeout = timeout)
    
    def lookup_funders(self, funder_ids: list = [], rate_limit: float = 0.1, timeout = 60):
        return lookup_funders(funder_ids=funder_ids, rate_limit=rate_limit, timeout = timeout)
    
    def search_funders(self, *args, limit: int = None, rate_limit: float = 0.1, timeout = 60): # type: ignore
        return search_funders(*args, limit=limit, rate_limit=rate_limit, timeout=timeout)
    
    def get_funder_works(self,
                        funder_id = 'request_input',
                        filter: dict = None, # type: ignore
                        select: list = None, # type: ignore
                        sample: int = None, # type: ignore
                        limit: int = None, # type: ignore
                        rate_limit: float = 0.1,
                        timeout: int = 60,
                        add_to_results: bool = False):
        
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
                        DOI: str = None, # type: ignore
                        publisher_name: str = None, # type: ignore
                        funder_name = None,
                        source: str = None, # type: ignore
                        link: str = None, # type: ignore
                        filter: dict = None, # type: ignore
                        select: list = None, # type: ignore
                        sample: int = None, # type: ignore
                        limit: int = None, # type: ignore
                        rate_limit: float = 0.1,
                        timeout: int = 60,
                        add_to_results: bool = False):
    
        df = search_funder_works(
                                funder_id=funder_id,
                                bibliographic=bibliographic,
                                title=title,
                                author=author,
                                author_affiliation=author_affiliation,
                                editor=editor,
                                entry_type=entry_type,
                                published_date=published_date,
                                DOI=DOI,
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
                self.results.add_dataframe(dataframe=df) # type: ignore
                self.format_authors()
        
        return df


    def crawl_stored_citations(self, max_depth=3, processing_limit=1000, format_authors = True, update_from_doi = False):

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
        

        if format_authors == True:
            self.format_authors()
        
        print(f'Crawl complete:\n    - Entries processed: {len(processed_indexes)}\n    - Results added: {final_len_diff}\n')

        return self.results

    def crawl_citations(
                    self,
                    use_api: bool = True,
                    crawl_limit: int = 5, 
                    depth_limit: int = 2,
                    be_polite: bool = True,
                    rate_limit: float = 0.05,
                    timeout: int = 60,
                    add_to_results = True
                    ):
    
        """
        Crawls a Result's object's entries, their citations, and so on.
        
        The crawler iterates through queue of works; extracts their citations; runs checks to validate each reference;
        based on these, selects a source to retrieve data from: 
            (a) if has a valid DOI: Crossref API.
            (b) if no valid DOI: bespoke web scraping for specific academic websites.
            (c) else if a link is present: general web scraping.
        
        Retrieves data and adds the entries to the dataframe. 

        Iterates through each set of added entries.
        
        Parameters
        ---------- 
        
        
        
        Returns
        -------
        result : object 
            an object containing the results of a crawl.
        """

        result = self.results.crawl_citations(
                                            use_api = use_api,
                                            crawl_limit = crawl_limit, 
                                            depth_limit = depth_limit,
                                            be_polite = be_polite,
                                            rate_limit = rate_limit,
                                            timeout = timeout,
                                            add_to_results = add_to_results
                                            ) # type: ignore
        
        self.format_citations()

        result = citation_crawler(
                    data = self,  # type: ignore
                    use_api = use_api,
                    crawl_limit = crawl_limit, 
                    depth_limit = depth_limit,
                    be_polite = be_polite,
                    rate_limit = rate_limit,
                    timeout = timeout
                    )
        
        if add_to_results == True:

            df = result.drop(labels=0, axis=0).reset_index().drop('index', axis=1)
            self.results.add_dataframe(df) # type: ignore
            self.format()

        
        return result







    ## Legacy code for saving reviews, taken from Projects class in IDEA. Requires overhaul.

    # def save_as(self, file_name = 'request_input', file_address = 'request_input', file_type = 'request_input'):
        
    #     """
    #     Saves the Review to a file or folder.
        
    #     Parameters
    #     ----------
    #     file_name : str
    #         name of file to create. Defaults to using the object's variable name.
    #     file_address : str
    #         directory address to create file in. defaults to requesting for user input.
    #     file_type : str
    #         type of file to save.
        
    #     Notes
    #     -----
    #     Options for file_type:
    #         * 'Review': saves to a .Review file. This is a specially labelled pickled .txt file.
    #         * 'text' or 'txt': saves to a pickled .txt file.
    #         * 'pickle': saves to pickled .txt file.
    #         * 'Excel': saves to a folder of .xlsx files.
    #         * 'xlsx': saves to a folder of .xlsx files.
    #         * 'csv': saves to a folder of folders of .csv files.
    #         * 'folder': saves to a folder.
    #     """
        
    #     if file_name == 'request_input':
    #         file_name = input('File name: ')

    #     if file_type == 'request_input':
    #         file_type = input('File type: ')
        
    #     if file_address == 'request_input':
    #         file_address = input('File address: ')
        
    #     file_type = file_type.strip().strip('.').strip().lower()
        
        
    #     if (file_type == None) or (file_type.lower().strip('.').strip() == '') or (file_type.lower().strip('.').strip() == '.Review') or (file_type.lower().strip('.').strip() == 'Review') or (file_type.lower().strip('.').strip() == 'text') or (file_type.lower().strip('.').strip() == 'txt') or (file_type.lower().strip('.').strip() == 'pickle'):
            
    #         self.export_txt(file_name = file_name, file_address = file_address)
        
        
    #     if (file_type.lower().strip('.').strip() == 'excel') or (file_type.lower().strip('.').strip() == 'xlsx'):

    #         file_address = file_address + '/' + file_name
    #         os.mkdir(file_address)
    #         cases = self.cases()
    #         for i in cases:
    #             self.get_case(i).export_excel(new_file = True, file_name = i, file_address = file_address)
        
        
    #     if (file_type.lower().strip('.').strip() == 'csv') or (file_type.lower().strip('.').strip() == 'csvs'):
            
    #         file_address = file_address + '/' + file_name
    #         os.mkdir(file_address)
    #         cases = self.cases()
    #         for i in cases:
    #             self.get_case(i).export_csv_folder(folder_address = file_address, folder_name = file_name)
    
    
    # def save(self, save_as = None, file_type = None, save_to = None):
        
    #     """
    #     Saves the Review to its source file. If no source given, saves to a new file.
        
    #     Parameters
    #     ----------
    #     save_as : str
    #         name of file to create. Defaults to using the object's variable name.
    #     save_to : str
    #         directory address to save file in. Defaults to requesting for user input.
    #     file_type : str
    #         type of file to save.
        
    #     Notes
    #     -----
    #     Options for file_type:
    #         * 'review': saves to a .Review file. This is a specially labelled pickled .txt file.
    #         * 'text' or 'txt': saves to a pickled .txt file.
    #         * 'pickle': saves to pickled .txt file.
    #         * 'Excel': saves to a folder of .xlsx files.
    #         * 'xlsx': saves to a folder of .xlsx files.
    #         * 'csv': saves to a folder of folders of .csv files.
    #         * 'folder': saves to a folder.
    #     """
        
    #     if save_as == None:
            
    #         try:
    #             save_as = self.properties.file_location.split('/')[-1].split('.')[0]
    #         except:
    #             save_as = self.case_name
        
    #     if file_type == None:
    #         file_type = self.file_type
        
    #     if save_to == None:
    #         save_to = self.file_location
        
    #     save_as = save_as.lower().strip('.').strip() 
        
    #     try:
    #         self.save_as(file_name = save_as, file_address = save_to, file_type = file_type)
    #     except:
    #         print('Save failed') 