from .utils.basics import Iterator, results_cols
from .utils.cleaners import strip_list_str
from .exporters.general_exporters import obj_to_folder
from .importers.pdf import read_pdf_to_table
from .importers.jstor import import_jstor_metadata, import_jstor_full
from .importers.crossref import items_to_df, references_to_df, search_works, lookup_doi, lookup_dois, lookup_journal, lookup_journals, search_journals, get_journal_entries, search_journal_entries, lookup_funder, lookup_funders, search_funders, get_funder_works, search_funder_works
from .datasets import stopwords
from .internet.scrapers import scrape_article, scrape_doi, scrape_google_scholar, scrape_google_scholar_search

import copy
from datetime import datetime
import pickle
from pathlib import Path

import pandas as pd
import numpy as np
from nltk.tokenize import word_tokenize

class Properties:
    
    """
    This is a class for properties to be assigned to Reviews.
    
    Parameters
    ----------
    review_name : str 
        name of the Review in the environment.
    file_location : str 
        directory address for source file.
    file_type : str 
        file type for source file.
        
    Attributes
    ----------
    review_name : str
        name of Review in the environment.
    obj_path : str
        path to object in the environment.
    created_at : str
        date and time created.
    last_changed_at : str
        date and time the object was last edited.
    obj_size : float
        size of the object in memory in bytes.
    """
    
    def __init__(self, review_name = None, file_location = None, file_type = None):
        
        """
        Initialises Properties instance.
        
        Parameters
        ----------
        parent_obj_path : str 
            name of parent object if object is an attribute of another object.
        size : int 
            size of object in memory.
        """
        if review_name == None:
            review_name = 'Review'

        self.review_name = review_name
        self.created_at = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.last_changed_at = self.created_at
        self.last_backup = None
        self.file_location = file_location
        self.file_type = file_type
    
    def __iter__(self):
        
        """
        Function to make Properties objects iterable.
        """
        
        return Iterator(self)
    
    def __repr__(self):
        
        """
        Defines how Properties objects are represented in string form.
        """
        
        self_dict = self.to_dict()
        output = '\n'
        for key in self_dict.keys():
            prop = self_dict[key]
            output = output + key + ': ' + str(prop) + '\n'
        
        return output
    
    def to_list(self):
        
        """
        Returns Properties object as a list.
        """
        
        return [i for i in self]

    def to_dict(self):
        
        """
        Returns Properties object as a dictionary.
        """
        
        output_dict = {}
        for index in self.__dict__.keys():
            output_dict[index] = self.__dict__[index]

        return output_dict
    
    
    def update_last_changed(self):
        
        """
        Updates the last_changed attribute to the current date and time.
        """
        
        self.last_changed_at = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

class Results(pd.DataFrame):

    """
    This is a Results object. It is a modified Pandas Dataframe object designed to store the results of an academic review.
    
    Parameters
    ----------
    
    
    Attributes
    ----------
    """

    def __init__(self, dataframe = None, index = []):
        
        """
        Initialises Results instance.
        
        Parameters
        ----------
        """

        if dataframe is None:

            global results_cols

            # Inheriting methods and attributes from Pandas.DataFrame class
            super().__init__(dtype=object, columns = results_cols, index = index
                            )
            
            self.replace(np.nan, None)
        
        else:
            df = dataframe
            if type(dataframe) == pd.DataFrame:
                self = Results.from_dataframe(dataframe = df)

    def add_pdf(self, path = 'request_input'):
        
        if path == 'request_input':
            path = input('Path to PDF (URL or filepath): ')

        table = read_pdf_to_table(path)
        table = table.replace(np.nan, None).astype(object)

        series = table.loc[0]

        index = len(self)
        self.loc[index] = series

        self.extract_authors()

        return self
    
    def add_row(self, data):

        if type(data) != pd.Series:
            raise TypeError(f'Results must be a Pandas.Series, not {type(data)}')

        data.index = data.index.astype(str).str.lower().str.replace(' ', '_')
        if len(data) != len(self.columns):
            for c in data.index:
                if c not in self.columns:
                    self[c] = pd.Series(dtype=object)

        index = len(self)
        self.loc[index] = data
        self.extract_authors()
    
    def add_dataframe(self, dataframe):
        
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
            index += 1
        
        self.extract_authors()

    def add_doi(self, doi: str = 'request_input', timeout: int = 60):
        df = lookup_doi(doi=doi, timeout=timeout)
        self.add_dataframe(dataframe=df)

    def add_dois(self, dois_list: list = [], rate_limit: float = 0.1, timeout = 60):
        df = lookup_dois(dois_list=dois_list, rate_limit=rate_limit, timeout=timeout)
        self.add_dataframe(dataframe=df)

    def __add__(self, results_obj):

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
        return self.copy(deep=True)
    
    def from_dataframe(dataframe):
        
        dataframe = dataframe.copy(deep=True).reset_index().drop('index', axis=1)
        results_table = Results(index = dataframe.index)
        results_table.columns = results_table.columns.astype(str).str.lower().str.replace(' ', '_')
        dataframe.columns = dataframe.columns.astype(str).str.lower().str.replace(' ', '_')

        for c in dataframe.columns:
            results_table[c] = dataframe[c]
        
        results_table.extract_authors()

        return results_table

    def import_excel(self, file_path = 'request_input', sheet_name = None):

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
                    self.at[i, col] = strip_list_str(items)
                    
                except:
                    pass
        
        self.extract_authors()

        return self

    def from_excel(file_path = 'request_input', sheet_name = None):

        results_table = Results()
        results_table = results_table.import_excel(file_path, sheet_name).replace(np.nan, None)
        results_table.extract_authors() # type: ignore


        return results_table

    def import_csv(self, file_path = 'request_input'):

            if file_path == 'request_input':
                file_path = input('File path: ')
                
            csv_import = pd.read_csv(file_path, header = 0, index_col = 0).replace({np.nan: None, 'none': None})
            self.add_dataframe(csv_import)

            cols = ['authors', 'keywords']
            for col in cols:
                for i in self.index:
                    items = self.loc[i, col]
                    
                    try:
                        self.at[i, col] = strip_list_str(items)
                        
                    except:
                        pass
            
            self.extract_authors()

            return self
    
    def from_csv(file_path = 'request_input'):

        results_table = Results()
        results_table.import_csv(file_path).replace(np.nan, None)


        return results_table

    def import_json(self, file_path = 'request_input'):

        if file_path == 'request_input':
                file_path = input('File path: ')

        json_import = pd.read_json(file_path)
        self.add_dataframe(json_import)

        cols = ['authors', 'keywords']
        for col in cols:
                for i in self.index:
                    items = self.loc[i, col]
                    
                    try:
                        self.at[i, col] = strip_list_str(items)
                        
                    except:
                        pass

        self = self.replace(np.nan, None)
        self.extract_authors() # type: ignore

        return self
    
    def from_json(file_path = 'request_input'):

        results_table = Results()
        results_table.import_json(file_path).replace(np.nan, None)
        
        return results_table
    
    def import_file(self, file_path = 'request_input', sheet_name = None):

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
        
        else:
            raise ValueError('File does not exist')
    
    def from_file(file_path = 'request_input', sheet_name = None):

        if file_path == 'request_input':
            file_path = input('File path: ')
        
        results_table = Results()

        path_obj = Path(file_path)
        suffix = path_obj.suffix

        if path_obj.exists() == True:

            if suffix.strip('.') == 'xlsx':
                return results_table.import_excel(file_path, sheet_name)
            
            if suffix.strip('.') == 'csv':
                return results_table.import_csv(file_path)
            
            if suffix.strip('.') == 'json':
                return results_table.import_json(file_path)
        
        else:
            raise ValueError('File does not exist')

    def import_jstor_metadata(self, file_path = 'request_input', clean_results = True):

        df = import_jstor_metadata(file_path = file_path, clean_results = clean_results)
        self.add_dataframe(df)
    
    def import_jstor_full(self, file_path = 'request_input', clean_results = True):

        df = import_jstor_full(file_path = file_path, clean_results = clean_results)
        self.add_dataframe(df)

    def search_field(self, field = 'request_input', any_kwds = 'request_input', all_kwds = None, not_kwds = None, case_sensitive = False, output = 'Results'):
        
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
                        not_kwds = pd.concat([not_kwds, rows])
        
        
        combined_df = pd.concat([contains_df, not_kwds_df])
        
        if all_kwds != None:
                
            if (type(all_kwds) != list) and (type(all_kwds) != str):
                raise TypeError('"all_kwds" must be a string or list')

            if type(all_kwds) == str:
                any_kwds = any_kwds.strip().split(',')
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

        if any_kwds == 'request_input':
            any_kwds = input('Any keywords: ')
            any_kwds = any_kwds.strip().split(',')
            any_kwds = [i.strip() for i in any_kwds]
    
        if all_kwds != None:
                
            if (type(all_kwds) != list) and (type(all_kwds) != str):
                raise TypeError('"all_kwds" must be a string or list')

            if type(all_kwds) == str:
                any_kwds = any_kwds.strip().split(',')
                any_kwds = [i.strip() for i in any_kwds]

            if case_sensitive == False:
                all_kwds = pd.Series(all_kwds).str.lower().to_list()

        global results_cols

        output_df = pd.DataFrame(columns = results_cols, dtype=object)

        if fields == 'all':
            
            output_df = pd.DataFrame(columns = self.columns.to_list(), dtype=object)
            
            for col in self.columns:
                rows = self.search_field(field = col, any_kwds = any_kwds, not_kwds = not_kwds, case_sensitive = case_sensitive, output = 'pandas.dataframe')
                output_df = pd.concat([output_df, rows])
            
            output_df = output_df[~output_df.index.duplicated(keep = 'first') == True]
            
            if all_kwds != None:
                
                for item in all_kwds:
                    for i in output_df.index:
                        if True not in (output_df.loc[i].str.contains(item) == True).to_list():
                            output_df = output_df.drop(i)
    
        else:
            
            if (type(fields) == str) or ((type(fields) == list) and (len(fields) == 1)):

                output_df = self.search_field(field = fields, any_kwds = any_kwds, not_kwds = not_kwds, case_sensitive = case_sensitive)
                output_df = output_df[~output_df.index.duplicated(keep = 'first') == True]
                
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
    
        output = []

        for i in self['keywords']:
            if type(i) == str:
                i = strip_list_str(i)
            
            if type(i) == list:
                output = output + i

        output = pd.Series(output).str.strip().str.lower()
        output = output.drop(output[output.values == 'none'].index).reset_index().drop('index', axis=1)[0]
        
        return output
    
    def get_keywords_list(self):        
        return self.get_keywords(self).to_list()
    
    def get_keywords_set(self):
        return set(self.get_keywords_list())
    
    def keyword_frequencies(self):
        return self.get_keywords().value_counts()

    def keyword_stats(self):
        return self.keyword_frequencies(self).describe()
    
    def get_titles_words(self, ignore_stopwords = True):

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
        return set(self.get_titles_words(ignore_stopwords = ignore_stopwords))
    
    def title_word_frequencies(self, ignore_stopwords = True):
        return pd.Series(self.get_titles_words(ignore_stopwords = ignore_stopwords)).value_counts()
                                               
    def title_words_stats(self, ignore_stopwords = True):
        return self.title_word_frequencies(ignore_stopwords = ignore_stopwords).describe()
    
    def drop_containing_keywords(self, keywords):
        
        if type(keywords) == str:
            keywords = [keywords]

        results = self.search(any_kwds = keywords).index
        return self.drop(index = results, axis=0).reset_index().drop('index', axis=1)

    def filter_by_keyword_frequency(self, cutoff = 3):

        keywords_freq = self.keyword_frequencies()

        frequent_kws = keywords_freq[keywords_freq.values > cutoff]
        frequent_kws = list(frequent_kws.index)
        
        output = pd.DataFrame(dtype=object)
        for i in frequent_kws:
            df = self.search(any_kwds = i).copy(deep=True)
            output = pd.concat([output, df])

        output = output.drop_duplicates('title')
        output = output.reset_index().drop('index', axis=1)

        print(f'Keywords: {frequent_kws}')

        return self.loc[output.index]
    
    def extract_citations(self):

        self['citations'] = self['citations_data'].apply(extract_references) # type: ignore
        return self['citations']
    
    def extract_authors(self):

        self['authors'] = self['authors_data'].apply(extract_authors) # type: ignore
        return self['authors']

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

    def from_dataframe(dataframe):
        
        dataframe = dataframe.copy(deep=True).reset_index().drop('index', axis=1)
        results_table = References(index = dataframe.index)
        results_table.columns = results_table.columns.astype(str).str.lower().str.replace(' ', '_')
        dataframe.columns = dataframe.columns.astype(str).str.lower().str.replace(' ', '_')

        for c in dataframe.columns:
            results_table[c] = dataframe[c]
        
        return results_table

def extract_references(references_list: list):

    df = pd.DataFrame(columns=results_cols, dtype=object)

    if (type(references_list) == list) and (type(references_list[0]) == dict):
        df = references_to_df(references_list)
    
    else:
        if (type(references_list) == list) and (type(references_list[0]) == str):
            df = pd.DataFrame(columns=results_cols, dtype=object)
            df['link'] = pd.Series(references_list, dtype=object)

    refs = References.from_dataframe(df)

    return refs

    

class ActivityLog(pd.DataFrame):

    """
    This is a ActivityLog object. It is a modified Pandas Dataframe object designed to store metadata about an academic review.
    
    Parameters
    ----------
    
    
    Attributes
    ----------
    """

    def __init__(self):
        
        """
        Initialises ActivityLog instance.
        
        Parameters
        ----------
        """


        # Inheriting methods and attributes from Pandas.DataFrame class
        super().__init__(dtype=object, columns = [
                                'activity',
                                'site'
                                ]
                         )
        
        self.replace(np.nan, None)

class Author():

    """
    This is an Author object. It is designed to store data about an individual author and their publications.
    
    Parameters
    ----------
    
    
    Attributes
    ----------
    """

    def __init__(self,
                 author_id: str = None, # type: ignore
                 full_name: str = None, # type: ignore
                 given_name: str = None, # type: ignore
                 family_name: str = None, # type: ignore
                 email: str = None, # type: ignore
                 affiliations: str = None, # type: ignore
                 publications: str = None, # type: ignore
                 orcid: str = None, # type: ignore
                 google_scholar: str = None, # type: ignore
                 crossref: str = None, # type: ignore
                 other_links: str = None # type: ignore
                 ):
        
        """
        Initialises Author instance.
        
        Parameters
        ----------
        """

        self.details = pd.DataFrame(columns = [
                                'author_id',
                                'full_name',
                                'given_name',
                                'family_name',
                                'email',
                                'affiliations',
                                'publications',
                                'orcid',
                                'google_scholar',
                                'crossref',
                                'other_links'
                                ],
                                dtype = object)
        
        self.details.loc[0] = pd.Series(dtype=object)

        self.details.loc[0, 'author_id'] = author_id
        self.details.loc[0, 'full_name'] = full_name
        self.details.loc[0, 'given_name'] = given_name
        self.details.loc[0, 'family_name'] = family_name
        self.details.loc[0, 'email'] = email
        self.details.loc[0, 'affiliations'] = affiliations
        self.details.loc[0, 'publications'] = publications
        self.details.loc[0, 'orcid'] = orcid
        self.details.loc[0, 'google_scholar'] = google_scholar
        self.details.loc[0, 'crossref'] = crossref
        self.details.loc[0, 'other_links'] = other_links

        self.update_full_name()

        self.publications = Results()

        
    
    def __getitem__(self, key):
        
        """
        Retrieves Author attribute using a key.
        """
        
        return self.__dict__[key]

    def __repr__(self) -> str:
        return str(self.details.loc[0, 'full_name'])
    
    def update_full_name(self) -> str:

            given = self.details.loc[0, 'given_name']
            family = self.details.loc[0, 'family_name']

            if given == None:
                given = ''
            
            if family == None:
                family = ''
            
            full = given + ' ' + family     # type: ignore

            if (full == '') or (full == ' '):
                full = 'no_name_given'

            full_name = self.details.loc[0, 'full_name']
            if (full_name == None) or (full_name == '') or (full_name == 'no_name_given'):
                self.details.loc[0, 'full_name'] = full

            return str(self.details.loc[0, 'full_name'])

    def name_set(self) -> set:

        given = str(self.details.loc[0, 'given_name'])
        family = str(self.details.loc[0, 'family_name'])

        return set([given, family])

    def import_crossref(self, crossref_result: dict):

        if 'given' in crossref_result.keys():
            self.details.loc[0, 'given_name'] = crossref_result['given']
        
        if 'family' in crossref_result.keys():
            self.details.loc[0, 'family_name'] = crossref_result['family']
        
        if 'email' in crossref_result.keys():
            self.details.loc[0, 'email'] = crossref_result['email']

        if 'affiliation' in crossref_result.keys():
            self.details.at[0, 'affiliations'] = crossref_result['affiliation'][0]

        if 'ORCID' in crossref_result.keys():
            self.details.loc[0, 'orcid'] = crossref_result['ORCID']

        # self.details.loc[0, 'google_scholar'] = google_scholar
        # self.details.loc[0, 'crossref'] = crossref
        # self.details.loc[0, 'other_links'] = other_links

        self.update_full_name()
    
    def from_crossref(crossref_result: dict): # type: ignore

        author = Author()
        author.import_crossref(crossref_result)

        return author
    


class Authors:

    """
    This is an Authors object. It contains a collection of Authors objects and compiles data about them.
    
    Parameters
    ----------
    
    
    Attributes
    ----------
    """

    def __init__(self, authors_data = None):
        
        """
        Initialises Authors instance.
        
        Parameters
        ----------
        """

        self.all = pd.DataFrame(columns = [
                                'author_id',
                                'full_name',
                                'given_name',
                                'family_name',
                                'email',
                                'affiliations',
                                'publications',
                                'orcid',
                                'google_scholar',
                                'crossref',
                                'other_links'
                                ],
                                dtype = object)
        
        self.data = authors_data

        if (type(authors_data) == list) and (type(authors_data[0]) == Author):

            for i in authors_data:
                auth = i.details.copy(deep=True)
                self.all = pd.concat([self.all, auth])

            self.all = self.all.reset_index().drop('index',axis=1)

        else:

            if type(authors_data) == dict:
                
                values = list(authors_data.values())

                if type(values[0]) == Author:

                    for a in authors_data.keys():
                        
                        index = len(self.all)
                        auth = a.details.copy(deep=True)
                        self.all = pd.concat([self.all, auth])
                        self.all.loc[index, 'author_id'] = a

                    self.all = self.all.reset_index().drop('index',axis=1)
                


    def __getitem__(self, key):
        
        """
        Retrieves Authors attribute using a key.
        """
        
        return self.__dict__[key]
    
    def __repr__(self) -> str:
        return self.all['full_name'].to_list().__repr__()
    
    def __add__(self, authors):

        left = self.all.copy(deep=True)
        right = authors.all.copy(deep=True)
        
        merged = pd.concat([left, right])

        self.all = merged

        left_data = self.data
        right_data = authors.data

        if left_data == None:
                left_data = []
            
        if right_data == None:
                right_data = []

        if (type(left_data) == Author) or (type(left_data) == str):
                left_data = [left_data]
            
        if (type(right_data) == Author) or (type(right_data) == str):
                right_data = [right_data]
            
        if type(left_data) == dict:
                left_data = list(left_data.values())
            
        if type(right_data) == dict:
                right_data = list(right_data.values())

        merged_data = left_data + right_data # type: ignore
        self.data = merged_data

        return self

    def add_author(self, author: Author):

        data = author.details
        self.all = pd.concat([self.all, data])
        self.all = self.all.reset_index().drop('index', axis=1)

    def add_authors_list(self, authors_list: list):
        
        for i in authors_list:
            if type(i) == Author:
                self.add_author(author = i)

    def import_crossref(self, crossref_result: list):

        for i in crossref_result:

            auth = Author.from_crossref(i) # type: ignore
            self.add_author(author = auth)
    
    def from_crossref(crossref_result: list): # type: ignore

        authors = Authors()
        authors.import_crossref(crossref_result)

        return authors

def extract_authors(author_data: list):

    try:
        if (author_data == None) or (author_data == ''):
            result = Authors()

        if type(author_data) == Authors:
            result = author_data

        if (type(author_data) == list) and (type(author_data[0]) == Author):
            result = Authors()
            result.add_authors_list(author_data)

        if (type(author_data) == list) and (type(author_data[0]) == dict):
            result = Authors.from_crossref(author_data) # type: ignore

        if (type(author_data) == dict) and (type(list(author_data.values())[0]) == str):
            author = Author.from_crossref(author_data) # type: ignore
            result = Authors()
            result.add_author(author)
    except:
        result = Authors()
    
    return result

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
        self.activity_log = ActivityLog()
        self.description = ''
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
        
        self.properties.review_id = id(self)
        self.properties.size = str(self.__sizeof__()) + ' bytes'
        self.properties.update_last_changed()
    
    def __repr__(self):
        
        """
        Defines how Reviews are represented in string form.
        """
        
        output = f'\n{"-"*(13+len(self.properties.review_name))}\nReview name: {self.properties.review_name}\n{"-"*(13+len(self.properties.review_name))}\n\nProperties:\n-----------\n{self.properties}\n\nDescription:\n------------\n\n{self.description}\n\nResults:\n--------\n\n{self.results}\n\nAuthors:\n--------\n\n{self.authors.all.head(10)}'
        
        return output
            
    def __iter__(self):
        
        """
        Implements iteration functionality for Review objects.
        """
        
        return Iterator(self)
    
    def __getitem__(self, index, col_index = None):
        
        """
        Retrieves Review contents or results using an index/key.
        """
        
        if index in self.__dict__.keys():
            return self.__dict__[index]

        else:

            if col_index == None:

                try:
                    self.results[index]
                except:
                    try:
                        self.results.loc[index]
                    except:
                        raise KeyError('Index not found')
            
            else:
                self.results.loc[index, col_index]

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
    
    def add_pdf(self, path = 'request_input'):
        
        self.results.add_pdf(path)
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

    def from_dataframe(dataframe): # type: ignore
        
        review = Review()
        review.results = Results.from_dataframe(dataframe)
        review.results.extract_authors()

        return review

    def extract_citations(self):
        return self.results.extract_citations() # type: ignore

    def import_excel(self, file_path = 'request_input', sheet_name = None):
        self.update_properties()
        return self.results.import_excel(file_path, sheet_name) # type: ignore
    
    def from_excel(file_path = 'request_input', sheet_name = None): # type: ignore

        review = Review()
        review.results = Results.from_excel(file_path, sheet_name)
        review.results.extract_authors() # type: ignore

        return review

    def import_csv(self, file_path = 'request_input'):
        self.update_properties()
        return self.results.import_csv(file_path) # type: ignore
    
    def from_csv(file_path = 'request_input'):

        review = Review()
        review.results = Results.from_csv(file_path)
        review.results.extract_authors()

        return review

    def import_json(self, file_path = 'request_input'):
        self.update_properties()
        return self.results.import_json(file_path) # type: ignore
    
    def from_json(file_path = 'request_input'):

        review = Review()
        review.results = Results.from_json(file_path)
        review.results.extract_authors() # type: ignore

        return review
    
    def import_file(self, file_path = 'request_input', sheet_name = None):
        self.update_properties()
        return self.results.import_file(file_path, sheet_name)
    
    def from_file(file_path = 'request_input', sheet_name = None):
        
        review = Review()
        review.results = Results.from_file(file_path, sheet_name)
        review.results.extract_authors() # type: ignore

        return review

    def import_jstor_metadata(self, file_path = 'request_input', clean_results = True):
        self.results.import_jstor_metadata(file_path = file_path, clean_results = clean_results)
    
    def import_jstor_full(self, file_path = 'request_input', clean_results = True):
        self.results.import_jstor_full(file_path = file_path, clean_results = clean_results)

    def search_field(self, field = 'request_input', any_kwds = 'request_input', all_kwds = None, not_kwds = None, case_sensitive = False, output = 'Results'):
        return self.results.search_field(field = field, any_kwds = any_kwds, all_kwds = all_kwds, not_kwds = not_kwds, case_sensitive = case_sensitive, output = output)

    def search(self, any_kwds = 'request_input', all_kwds = None, not_kwds = None, fields = 'all', case_sensitive = False, output = 'Results'):
        return self.results.search(fields = fields, any_kwds = any_kwds, all_kwds = all_kwds, not_kwds = not_kwds, case_sensitive = case_sensitive, output = output)

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
        self.results.add_dataframe(df)

    def scrape_doi(self, doi = 'request_input'):
        
        if doi == 'request_input':
            doi = input('DOI or URL: ')

        df = scrape_doi(doi)
        self.results.add_dataframe(df)

    def scrape_google_scholar(self, url = 'request_input'):

        if url == 'request_input':
            url = input('URL: ')

        df = scrape_google_scholar(url)
        self.results.add_dataframe(df)
    
    def scrape_google_scholar_search(self, url = 'request_input'):

        if url == 'request_input':
            url = input('URL: ')

        df = scrape_google_scholar_search(url)
        self.results.add_dataframe(df)
    
    def search_crossref(self,
                bibliographic: str = None,
                title: str = None,
                author: str = None,
                author_affiliation: str = None,
                editor: str = None,
                entry_type: str = None,
                published_date: str = None,
                DOI: str = None,
                ISSN: str = None,
                publisher_name: str = None,
                funder_name = None,
                source: str = None,
                link: str = None,
                filter: dict = None,
                select: list = None,
                sample: int = None,
                limit: int = None,
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
            self.results.add_dataframe(dataframe=df)
        
        return df

    def lookup_doi(self, doi = 'request_input', timeout = 60):
        return lookup_doi(doi=doi, timeout=timeout)
    
    def add_doi(self, doi = 'request_input', timeout = 60):
        return self.results.add_doi(doi=doi, timeout=timeout)

    def lookup_dois(self, dois_list: list = [], rate_limit: float = 0.1, timeout = 60):
        return lookup_dois(dois_list=dois_list, rate_limit=rate_limit, timeout=timeout)
    
    def add_dois(self, dois_list: list = [], rate_limit: float = 0.1, timeout = 60):
        return self.results.add_dois(dois_list=dois_list, rate_limit=rate_limit, timeout=timeout)
    
    def lookup_journal(self, issn = 'request_input', timeout = 60):
        return lookup_journal(issn = issn, timeout = timeout)
    
    def lookup_journals(self, issns_list: list = [], rate_limit: float = 0.1, timeout: int = 60):
        return lookup_journals(issns_list = issns_list, rate_limit = rate_limit, timeout = timeout)
    
    def search_journals(self, *args, limit: int = None, rate_limit: float = 0.1, timeout = 60):
        return search_journals(*args, limit = limit, rate_limit=rate_limit, timeout = timeout)
    
    def get_journal_entries(self,
                        issn = 'request_input',
                        filter: dict = None,
                        select: list = None,
                        sample: int = None,
                        limit: int = None,
                        rate_limit: float = 0.1,
                        timeout = 60):
        
        return get_journal_entries(issn = issn, filter = filter, select = select, sample = sample, limit = limit, rate_limit = rate_limit, timeout = timeout)
    
    def search_journal_entries(
                        self,
                        issn = 'request_input',
                        bibliographic: str = None,
                        title: str = None,
                        author: str = None,
                        author_affiliation: str = None,
                        editor: str = None,
                        entry_type: str = None,
                        published_date: str = None,
                        DOI: str = None,
                        publisher_name: str = None,
                        funder_name: str = None,
                        source: str = None,
                        link: str = None,
                        filter: dict = None,
                        select: list = None,
                        sample: int = None,
                        limit: int = None,
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
                self.results.add_dataframe(dataframe=df)
        
            return df
    
    def lookup_funder(self, funder_id = 'request_input', timeout = 60):
        return lookup_funder(funder_id = funder_id, timeout = timeout)
    
    def lookup_funders(self, funder_ids: list = [], rate_limit: float = 0.1, timeout = 60):
        return lookup_funders(funder_ids=funder_ids, rate_limit=rate_limit, timeout = timeout)
    
    def search_funders(self, *args, limit: int = None, rate_limit: float = 0.1, timeout = 60):
        return search_funders(*args, limit=limit, rate_limit=rate_limit, timeout=timeout)
    
    def get_funder_works(self,
                        funder_id = 'request_input',
                        filter: dict = None,
                        select: list = None,
                        sample: int = None,
                        limit: int = None,
                        rate_limit: float = 0.1,
                        timeout: int = 60,
                        add_to_results: bool = False):
        
        df = get_funder_works(funder_id=funder_id, filter=filter, select=select, sample=sample, limit=limit, rate_limit=rate_limit, timeout=timeout)

        if add_to_results == True:
                self.results.add_dataframe(dataframe=df)
        
        return df
    
    def search_funder_works(self,
                        funder_id = 'request_input',
                        bibliographic: str = None,
                        title: str = None,
                        author: str = None,
                        author_affiliation: str = None,
                        editor: str = None,
                        entry_type: str = None,
                        published_date: str = None,
                        DOI: str = None,
                        publisher_name: str = None,
                        funder_name = None,
                        source: str = None,
                        link: str = None,
                        filter: dict = None,
                        select: list = None,
                        sample: int = None,
                        limit: int = None,
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
                self.results.add_dataframe(dataframe=df)
        
        return df

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