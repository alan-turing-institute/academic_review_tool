from .utils.basics import Iterator, results_cols
from .utils.cleaners import strip_list_str
from .exporters.general_exporters import obj_to_folder
from .importers.pdf import read_pdf_to_table
from .importers.jstor import import_jstor_metadata, import_jstor_full
from .importers.crossref import items_to_df, references_to_df, search_works, lookup_doi, lookup_dois, lookup_journal, lookup_journals, search_journals, get_journal_entries, search_journal_entries, lookup_funder, lookup_funders, search_funders, get_funder_works, search_funder_works
from .importers.orcid import lookup_orcid
from .datasets import stopwords
from .internet.scrapers import scrape_article, scrape_doi, scrape_google_scholar, scrape_google_scholar_search

import copy
from datetime import datetime
import pickle
from pathlib import Path

import pandas as pd
import numpy as np
from nltk.tokenize import word_tokenize # type: ignore

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

def generate_work_id(work_data: pd.Series):

        work_id = 'W:'
        work_data = work_data.astype(str)
        authors = work_data['authors']
        title = work_data['title']
        date = work_data['date']

        if (authors != None) and (authors != '') and (authors != 'None'):
            authors_str = str(authors).lower().strip().replace('[','').replace(']','').replace("'", "").replace('"', '').replace(' ','-')
            authors_list = authors_str.split(',')
            authors_list = [i.strip() for i in authors_list]
            if len(authors_list) > 0:
                first_author = authors_list[0]
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
            work_id = work_id + '-' + title_shortened
        
        if (date != None) and (date != '') and (date != 'None'):
            work_id = work_id + '-' + str(date)

        uid = work_data['doi']
        if (uid == None) or (uid == 'None') or (uid == ''):
            uid = work_data['isbn']
            if (uid == None) or (uid == 'None') or (uid == ''):
                uid = work_data['issn']
                if (uid == None) or (uid == 'None') or (uid == ''):
                    uid = work_data['link']
                    if (uid == None) or (uid == 'None') or (uid == ''):
                        uid = ''
        
        uid_shortened = uid.replace('https://', '').replace('http://', '').replace('www.', '').replace('doi.org.','').replace('scholar.google.com/','')[:30]

        work_id = work_id + '-' + uid_shortened
        work_id = work_id.replace('W:-', 'W:').strip('-').strip('.')
        work_id = work_id[:35]

        return work_id
    

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

    def get(self, work_id: str):

        indexes = self[self['work_id'] == work_id].index.to_list()
        if len(indexes) > 0:
            index = indexes[0]
            return self.loc[index]
        else:
            raise KeyError('work_id not found')

    def add_pdf(self, path = 'request_input'):
        
        if path == 'request_input':
            path = input('Path to PDF (URL or filepath): ')

        table = read_pdf_to_table(path)
        table = table.replace(np.nan, None).astype(object)

        series = table.loc[0]
        work_id = generate_work_id(series)
        series['work_id'] = work_id

        index = len(self)
        self.loc[index] = series

        self.format_authors()

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

        work_id = generate_work_id(data)
        work_id = self.get_unique_id(work_id, index)
        data['work_id'] = work_id

        
        self.loc[index] = data
        self.format_authors()
        
    def get_unique_id(self, work_id, index):

        if (type(work_id) == str) and (work_id != ''):
            try:
                work_id = str(work_id.split('#')[0])
                if work_id in self['work_id'].to_list():
                
                    df = self.copy(deep=True).astype(str)
                    df['work_id'] = df['work_id'].astype(str)
                    masked = df[df['work_id'].str.contains(work_id)]
                    masked_indexes = masked.index.to_list()
                    if index not in masked_indexes:
                        id_count = len(masked) # type: ignore
                        work_id = work_id + f'#{id_count + 1}'
            except:
                pass
        return work_id

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
            work_id = generate_work_id(dataframe.loc[i])
            work_id = self.get_unique_id(work_id, i)
            self.loc[index, 'work_id'] = work_id
            index += 1
        
        self.format_authors()

    def add_doi(self, doi: str = 'request_input', timeout: int = 60):
        df = lookup_doi(doi=doi, timeout=timeout)
        self.add_dataframe(dataframe=df)

    def add_dois(self, dois_list: list = [], rate_limit: float = 0.1, timeout = 60):
        df = lookup_dois(dois_list=dois_list, rate_limit=rate_limit, timeout=timeout)
        self.add_dataframe(dataframe=df)

    def correct_dois(self):

        no_doi = self[(self['doi'] == None) | (self['doi'] == np.nan) | (self['doi'] == 'None')]
        doi_in_link = no_doi[no_doi['link'].str.contains('doi.org')]

        for i in doi_in_link.index:
            link = str(doi_in_link.loc[i, 'link'])
            doi = link.replace('http://', '').replace('https://', '').replace('www.', '').replace('dx.', '').replace('doi.org/', '').strip()
            doi_in_link.loc[i, 'doi'] = doi

    def generate_work_ids(self):

        for i in self.index:
            work_id = generate_work_id(self.loc[i])
            self.loc[i, 'work_id'] = work_id

    def update_work_ids(self):

        for i in self.index:
            work_id = generate_work_id(self.loc[i])
            if self.loc[i, 'work_id'] != work_id:
                work_id = self.get_unique_id(work_id, i)
                self.loc[i, 'work_id'] = work_id


    def update_from_doi(self, index, timeout: int = 60):
        
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

    def update_from_dois(self, timeout: int = 60):

        self.correct_dois()

        for i in self.index:
            self.update_from_doi(index = i, timeout=timeout)

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
        
        results_table.format_authors()

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
        
        self.format_authors()

        return self

    def from_excel(file_path = 'request_input', sheet_name = None):

        results_table = Results()
        results_table = results_table.import_excel(file_path, sheet_name).replace(np.nan, None)
        results_table.format_authors() # type: ignore


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
            
            self.format_authors()

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
        self.format_authors() # type: ignore

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

        frequent_kws = keywords_freq[keywords_freq.values > cutoff] # type: ignore
        frequent_kws = list(frequent_kws.index)
        
        output = pd.DataFrame(dtype=object)
        for i in frequent_kws:
            df = self.search(any_kwds = i).copy(deep=True)
            output = pd.concat([output, df])

        output = output.drop_duplicates('title')
        output = output.reset_index().drop('index', axis=1)

        print(f'Keywords: {frequent_kws}')

        return self.loc[output.index]
    
    def format_citations(self):

        self['citations'] = self['citations'].replace({np.nan: None})
        self['citations_data'] = self['citations_data'].replace({np.nan: None})
        self['citations'] = self['citations_data'].apply(extract_references) # type: ignore
        
        return self['citations']
    
    def format_authors(self):

        self['authors'] = self['authors_data'].apply(format_authors) # type: ignore
        return self['authors']

    def add_citations_to_results(self):
        
        self.format_citations()
        citations = self['citations'].to_list()
        
        for i in citations:
            if (type(i) == References) or (type(i) == Results) or (type(i) == pd.DataFrame):
                df = i.copy(deep=True)
                self.add_dataframe(dataframe=df)
        
        self.format_authors()

        return self




    

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

    refs = References()

    if type(references_list) == References:
        refs = references_list

    if (references_list is np.nan) or (references_list == None):
        df = pd.DataFrame(columns=results_cols, dtype=object)
        df.replace({np.nan: None})
        refs = References.from_dataframe(df) # type: ignore
        refs.generate_work_ids()

    if (type(references_list) == list) and (type(references_list[0]) == dict):
        df = references_to_df(references_list)
        df.replace({np.nan: None})
        refs = References.from_dataframe(df) # type: ignore
        refs.generate_work_ids()
    
    if (type(references_list) == list) and (type(references_list[0]) == str):
        df = pd.DataFrame(columns=results_cols, dtype=object)
        df['link'] = pd.Series(references_list, dtype=object)
        df.replace({np.nan: None})

        refs = References.from_dataframe(df) # type: ignore
        refs.generate_work_ids()

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

def generate_author_id(author_data: pd.Series):

        author_id = 'A:'

        given_name = author_data['given_name']
        family_name = author_data['family_name']
        full_name = author_data['full_name']

        if (family_name == None) and (full_name != None):
            
            if full_name == 'no_name_given':
                author_id = author_id + '-' + '000'
            
            else:
            
                full_split = full_name.lower().split(' ')
                first = full_split[0]
                first_shortened = first[0]
                last = full_split[-1]

                author_id = author_id + '-' + first_shortened + '-' + last

        else:

            if given_name != None:
                given_shortened = given_name.lower()[0]
                author_id = author_id + '-' + given_shortened
            
            
            if family_name != None:
                family_clean = family_name.lower().replace(' ', '-')
                author_id = author_id + '-' + family_clean

        uid = author_data['orcid']
        if (uid == None) or (uid == 'None') or (uid == ''):
            uid = author_data['google_scholar']
            if (uid == None) or (uid == 'None') or (uid == ''):
                uid = author_data['scopus']
                if (uid == None) or (uid == 'None') or (uid == ''):
                    uid = author_data['crossref']
                    if (uid == None) or (uid == 'None') or (uid == ''):
                        uid = ''
        
        uid_shortened = uid.replace('https://', '').replace('http://', '').replace('www.', '').replace('orcid.org/','').replace('scholar.google.com/','').replace('citations?','').replace('user=','')[:20]

        author_id = author_id + '-' + uid_shortened
        author_id = author_id.replace('A:-', 'A:').strip('-')

        return author_id


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
                 full_name = None, # type: ignore
                 given_name = None, # type: ignore
                 family_name = None, # type: ignore
                 email: str = None, # type: ignore
                 affiliations: str = None, # type: ignore
                 publications: str = None, # type: ignore
                 orcid: str = None, # type: ignore
                 google_scholar: str = None, # type: ignore
                 scopus: str = None, # type: ignore
                 crossref: str = None, # type: ignore
                 other_links: str = None # type: ignore
                 ):
        
        """
        Initialises Author instance.
        
        Parameters
        ----------
        """

        if type(given_name) == str:
            given_name = given_name.strip()

        if type(family_name) == str:
            family_name = family_name.strip()

        if ((type(family_name) == str) and (',' in family_name)) and ((given_name == None) or (given_name == '')):
            split_name = family_name.split(',')
            given_name = split_name[0].strip()
            family_name = split_name[1].strip()

        

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
                                'scopus',
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
        self.details.loc[0, 'scopus'] = scopus
        self.details.loc[0, 'crossref'] = crossref
        self.details.loc[0, 'other_links'] = other_links

        full_name = self.get_full_name()
        if full_name != self.details.loc[0, 'full_name']:
            self.details.loc[0, 'full_name'] = full_name

        self.publications = Results()

    def generate_id(self):

        author_data = self.details.loc[0]

        author_id = generate_author_id(author_data) # type: ignore
        return author_id

    def update_id(self):

        current_id = self.details.loc[0, 'author_id']

        if (current_id == None) or (current_id == 'None') or (current_id == '') or (current_id == 'A:000'):
            auth_id = self.generate_id()
            self.details.loc[0, 'author_id'] = auth_id
        
    
    def __getitem__(self, key):
        
        """
        Retrieves Author attribute using a key.
        """
        
        if key in self.__dict__.keys():
            return self.__dict__[key]
        
        if key in self.details.columns:
            return self.details.loc[0, key]
        
        if key in self.publications.columns:
            return self.publications[key]

    def __repr__(self) -> str:
        return str(self.details.loc[0, 'full_name'])
    
    def get_full_name(self):

            given = self.details.loc[0, 'given_name']
            family = self.details.loc[0, 'family_name']

            if given == None:
                given = ''
            
            if family == None:
                family = ''

            if ((type(family) == str) and (',' in family)) and ((given == None) or (given == 'None') or (given == '')):
                split_name = family.split(',')
                given = split_name[0].strip()
                self.details.loc[0, 'given_name'] = given

                family = split_name[1].strip()
                self.details.loc[0, 'family_name'] = family

            full = given + ' ' + family
            full = full.strip()

            if (full == '') or (full == ' '):
                full = 'no_name_given'

            full_name = self.details.loc[0, 'full_name']
            if (full_name == None) or (full_name == 'None') or (full_name == '') or (full_name == 'no_name_given'):
                result = full
            else:
                result = full_name

            return result


    def update_full_name(self):

            full_name = self.get_full_name()
            self.details.loc[0, 'full_name'] = full_name

    def name_set(self) -> set:

        given = str(self.details.loc[0, 'given_name'])
        family = str(self.details.loc[0, 'family_name'])

        return set([given, family])

    def has_orcid(self) -> bool:

        orcid = self.details.loc[0, 'orcid']

        if (type(orcid) == str) and (orcid != ''):
            return True
        else:
            return False

    def add_series(self, series: pd.Series):
        self.details.loc[0] = series

    def from_series(series: pd.Series): # type: ignore
        author = Author()
        author.add_series(series)
    
    def add_dataframe(self, dataframe: pd.DataFrame):
        series = dataframe.loc[0]
        self.add_series(series) # type: ignore

    def from_dataframe(dataframe: pd.DataFrame): # type: ignore
        author = Author()
        author.add_dataframe(dataframe)

    def import_crossref(self, crossref_result: dict):

        if 'given' in crossref_result.keys():
            self.details.loc[0, 'given_name'] = crossref_result['given']
        
        if 'family' in crossref_result.keys():
            self.details.loc[0, 'family_name'] = crossref_result['family']
        
        if 'email' in crossref_result.keys():
            self.details.loc[0, 'email'] = crossref_result['email']

        if 'affiliation' in crossref_result.keys():
            if (type(crossref_result['affiliation']) == list) and (len(crossref_result['affiliation']) > 0):
                self.details.at[0, 'affiliations'] = crossref_result['affiliation'][0]

            else:
                if (type(crossref_result['affiliation']) == dict) and (len(crossref_result['affiliation'].keys()) > 0):
                    key = list(crossref_result['affiliation'].keys())[0]
                    self.details.at[0, 'affiliations'] = crossref_result['affiliation'][key]

        if 'ORCID' in crossref_result.keys():
            self.details.loc[0, 'orcid'] = crossref_result['ORCID']
        
        else:
            if 'orcid' in crossref_result.keys():
                self.details.loc[0, 'orcid'] = crossref_result['orcid']

        # self.details.loc[0, 'google_scholar'] = google_scholar
        # self.details.loc[0, 'crossref'] = crossref
        # self.details.loc[0, 'other_links'] = other_links

        self.update_full_name()
    
    def import_orcid(self, orcid_id: str):

        auth_df = lookup_orcid(orcid_id)
        cols = auth_df.columns.to_list()

        if len(auth_df) > 0:

            author_details = auth_df.loc[0]

            if 'name' in cols:
                self.details.loc[0, 'given_name'] = author_details['name']
            
            if 'family name' in cols:
                self.details.loc[0, 'family_name'] = author_details['family name']
            
            if 'emails' in cols:
                self.details.at[0, 'email'] = author_details['emails']
            
            if 'employment' in cols:
                self.details.at[0, 'affiliations'] = author_details['employment']
            
            if 'works' in cols:
                self.details.at[0, 'publications'] = author_details['works']
            
            self.details.loc[0, 'orcid'] = orcid_id

            self.update_full_name()
        
        else:
            self.details.loc[0, 'orcid'] = orcid_id
            self.update_full_name()
        

    def from_crossref(crossref_result: dict): # type: ignore

        author = Author()
        author.import_crossref(crossref_result)
        author.update_full_name()

        return author
    
    def from_orcid(orcid_id: str): # type: ignore

        author = Author()
        author.import_orcid(orcid_id)
        author.update_full_name()

        return author
    
    def update_from_orcid(self):

        orcid = self.details.loc[0, 'orcid']

        if (orcid != None) and (orcid != '') and (orcid != 'None'):
            
            orcid = str(orcid).replace('https://', '').replace('http://', '').replace('orcid.org/', '')

            self.import_orcid(orcid_id = orcid)

        
    





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
        

        self.details = dict()

        self.data = []
        self.data.append(authors_data)

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
        
        if key in self.__dict__.keys():
            return self.__dict__[key]
        
        if key in self.details.keys():
            return self.details[key]

        if key in self.all.columns:
            return self.all[key]
        
        if (type(key) == int) and (key <= len(self.data)):
            return self.data[key]
    
    def __repr__(self) -> str:
        return self.all['full_name'].to_list().__repr__()
    
    def merge(self, authors):

        left = self.all.copy(deep=True)
        right = authors.all.copy(deep=True)
        
        merged = pd.concat([left, right])

        self.all = merged.drop_duplicates(subset=['author_id', 'family_name', 'orcid'], ignore_index=True)

        for i in authors.details.keys():
            if i not in self.details.keys():
                self.details[i] = authors.details[i]

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
        merged_data = pd.Series(merged_data).value_counts().index.to_list()

        self.data = merged_data

        return self

    def add_author(self, author: Author, data = None, update_from_orcid = False):

        if update_from_orcid == True:
            orcid = author.details.loc[0,'orcid']
            if (orcid != None) and (orcid != '') and (orcid != 'None'):
                author.update_from_orcid()

        author.update_id()

        author_id = str(author.details.loc[0, 'author_id'])

        if author_id in self.all['author_id'].to_list():
            id_count = len(self.all[self.all['author_id'].str.contains(author_id)]) # type: ignore
            author_id = author_id + f'#{id_count + 1}'
            author.details.loc[0, 'author_id'] = author_id

        self.all = pd.concat([self.all, author.details])
        self.all = self.all.reset_index().drop('index', axis=1)

        self.details[author_id] = author

        if data == None:
            data = author.details.to_dict(orient='index')
        
        self.data.append(data)


    def add_authors_list(self, authors_list: list):
        
        for i in authors_list:
            if type(i) == Author:
                self.add_author(author = i)

    def sync_all(self):

        for i in self.details.keys():
            author = self.details[i]
            author.update_id()
            series = author.details.loc[0]
            all = self.all.copy(deep=True).astype(str)
            auth_index = all[all['author_id'] == i].index.to_list()[0]
            self.all.loc[auth_index] = series

    def update_from_orcid(self):

        author_ids = self.details.keys()

        for a in author_ids:

            self.details[a].update_from_orcid()
            details = self.details[a].details.loc[0]
            
            df_index = self.all[self.all['author_id'] == a].index.to_list()[0]
            self.all.loc[df_index] = details

            new_id = details['author_id']
            if new_id != a:
                self.details[new_id] = self.details[a]
                del self.details[a]

    def import_orcid_ids(self, orcid_ids: list):

        for i in orcid_ids:

            auth = Author.from_orcid(i) # type: ignore
            self.add_author(author = auth, data = i)

    def from_orcid_ids(orcid_ids: list): # type: ignore

        authors = Authors()
        authors.import_orcid_ids(orcid_ids)

        return authors

    def with_orcid(self):
        return self.all[~self.all['orcid'].isna()]

    def import_crossref(self, crossref_result: list):

        for i in crossref_result:

            auth = Author.from_crossref(i) # type: ignore
            self.add_author(author = auth, data = i)
    
    def from_crossref(crossref_result: list): # type: ignore

        authors = Authors()
        authors.import_crossref(crossref_result)

        return authors

def format_authors(author_data: list):
        
        result = Authors()

        if (author_data == None) or (author_data == ''):
            result = Authors()

        if type(author_data) == Authors:
            result = author_data

        if (type(author_data) == list) and (len(author_data) > 0) and (type(author_data[0]) == Author):
            result = Authors()
            result.add_authors_list(author_data)

        if (type(author_data) == list) and (len(author_data) > 0) and (type(author_data[0]) == dict):
            result = Authors.from_crossref(author_data) # type: ignore

        if (type(author_data) == dict) and (len(author_data.values()) > 0) and (type(list(author_data.values())[0]) == str):
            author = Author.from_crossref(author_data) # type: ignore
            result = Authors()
            result.add_author(author)
    
    
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
        review.results = Results.from_dataframe(dataframe) # type: ignore
        review.format_authors()

        return review

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
    
    def add_citations_to_results(self):
        self.results.add_citations_to_results()
        self.format_authors()


    def update_from_orcid(self):
        self.authors.update_from_orcid()

    def import_excel(self, file_path = 'request_input', sheet_name = None):
        self.update_properties()
        return self.results.import_excel(file_path, sheet_name) # type: ignore
    
    def from_excel(file_path = 'request_input', sheet_name = None): # type: ignore

        review = Review()
        review.results = Results.from_excel(file_path, sheet_name)
        review.format_authors() # type: ignore

        return review

    def import_csv(self, file_path = 'request_input'):
        self.update_properties()
        return self.results.import_csv(file_path) # type: ignore
    
    def from_csv(file_path = 'request_input'):

        review = Review()
        review.results = Results.from_csv(file_path)
        review.format_authors()

        return review

    def import_json(self, file_path = 'request_input'):
        self.update_properties()
        return self.results.import_json(file_path) # type: ignore
    
    def from_json(file_path = 'request_input'):

        review = Review()
        review.results = Results.from_json(file_path)
        review.format_authors() # type: ignore

        return review
    
    def import_file(self, file_path = 'request_input', sheet_name = None):
        self.update_properties()
        return self.results.import_file(file_path, sheet_name)
    
    def from_file(file_path = 'request_input', sheet_name = None):
        
        review = Review()
        review.results = Results.from_file(file_path, sheet_name)
        review.format_authors() # type: ignore

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
            self.format_authors()
        
        return df

    def lookup_doi(self, doi = 'request_input', timeout = 60):
        return lookup_doi(doi=doi, timeout=timeout)
    
    def add_doi(self, doi = 'request_input', timeout = 60):
        return self.results.add_doi(doi=doi, timeout=timeout)

    def lookup_dois(self, dois_list: list = [], rate_limit: float = 0.1, timeout = 60):
        return lookup_dois(dois_list=dois_list, rate_limit=rate_limit, timeout=timeout)
    
    def add_dois(self, dois_list: list = [], rate_limit: float = 0.1, timeout = 60):
        return self.results.add_dois(dois_list=dois_list, rate_limit=rate_limit, timeout=timeout)
    
    def update_from_dois(self, timeout: int = 60):
        self.results.update_from_dois(timeout=timeout) # type: ignore
        self.format_authors()
        self.format_citations()

    def sync_apis(self, timeout: int = 60):

        self.update_from_dois(timeout=timeout)
        self.update_from_orcid()

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
                self.format_authors()
        
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
                self.format_authors()
        
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
                self.format_authors()
        
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