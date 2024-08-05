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

        work_data = work_data.copy(deep=True).dropna()

        work_id = 'W:'
        
        if 'authors' in work_data.index:
            auths_type = type(work_data['authors'])
            auths_type_str = str(auths_type)

            if auths_type == list:
                work_data['authors'] = pd.Series(work_data['authors'],  dtype=object).sort_values().to_list()
            
            else:
                if '.Authors' in auths_type_str:
                    work_data['authors'] = work_data['authors'].all['full_name'].sort_values().to_list()
            

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
            super().__init__(dtype=object, columns = results_cols, index = index # type: ignore
                            ) 
            
            self.replace(np.nan, None)
        
        else:
            df = dataframe
            if type(dataframe) == pd.DataFrame:
                self = Results.from_dataframe(dataframe = df)

    def drop_empty_rows(self):

        ignore_cols = ['work_id', 'authors', 'funder', 'citations']

        df = self.dropna(axis=0, how='all')
        drop_cols = [c for c in df.columns if c not in ignore_cols]
        df = df.dropna(axis=0, how='all', subset=drop_cols).reset_index().drop('index', axis=1)

        results = Results.from_dataframe(dataframe=df, drop_duplicates=False) # type: ignore

        self.__dict__.update(results.__dict__)

        return self




    def remove_duplicates(self, drop_empty_rows = True, update_from_api = False):

        if drop_empty_rows == True:
            self.drop_empty_rows()

        self['doi'] = self['doi'].str.replace('https://', '', regex = False).str.replace('http://', '', regex = False).str.replace('dx.', '', regex = False).str.replace('doi.org/', '', regex = False).str.replace('doi/', '', regex = False)

        df = deduplicate(self)
        results = Results.from_dataframe(dataframe = df, drop_duplicates=False)

        if update_from_api == True:
            results.update_from_dois()
        
        results.update_work_ids()
        df2 = results.drop_duplicates(subset='work_id').reset_index().drop('index',axis=1)

        results2 = Results.from_dataframe(dataframe=df2, drop_duplicates=False) # type: ignore
        
        self.__dict__.update(results2.__dict__)

        return self

        

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
        work_id = generate_work_id(series) # type: ignore
        series['work_id'] = work_id

        index = len(self)
        self.loc[index] = series

        return self
    
    def add_row(self, data, drop_duplicates=True):

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
        df = lookup_doi(doi=doi, timeout=timeout)
        self.add_dataframe(dataframe=df)

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=False)
        
        if drop_empty_rows == True:
            self.drop_empty_rows()



    def add_dois(self, dois_list: list = [], drop_empty_rows = True, rate_limit: float = 0.1, timeout = 60):
        df = lookup_dois(dois_list=dois_list, rate_limit=rate_limit, timeout=timeout)
        self.add_dataframe(dataframe=df, drop_empty_rows = True)

    def correct_dois(self, drop_duplicates = False):

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

        for i in self.index:
            work_id = generate_work_id(self.loc[i])
            self.loc[i, 'work_id'] = work_id

    def update_work_ids(self, drop_duplicates = False):

        for i in self.index:
            work_id = generate_work_id(self.loc[i])
            if self.loc[i, 'work_id'] != work_id:
                # work_id = self.get_unique_id(work_id, i)
                self.loc[i, 'work_id'] = work_id
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=False)


    def update_from_doi(self, index, drop_empty_rows = True, drop_duplicates = False, timeout: int = 60):
        
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

        self.correct_dois(drop_duplicates=False)

        for i in self.index:
            self.update_from_doi(index = i, drop_duplicates = False, timeout=timeout)
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=False)
        
        if drop_empty_rows == True:
            self.drop_empty_rows()

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
    
    def from_dataframe(dataframe, drop_empty_rows = False, drop_duplicates = False): # type: ignore
        
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

        bib_data = self.to_pybtex()
        return bib_data.to_string('bibtex')
    
    def to_yaml(self):
        bib_data = self.to_pybtex()
        return bib_data.to_string('yaml')
    
    def export_bibtex(self, file_name = 'request_input', folder_path = 'request_input'):

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

        results = Results()
        self.__dict__.update(results.__dict__)

        return self

    def import_bibtex(self, file_path = 'request_input'):

        df = import_bibtex(file_path = file_path)
        self.add_dataframe(dataframe=df, drop_duplicates=False, drop_empty_rows=False)
    
    def from_bibtex(file_path = 'request_input'):

        results = Results()
        results.import_bibtex(file_path=file_path)

        return results


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
                    self.at[i, col] = strip_list_str(items) # type: ignore
                    
                except:
                    pass

        return self

    def from_excel(file_path = 'request_input', sheet_name = None): # type: ignore

        results_table = Results()
        results_table = results_table.import_excel(file_path, sheet_name).replace(np.nan, None) # type: ignore
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
                        self.at[i, col] = strip_list_str(items) # type: ignore
                        
                    except:
                        pass

            return self
    
    def from_csv(file_path = 'request_input'): # type: ignore

        results_table = Results()
        results_table.import_csv(file_path).replace(np.nan, None) # type: ignore


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
                        self.at[i, col] = strip_list_str(items) # type: ignore
                        
                    except:
                        pass

        self = self.replace(np.nan, None)

        return self
    
    def from_json(file_path = 'request_input'): # type: ignore

        results_table = Results()
        results_table.import_json(file_path).replace(np.nan, None) # type: ignore
        
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
    
    def from_file(file_path = 'request_input', sheet_name = None): # type: ignore

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

        df = import_jstor(file_path = file_path)
        self.add_dataframe(dataframe=df, drop_empty_rows = drop_empty_rows, drop_duplicates = drop_duplicates, update_work_ids = update_work_ids)

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
    
        output = []

        for i in self['keywords']:
            if type(i) == str:
                i = strip_list_str(i)  # type: ignore
            
            if type(i) == list:
                output = output + i

        output = pd.Series(output).str.strip().str.lower()
        output = output.drop(output[output.values == 'none'].index).reset_index().drop('index', axis=1)[0] # type: ignore
        
        return output
    
    def get_keywords_list(self):        
        return self.get_keywords().to_list()
    
    def get_keywords_set(self):
        return set(self.get_keywords_list())
    
    def keyword_frequencies(self):
        return self.get_keywords().value_counts()

    def keyword_stats(self):
        return self.keyword_frequencies().describe()
    
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

        results = self.search(any_kwds = keywords).index # type: ignore
        return self.drop(index = results, axis=0).reset_index().drop('index', axis=1)

    def filter_by_keyword_frequency(self, cutoff = 3):

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
        return self[~self['citations_data'].isna()]

    def has(self, column):
        return self[~self[column].isna()]
    
    def contains(self, query: str = 'request_input', ignore_case: bool = True) -> bool:

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

        try:
            funders = self['funder'].apply(format_funders) # type: ignore
        except:
            funders = self['funder']

        self['funder'] = funders

Entity.publications = Results() # type: ignore
Funder.publications = Results() # type: ignore


