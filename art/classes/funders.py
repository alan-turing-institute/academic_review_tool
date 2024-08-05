from ..utils.cleaners import deduplicate
from ..importers.crossref import search_funder_works, lookup_funder
from ..datasets.stopwords.stopwords import all_stopwords

from .entities import Entity, Entities

# from .results import Results

import pandas as pd
import numpy as np



from nltk.tokenize import word_tokenize # type: ignore

def generate_funder_id(funder_data: pd.Series):

        funder_data = funder_data.copy(deep=True).dropna().astype(str).str.lower()

        funder_id = 'F:'

        if 'name' in funder_data.index:
            name = funder_data['name']
        else:
            name = ''

        if (name == None) or (name == ''):
            name = 'no_name_given'
        
        if name != 'no_name_given':

            name = name.strip().lower()
            name_tokens = list(word_tokenize(name))
            name_tokens = [i for i in name_tokens if ((i not in all_stopwords) and (i != "'s"))]
            name_first_3 = name_tokens[:3]
            name_last = name_tokens[-1]

            if name_last in name_first_3:
                name_last = ''
            
            name_shortened = '-'.join(name_first_3) + '-' + name_last

        else:
            name_shortened = name

        funder_id = funder_id + '-' + name_shortened
        
        uid = ''

        if 'uri' in funder_data.index:
            uid = funder_data['uri']

            if (uid == None) or (uid == 'None') or (uid == ''):
                if 'crossref_id' in funder_data.index:
                    uid = funder_data['crossref_id']
                    
                    if (uid == None) or (uid == 'None') or (uid == ''):
                        if 'website' in funder_data.index:
                            uid = funder_data['website']

                            if (uid == None) or (uid == 'None') or (uid == ''):
                                    uid = ''
        
        uid_shortened = uid.replace('https://', '').replace('http://', '').replace('www.', '').replace('dx.','').replace('doi.org/','').replace('user=','')[:24]

        funder_id = funder_id + '-' + uid_shortened
        funder_id = funder_id.replace('F:-', 'F:').replace("'s", '').replace('\r', '').replace('\n', '').replace('.', '').replace("'", "").replace('"', '').replace('(','').replace(')','').replace('`','').replace('’','').replace('--', '-').replace('F:-', 'F:').strip('-')

        return funder_id

class Funder(Entity):

    """
    This is a Funder object. It is designed to store data about an individual funder and their publications.
    
    Parameters
    ----------
    
    
    Attributes
    ----------
    """

    def __init__(self,
                 funder_id: str = None, # type: ignore
                 name: str = None, # type: ignore
                 alt_names = [], # type: ignore
                 location: str = None, # type: ignore
                 email: str = None, # type: ignore
                 uri: str = None, # type: ignore
                 crossref_id: int = None, # type: ignore
                 work_count: int = None,  # type: ignore
                 tokens = [], # type: ignore
                 website: str = None,  # type: ignore
                 other_links = [], # type: ignore
                 use_api: bool = False
                 ):
        
        """
        Initialises funder instance.
        
        Parameters
        ----------
        """

        super().__init__()

        if type(name) == str:
            name = name.strip()

        if type(alt_names) == str:
            alt_names = alt_names.replace('[','').replace(']','').split(',')
            alt_names = [i.strip() for i in alt_names if type(i) == str]
        
        if name == None:
            if type(alt_names) == str:
                name = alt_names
            
            else:
                if (type(alt_names) == list) and (len(alt_names) > 0) and (type(alt_names[0]) == str):
                    name = alt_names[0]
        
        if type(other_links) == str:
            other_links = other_links.replace('[','').replace(']','').split(',')
            other_links = [i.strip() for i in other_links]
        
        if type(tokens) == str:
            tokens = tokens.replace('[','').replace(']','').split(',')
            tokens = [i.strip() for i in tokens]
        

        self.summary = pd.DataFrame(columns = [
                                'funder_id',
                                'name',
                                'alt_names',
                                'location',
                                'email',
                                'uri',
                                'crossref_id',
                                'work_count',
                                'tokens',
                                'website',
                                'other_links'
                                ],
                                dtype = object)
        
        
        self.summary.loc[0] = pd.Series(dtype=object)
        self.summary.loc[0, 'funder_id'] = funder_id
        self.summary.loc[0, 'name'] = name
        self.summary.at[0, 'alt_names'] = alt_names
        self.summary.loc[0, 'location'] = location
        self.summary.loc[0, 'email'] = email
        self.summary.loc[0, 'uri'] = uri
        self.summary.loc[0, 'crossref_id'] = crossref_id
        self.summary.loc[0, 'work_count'] = work_count   
        self.summary.at[0, 'tokens'] = tokens
        self.summary.loc[0, 'website'] = website
        self.summary.at[0, 'other_links'] = other_links

        # self.publications = Results()

        if use_api == True:
            self.update_from_crossref()
        
        self.update_id()

    def generate_id(self):

        funder_data = self.summary.loc[0]

        funder_id = generate_funder_id(funder_data) # type: ignore
        return funder_id

    def update_id(self):

        current_id = self.summary.loc[0, 'funder_id']

        if (current_id == None) or (current_id == 'None') or (current_id == '') or (current_id == 'F:000'):
            auth_id = self.generate_id()
            self.summary.loc[0, 'funder_id'] = auth_id
        
    
    def __getitem__(self, key):
        
        """
        Retrieves funder attribute using a key.
        """
        
        if key in self.__dict__.keys():
            return self.__dict__[key]
        
        if key in self.summary.columns:
            return self.summary.loc[0, key]
        
        # if key in self.publications.columns:
        #     return self.publications[key]

    def __repr__(self) -> str:
        
        return str(self.summary.loc[0, 'name'])

    def has_uri(self) -> bool:

        uri = self.summary.loc[0, 'uri']

        if (type(uri) == str) and (uri != ''):
            return True
        else:
            return False

    def add_dict(self, data: dict):

        if 'name' in data.keys():
            name = data['name']
            self.summary.loc[0, 'name'] = name

        if 'doi' in data.keys():
            uri = data['doi'].replace('http', '').replace('https', '').replace('dx.', '').replace('doi.org/', '').strip()
            self.summary.loc[0, 'uri'] = 'https://doi.org/' + uri
    
    def from_dict(data: dict, use_api=False): # type: ignore

        funder = Funder()
        funder.add_dict(data=data)

        if use_api == True:
            funder.update_from_crossref()

        return funder
        
    def add_series(self, series: pd.Series):
        self.summary.loc[0] = series

    def from_series(data: pd.Series): # type: ignore
        funder = Funder()
        funder.add_series(data)

        return funder

    def add_dataframe(self, dataframe: pd.DataFrame):
        series = dataframe.loc[0]
        self.add_series(series) # type: ignore

    def from_dataframe(data: pd.DataFrame): # type: ignore
        funder = Funder()
        funder.add_dataframe(data)

        return funder

    def import_crossref_result(self, crossref_result: pd.Series):
        
        if 'name' in crossref_result.index:
            name = crossref_result['name']
        else:
            name = self.summary.loc[0, 'name']

        if 'alt-names' in crossref_result.index:
            alt_names = crossref_result['alt-names']
        else:
            alt_names = self.summary.loc[0, 'alt_names']

        if 'location' in crossref_result.index:
            location = crossref_result['location']
        else:
            location = self.summary.loc[0, 'location']

        if 'email' in crossref_result.index:
            email = crossref_result['email']
        else:
            email = self.summary.loc[0, 'email']

        if 'uri' in crossref_result.index:
            uri  =crossref_result['uri']
        else:
            uri = self.summary.loc[0, 'uri']

        if 'id' in crossref_result.index:
            crossref_id = crossref_result['id']
        else:
            crossref_id = self.summary.loc[0, 'crossref_id']

        if 'work-count' in crossref_result.index:
            work_count = crossref_result['work-count']
        else:
            work_count = self.summary.loc[0, 'work_count']

        if 'tokens' in crossref_result.index:
            tokens = crossref_result['tokens']
        else:
            tokens = self.summary.loc[0, 'tokens']
        
        self.summary.loc[0, 'name'] = name
        self.summary.loc[0, 'alt_names'] = alt_names
        self.summary.loc[0, 'location'] = location
        self.summary.loc[0, 'email'] = email
        self.summary.loc[0, 'uri'] = uri
        self.summary.loc[0, 'crossref_id'] = crossref_id
        self.summary.loc[0, 'work_count'] = work_count
        self.summary.loc[0, 'tokens'] = tokens
    
    def from_crossref_result(crossref_result: pd.Series): # type: ignore

        funder = Funder()
        funder.import_crossref_result(crossref_result=crossref_result)

        return funder

    def import_crossref(self, crossref_id: str, timeout = 60):

        res = lookup_funder(crossref_id, timeout)
        self.import_crossref_result(res.loc[0]) # type: ignore

    def from_crossref(crossref_id: str): # type: ignore
        
        funder = Funder()
        funder.import_crossref(crossref_id) # type: ignore

        return funder
    
    def import_uri(self, uri: str, timeout = 60):

        res = lookup_funder(uri, timeout)
        self.import_crossref_result(res.loc[0]) # type: ignore

    def from_uri(uri: str): # type: ignore
        
        funder = Funder()
        funder.import_uri(uri) # type: ignore

        return funder

    def update_from_crossref(self, timeout = 60):

        uid = self.summary.loc[0,'crossref_id']
        if uid == None:
            uid = self.summary.loc[0,'uri']
            if uid == None:
                uid = ''

        res = lookup_funder(funder_id = uid, timeout = timeout) # type: ignore
        if len(res) > 0:
            self.import_crossref_result(res.loc[0]) # type: ignore

    def update_from_uri(self, timeout = 60):

        uid = self.summary.loc[0,'uri']
        if uid == None:
            uid = self.summary.loc[0,'crossref']
            if uid == None:
                uid = ''

        res = lookup_funder(funder_id = uid, timeout = timeout) # type: ignore
        if len(res) > 0:
            self.import_crossref_result(res.loc[0]) # type: ignore
    
    def search_works(self, bibliographic: str = None, # type: ignore
                     title: str = None, # type: ignore
                     author: str = None, # type: ignore
                     author_affiliation: str = None, # type: ignore
                     editor: str = None, # type: ignore
                     entry_type: str = None, # type: ignore
                     published_date: str = None, # type: ignore
                     doi: str = None, # type: ignore
                     publisher_name: str = None,# type: ignore
                    source: str = None, # type: ignore
                    link: str = None, # type: ignore
                    filter: dict = None, # type: ignore
                    select: list = None, # type: ignore
                    sample: int = None, # type: ignore
                    limit: int = 10, 
                    rate_limit: float = 0.05, 
                    timeout: int = 60, 
                    add_to_publications = False) -> pd.DataFrame:
        
        uid = self.summary.loc[0, 'crossref_id']
        if (uid == None) or (uid == ''):
            uid = self.summary.loc[0, 'uri']
            if (uid == None) or (uid == ''):
                uid = ''
        
        uid = str(uid)


        result = search_funder_works(funder_id=uid,  
                                     bibliographic=bibliographic, 
                                     title=title, author=author, 
                                     author_affiliation=author_affiliation, 
                                     editor=editor,
                                     entry_type=entry_type,
                                     published_date=published_date,
                                     doi=doi,
                                     publisher_name=publisher_name,
                                     source=source,
                                     link=link,
                                     filter=filter,
                                     select=select,
                                     sample=sample,
                                     limit=limit,
                                     rate_limit=rate_limit,
                                     timeout=timeout
                                     )
        
        # if add_to_publications == True:
        #     self.publications.add_dataframe(result)
        
        return result

class Funders(Entities):

    """
    This is a Funders object. It contains a collection of Funders objects and compiles data about them.
    
    Parameters
    ----------
    
    
    Attributes
    ----------
    """

    def __init__(self, funders_data = None):
        
        """
        Initialises Funders instance.
        
        Parameters
        ----------
        """

        super().__init__()

        self.summary = pd.DataFrame(columns = 
                                ['funder_id',
                                'name',
                                'alt_names',
                                'location',
                                'email',
                                'uri',
                                'crossref_id',
                                'work_count',
                                'tokens',
                                'website',
                                'other_links'
                                ],
                                dtype = object)
        

        self.all = dict()

        self.data = []
        self.data.append(funders_data)

        if (type(funders_data) == list) and (type(funders_data[0]) == Funder):

            for i in funders_data:
                fu = i.summary.copy(deep=True)
                self.summary = pd.concat([self.summary, fu])

            self.summary = self.summary.reset_index().drop('index',axis=1)

        else:

            if type(funders_data) == dict:
                
                values = list(funders_data.values())

                if type(values[0]) == Funder:

                    for f in funders_data.keys():
                        
                        index = len(self.summary)
                        fu = f.summary.copy(deep=True)
                        self.summary = pd.concat([self.summary, fu])
                        self.summary.loc[index, 'funder_id'] = f

                    self.summary = self.summary.reset_index().drop('index',axis=1)
                
        self.update_ids()

    def __getitem__(self, key):
        
        """
        Retrieves funders attribute using a key.
        """
        
        if key in self.__dict__.keys():
            return self.__dict__[key]
        
        if key in self.all.keys():
            return self.all[key]

        if key in self.summary.columns:
            return self.summary[key]
        
        if (type(key) == int) and (key <= len(self.data)):
            return self.data[key]
    
    def __repr__(self) -> str:

        alphabetical = str(self.summary['name'].sort_values().to_list()).replace('[','').replace(']','')
        return alphabetical
    
    def __len__(self) -> int:
        return len(self.all.keys())

    def merge(self, funders, drop_empty_rows = True, drop_duplicates = False):

        left = self.summary.copy(deep=True)
        right = funders.summary.copy(deep=True)
        
        merged = pd.concat([left, right])

        self.summary = merged.drop_duplicates(subset=['funder_id', 'name', 'crossref_id'], ignore_index=True)

        for i in funders.all.keys():
            if i not in self.all.keys():
                self.all[i] = funders.all[i]

        left_data = self.data
        right_data = funders.data

        if left_data == None:
                left_data = []
            
        if right_data == None:
                right_data = []

        if (type(left_data) == Funder) or (type(left_data) == str):
                left_data = [left_data]
            
        if (type(right_data) == Funder) or (type(right_data) == str):
                right_data = [right_data]
            
        if type(left_data) == dict:
                left_data = list(left_data.values())
            
        if type(right_data) == dict:
                right_data = list(right_data.values())

        merged_data = left_data + right_data # type: ignore
        merged_data = pd.Series(merged_data).value_counts().index.to_list()

        self.data = merged_data

        if drop_empty_rows == True:
            self.drop_empty_rows()
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        self.update_ids()

        return self


    def add_funder(self, funder: Funder =  None, uri: str = None, crossref_id: int = None, data = None, use_api = True, drop_empty_rows = False, drop_duplicates = False): # type: ignore

        

        if type(funder) == str:

            if use_api == True:
                details = lookup_funder(funder_id=funder)
                crossref_id = details.loc[0, 'id'] # type: ignore
                uri = details.loc[0, 'uri'] # type: ignore

            else:
                crossref_id = funder

            funder = None # type: ignore

        if data is not None:

            if type(data) == dict:
                funder = Funder.from_dict(data=data, use_api = use_api) # type: ignore
            
            else:
                if type(data) == pd.Series:
                    funder = Funder.from_series(data=data) # type: ignore
                
                else:
                    if type(data) == pd.DataFrame:
                        funder = Funder.from_dataframe(data=data) # type: ignore


        if funder is None:
            funder = Funder(uri=uri, crossref_id=crossref_id)

        if use_api == True:
            funder.update_from_crossref()

        funder.update_id()

        funder_id = str(funder.summary.loc[0, 'funder_id'])

        # if funder_id in self.summary['funder_id'].to_list():
        #     id_count = len(self.summary[self.summary['funder_id'].str.contains(funder_id)]) # type: ignore
        #     funder_id = funder_id + f'#{id_count + 1}'
        #     funder.summary.loc[0, 'funder_id'] = funder_id

        self.summary = pd.concat([self.summary, funder.summary])
        self.summary = self.summary.reset_index().drop('index', axis=1)

        self.all[funder_id] = funder

        if data is None:
            data = funder.summary.to_dict(orient='index')
        
        self.data.append(data)

        self.update_ids()

        if drop_empty_rows == True:
            self.drop_empty_rows()
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)


    def add_funders_list(self, funders_list: list, drop_empty_rows = False, drop_duplicates = False):
        
        for i in funders_list:
            if type(i) == Funder:
                self.add_funder(funder = i)
        
        if drop_empty_rows == True:
            self.drop_empty_rows()
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

    def drop_empty_rows(self):

        ignore_cols = ['funder_id', 'alt_names', 'publications', 'tokens', 'other_links']

        df = self.summary.copy(deep=True)
        df['name'] = df['name'].replace('no_name_given', None)
        df = df.dropna(axis=0, how='all')
        drop_cols = [c for c in df.columns if c not in ignore_cols]
        df = df.dropna(axis=0, how='all', subset=drop_cols).reset_index().drop('index', axis=1)

        self.summary = df

        return self

    def remove_duplicates(self, drop_empty_rows = True, sync = False):

        if drop_empty_rows == True:
            self.drop_empty_rows()
        
        df = self.summary.copy(deep=True)
        df['uri'] = df['uri'].str.replace('http://', '', regex=False).str.replace('https://', '', regex=False).str.replace('wwww.', '', regex=False).str.replace('dx.', '', regex=False).str.replace('doi.org/', '', regex=False).str.strip('/')
        
        df = df.sort_values(by = ['uri', 'crossref_id', 'website', 'name']).reset_index().drop('index', axis=1)
        self.summary = deduplicate(self.summary)

        if sync == True:
            self.sync_details(drop_duplicates=False, drop_empty_rows=False)

        return self

    def sync_all(self, drop_duplicates = False, drop_empty_rows=False):

        for i in self.all.keys():
            funder = self.all[i]
            funder.update_id()
            series = funder.summary.copy(deep=True).loc[0]
            all = self.summary.copy(deep=True).astype(str)
            indexes = all[all['funder_id'] == i].index.to_list()
            if len(indexes) > 0:
                auth_index = indexes[0]
                all_copy = self.summary.copy(deep=True)
                all_copy.loc[auth_index] = series
                self.summary = all_copy
        
        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

    def sync_details(self, drop_duplicates = False, drop_empty_rows=False):

        self.update_ids(sync=False)

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        for i in self.summary.index:

            f_data = self.summary.loc[i]
            f_id = f_data['funder_id']

            if f_id != None:
                f = Funder.from_series(f_data) # type: ignore
                self.all[f_id] = f

            else:
                f_id = generate_funder_id(f_data)
                f_data['funder_id'] = f_id
                f = Funder.from_series(f_data) # type: ignore
                self.all[f_id] = f
        
        keys = list(self.all.keys())
        for key in keys:
            f_ids = self.summary['funder_id'].to_list()
            if key not in f_ids:
                del self.all[key]

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)


    def sync(self, drop_duplicates = False, drop_empty_rows=False):
        
        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        all_len = len(self.summary)
        details_len = len(self.all)

        if all_len > details_len:
            self.sync_details(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
            return
        else:
            if details_len > all_len:
                self.sync_all(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
                return
            else:
                self.sync_details(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
                self.sync_all(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
                return

    def update_ids(self, sync=False):
        
        if sync == True:
            self.sync()

        for i in self.summary.index:
            all_copy = self.summary.copy(deep=True)
            data = all_copy.loc[i]
            old_id = all_copy.loc[i, 'funder_id']
            new_id = generate_funder_id(data)

            # if new_id in self.summary['funder_id'].to_list():
            #     df_copy = self.summary.copy(deep=True)
            #     df_copy = df_copy.astype(str)
            #     id_count = len(df_copy[df_copy['funder_id'].str.contains(new_id)]) # type: ignore
            #     new_id = new_id + f'#{id_count + 1}'

            all_copy2 = self.summary.copy(deep=True)
            all_copy2.loc[i, 'funder_id'] = new_id
            self.summary = all_copy2

            if old_id in self.all.keys():
                self.all[new_id] = self.all[old_id]
                self.all[new_id].summary.loc[0, 'funder_id'] = new_id
                del self.all[old_id]

            else:
                funder = Funder.from_series(data) # type: ignore
                funder.summary.loc[0, 'funder_id'] = new_id
                self.all[new_id] = funder


    def update_from_crossref(self, drop_duplicates = False, drop_empty_rows=False):

        funder_ids = self.all.keys()

        for a in funder_ids:

            self.all[a].update_from_crossref()
            details = self.all[a].summary.loc[0]
            
            df_index = self.summary[self.summary['funder_id'] == a].index.to_list()[0]
            self.summary.loc[df_index] = details

            new_id = details['funder_id']
            if new_id != a:
                self.all[new_id] = self.all[a]
                del self.all[a]

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        self.update_ids()
        


    def import_crossref_ids(self, crossref_ids: list, drop_duplicates = False, drop_empty_rows=False):

        for i in crossref_ids:

            auth = Funder.from_crossref(i) # type: ignore
            self.add_funder(funder = auth, data = i)

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        self.update_ids()


    def from_crossref_ids(crossref_ids: list, drop_duplicates = False, drop_empty_rows=False): # type: ignore

        funders = Funders()
        funders.import_crossref_ids(crossref_ids, drop_duplicates = drop_duplicates, drop_empty_rows=drop_empty_rows)

        return funders

    def with_crossref(self):
        return self.summary[~self.summary['crossref_id'].isna()]
    
    def with_uri(self):
        return self.summary[~self.summary['uri'].isna()]

    def import_crossref_result(self, crossref_result: pd.DataFrame, use_api = False, drop_duplicates = False, drop_empty_rows=False):

        for i in crossref_result.index:
            
            data = crossref_result.loc[i]
            fu = Funder.from_crossref_result(crossref_result=data) # type: ignore
            self.add_funder(funder = fu, data = data, use_api=use_api)
        
        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        self.update_ids()


    def from_crossref_result(crossref_result: pd.DataFrame, use_api = False, drop_duplicates = False, drop_empty_rows=False): # type: ignore

        funders = Funders()
        funders.import_crossref_result(crossref_result, use_api=use_api, drop_duplicates = drop_duplicates, drop_empty_rows=drop_empty_rows)

        return funders

    def search_works(self, 
                     funder_id: str = None, # type: ignore
                    index: int = None, # type: ignore
                    crossref_id: int = None, # type: ignore
                    uri: str = None,# type: ignore
                     bibliographic: str = None, # type: ignore
                     title: str = None, # type: ignore
                     author: str = None, # type: ignore
                     author_affiliation: str = None, # type: ignore
                     editor: str = None, # type: ignore
                     entry_type: str = None, # type: ignore
                     published_date: str = None, # type: ignore
                     doi: str = None, # type: ignore
                     publisher_name: str = None,# type: ignore
                    source: str = None, # type: ignore
                    link: str = None, # type: ignore
                    filter: dict = None, # type: ignore
                    select: list = None, # type: ignore
                    sample: int = None, # type: ignore
                    limit: int = 10, 
                    rate_limit: float = 0.05, 
                    timeout: int = 60, 
                    add_to_publications = False) -> pd.DataFrame:
        
        if (funder_id != None) and (funder_id in self.all.keys()):
            funder = self.all[funder_id]
            result = funder.search_works(
                                     bibliographic=bibliographic, 
                                     title=title, author=author, 
                                     author_affiliation=author_affiliation, 
                                     editor=editor,
                                     entry_type=entry_type,
                                     published_date=published_date,
                                     doi=doi,
                                     publisher_name=publisher_name,
                                     source=source,
                                     link=link,
                                     filter=filter,
                                     select=select,
                                     sample=sample,
                                     limit=limit,
                                     rate_limit=rate_limit,
                                     timeout=timeout,
                                     add_to_publications=add_to_publications
                                     )
        
        else:
            if index != None:
    
                uid = self.summary.loc[index, 'crossref_id']
                if (uid==None) or (uid == ''):
                    uid = self.summary.loc[index, 'uri']
                    if (uid==None) or (uid == ''):
                        uid = ''

                funder_id = self.summary.loc[index, 'funder_id'] # type: ignore

                uid = str(uid)
            
            else:
                if crossref_id != None:
                    index = self.summary[self.summary['crossref_id'] == crossref_id].index.to_list()[0]
                    funder_id = self.summary.loc[index, 'funder_id'] # type: ignore
                    uid = str(crossref_id)
                    
                else:
                    if uri != None:
                        index = self.summary[self.summary['uri'] == uri].index.to_list()[0]
                        funder_id = self.summary.loc[index, 'funder_id'] # type: ignore
                        uid = str(uri)
                        

            result = search_funder_works(funder_id=uid,  
                                     bibliographic=bibliographic, 
                                     title=title, author=author, 
                                     author_affiliation=author_affiliation, 
                                     editor=editor,
                                     entry_type=entry_type,
                                     published_date=published_date,
                                     doi=doi,
                                     publisher_name=publisher_name,
                                     source=source,
                                     link=link,
                                     filter=filter,
                                     select=select,
                                     sample=sample,
                                     limit=limit,
                                     rate_limit=rate_limit,
                                     timeout=timeout
                                     )
        
            if add_to_publications == True:
                self.all[funder_id].publications.add_dataframe(result)
        
        return result

def format_funders(funder_data, use_api = False, drop_duplicates = False, drop_empty_rows=False):
        
        result = Funders()

        funder_type = type(funder_data)

        if (
            (funder_type != pd.DataFrame) 
            and (funder_type != pd.Series) 
            and (funder_type != Funder) 
            and (funder_type != Funders) 
            and (funder_type != list) 
            and (funder_type != dict)
            ):
            result = Funders()
            return result

        if funder_type == Funders:
            result = funder_data
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result

        if funder_type == Funder:
            result.add_funder(funder=funder_data, use_api=use_api)
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result

        if funder_type == pd.Series:
            funder = Funder()
            funder.add_series(funder_data)
            result.add_funder(funder=funder, use_api=use_api)
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result

        if funder_type == pd.DataFrame:
            result.import_crossref_result(funder_data, use_api=use_api) # type: ignore
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result
        
        if (funder_type == dict):
            funder = Funder.from_dict(funder_data) # type: ignore
            result.add_funder(funder = funder, use_api=use_api) # type: ignore
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result

        if (funder_type == list) and (len(funder_data) > 0) and (type(funder_data[0]) == Funder):
            result = Funders()
            result.add_funders_list(funder_data)
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result

        if (funder_type == list) and (len(funder_data) > 0) and (type(funder_data[0]) == dict):

            for i in funder_data:
                funder = Funder.from_dict(i) # type: ignore
                result.add_funder(funder = funder, use_api=use_api) # type: ignore

            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)

            return result