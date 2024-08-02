from ..exporters.general_exporters import obj_to_folder, art_class_to_folder

import pandas as pd
import numpy as np

class Entity:

    """
    This is an Entity object. It is designed to store data about an individual entity and their publications.
    
    Parameters
    ----------
    
    
    Attributes
    ----------
    """

    def __init__(self):
        
        """
        Initialises entity instance.
        
        Parameters
        ----------
        """


        self.summary = pd.DataFrame(dtype = object)


    
    def __getitem__(self, key):
        
        """
        Retrieves entity attribute using a key.
        """
        
        if key in self.__dict__.keys():
            return self.__dict__[key]
        
        if key in self.summary.columns:
            return self.summary.loc[0, key]

    def get(self, key):
        return self[key]

    def __repr__(self) -> str:
        return str(self.summary.loc[0])

    def search(self, query: str = 'request_input'):

        if query == 'request_input':
            query = input('Search query').strip()
        
        query = query.strip().lower()
        
        self_str = self.summary.copy(deep=True).loc[0].astype(str).str.lower()
        masked = self_str[self_str.str.contains(query)].index
        
        return self.summary.loc[0][masked]
        

    def has_uri(self) -> bool:

        if 'uri' in self.summary.columns:
            uri = self.summary.loc[0, 'uri']

            if (type(uri) == str) and (uri != ''):
                return True
            else:
                return False
        else:
            return False

    def add_dict(self, data: dict):

        if 'name' in data.keys():
            name = data['name']
            self.summary.loc[0, 'name'] = name

        if 'DOI' in data.keys():
            uri = data['DOI'].replace('http', '').replace('https', '').replace('dx.', '').replace('doi.org/', '').strip()
            self.summary.loc[0, 'uri'] = 'https://doi.org/' + uri
    
    def from_dict(data: dict): # type: ignore

        entity = Entity()
        entity.add_dict(data=data)

        return entity
        
    def add_series(self, series: pd.Series):
        self.summary.loc[0] = series

    def from_series(data: pd.Series): # type: ignore
        entity = Entity()
        entity.add_series(data)

        return entity

    def add_dataframe(self, dataframe: pd.DataFrame):
        series = dataframe.loc[0]
        self.add_series(series) # type: ignore

    def from_dataframe(data: pd.DataFrame): # type: ignore
        entity = Entity()
        entity.add_dataframe(data)

        return entity
    
    def export_folder(self, 
                      folder_name = 'request_input', 
                      folder_address: str = 'request_input', 
                      export_str_as: str = 'txt', 
                      export_dict_as: str = 'json', 
                      export_pandas_as: str = 'csv', 
                      export_network_as: str = 'graphML'
                      ):
        
        art_class_to_folder(obj=self, folder_name=folder_name, folder_address=folder_address, export_str_as=export_str_as, export_dict_as=export_dict_as, export_pandas_as=export_pandas_as, export_network_as=export_network_as)

    
class Entities:

    """
    This is an Entities object. It contains a collection of Entities objects and compiles data about them.
    
    Parameters
    ----------
    
    
    Attributes
    ----------
    """

    def __init__(self):
        
        """
        Initialises Entities instance.
        
        Parameters
        ----------
        """

        self.summary = pd.DataFrame(dtype = object)
        

        self.all = dict()

        self.data = []
        

    def __getitem__(self, key):
        
        """
        Retrieves entities attribute using a key.
        """
        
        if key in self.__dict__.keys():
            return self.__dict__[key]
        
        if key in self.all.keys():
            return self.all[key]

        if key in self.summary.columns:
            return self.summary[key]
        
        if key in self.summary.index:
             return self.summary.loc[key]
        
        if (type(key) == int) and (key <= len(self.data)):
            return self.data[key]
    
    def __repr__(self) -> str:

        string = str(self.summary).replace('[','').replace(']','')
        return string
    
    def __len__(self) -> int:
        return len(self.all.keys())

    def export_folder(self, 
                      folder_name = 'request_input', 
                      folder_address: str = 'request_input', 
                      export_str_as: str = 'txt', 
                      export_dict_as: str = 'json', 
                      export_pandas_as: str = 'csv', 
                      export_network_as: str = 'graphML'
                      ):
        
        
        art_class_to_folder(obj=self, folder_name=folder_name, folder_address=folder_address, export_str_as=export_str_as, export_dict_as=export_dict_as, export_pandas_as=export_pandas_as, export_network_as=export_network_as)

    def save_as(self,
                filetype = 'folder',
                file_name = 'request_input', 
                      folder_address: str = 'request_input', 
                      export_str_as: str = 'txt', 
                      export_dict_as: str = 'json', 
                      export_pandas_as: str = 'csv', 
                      export_network_as: str = 'graphML'):
        
        if filetype == 'folder':
            self.export_folder(folder_name=file_name, folder_address=folder_address, export_str_as=export_str_as, export_dict_as=export_dict_as, export_pandas_as=export_pandas_as, export_network_as=export_network_as)


    def drop(self, entity_id):

        if entity_id in self.all.keys():
            del self.all[entity_id]

        id_col = None
        if 'author_id' in self.summary.columns:
            id_col = 'author_id'
        if 'funder_id' in self.summary.columns:
            id_col = 'funder_id'
        if 'affiliation_id' in self.summary.columns:
            id_col = 'affiliation_id'

        i_to_drop = self.summary[self.summary[id_col] == entity_id].index.to_list()

        if len(i_to_drop) > 0:
            self.summary.drop(labels=i_to_drop, axis=0)


    def merge(self, entities):

        left = self.summary.copy(deep=True)
        right = entities.summary.copy(deep=True)
        
        merged = pd.concat([left, right])

        self.summary = merged.drop_duplicates(ignore_index=True)

        for i in entities.all.keys():
            if i not in self.all.keys():
                self.all[i] = entities.all[i]

        left_data = self.data
        right_data = entities.data

        if left_data == None:
                left_data = []
            
        if right_data == None:
                right_data = []

        if (type(left_data) == Entity) or (type(left_data) == str):
                left_data = [left_data]
            
        if (type(right_data) == Entity) or (type(right_data) == str):
                right_data = [right_data]
            
        if type(left_data) == dict:
                left_data = list(left_data.values())
            
        if type(right_data) == dict:
                right_data = list(right_data.values())

        merged_data = left_data + right_data # type: ignore
        merged_data = pd.Series(merged_data).value_counts().index.to_list()

        self.data = merged_data

        return self

    def with_crossref(self):

        if 'crossref_id' in self.summary.columns:
            return self.summary[~self.summary['crossref_id'].isna()]
        else:
            return pd.DataFrame(index=self.summary.columns, dtype=object)
    
    def with_uri(self):

        if 'uri' in self.summary.columns:
            return self.summary[~self.summary['uri'].isna()]
        else:
            return pd.DataFrame(index=self.summary.columns, dtype=object)
    
    def contains(self, query: str = 'request_input', ignore_case: bool = True) -> bool:

        if query == 'request_input':
            query = input('Search query').strip()

        query = query.strip()

        all_str = self.summary.copy(deep=True).astype(str)
        
        if ignore_case == True:
            query = query.lower()
            for c in all_str.columns:
                all_str[c] = all_str[c].str.lower()
            

        cols = self.summary.columns

        for c in cols:

            if '_id' in c:
                res = all_str[all_str[c].str.contains(query)]
                if len(res) > 0:
                    return True
            
            if (c == 'name') or (c == 'full_name'):
                res = all_str[all_str[c] == query]
                if len(res) > 0:
                    return True
            
            if c == 'uri':
                res = all_str[all_str[c].str.contains(query)]
                if len(res) > 0:
                    return True
            
            if c == 'orcid':
                res = all_str[all_str[c].str.contains(query)]
                if len(res) > 0:
                    return True
            
            if c == 'google_scholar':
                res = all_str[all_str[c].str.contains(query)]
                if len(res) > 0:
                    return True
            
            if c == 'scopus':
                res = all_str[all_str[c].str.contains(query)]
                if len(res) > 0:
                    return True
            
            if (c == 'crossref_id') or (c == 'crossref'):
                res = all_str[all_str[c] == query]
                if len(res) > 0:
                    return True
            
            if (c == 'website') or (c == 'link'):
                res = all_str[all_str[c] == query]
                if len(res) > 0:
                    return True
            
        return False
        

    def search_ids(self, query: str = 'request_input'):

        if query == 'request_input':
            query = input('Search query').strip()
        
        query = query.strip().lower()

        cols = [c for c in self.summary.columns if (('_id' in c) or (c == 'uri'))]

        masked_indexes = []
        for col in cols:
                col_str = self.summary[col].copy(deep=True).astype(str).str.lower()
                indexes = col_str[col_str.str.contains(query)].index.to_list()
                masked_indexes = masked_indexes + indexes
        
        masked_indexes = list(set(masked_indexes))
        all_res = self.summary.loc[masked_indexes].copy(deep=True)
        final_indexes = all_res.astype(str).drop_duplicates().index
        result = self.summary.loc[final_indexes]

        return result
        

    def search(self, query: str = 'request_input'):
        
        if query == 'request_input':
            query = input('Search query').strip()
        
        query = query.strip()
        query_list = []

        if 'AND' in query:
            query_list = query.split('AND')
            query_list = [i.strip() for i in query_list]
        else:
            if '&' in query:
                query_list = query.split('&')
                query_list = [i.strip() for i in query_list]

        if len(query_list) > 0:
            
            results_list = []

            for i in query_list:
                res_indexes = set(self.search(query=i).index)
                results_list.append(res_indexes)
            
            intersect = set.intersection(*results_list)
            indexes = list(intersect)

            result = self.summary.loc[indexes]

            return result
            

        
        else:
            query = query.lower()
            masked_indexes = []
            for col in self.summary.columns:
                col_str = self.summary[col].copy(deep=True).astype(str).str.lower()
                indexes = col_str[col_str.str.contains(query)].index.to_list()
                masked_indexes = masked_indexes + indexes
                
            masked_indexes = list(set(masked_indexes))
            all_res = self.summary.loc[masked_indexes].copy(deep=True)

            if 'affiliations' in self.summary.columns:

                affils = self.summary['affiliations']
                
                for i in self.summary.index:
                    
                    a = affils[i]
                    
                    if 'search' in a.__dir__():

                        a_res = a.search(query)
                        val = len(a_res)
                        if val > 0:
                            series = self.summary.loc[i]
                            all_res.loc[i] = series
                
            final_indexes = all_res.astype(str).drop_duplicates().index
            
            result = self.summary.loc[final_indexes]

        return result


                  