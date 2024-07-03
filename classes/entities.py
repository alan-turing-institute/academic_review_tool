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


        self.details = pd.DataFrame(dtype = object)


    
    def __getitem__(self, key):
        
        """
        Retrieves entity attribute using a key.
        """
        
        if key in self.__dict__.keys():
            return self.__dict__[key]
        
        if key in self.details.columns:
            return self.details.loc[0, key]

    def get(self, key):
        return self[key]

    def __repr__(self) -> str:
        return str(self.details.loc[0])

    def search(self, query: str = 'request_input'):

        if query == 'request_input':
            query = input('Search query').strip()
        
        query = query.strip().lower()
        
        self_str = self.details.copy(deep=True).loc[0].astype(str).str.lower()
        masked = self_str[self_str.str.contains(query)].index
        
        return self.details.loc[0][masked]
        

    def has_uri(self) -> bool:

        if 'uri' in self.details.columns:
            uri = self.details.loc[0, 'uri']

            if (type(uri) == str) and (uri != ''):
                return True
            else:
                return False
        else:
            return False

    def add_dict(self, data: dict):

        if 'name' in data.keys():
            name = data['name']
            self.details.loc[0, 'name'] = name

        if 'DOI' in data.keys():
            uri = data['DOI'].replace('http', '').replace('https', '').replace('dx.', '').replace('doi.org/', '').strip()
            self.details.loc[0, 'uri'] = 'https://doi.org/' + uri
    
    def from_dict(data: dict): # type: ignore

        entity = Entity()
        entity.add_dict(data=data)

        return entity
        
    def add_series(self, series: pd.Series):
        self.details.loc[0] = series

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

        self.all = pd.DataFrame(dtype = object)
        

        self.details = dict()

        self.data = []
        

    def __getitem__(self, key):
        
        """
        Retrieves entities attribute using a key.
        """
        
        if key in self.__dict__.keys():
            return self.__dict__[key]
        
        if key in self.details.keys():
            return self.details[key]

        if key in self.all.columns:
            return self.all[key]
        
        if key in self.all.index:
             return self.all.loc[key]
        
        if (type(key) == int) and (key <= len(self.data)):
            return self.data[key]
    
    def __repr__(self) -> str:

        string = str(self.all).replace('[','').replace(']','')
        return string
    
    def __len__(self) -> int:
        return len(self.details.keys())

    def merge(self, entities):

        left = self.all.copy(deep=True)
        right = entities.all.copy(deep=True)
        
        merged = pd.concat([left, right])

        self.all = merged.drop_duplicates(ignore_index=True)

        for i in entities.details.keys():
            if i not in self.details.keys():
                self.details[i] = entities.details[i]

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

        if 'crossref_id' in self.all.columns:
            return self.all[~self.all['crossref_id'].isna()]
        else:
            return pd.DataFrame(index=self.all.columns, dtype=object)
    
    def with_uri(self):

        if 'uri' in self.all.columns:
            return self.all[~self.all['uri'].isna()]
        else:
            return pd.DataFrame(index=self.all.columns, dtype=object)
        
    def search(self, query: str = 'request_input'):
        
        if query == 'request_input':
            query = input('Search query').strip()
        
        query = query.strip().lower()
        
        
        masked_indexes = []
        for col in self.all.columns:
            col_str = self.all[col].copy(deep=True).astype(str).str.lower()
            indexes = col_str[col_str.str.contains(query)].index.to_list()
            masked_indexes = masked_indexes + indexes
            
        masked_indexes = list(set(masked_indexes))
        all_res = self.all.loc[masked_indexes].copy(deep=True)

        if 'affiliations' in self.all.columns:
            affils = self.all['affiliations']
            
            for i in affils.index:
                  
                  a = affils[i]
                  
                  if 'search' in a.__dir__():

                    a_res = a.search(query)
                    val = len(a_res)
                    if val > 0:
                        series = self.all.loc[i]
                        all_res = pd.concat([all_res, series])
            
        all_res = all_res.drop_duplicates()
        
        return all_res



                  