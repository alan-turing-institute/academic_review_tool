from ..datasets.stopwords.stopwords import all_stopwords

from .results import Results

import pandas as pd
import numpy as np

from geopy.geocoders import Nominatim # type: ignore
from nltk.tokenize import word_tokenize # type: ignore

def generate_affiliation_id(affiliation_data: pd.Series):

        affiliation_id = 'AFFIL:'

        name = affiliation_data['name']

        if (name == None) or (name == ''):
            name = 'no_name_given'
        
        if name != 'no_name_given':
            name = name.strip().lower()
            name_tokens = list(word_tokenize(name))
            name_tokens = [i for i in name_tokens if i not in all_stopwords]
            name_first_3 = name_tokens[:3]
            name_last = name_tokens[-1]

            if name_last in name_first_3:
                name_last = ''
            
            name_shortened = '-'.join(name_first_3) + '-' + name_last

        else:
            name_shortened = name

        affiliation_id = affiliation_id + '-' + name_shortened

        location = affiliation_data['location']
        if type(location) == str:
            location = location.split(',')
            location_shortened = location[0].strip()
            if location[0] != location[-1]:
                location_shortened = location_shortened + '-' + location[-1].strip()
        else:
            location_shortened = ''
        
        affiliation_id = affiliation_id + '-' + location_shortened

        uid = affiliation_data['uri']
        if (uid == None) or (uid == 'None') or (uid == ''):
            uid = affiliation_data['crossref_id']
            if (uid == None) or (uid == 'None') or (uid == ''):
                uid = affiliation_data['website']
                if (uid == None) or (uid == 'None') or (uid == ''):
                        uid = ''
        
        uid_shortened = uid.replace('https://', '').replace('http://', '').replace('www.', '').replace('dx.','').replace('doi.org/','').replace('user=','')[:20]

        affiliation_id = affiliation_id + '-' + uid_shortened
        affiliation_id = affiliation_id.replace('AFFIL:-', 'AFFIL:').replace('--', '-').replace('AFFIL:-', 'AFFIL:').strip('-')

        return affiliation_id

class Affiliation:

    """
    This is a Affiliation object. It is designed to store data about an organisation that an author is affiliated with.
    
    Parameters
    ----------
    
    
    Attributes
    ----------
    """

    def __init__(self,
                 affiliation_id: str = None, # type: ignore
                 name: str = None, # type: ignore
                 location: str = None, # type: ignore
                 address: str = None, # type: ignore
                 email: str = None, # type: ignore
                 uri: str = None, # type: ignore
                 crossref_id: int = None, # type: ignore
                 website: str = None,  # type: ignore
                 other_links = [], # type: ignore
                 use_api: bool = False
                 ):
        
        """
        Initialises affiliation instance.
        
        Parameters
        ----------
        """

        orig_name_data = name

        if type(name) == str:
            name = name.strip()
        
            if (location is None) and (address is None):

                name_split = name.split(',')
                name_split = [i.strip() for i in name_split]

                address = name
                location = name_split[-1]
                name = name_split[0]
        
        if type(name) == list:

            if (location is None) and (address is None):
                address = ', '.join(name)
                location = name[-1]
            
            name = name[0]

        if type(other_links) == str:
            other_links = other_links.replace('[','').replace(']','').split(',')
            other_links = [i.strip() for i in other_links]
        

        self.details = pd.DataFrame(columns = [
                                'affiliation_id',
                                'name',
                                'location',
                                'address',
                                'email',
                                'uri',
                                'crossref_id',
                                'website',
                                'other_links'
                                ],
                                dtype = object)
        
        
        self.details.loc[0] = pd.Series(dtype=object)
        self.details.loc[0, 'affiliation_id'] = affiliation_id
        self.details.loc[0, 'name'] = name
        self.details.loc[0, 'location'] = location
        self.details.loc[0, 'address'] = address
        self.details.loc[0, 'email'] = email
        self.details.loc[0, 'uri'] = uri
        self.details.loc[0, 'crossref_id'] = crossref_id
        self.details.loc[0, 'website'] = website
        self.details.at[0, 'other_links'] = other_links

        if use_api == True:
            
            if (type(orig_name_data) != str) and (type(orig_name_data) != list):
                orig_name_data = str(orig_name_data).strip().replace('{','').replace('}','').replace('[','').replace(']','')

            if type(orig_name_data) == str:
                orig_name_data = orig_name_data.split(',')
                orig_name_data = [i.strip() for i in orig_name_data]

            if type(orig_name_data) == list:
                location_data = orig_name_data[-3:]
                location_data = ', '.join(location_data)
            else:
                location_data = ''

            geolocator = Nominatim(user_agent="location_app")

            try:
                loc = geolocator.geocode(orig_name_data)
                print(loc)
            except:
                loc = None
            
            if loc != None:
                self.details.loc[0, 'address'] = loc.address

                if self.details.loc[0, 'name'] == None:
                    self.details.loc[0, 'name'] = loc.name
                
                if self.details.loc[0, 'location'] == None:
                    self.details.loc[0, 'location'] = loc.display_name

            
        
        self.update_id()

    def generate_id(self):

        affiliation_data = self.details.loc[0]

        affiliation_id = generate_affiliation_id(affiliation_data) # type: ignore
        return affiliation_id

    def update_id(self):

        current_id = str(self.details.loc[0, 'affiliation_id'])

        if (current_id == None) or (current_id == 'None') or (current_id == '') or (current_id == 'AFFIL:000') or ('no_name_given' in current_id):
            auth_id = self.generate_id()
            self.details.loc[0, 'affiliation_id'] = auth_id
        
    
    def __getitem__(self, key):
        
        """
        Retrieves affiliation attribute using a key.
        """
        
        if key in self.__dict__.keys():
            return self.__dict__[key]

        if key in self.details.columns:
            return self.details.loc[0, key]

    def __repr__(self) -> str:
        
        return str(self.details.loc[0, 'name'])

    def has_uri(self) -> bool:

        uri = self.details.loc[0, 'uri']

        if (type(uri) == str) and (uri != ''):
            return True
        else:
            return False

    def add_dict(self, data: dict):

        if 'name' in data.keys():
            name = data['name']
            self.details.loc[0, 'name'] = name
        
        if 'location' in data.keys():
            location = data['location']
            self.details.loc[0, 'location'] = location

        if 'address' in data.keys():
            address = data['address']
            self.details.loc[0, 'address'] = address
        
        if 'crossref_id' in data.keys():
            crossref_id = data['crossref_id']
            self.details.loc[0, 'crossref_id'] = crossref_id

        if 'DOI' in data.keys():
            uri = data['DOI'].replace('http', '').replace('https', '').replace('dx.', '').replace('doi.org/', '').strip()
            self.details.loc[0, 'uri'] = 'https://doi.org/' + uri
        else:
            if 'uri' in data.keys():
                uri = data['DOI'].replace('http', '').replace('https', '').replace('dx.', '').replace('doi.org/', '').strip()
                self.details.loc[0, 'uri'] = 'https://doi.org/' + uri

        if 'url' in data.keys():
            website = data['url']
            self.details.loc[0, 'website'] = website
        else:
            if 'link' in data.keys():
                website = data['link']
                self.details.loc[0, 'website'] = website
            else:
                if 'website' in data.keys():
                    website = data['website']
                    self.details.loc[0, 'website'] = website

    def from_dict(data: dict, use_api=False): # type: ignore

        if 'name' in data.keys():
            name = data['name']
        else:
            name = None
        
        if 'location' in data.keys():
            location = data['location']
        else:
            location = None

        if 'address' in data.keys():
            address = data['address']
        else:
            address = None
        
        if 'email' in data.keys():
            email = data['email']
        else:
            email = None

        if 'crossref_id' in data.keys():
            crossref_id = data['crossref_id']
        else:
            crossref_id = None

        if 'DOI' in data.keys():
            uri = data['DOI'].replace('http', '').replace('https', '').replace('dx.', '').replace('doi.org/', '').strip()
            uri = 'https://doi.org/' + uri
        else:
            if 'doi' in data.keys():
                uri = data['doi'].replace('http', '').replace('https', '').replace('dx.', '').replace('doi.org/', '').strip()
                uri = 'https://doi.org/' + uri
            else:
                if 'URI' in data.keys():
                    uri = data['URI'].replace('http', '').replace('https', '').replace('dx.', '').replace('doi.org/', '').strip()
                    uri = 'https://doi.org/' + uri
                else:
                    if 'uri' in data.keys():
                        uri = data['DOI'].replace('http', '').replace('https', '').replace('dx.', '').replace('doi.org/', '').strip()
                        uri = 'https://doi.org/' + uri
                    else:
                        uri = None

        if 'URL' in data.keys():
            website = data['URL']
        else:
            if 'url' in data.keys():
                website = data['url']
            else:
                if 'link' in data.keys():
                    website = data['link']
                else:
                    if 'website' in data.keys():
                        website = data['website']
                    else:
                        website = None

        affiliation = Affiliation(name=name, location=location, address=address, email=email, uri=uri, crossref_id=crossref_id, website=website, use_api=use_api) # type: ignore

        return affiliation
        
    # def add_series(self, series: pd.Series):
    #     self.details.loc[0] = series

    # def from_series(data: pd.Series): # type: ignore
    #     affiliation = Affiliation()
    #     affiliation.add_series(data)

    #     return affiliation

    # def add_dataframe(self, dataframe: pd.DataFrame):
    #     series = dataframe.loc[0]
    #     self.add_series(series) # type: ignore

    # def from_dataframe(data: pd.DataFrame): # type: ignore
    #     affiliation = Affiliation()
    #     affiliation.add_dataframe(data)

    #     return affiliation

    # def import_crossref_result(self, crossref_result: pd.Series):
        
    #     if 'name' in crossref_result.index:
    #         name = crossref_result['name']
    #     else:
    #         name = self.details.loc[0, 'name']

    #     if 'alt-names' in crossref_result.index:
    #         alt_names = crossref_result['alt-names']
    #     else:
    #         alt_names = self.details.loc[0, 'alt_names']

    #     if 'location' in crossref_result.index:
    #         location = crossref_result['location']
    #     else:
    #         location = self.details.loc[0, 'location']

    #     if 'email' in crossref_result.index:
    #         email = crossref_result['email']
    #     else:
    #         email = self.details.loc[0, 'email']

    #     if 'uri' in crossref_result.index:
    #         uri  =crossref_result['uri']
    #     else:
    #         uri = self.details.loc[0, 'uri']

    #     if 'id' in crossref_result.index:
    #         crossref_id = crossref_result['id']
    #     else:
    #         crossref_id = self.details.loc[0, 'crossref_id']

    #     if 'work-count' in crossref_result.index:
    #         work_count = crossref_result['work-count']
    #     else:
    #         work_count = self.details.loc[0, 'work_count']

    #     if 'tokens' in crossref_result.index:
    #         tokens = crossref_result['tokens']
    #     else:
    #         tokens = self.details.loc[0, 'tokens']
        
    #     self.details.loc[0, 'name'] = name
    #     self.details.loc[0, 'alt_names'] = alt_names
    #     self.details.loc[0, 'location'] = location
    #     self.details.loc[0, 'email'] = email
    #     self.details.loc[0, 'uri'] = uri
    #     self.details.loc[0, 'crossref_id'] = crossref_id
    #     self.details.loc[0, 'work_count'] = work_count
    #     self.details.loc[0, 'tokens'] = tokens
    
    # def from_crossref_result(crossref_result: pd.Series): # type: ignore

    #     affiliation = Affiliation()
    #     affiliation.import_crossref_result(crossref_result=crossref_result)

    #     return affiliation

    # def import_crossref(self, crossref_id: str, timeout = 60):

    #     res = lookup_affiliation(crossref_id, timeout)
    #     self.import_crossref_result(res.loc[0]) # type: ignore

    # def from_crossref(crossref_id: str): # type: ignore
        
    #     affiliation = Affiliation()
    #     affiliation.import_crossref(crossref_id) # type: ignore

    #     return affiliation
    
    # def import_uri(self, uri: str, timeout = 60):

    #     res = lookup_affiliation(uri, timeout)
    #     self.import_crossref_result(res.loc[0]) # type: ignore

    # def from_uri(uri: str): # type: ignore
        
    #     affiliation = Affiliation()
    #     affiliation.import_uri(uri) # type: ignore

    #     return affiliation

    # def update_from_crossref(self, timeout = 60):

    #     uid = self.details.loc[0,'crossref_id']
    #     if uid == None:
    #         uid = self.details.loc[0,'uri']
    #         if uid == None:
    #             uid = ''

    #     res = lookup_affiliation(affiliation_id = uid, timeout = timeout) # type: ignore
    #     if len(res) > 0:
    #         self.import_crossref_result(res.loc[0]) # type: ignore

    # def update_from_uri(self, timeout = 60):

    #     uid = self.details['uri']
    #     if uid == None:
    #         uid = self.details['crossref']
    #         if uid == None:
    #             uid = ''

    #     res = lookup_affiliation(affiliation_id = uid, timeout = timeout) # type: ignore
    #     if len(res) > 0:
    #         self.import_crossref_result(res.loc[0]) # type: ignore
   



# class Affiliations:

#     """
#     This is a Affiliations object. It contains a collection of Affiliations objects and compiles data about them.
    
#     Parameters
#     ----------
    
    
#     Attributes
#     ----------
#     """

#     def __init__(self, affiliations_data = None):
        
#         """
#         Initialises Affiliations instance.
        
#         Parameters
#         ----------
#         """

#         self.all = pd.DataFrame(columns = 
#                                 ['affiliation_id',
#                                 'name',
#                                 'alt_names',
#                                 'location',
#                                 'email',
#                                 'uri',
#                                 'crossref_id',
#                                 'work_count',
#                                 'tokens',
#                                 'website',
#                                 'other_links'
#                                 ],
#                                 dtype = object)
        

#         self.details = dict()

#         self.data = []
#         self.data.append(affiliations_data)

#         if (type(affiliations_data) == list) and (type(affiliations_data[0]) == Affiliation):

#             for i in affiliations_data:
#                 fu = i.details.copy(deep=True)
#                 self.all = pd.concat([self.all, fu])

#             self.all = self.all.reset_index().drop('index',axis=1)

#         else:

#             if type(affiliations_data) == dict:
                
#                 values = list(affiliations_data.values())

#                 if type(values[0]) == Affiliation:

#                     for f in affiliations_data.keys():
                        
#                         index = len(self.all)
#                         fu = f.details.copy(deep=True)
#                         self.all = pd.concat([self.all, fu])
#                         self.all.loc[index, 'affiliation_id'] = f

#                     self.all = self.all.reset_index().drop('index',axis=1)
                
#         self.update_ids()

#     def __getitem__(self, key):
        
#         """
#         Retrieves affiliations attribute using a key.
#         """
        
#         if key in self.__dict__.keys():
#             return self.__dict__[key]
        
#         if key in self.details.keys():
#             return self.details[key]

#         if key in self.all.columns:
#             return self.all[key]
        
#         if (type(key) == int) and (key <= len(self.data)):
#             return self.data[key]
    
#     def __repr__(self) -> str:

#         alphabetical = str(self.all['name'].sort_values())
#         return alphabetical
    
#     def __len__(self) -> int:
#         return len(self.details.keys())

#     def merge(self, affiliations):

#         left = self.all.copy(deep=True)
#         right = affiliations.all.copy(deep=True)
        
#         merged = pd.concat([left, right])

#         self.all = merged.drop_duplicates(subset=['affiliation_id', 'family_name', 'crossref'], ignore_index=True)

#         for i in affiliations.details.keys():
#             if i not in self.details.keys():
#                 self.details[i] = affiliations.details[i]

#         left_data = self.data
#         right_data = affiliations.data

#         if left_data == None:
#                 left_data = []
            
#         if right_data == None:
#                 right_data = []

#         if (type(left_data) == Affiliation) or (type(left_data) == str):
#                 left_data = [left_data]
            
#         if (type(right_data) == Affiliation) or (type(right_data) == str):
#                 right_data = [right_data]
            
#         if type(left_data) == dict:
#                 left_data = list(left_data.values())
            
#         if type(right_data) == dict:
#                 right_data = list(right_data.values())

#         merged_data = left_data + right_data # type: ignore
#         merged_data = pd.Series(merged_data).value_counts().index.to_list()

#         self.data = merged_data
#         self.update_ids()

#         return self


#     def add_affiliation(self, affiliation: Affiliation =  None, uri: str = None, crossref_id: int = None, data = None, use_api = True): # type: ignore

        

#         if type(affiliation) == str:

#             if use_api == True:
#                 details = lookup_affiliation(affiliation_id=affiliation)
#                 crossref_id = details.loc[0, 'id'] # type: ignore
#                 uri = details.loc[0, 'uri'] # type: ignore

#             else:
#                 crossref_id = affiliation

#             affiliation = None # type: ignore

#         if data is not None:

#             if type(data) == dict:
#                 affiliation = Affiliation.from_dict(data=data, use_api = use_api) # type: ignore
            
#             else:
#                 if type(data) == pd.Series:
#                     affiliation = Affiliation.from_series(data=data) # type: ignore
                
#                 else:
#                     if type(data) == pd.DataFrame:
#                         affiliation = Affiliation.from_dataframe(data=data) # type: ignore


#         if affiliation is None:
#             affiliation = Affiliation(uri=uri, crossref_id=crossref_id)

#         if use_api == True:
#             affiliation.update_from_crossref()

#         affiliation.update_id()

#         affiliation_id = str(affiliation.details.loc[0, 'affiliation_id'])

#         if affiliation_id in self.all['affiliation_id'].to_list():
#             id_count = len(self.all[self.all['affiliation_id'].str.contains(affiliation_id)]) # type: ignore
#             affiliation_id = affiliation_id + f'#{id_count + 1}'
#             affiliation.details.loc[0, 'affiliation_id'] = affiliation_id

#         self.all = pd.concat([self.all, affiliation.details])
#         self.all = self.all.reset_index().drop('index', axis=1)

#         self.details[affiliation_id] = affiliation

#         if data is None:
#             data = affiliation.details.to_dict(orient='index')
        
#         self.data.append(data)

#         self.update_ids()


#     def add_affiliations_list(self, affiliations_list: list):
        
#         for i in affiliations_list:
#             if type(i) == Affiliation:
#                 self.add_affiliation(affiliation = i)

#     def sync_all(self):

#         for i in self.details.keys():
#             affiliation = self.details[i]
#             affiliation.update_id()
#             series = affiliation.details.loc[0]
#             all = self.all.copy(deep=True).astype(str)
#             indexes = all[all['affiliation_id'] == i].index.to_list()
#             if len(indexes) > 0:
#                 auth_index = indexes[0]
#                 self.all.loc[auth_index] = series

#     def update_ids(self):

#         self.sync_all()

#         for i in self.all.index:
#             data = self.all.loc[i]
#             old_id = self.all.loc[i, 'affiliation_id']
#             new_id = generate_affiliation_id(self.all.loc[i])

#             if new_id in self.all['affiliation_id'].to_list():
#                 df_copy = self.all.copy(deep=True).astype(str)
#                 id_count = len(df_copy[df_copy['affiliation_id'].str.contains(new_id)]) # type: ignore
#                 new_id = new_id + f'#{id_count + 1}'

#             self.all.loc[i, 'affiliation_id'] = new_id
#             if old_id in self.details.keys():
#                 self.details[new_id] = self.details[old_id]
#                 self.details[new_id].details.loc[0, 'affiliation_id'] = new_id
#                 del self.details[old_id]

#             else:
#                 affiliation = Affiliation.from_series(data) # type: ignore
#                 affiliation.details.loc[0, 'affiliation_id'] = new_id
#                 self.details[new_id] = affiliation


#     def update_from_crossref(self):

#         affiliation_ids = self.details.keys()

#         for a in affiliation_ids:

#             self.details[a].update_from_crossref()
#             details = self.details[a].details.loc[0]
            
#             df_index = self.all[self.all['affiliation_id'] == a].index.to_list()[0]
#             self.all.loc[df_index] = details

#             new_id = details['affiliation_id']
#             if new_id != a:
#                 self.details[new_id] = self.details[a]
#                 del self.details[a]

#         self.update_ids()


#     def import_crossref_ids(self, crossref_ids: list):

#         for i in crossref_ids:

#             auth = Affiliation.from_crossref(i) # type: ignore
#             self.add_affiliation(affiliation = auth, data = i)

#         self.update_ids()


#     def from_crossref_ids(crossref_ids: list): # type: ignore

#         affiliations = Affiliations()
#         affiliations.import_crossref_ids(crossref_ids)

#         return affiliations

#     def with_crossref(self):
#         return self.all[~self.all['crossref_id'].isna()]
    
#     def with_uri(self):
#         return self.all[~self.all['uri'].isna()]

#     def import_crossref_result(self, crossref_result: pd.DataFrame, use_api = False):

#         for i in crossref_result.index:
            
#             data = crossref_result.loc[i]
#             fu = Affiliation.from_crossref_result(crossref_result=data) # type: ignore
#             self.add_affiliation(affiliation = fu, data = data, use_api=use_api)

#         self.update_ids()


#     def from_crossref_result(crossref_result: pd.DataFrame): # type: ignore

#         affiliations = Affiliations()
#         affiliations.import_crossref_result(crossref_result)

#         return affiliations

#     def search_works(self, 
#                      affiliation_id: str = None, # type: ignore
#                     index: int = None, # type: ignore
#                     crossref_id: int = None, # type: ignore
#                     uri: str = None,# type: ignore
#                      bibliographic: str = None, # type: ignore
#                      title: str = None, # type: ignore
#                      author: str = None, # type: ignore
#                      author_affiliation: str = None, # type: ignore
#                      editor: str = None, # type: ignore
#                      entry_type: str = None, # type: ignore
#                      published_date: str = None, # type: ignore
#                      DOI: str = None, # type: ignore
#                      publisher_name: str = None,# type: ignore
#                     source: str = None, # type: ignore
#                     link: str = None, # type: ignore
#                     filter: dict = None, # type: ignore
#                     select: list = None, # type: ignore
#                     sample: int = None, # type: ignore
#                     limit: int = 10, 
#                     rate_limit: float = 0.05, 
#                     timeout: int = 60, 
#                     add_to_publications = False) -> pd.DataFrame:
        
#         if (affiliation_id != None) and (affiliation_id in self.details.keys()):
#             affiliation = self.details[affiliation_id]
#             result = affiliation.search_works(
#                                      bibliographic=bibliographic, 
#                                      title=title, author=author, 
#                                      author_affiliation=author_affiliation, 
#                                      editor=editor,
#                                      entry_type=entry_type,
#                                      published_date=published_date,
#                                      DOI=DOI,
#                                      publisher_name=publisher_name,
#                                      source=source,
#                                      link=link,
#                                      filter=filter,
#                                      select=select,
#                                      sample=sample,
#                                      limit=limit,
#                                      rate_limit=rate_limit,
#                                      timeout=timeout,
#                                      add_to_publications=add_to_publications
#                                      )
        
#         else:
#             if index != None:
    
#                 uid = self.all.loc[index, 'crossref_id']
#                 if (uid==None) or (uid == ''):
#                     uid = self.all.loc[index, 'uri']
#                     if (uid==None) or (uid == ''):
#                         uid = ''

#                 affiliation_id = self.all.loc[index, 'affiliation_id'] # type: ignore

#                 uid = str(uid)
            
#             else:
#                 if crossref_id != None:
#                     index = self.all[self.all['crossref_id'] == crossref_id].index.to_list()[0]
#                     affiliation_id = self.all.loc[index, 'affiliation_id'] # type: ignore
#                     uid = str(crossref_id)
                    
#                 else:
#                     if uri != None:
#                         index = self.all[self.all['uri'] == uri].index.to_list()[0]
#                         affiliation_id = self.all.loc[index, 'affiliation_id'] # type: ignore
#                         uid = str(uri)
                        

#             result = search_affiliation_works(affiliation_id=uid,  
#                                      bibliographic=bibliographic, 
#                                      title=title, author=author, 
#                                      author_affiliation=author_affiliation, 
#                                      editor=editor,
#                                      entry_type=entry_type,
#                                      published_date=published_date,
#                                      DOI=DOI,
#                                      publisher_name=publisher_name,
#                                      source=source,
#                                      link=link,
#                                      filter=filter,
#                                      select=select,
#                                      sample=sample,
#                                      limit=limit,
#                                      rate_limit=rate_limit,
#                                      timeout=timeout
#                                      )
        
#             if add_to_publications == True:
#                 self.details[affiliation_id].publications.add_dataframe(result)
        
#         return result

# def format_affiliations(affiliation_data, use_api = False):
        
#         result = Affiliations()

#         if ((type(affiliation_data) != pd.DataFrame) and (type(affiliation_data) != pd.Series)) and ((affiliation_data == None) or (affiliation_data == '')):
#             result = Affiliations()

#         if type(affiliation_data) == Affiliations:
#             result = affiliation_data

#         if type(affiliation_data) == Affiliation:
#             result.add_affiliation(affiliation=affiliation_data, use_api=use_api)

#         if type(affiliation_data) == pd.Series:
#             affiliation = Affiliation()
#             affiliation.add_series(affiliation_data)
#             result.add_affiliation(affiliation=affiliation, use_api=use_api)

#         if type(affiliation_data) == pd.DataFrame:
#             result.import_crossref_result(affiliation_data, use_api=use_api) # type: ignore

#         if (type(affiliation_data) == list) and (len(affiliation_data) > 0) and (type(affiliation_data[0]) == Affiliation):
#             result = Affiliations()
#             result.add_affiliations_list(affiliation_data)

#         if (type(affiliation_data) == list) and (len(affiliation_data) > 0) and (type(affiliation_data[0]) == dict):

#             for i in affiliation_data:
#                 affiliation = Affiliation.from_dict(i) # type: ignore
#                 result.add_affiliation(affiliation = affiliation, use_api=use_api) # type: ignorex
    
#         return result