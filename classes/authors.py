from ..importers.orcid import lookup_orcid
from .results import Results
from .affiliations import Affiliation, Affiliations, format_affiliations

import pandas as pd
import numpy as np

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

            full = given + ' ' + family # type: ignore
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

    def format_affiliations(self):

        affils_data = self.details.loc[0, 'affiliations']
        affiliations = format_affiliations(affils_data)
        self.details.at[0, 'affiliations'] = affiliations

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

        alphabetical = self.all['full_name'].sort_values().to_list().__repr__()
        return alphabetical
    
    def __len__(self) -> int:
        return len(self.details.keys())

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

    def format_affiliations(self):

        affils = self.all['affiliations'].apply(func=format_affiliations) # type: ignore
        self.all['affiliations'] = affils

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

def format_authors(author_data):
        
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
