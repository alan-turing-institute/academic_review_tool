
from ..utils.cleaners import deduplicate
from ..importers.orcid import lookup_orcid, get_author, get_author_works
from ..importers.orcid import search as search_orcid # type: ignore
from .entities import Entity, Entities
from .results import Results
from .affiliations import Affiliation, Affiliations, format_affiliations

import pandas as pd
import numpy as np
from pyorcid import Orcid # type: ignore

author_cols = ['author_id',
                                'full_name',
                                'given_name',
                                'family_name',
                                'email',
                                'keywords',
                                'affiliations',
                                'publications',
                                'orcid',
                                'google_scholar',
                                'scopus',
                                'wos',
                                'crossref',
                                'other_links',
                                'other_ids'
                                ]

def get_full_name(series: pd.Series) -> str:

            """
            Takes a Pandas Series containing author details and returns the author's name data as a string in full name ({given_name} {family_name}") format.

            Parameters
            ----------
            series : pandas.Series
                a series containing author data

            Returns
            -------
            result : str
                a full name.
            """

            given = series.loc['given_name']
            family = series.loc['family_name']

            if (given is None) or (given is np.nan):
                given = ''
            
            if (family is None) or (family is np.nan):
                family = ''

            if ((type(family) == str) and (',' in family)) and ((given == None) or (given == 'None') or (given == '')):
                split_name = family.split(',')
                given = split_name[0].strip()
                series.loc['given_name'] = given

                family = split_name[1].strip()
                series.loc['family_name'] = family

            full = given + ' ' + family # type: ignore
            full = full.strip()

            if (full == '') or (full == ' '):
                full = 'no_name_given'

            full_name = series.loc['full_name']
            if (full_name == None) or (full_name == 'None') or (full_name == '') or (full_name == 'no_name_given'):
                result = full
            else:
                result = full_name

            return result

def generate_author_id(author_data: pd.Series) -> str:

        """
            Takes a Pandas Series containing author details and returns a unique identifier code.

            Parameters
            ----------
            author_data : pandas.Series
                a series containing author data

            Returns
            -------
            author_id : str
                an author ID.
        """

        author_data = author_data.copy(deep=True).dropna().astype(str).str.lower()

        author_id = 'A:'

        if 'given_name' in author_data.index:
            given_name = author_data['given_name']
        else:
            given_name = ''
        
        if 'family_name' in author_data.index:
            family_name = author_data['family_name']
        else:
            family_name = ''

        if 'full_name' in author_data.index:
            full_name = author_data['full_name']
        else:
            full_name = ''

        if ((family_name == None) or (family_name == '') or (family_name == 'None')) and ((full_name != None) and (len(full_name)>0)):
            
            if full_name == 'no_name_given':
                author_id = author_id + '#NA#'
            
            else:
                

                if ' ' in full_name:

                    full_split = full_name.lower().split(' ')
                    first = full_split[0].split(' ')[0].strip()
                    last = full_split[-1]
                
                else:
                    if ',' in full_name:
                        full_split = full_name.lower().split(',')
                        first = full_split[-1].split(' ')[0].strip()
                        last = full_split[0].strip()
                    else:
                        first = full_name
                        last = ''


                author_id = author_id + '-' + first + '-' + last

        else:

            if (given_name != None) and (len(given_name)>0):
                first = given_name.lower().split(' ')[0].strip()
                author_id = author_id + '-' + first
            
            
            if (family_name != None) and (len(family_name)>0):
                family_clean = family_name.lower().strip().replace(' ', '-')
                author_id = author_id + '-' + family_clean

        uid = ''

        if 'orcid' in author_data.index:
            uid = author_data['orcid']
        
        if (uid == None) or (uid == 'None') or (uid == ''):
            
            if 'google_scholar' in author_data.index:
                uid = author_data['google_scholar']
            
            if (uid == None) or (uid == 'None') or (uid == ''):
                if 'scopus' in author_data.index:
                    uid = author_data['scopus']
                
                if (uid == None) or (uid == 'None') or (uid == ''):
                    if 'crossref' in author_data.index:
                        uid = author_data['crossref']
                    
                    if (uid == None) or (uid == 'None') or (uid == ''):
                            uid = ''
        
        uid_shortened = uid.replace('https://', '').replace('http://', '').replace('www.', '').replace('orcid.org/','').replace('scholar.google.com/','').replace('citations?','').replace('user=','')[:30]

        author_id = author_id + '-' + uid_shortened
        author_id = author_id.replace('A:-', 'A:').replace("'s", '').replace('\r', '').replace('\n', '').replace('.', '').replace('(','').replace(')','').replace("'", "").replace('"', '').replace('`','').replace('’','').replace('--', '-').replace('A:-', 'A:').strip('-')

        return author_id

class Author(Entity):

    """
    This is an Author object. It is designed to store data about an individual author and their publications.
    
    Parameters
    ----------
    author_id : str
        a unique identifier assigned to the author. Defaults to None.
    full_name : str
        the author's name in full name ("{given_name} {family_name}") format. Defaults to None.
    given_name : str
        the author's given name or first name. Defaults to None.
    family_name : str
        the author's family name or last name. Defaults to None.
    email : str
        the author's email address. Defaults to None.
    affiliations : list or dict or Affiliations
        data on the author's organisational affiliations. May be a list, dict, or Affiliations object. Defaults to None.
    publications : list or dict or pandas.DataFrame or Results
        data on the author's publications. May be a list, dict, Pandas DataFrame or Results object. Defaults to None.
    orcid : str
        an Orcid identifier assigned to the author. Defaults to None.
    google_scholar : str
        a Google Scholar identifier assigned to the author. Defaults to None.
    scopus : str
        an Scopus identifier assigned to the author. Defaults to None.
    crossref : str
        an CrossRef identifier assigned to the author. Defaults to None.
    other_links : str or list
        any other links associated with the author. Defaults to None.
    
    Attributes
    ----------
    summary : pandas.DataFrame
        a dataframe summarising the Author's data.
    publications : Results
        a Results dataframe containing data on the Author's publications.
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
        author_id : str
            a unique identifier assigned to the author. Defaults to None.
        full_name : str
            the author's full name formatted as "{given_name} {family_name}". Defaults to None.
        given_name : str
            the author's given name or first name. Defaults to None.
        family_name : str
            the author's family name or last name. Defaults to None.
        email : str
            the author's email address. Defaults to None.
        affiliations : list or dict or Affiliations
            data on the author's organisational affiliations. May be a list, dict, or Affiliations object. Defaults to None.
        publications : list or dict or pandas.DataFrame or Results
            data on the author's publications. May be a list, dict, Pandas DataFrame or Results object. Defaults to None.
        orcid : str
            an Orcid identifier assigned to the author. Defaults to None.
        google_scholar : str
            a Google Scholar identifier assigned to the author. Defaults to None.
        scopus : str
            an Scopus identifier assigned to the author. Defaults to None.
        crossref : str
            an CrossRef identifier assigned to the author. Defaults to None.
        other_links : str or list
            any other links associated with the author. Defaults to None.
        """

        super().__init__()

        if type(given_name) == str:
            given_name = given_name.strip()

        if type(family_name) == str:
            family_name = family_name.strip()

        if ((type(family_name) == str) and (',' in family_name)) and ((given_name == None) or (given_name == '')):
            split_name = family_name.split(',')
            given_name = split_name[0].strip()
            family_name = split_name[1].strip()

        
        global author_cols

        self.summary = pd.DataFrame(columns = author_cols,
                                dtype = object)
        
        
        self.summary.loc[0] = pd.Series(dtype=object)
        self.summary.loc[0, 'author_id'] = author_id
        self.summary.loc[0, 'full_name'] = full_name
        self.summary.loc[0, 'given_name'] = given_name
        self.summary.loc[0, 'family_name'] = family_name
        self.summary.loc[0, 'email'] = email
        self.summary.loc[0, 'affiliations'] = affiliations
        self.summary.loc[0, 'publications'] = publications
        self.summary.loc[0, 'orcid'] = orcid
        self.summary.loc[0, 'google_scholar'] = google_scholar
        self.summary.loc[0, 'scopus'] = scopus
        self.summary.loc[0, 'crossref'] = crossref
        self.summary.loc[0, 'other_links'] = other_links

        full_name = self.full_name()
        if full_name != self.summary.loc[0, 'full_name']:
            self.summary.loc[0, 'full_name'] = full_name

        self.publications = Results()

    def generate_id(self):

        """
        Returns a unique identifier based on the Author's data.

        Returns
        -------
        author_id : str
            an author identifier.
        """

        author_data = self.summary.loc[0]

        author_id = generate_author_id(author_data) # type: ignore
        return author_id

    def update_id(self):

        """
        Replaces the Author's existing unique identifier with a newly generated unique identifier based on the Author's data.
        """

        current_id = self.summary.loc[0, 'author_id']
        new_id = self.generate_id()

        if (current_id != new_id) or (current_id == None) or (current_id == 'None') or (current_id == '') or (current_id == 'A:#NA#'):
            self.summary.loc[0, 'author_id'] = new_id
        
    
    def __getitem__(self, key) -> object:
        
        """
        Retrieves an Author attribute or datapoint using a key. The key may be an attribute name, dataframe index position, or dataframe column name.

        Parameters
        ----------
        key : object
            an attribute name, dataframe index position, or dataframe column name.
        
        Returns
        -------
        value : object
            an object associated with the inputted key.
        """
        
        if key in self.__dict__.keys():
            return self.__dict__[key]
        
        if key in self.summary.columns:
            return self.summary.loc[0, key]
        
        if key in self.publications.columns:
            return self.publications[key]

    def __repr__(self) -> str:

        """
        Defines how Author objects are represented in string form.
        """

        return str(self.summary.loc[0, 'full_name'])
    
    def full_name(self) -> str:

        """
        Returns the author's name data as a string in full name ("{given_name} {family_name}") format.

        Returns
        -------
        result : str
            the Author's name data in full name ("{given_name} {family_name}") format.
        """

        series = self.summary.loc[0]
        return get_full_name(series=series) # type: ignore


    def update_full_name(self):

            """
            Updates the Author's full name entry using the current Author data.
            """

            full_name = self.full_name()
            self.summary.loc[0, 'full_name'] = full_name

    def name_set(self) -> set:

        """
        Returns the Author's given name and family name as a set.
        """

        given = str(self.summary.loc[0, 'given_name'])
        family = str(self.summary.loc[0, 'family_name'])

        return set([given, family])

    def has_orcid(self) -> bool:

        """
        Returns True if the Author has an Orcid ID associated. Else, returns False.
        """

        orcid = self.summary.loc[0, 'orcid']

        if (type(orcid) == str) and (orcid != ''):
            return True
        else:
            return False

    def format_affiliations(self):

        """
        Formats the Author's affiliations data as an Affiliations object.
        """

        affils_data = self.summary.loc[0, 'affiliations']
        affiliations = format_affiliations(affils_data)
        self.summary.at[0, 'affiliations'] = affiliations

    def add_series(self, series: pd.Series):

        """
        Adds a Pandas Series object to the Author's summary data.
        """

        self.summary.loc[0] = series

    def from_series(series: pd.Series): # type: ignore

        """
        Takes a Pandas Series and returns an Author object.

        Parameters
        ----------
        series : pandas.Series
            a Pandas Series with indices that match the names of columns in the Author summary dataframe.

        Returns
        -------
        author : Author
            an Author object.
        """

        author = Author()
        author.add_series(series)
        return author
    
    def add_dataframe(self, dataframe: pd.DataFrame):

        """
        Adds data from a Pandas DataFrame to the Author object.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            a Pandas DataFrame with columns that match the names of columns in the Author's summary dataframe.
        """

        series = dataframe.loc[0]
        self.add_series(series) # type: ignore

    def from_dataframe(dataframe: pd.DataFrame): # type: ignore

        """
        Takes a Pandas DataFrame and returns an Author object.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            a Pandas DataFrame with columns that match the names of columns in the Author's summary dataframe.

        Returns
        -------
        author : Author
            an Author object.
        """

        author = Author()
        author.add_dataframe(dataframe)
        return author

    def import_crossref(self, crossref_result: dict):

        """
        Reads a CrossRef API result formatted as a dictionary and adds its data to the Author object.

        Parameters
        ----------
        crossref_result : dict
            CrossRef API result formatted as a dictionary.
        """

        if 'given' in crossref_result.keys():
            self.summary.loc[0, 'given_name'] = crossref_result['given']
        
        if 'family' in crossref_result.keys():
            self.summary.loc[0, 'family_name'] = crossref_result['family']
        
        if 'email' in crossref_result.keys():
            self.summary.loc[0, 'email'] = crossref_result['email']

        if 'affiliation' in crossref_result.keys():
            if (type(crossref_result['affiliation']) == list) and (len(crossref_result['affiliation']) > 0):
                self.summary.at[0, 'affiliations'] = crossref_result['affiliation'][0]

            else:
                if (type(crossref_result['affiliation']) == dict) and (len(crossref_result['affiliation'].keys()) > 0):
                    key = list(crossref_result['affiliation'].keys())[0]
                    self.summary.at[0, 'affiliations'] = crossref_result['affiliation'][key]

        if 'ORCID' in crossref_result.keys():
            self.summary.loc[0, 'orcid'] = crossref_result['ORCID']
        
        else:
            if 'orcid' in crossref_result.keys():
                self.summary.loc[0, 'orcid'] = crossref_result['orcid']

        # self.summary.loc[0, 'google_scholar'] = google_scholar
        # self.summary.loc[0, 'crossref'] = crossref
        # self.summary.loc[0, 'other_links'] = other_links

        self.update_full_name()
    
    def import_orcid(self, orcid_id: str):

        """
        Looks up an author record in the ORCID API using an ORCID author ID. If one is found, adds its data to the Author object.

        Parameters
        ----------
        orcid_id : str
            ORCID author ID.
        """

        try:
            auth_res = get_author(orcid_id)
            auth_record = auth_res.record()

        except:
            auth_record = {}
            auth_res = None

            try:
                auth_df = lookup_orcid(orcid_id)
                cols = auth_df.columns.to_list()

                if len(auth_df) > 0:

                    author_details = auth_df.loc[0]

                    if 'name' in cols:
                        self.summary.loc[0, 'given_name'] = author_details['name']
                    
                    if 'family name' in cols:
                        self.summary.loc[0, 'family_name'] = author_details['family name']
                    
                    if 'emails' in cols:
                        self.summary.at[0, 'email'] = author_details['emails']
                    
                    if 'employment' in cols:
                        self.summary.at[0, 'affiliations'] = author_details['employment']
                    
                    if 'works' in cols:
                        self.summary.at[0, 'publications'] = author_details['works']
                    
                    self.summary.loc[0, 'orcid'] = orcid_id
                    self.summary.loc[0, 'orcid'] = orcid_id
                    self.update_full_name()

                    return

            except:
                pass


        if 'person' in auth_record.keys():
            author_details = auth_record['person']
        else:
            author_details = {}
        
        details_keys = author_details.keys()
        
        if 'name' in details_keys:
            
            if 'given-names' in author_details['name']:
                given_list = list(author_details['name']['given-names'].values())
                given = ' '.join(given_list)
                self.summary.loc[0, 'given_name'] = given

            if 'family-name' in author_details['name']:
                family_list = list(author_details['name']['family-name'].values())
                family = ' '.join(family_list)
                self.summary.loc[0, 'family_name'] = family
            
        if 'emails' in details_keys:
            emails_dict = author_details['emails']
            if type(emails_dict) == dict:
                if 'email' in emails_dict.keys():
                    emails_data = emails_dict['email']
                    emails_list = []

                    if (type(emails_data) == list) and (len(emails_data)>0):
                        for i in emails_data:
                            email_addr = i['email']
                            emails_list.append(email_addr)

                    self.summary.at[0, 'email'] = emails_list
        
        if 'keywords' in details_keys:
            kws_dict = author_details['keywords']
            if type(kws_dict) == dict:
                if 'keyword' in kws_dict.keys():
                    kws_data = kws_dict['keyword']
                    kws_list = []

                    if (type(kws_data) == list) and (len(kws_data)>0):
                        for i in kws_data:
                            kwd = i['content']
                            kws_list.append(kwd)

                    self.summary.at[0, 'keywords'] = kws_list

        if 'external-identifiers' in details_keys:
            ext_ids_dict = author_details['external-identifiers']
            if type(ext_ids_dict) == dict:
                if 'external-identifier' in ext_ids_dict.keys():
                    ext_ids_data = ext_ids_dict['external-identifier']
                    ext_ids_formatted = {}

                    if (type(ext_ids_data) == list) and (len(ext_ids_data)>0):
                        for i in ext_ids_data:
                            id_type = i['external-id-type']
                            value = i['external-id-value']
                            url = i['external-id-url']
                            ext_ids_formatted[id_type] = {'id': value, 'url': url}

                    self.summary.at[0, 'keywords'] = ext_ids_formatted

        
        
        self.summary.loc[0, 'orcid'] = orcid_id

        if type(auth_res) == Orcid:
            try:
                works = auth_res.works() # type: ignore
            except:
                works = tuple()
        else:
            works = tuple()
        
        if len(works) > 0:

            works_list = works[0]

            df = pd.DataFrame(works_list)
            df = df.rename(columns={'publication-date': 'date', 'journal title':'source', 'url':'link'})
            df = df.drop(['organization', 'organization-address'], axis=1)

            self.publications.add_dataframe(df)
            self.publications.drop_empty_rows()
            self.publications.remove_duplicates(drop_empty_rows=True)
        
        self.update_full_name()
        

    def from_crossref(crossref_result: dict): # type: ignore

        """
        Reads a CrossRef API result formatted as a dictionary and returns as an Author object.

        Parameters
        ----------
        crossref_result : dict
            CrossRef API result formatted as a dictionary.
        
        Returns
        -------
        author : Author
            an Author object.
        """

        author = Author()
        author.import_crossref(crossref_result)
        author.update_full_name()

        return author
    
    def from_orcid(orcid_id: str): # type: ignore

        """
        Looks up an author record in the ORCID API using an ORCID author ID. If one is found, returns as an Author object.

        Parameters
        ----------
        orcid_id : str
            ORCID author ID.

        Returns
        -------
        author : Author
            an Author object.
        """

        author = Author()
        author.import_orcid(orcid_id)
        author.update_full_name()

        return author
    
    def update_from_orcid(self):

        """
        Looks up Author's ORCID author ID. If one is found, uses to update the Author object.
        """

        orcid = self.summary.loc[0, 'orcid']

        if (orcid != None) and (orcid != '') and (orcid != 'None'):
            
            orcid = str(orcid).replace('https://', '').replace('http://', '').replace('orcid.org/', '')
            self.summary.loc[0, 'orcid'] = orcid

            self.import_orcid(orcid_id = orcid)

class Authors(Entities):

    """
    This is an Authors object. It contains a collection of Author objects and a summary of data about them.
    
    Parameters
    ----------
    authors_data : list or dict
            Optional: an iterable of authors data. Data on individual authors must be formatted as a dictionary.
    
    Attributes
    ----------
    summary : pandas.DataFrame
        a dataframe summarising the Authors collection's data.
    all : dict
        a dictionary storing formatted Author objects.
    data : list
        a list of any unformatted data associated with Author objects in the collection.
    """

    def __init__(self, authors_data = []):
        
        """
        Initialises Authors instance.
        
        Parameters
        ----------
        authors_data : list or dict
            Optional: an iterable of authors data. Data on individual authors must be formatted as a dictionary.

        """

        super().__init__()
        
        global author_cols
        self.summary = pd.DataFrame(columns = author_cols,
                                dtype = object)
        
        self.data = authors_data

        if (type(authors_data) == list) and (len(authors_data)>0) and (type(authors_data[0]) == Author):

            for i in authors_data:
                auth = i.summary.copy(deep=True)
                self.summary = pd.concat([self.summary, auth])

            self.summary = self.summary.reset_index().drop('index',axis=1)

        else:

            if type(authors_data) == dict:
                
                values = list(authors_data.values())

                if type(values[0]) == Author:

                    for a in authors_data.keys():
                        
                        index = len(self.summary)
                        auth = a.summary.copy(deep=True)
                        self.summary = pd.concat([self.summary, auth])
                        self.summary.loc[index, 'author_id'] = a

                    self.summary = self.summary.reset_index().drop('index',axis=1)
                


    def __getitem__(self, key):
        
        """
        Retrieves Authors attribute using a key.
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

        """
        Defines how Authors objects are represented in string form.
        """

        alphabetical = self.summary['full_name'].sort_values().to_list().__repr__()
        return alphabetical
    
    def __len__(self) -> int:

        """
        Returns the number of Author objects in the Authors collection. Uses the number of Author objects stored in the Authors.all dictionary.

        Returns
        -------
        result : int
            the number of Author objects contained in the Authors.all dictionary.
        """

        return len(self.all.keys())

    def remove_duplicates(self, drop_empty_rows = True, sync=True):

        """
        Removes duplicate Author entries.

        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to True.
        sync : bool
            whether to synchronise the Authors.summary dataframe with the Authors.all dictionary. Defaults to True.
        
        Returns
        -------
        self : Authors
            an Authors object.
        """

        if drop_empty_rows == True:
            self.drop_empty_rows()
        
        df = self.summary.copy(deep=True)
        df['orcid'] = df['orcid'].str.replace('http://', '', regex=False).str.replace('https://', '', regex=False).str.replace('orcid.org/', '', regex=False).str.strip('/')
        df['google_scholar'] = df['google_scholar'].str.replace('http://', '', regex=False).str.replace('https://', '', regex=False).str.replace('scholar.google.com/', '', regex=False).str.replace('citations?', '', regex=False).str.replace('user=', '', regex=False).str.strip('/')
       
        df = df.sort_values(by = ['orcid', 'google_scholar', 'crossref', 'full_name']).reset_index().drop('index', axis=1)
        self.summary = deduplicate(self.summary)

        if sync == True:
            self.sync_summary()

        return self
        


    def merge(self, authors, drop_duplicates = False, drop_empty_rows=False):

        """
        Merges the Authors collection with another Authors collection.

        Parameters
        ----------
        authors : Authors
            the Authors collection to merge with.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        
        Returns
        -------
        self : Authors
            the merged Authors collection.
        """

        left = self.summary.copy(deep=True)
        right = authors.summary.copy(deep=True)
        
        merged = pd.concat([left, right])

        self.summary = merged.drop_duplicates(subset=['author_id', 'family_name', 'orcid'], ignore_index=True)

        for i in authors.all.keys():
            if i not in self.all.keys():
                self.all[i] = authors.all[i]

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

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        return self

    def update_full_names(self):

        """
        Checks all Author entries for name data and uses this to update the 'full_name' field.
        """

        for i in self.summary.index:
            new = get_full_name(self.summary.loc[i])
            old = self.summary.loc[i, 'full_name']

            if (type(old) != str) or ((type(old) == str) and (len(old) == 0)) or (old is None) or (old == '') or (old is np.nan):
                self.summary.loc[i, 'full_name'] = new
        
        self.update_author_ids()
        self.sync_summary()

    def add_author(self, author: Author, data = None, drop_duplicates = False, drop_empty_rows = False, update_from_orcid = False):

        """
        Adds an Author or author data to the Authors collection.

        Parameters
        ----------
        author : Author
            an Author object to add.
        data : dict
            Optional: a dictionary containing author data. Dictionary keys must match the names of columns in the Authors.summary dataframe.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        update_from_orcid : bool
            whether to update the Author data using the ORCID API (if an ORCID ID is present). Defaults to False.
        """

        if update_from_orcid == True:
            orcid = author.summary.loc[0,'orcid']
            if (orcid != None) and (orcid != '') and (orcid != 'None'):
                author.update_from_orcid()

        author.update_id()

        author_id = str(author.summary.loc[0, 'author_id'])

        if (author_id in self.all.keys()) or (author_id in self.summary['author_id'].to_list()):
            print(f'Warning: {author_id} is already in authors')

        # if author_id in self.summary['author_id'].to_list():
        #     id_count = len(self.summary[self.summary['author_id'].str.contains(author_id)]) # type: ignore
        #     author_id = author_id + f'#{id_count + 1}'
        #     author.summary.loc[0, 'author_id'] = author_id

        self.summary = pd.concat([self.summary, author.summary])
        self.summary = self.summary.reset_index().drop('index', axis=1)

        self.all[author_id] = author

        if data == None:
            data = author.summary.to_dict(orient='index')
        
        self.data.append(data)

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)


    def add_authors_list(self, authors_list: list, drop_duplicates = False, drop_empty_rows = False):
        
        """
        Adds a list containing Author objects to the Authors collection.

        Parameters
        ----------
        authors_list : list[Author]
            a list of Author objects.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        for i in authors_list:
            if type(i) == Author:
                self.add_author(author = i, drop_duplicates=False)
        
        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

    def mask_entities(self, column, query: str = 'request_input', ignore_case: bool = True):

        """
        Selects rows in Authors.summary dataframe with affiliations and funders that contain a query string.

        Parameters
        ----------
        column : str
            name of column to mask.
        query : str
            a string to check for. Defaults to requesting from user input.
        ignore_case : bool
            whether to ignore the case of string data. Defaults to True.

        Returns
        -------
        masked : pandas.DataFrame
            selected rows from the Authors.summary dataframe.
        """

        if query == 'request_input':
            query = input('Search query').strip()

        query = str(query).strip()

        def entity_masker(entities):
            
            if 'contains' in entities.__dir__():
                res = entities.contains(query=query, ignore_case=ignore_case)
                
            else:
                res = False

            return res
        
        masked = self.summary[self.summary[column].apply(entity_masker)]

        return masked

    def update_author_ids(self):
        
        """
        Updates author IDs for all rows in the Authors.summary dataframe.
        """

        for i in self.summary.index:
            author_data = self.summary.loc[i]
            author_id = generate_author_id(author_data)
            self.summary.loc[i, 'author_id'] = author_id
        


    def sync_all(self, drop_duplicates = False, drop_empty_rows=False):
        
        """
        Updates the Authors.summary dataframe using the Author objects in the Authors.all dictionary.

        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        for i in self.all.keys():
            author = self.all[i]
            author.update_id()
            series = author.summary.loc[0]
            all = self.summary.copy(deep=True).astype(str)
            auth_index = all[all['author_id'] == i].index.to_list()[0]
            self.summary.loc[auth_index] = series

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

    def sync_summary(self, drop_duplicates = False, drop_empty_rows=False):

        """
        Updates all Author objects in the Authors.all dictionary using the Authors.summary dataframe.

        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        self.update_author_ids()

        for i in self.summary.index:

            auth_data = self.summary.loc[i]
            auth_id = auth_data['author_id']

            if auth_id != None:
                auth = Author.from_series(auth_data) # type: ignore
                self.all[auth_id] = auth

            else:
                auth_id = generate_author_id(auth_data)
                auth_data['author_id'] = auth_id
                auth = Author.from_series(auth_data) # type: ignore
                self.all[auth_id] = auth
        
        keys = list(self.all.keys())
        for key in keys:
            auth_ids = self.summary['author_id'].to_list()
            if key not in auth_ids:
                del self.all[key]

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

    def sync(self, drop_duplicates = False, drop_empty_rows=False):
        
        """
        Synchronises the Authors.summary dataframe with the Author objects in the Authors.all dictionary.
        
        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        all_len = len(self.summary)
        details_len = len(self.all)

        if all_len > details_len:
            self.sync_summary(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
            return
        else:
            if details_len > all_len:
                self.sync_all(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
                return
            else:
                self.sync_summary(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
                self.sync_all(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
                return


    def drop_empty_rows(self):

        """
        Drops rows that contain no data from Authors.summary dataframe.

        Returns
        -------
        self : Authors
            an Authors object.
        """

        ignore_cols = ['author_id', 'affiliations', 'publications', 'other_links']

        df = self.summary.copy(deep=True)
        df['full_name'] = df['full_name'].replace('no_name_given', None)
        df = df.dropna(axis=0, how='all')
        drop_cols = [c for c in df.columns if c not in ignore_cols]
        df = df.dropna(axis=0, how='all', subset=drop_cols).reset_index().drop('index', axis=1)

        self.summary = df
        self.sync_summary()

        return self

    def format_affiliations(self, drop_empty_rows=False):

        """
        Formats authors' affiliations data as Affiliations objects and stores in Review's Affiliations attribute.

        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        """

        if drop_empty_rows == True:
            self.drop_empty_rows()

        affils = self.summary['affiliations'].apply(func=format_affiliations) # type: ignore
        self.summary['affiliations'] = affils
        self.sync_summary()

    def update_from_orcid(self, drop_duplicates = False, drop_empty_rows=False):

        """
        Looks up all Authors ORCID author IDs and, if found, uses to update the Authors collection.

        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        self.sync()

        author_ids = self.all.keys()

        for a in author_ids:

            self.all[a].update_from_orcid()
            details = self.all[a].summary.loc[0]
            
            df_index = self.summary[self.summary['author_id'] == a].index.to_list()[0]
            self.summary.loc[df_index] = details

            new_id = details['author_id']
            if new_id != a:
                self.all[new_id] = self.all[a]
                del self.all[a]
        
        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

    def import_orcid_ids(self, orcid_ids: list, drop_duplicates = False, drop_empty_rows=False):

        """
        Looks up a list of ORCID author IDs using the ORCID API and adds any data found to the Authors collection.

        Parameters
        ----------
        orcid_ids : list[str]
            list containing ORCID author IDs.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        for i in orcid_ids:

            auth = Author.from_orcid(i) # type: ignore
            self.add_author(author = auth, data = i)
        
        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

    def from_orcid_ids(orcid_ids: list, drop_duplicates = False, drop_empty_rows=False): # type: ignore

        """
        Looks up a list of ORCID author IDs using the ORCID API and returns all data found as an Authors object.

        Parameters
        ----------
        orcid_ids : list[str]
            list containing ORCID author IDs.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.

        Returns
        -------
        authors : Authors
            an Authors object.
        """

        authors = Authors()
        authors.import_orcid_ids(orcid_ids, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        return authors

    def has_orcid(self):

        """
        Returns all rows in Authors.summary which contain ORCID IDs.
        """

        return self.summary[~self.summary['orcid'].isna()]

    def import_crossref(self, crossref_result: list, drop_duplicates = False, drop_empty_rows=False):

        """
        Reads a list of CrossRef API results and adds the data to the Authors collection.

        Parameters
        ----------
        crossref_result : list
            list of CrossRef API results.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        for i in crossref_result:

            auth = Author.from_crossref(i) # type: ignore
            self.add_author(author = auth, data = i)
        
        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)
    
    def from_crossref(crossref_result: list, drop_duplicates = False, drop_empty_rows=False): # type: ignore

        """
        Reads a list of CrossRef API results and returns as an Authors object.

        Parameters
        ----------
        crossref_result : list
            list of CrossRef API results.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.

        Returns
        -------
        authors : Authors
            an Authors object.
        """

        authors = Authors()
        authors.import_crossref(crossref_result, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        return authors
    
    def import_wos(self, wos_result, drop_duplicates = False, drop_empty_rows=False):
        
        """
        Reads a Web of Science API result and adds the data to the Authors collection.

        Parameters
        ----------
        wos_result : list or dict
            list of Web of Science API results.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        authors_data = []

        if type(wos_result) == list:
            authors_data = wos_result
        
        elif type(wos_result) == dict:

            if 'authors' in wos_result.keys():
                authors_data = wos_result['authors']
            else:
                authors_data = []
            
            if 'bookEditors' in wos_result.keys():
                editors_data = wos_result['bookEditors']
            else:
                editors_data = []
        
        authors_df = pd.DataFrame(authors_data)
        authors_df = authors_df.rename(columns={'displayName': 'full_name', 'researcherId': 'wos'}).drop('wosStandard',axis=1)
        
        names_split = authors_df['full_name'].str.split(',').to_list()
        for i in range(0,len(names_split)):
            
            name = names_split[i]

            if len(name) == 0:
                continue

            if len(name) == 1:
                family = name[0]
                full = name[0]
                given = None

            if len(name) > 1:
                given = name[1].strip()
                family = name[0].strip()
                full = given + ' ' + family

            authors_df.loc[i, 'full_name'] = full
            authors_df.loc[i, 'given_name'] = given
            authors_df.loc[i, 'family_name'] = family

        self.summary = pd.concat([self.summary, authors_df])

        if drop_empty_rows == True:
            self.drop_empty_rows()
        
        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        self.sync_summary(drop_duplicates=False, drop_empty_rows=False)

    def from_wos(wos_result, drop_duplicates = False, drop_empty_rows=False): # type: ignore

        """
        Reads a Web of Science API result and returns as an Authors collection.

        Parameters
        ----------
        wos_result : list or dict
            list of Web of Science API results.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        
        Returns
        -------
        authors : Authors
            an Authors object.
        """

        authors = Authors()
        authors.import_wos(wos_result=wos_result, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        return authors

    def affiliations(self, drop_duplicates = False, drop_empty_rows=False) -> dict:

        """
        Returns a dictionary containing authors and their associated affiliations.
            * Keys: author IDs
            * Values: Affiliations objects
        """

        self.sync_summary(drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)
        
        output = {}
        for auth_id in self.all.keys():
            auth = self.all[auth_id]
            affiliation = auth.summary.loc[0, 'affiliations']
            output[auth_id] = affiliation
        
        return output
    
    def search_orcid(self, query: str = 'request_input', add_to_authors: bool = True):

        """
        Searches for author records using the Orcid API.

        Parameters
        ----------
        query : str
            query to search. Allows for keywords and Boolean logic.
        add_to_authors : bool
            whether to add results to the Authors collection.
        
        Returns
        -------
        result : pandas.DataFrame
            search result.
        """


        res = search_orcid(query=query)
        res = res.rename(columns={'credit-name': 'full_name', 'given-names': 'given_name', 'family-name': 'family_name', 'family-names': 'family_name', 'institution-name': 'affiliations', 'orcid-id': 'orcid'}) # type: ignore

        res = res.replace(np.nan, '')

        if add_to_authors == True:

            self.summary = pd.concat([self.summary, res])
            self.update_full_names()
            self.drop_empty_rows()
        
        return res

def format_authors(author_data, drop_duplicates = False, drop_empty_rows=False):
        
        """
        Formats a collection of authors data as an Authors object.

        Parameters
        ----------
        author_data : object
            a collection of authors data.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

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
        
        if (type(author_data) == dict) and ('authors' in author_data.keys()):
            result = Authors.from_wos(wos_result=author_data, drop_duplicates=True, drop_empty_rows=True) # type: ignore
    
        result.format_affiliations()

        if drop_empty_rows == True:
            result.drop_empty_rows()

        if drop_duplicates == True:
            result.remove_duplicates(drop_empty_rows=drop_empty_rows)

        return result




