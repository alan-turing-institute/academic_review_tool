from ..utils.cleaners import deduplicate
from ..datasets.stopwords.stopwords import all_stopwords
from ..importers.crossref import lookup_funder

from .entities import Entity, Entities


import pandas as pd
import numpy as np

from geopy.geocoders import Nominatim # type: ignore
from nltk.tokenize import word_tokenize # type: ignore

def generate_affiliation_id(affiliation_data: pd.Series):

        """
            Takes a Pandas Series containing affiliation details and returns a unique identifier code (affiliation ID).

            Parameters
            ----------
            affiliation_data : pandas.Series
                a series containing affiliation data.

            Returns
            -------
            affiliation_id : str
                an affiliation ID.
        """

        affiliation_data = affiliation_data.copy(deep=True).dropna().astype(str).str.lower()

        affiliation_id = 'AFFIL:'

        if 'name' in affiliation_data.index:
            name = affiliation_data['name']
        else:
            name = ''

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

        affiliation_id = affiliation_id + '-' + name_shortened.lower()

        if 'location' in affiliation_data.index:
            location = affiliation_data['location']
            if type(location) == str:
                location = location.split(',')
                location_shortened = location[0].strip()
                if location[0] != location[-1]:
                    location_shortened = location_shortened + '-' + location[-1].strip()
            else:
                location_shortened = ''
        else:
            location_shortened = ''
        
        affiliation_id = affiliation_id + '-' + location_shortened.lower()

        uid = ''

        if 'uri' in affiliation_data.index:
            uid = affiliation_data['uri']

            if (uid == None) or (uid == 'None') or (uid == ''):
                if 'crossref_id' in affiliation_data.index:
                    uid = affiliation_data['crossref_id']

                    if (uid == None) or (uid == 'None') or (uid == ''):
                        if 'website' in affiliation_data.index:
                            uid = affiliation_data['website']

                            if (uid == None) or (uid == 'None') or (uid == ''):
                                uid = ''
        
        uid_shortened = uid.replace('https://', '').replace('http://', '').replace('www.', '').replace('dx.','').replace('doi.org/','').replace('user=','')[:24]

        affiliation_id = affiliation_id + '-' + uid_shortened
        affiliation_id = affiliation_id.replace('AFFIL:-', 'AFFIL:').replace("'s", '').replace('\r', '').replace('\n', '').replace('.', '').replace("'", "").replace('"', '').replace('(','').replace(')','').replace('`','').replace('’','').replace('--', '-').replace('AFFIL:-', 'AFFIL:').replace(' ', '-').strip('-')

        return affiliation_id

class Affiliation(Entity):

    """
        This is a Affiliation object. It is designed to store data about an organisation that an author is affiliated with.
    
        Parameters
        ----------
        affiliation_id : str
            a unique identifier assigned to the affiliation. Defaults to None.
        name : str
            the affiliation's name. Defaults to None.
        location : str
            a street address associted with the affiliation. Defaults to None.
        location : str
            an place name associted with the affiliation. Defaults to None.
        email : str
            the affiliation's email address. Defaults to None.
        uri : str
            a DOI or other unique identifier assigned to the affiliation. Defaults to None.
        crossref_id : str
            a CrossRef identifier assigned to the affiliation. Defaults to None.
        website : str
            the affiliation's website. Defaults to None
        other_links : str or list
            any other links associated with the affiliation. Defaults to None.
        use_api : bool
            whether to update affiliation data using APIs (e.g. CrossRef). Defaults to None.
    
        Attributes
        ----------
        summary : pandas.DataFrame
            a dataframe summarising the Funder's data.
        publications : Results
            a Results dataframe containing data on the Funder's publications.
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
        Initialises an Affiliation instance.
        
        Parameters
        ----------
        affiliation_id : str
            a unique identifier assigned to the affiliation. Defaults to None.
        name : str
            the affiliation's name. Defaults to None.
        location : str
            a street address associted with the affiliation. Defaults to None.
        location : str
            an place name associted with the affiliation. Defaults to None.
        email : str
            the affiliation's email address. Defaults to None.
        uri : str
            a DOI or other unique identifier assigned to the affiliation. Defaults to None.
        crossref_id : str
            a CrossRef identifier assigned to the affiliation. Defaults to None.
        website : str
            the affiliation's website. Defaults to None
        other_links : str or list
            any other links associated with the affiliation. Defaults to None.
        use_api : bool
            whether to update affiliation data using APIs (e.g. CrossRef). Defaults to None.
        """

        super().__init__()

        orig_name_data = name

        if type(name) == str:
            name = name.strip().replace('&amp;', '&').replace('\n', '').replace('\n', ' ')
        
            if (location is None) and (address is None):

                name_split = name.split(',')
                name_split = [i.strip() for i in name_split]

                address = name
                location = name_split[-1]

                if (len(name_split) > 1) and (('department' in name_split[0].lower()) or ('institute of' in name_split[0].lower())):
                    name = name_split[0] + ', ' + name_split[1]
                else:
                    name = name_split[0]
        
        if type(name) == list:

            if (location is None) and (address is None):
                address = ', '.join(name)
                location = name[-1]
            
            name = name[0]

        if type(other_links) == str:
            other_links = other_links.replace('[','').replace(']','').split(',')
            other_links = [i.strip() for i in other_links]
        

        self.summary = pd.DataFrame(columns = [
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
        
        
        self.summary.loc[0] = pd.Series(dtype=object)
        self.summary.loc[0, 'affiliation_id'] = affiliation_id
        self.summary.loc[0, 'name'] = name
        self.summary.loc[0, 'location'] = location
        self.summary.loc[0, 'address'] = address
        self.summary.loc[0, 'email'] = email
        self.summary.loc[0, 'uri'] = uri
        self.summary.loc[0, 'crossref_id'] = crossref_id
        self.summary.loc[0, 'website'] = website
        self.summary.at[0, 'other_links'] = other_links

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
            except:
                loc = None
            
            if loc != None:
                self.summary.loc[0, 'address'] = loc.address

                if self.summary.loc[0, 'name'] == None:
                    self.summary.loc[0, 'name'] = loc.name
                
                if self.summary.loc[0, 'location'] == None:
                    self.summary.loc[0, 'location'] = loc.display_name

        self.update_id()

    def generate_id(self):

        """
        Returns a unique identifier (affiliation ID) based on the Affiliation's data.

        Returns
        -------
        affiliation_id : str
            an affiliation ID.
        """

        affiliation_data = self.summary.loc[0]

        affiliation_id = generate_affiliation_id(affiliation_data) # type: ignore
        return affiliation_id

    def update_id(self):

        """
        Replaces the Affiliation's existing unique identifier with a newly generated unique identifier based on the Affiliation's data.
        """

        current_id = str(self.summary.loc[0, 'affiliation_id'])

        if (current_id == None) or (current_id == 'None') or (current_id == '') or (current_id == 'AFFIL:000') or ('no_name_given' in current_id):
            auth_id = self.generate_id()
            self.summary.loc[0, 'affiliation_id'] = auth_id
        
    
    def __getitem__(self, key):
        
        """
        Retrieves an Affiliation attribute or datapoint using a key. The key may be an attribute name, dataframe index position, or dataframe column name.

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

    def __repr__(self) -> str:
        
        """
        Defines how Affiliation objects are represented in string form.
        """

        return str(self.summary.loc[0, 'name'])

    def has_uri(self) -> bool:
        
        """
        Returns True if the Affilation has a URI associated. Else, returns False.
        """

        uri = self.summary.loc[0, 'uri']

        if (type(uri) == str) and (uri != ''):
            return True
        else:
            return False

    def add_dict(self, data: dict):

        """
        Adds a dictionary of affiliation data to the Affiliation's summary dataframe.
        """

        if 'name' in data.keys():
            name = data['name']
            self.summary.loc[0, 'name'] = name
        
        if 'location' in data.keys():
            location = data['location']
            self.summary.loc[0, 'location'] = location

        if 'address' in data.keys():
            address = data['address']
            self.summary.loc[0, 'address'] = address
        
        if 'crossref_id' in data.keys():
            crossref_id = data['crossref_id']
            self.summary.loc[0, 'crossref_id'] = crossref_id

        if 'DOI' in data.keys():
            uri = data['DOI'].replace('http', '').replace('https', '').replace('dx.', '').replace('doi.org/', '').strip()
            self.summary.loc[0, 'uri'] = 'https://doi.org/' + uri
        else:
            if 'uri' in data.keys():
                uri = data['DOI'].replace('http', '').replace('https', '').replace('dx.', '').replace('doi.org/', '').strip()
                self.summary.loc[0, 'uri'] = 'https://doi.org/' + uri

        if 'url' in data.keys():
            website = data['url']
            self.summary.loc[0, 'website'] = website
        else:
            if 'link' in data.keys():
                website = data['link']
                self.summary.loc[0, 'website'] = website
            else:
                if 'website' in data.keys():
                    website = data['website']
                    self.summary.loc[0, 'website'] = website

    def from_dict(data: dict, use_api=False): # type: ignore

        """
        Takes a dictionary of affiliation data and returns an Affiliation object.

        Parameters
        ----------
        data : dict
            a dictionary of affiliation data. The dictionary must contain at least one of the following keys: 'name', 'location', 'address', 'email', 'crossref_id', 'DOI', 'URL'.
        use_api : bool
            whether to update affiliation data using APIs (e.g. CrossRef). Defaults to None.

        Returns
        -------
        affiliation : Affiliation
            an Affiliation object.
        """

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
        
    def add_series(self, series: pd.Series):

        """
        Adds a Pandas Series object to the Affiliation's summary dataframe.
        """

        self.summary.loc[0] = series

    def from_series(data: pd.Series): # type: ignore

        """
        Takes a Pandas Series and returns an Affiliation object.

        Parameters
        ----------
        data : pandas.Series
            a Pandas Series with indices that match the names of columns in the Affiliation's summary dataframe.

        Returns
        -------
        affiliation : Affiliation
            a Affiliation object.
        """

        affiliation = Affiliation()
        affiliation.add_series(data)

        return affiliation

    def add_dataframe(self, dataframe: pd.DataFrame):

        """
        Adds data from a Pandas DataFrame to the Affiliation object.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            a Pandas DataFrame with columns that match the names of columns in the Affiliation's summary dataframe.
        """

        series = dataframe.loc[0]
        self.add_series(series) # type: ignore

    def from_dataframe(data: pd.DataFrame): # type: ignore

        """
        Takes a Pandas DataFrame and returns an Affiliation object.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            a Pandas DataFrame with columns that match the names of columns in the Affiliation object's summary dataframe.

        Returns
        -------
        affiliation : Affiliation
            a Affiliation object.
        """

        affiliation = Affiliation()
        affiliation.add_dataframe(data)

        return affiliation

    def import_crossref_result(self, crossref_result: pd.Series):
        
        """
        Reads a CrossRef API result formatted as a pandas.Series and adds its data to the Affiliation object.

        Parameters
        ----------
        crossref_result : pandas.Series
            CrossRef API result.
        """

        if 'name' in crossref_result.index:
            name = crossref_result['name']
        else:
            name = self.summary.loc[0, 'name']

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
        
        self.summary.loc[0, 'name'] = name
        self.summary.loc[0, 'location'] = location
        self.summary.loc[0, 'email'] = email
        self.summary.loc[0, 'uri'] = uri
        self.summary.loc[0, 'crossref_id'] = crossref_id

    def from_crossref_result(crossref_result: pd.Series, use_api: bool = False): # type: ignore
        
        """
        Reads a CrossRef API result formatted as a pandas.Series and returns as an Affiliation object.

        Parameters
        ----------
        crossref_result : pandas.Series.
            CrossRef API result.
        
        Returns
        -------
        affiliation : Affiliation
            an Affiliation object.
        """

        if 'name' in crossref_result.index:
            name = crossref_result['name']
        else:
            name = None

        if 'location' in crossref_result.index:
            location = crossref_result['location']
        else:
            location = None

        if 'email' in crossref_result.index:
            email = crossref_result['email']
        else:
            email = None

        if 'uri' in crossref_result.index:
            uri  =crossref_result['uri']
        else:
            uri = None

        if 'id' in crossref_result.index:
            crossref_id = crossref_result['id']
        else:
            crossref_id = None

        affiliation = Affiliation(name=name, location=location, email=email, uri=uri, crossref_id=crossref_id, use_api=use_api) # type: ignore

        return affiliation

    def import_crossref(self, crossref_id: str, timeout = 60):

        """
        Looks up a CrossRef affiliation ID and adds the result to the Affiliation object.

        Parameters
        ----------
        crossref_id : str
            CrossRef funder ID.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        """

        res = lookup_funder(crossref_id, timeout)
        self.import_crossref_result(res.loc[0]) # type: ignore

    def from_crossref(crossref_id: str, use_api=True, timeout = 60): # type: ignore

        """
        Looks up a CrossRef affiliation ID and returns the result as a Affiliation object.

        Parameters
        ----------
        crossref_result : pandas.Series.
            CrossRef API result.
        timeout : int
            maximum time to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        
        Returns
        -------
        affiliation : Affiliation
            an Affiliation object.
        """

        res = lookup_funder(crossref_id, timeout)
        affiliation = Affiliation.from_crossref_result(crossref_result=res, use_api=use_api) # type: ignore

        return affiliation
    
    def import_uri(self, uri: int, timeout = 60):

        """
        Looks up an affiliation URI using the CrossRef API and adds the result to the Affiliation object.

        Parameters
        ----------
        uri : str
            affiliation URI.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        """

        uri_str = str(uri)
        res = lookup_funder(uri_str, timeout)
        self.import_crossref_result(res.loc[0]) # type: ignore

    def from_uri(uri: int, use_api=True, timeout = 60): # type: ignore

        """
        Looks up an affiliation URI using the CrossRef API and returns the result as an Affiliation object.

        Parameters
        ----------
        uri : str
            affiliation URI.
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.

        Returns
        -------
        affiliation : Affiliation
            a Affiliation object.
        """

        uri_str = str(uri)
        res = lookup_funder(uri_str, timeout)
        affiliation = Affiliation.from_crossref_result(crossref_result=res, use_api=use_api) # type: ignore

        return affiliation
    
    def update_address(self):
        
        """
        Updates the Affiliation's street address by looking up its name, location, and/or existing address data using geopy.
        """

        if self.summary.loc[0, 'name'] != None:
            name = str(self.summary.loc[0, 'name']).strip().replace('{','').replace('}','').replace('[','').replace(']','').replace(',',' ').replace('  ',' ').strip()
        else:
            name = ''
        
        if self.summary.loc[0, 'location'] != None:
            location = str(self.summary.loc[0, 'location']).strip().replace('{','').replace('}','').replace('[','').replace(']','').replace('  ',' ').strip()
        else:
            location = ''

        if self.summary.loc[0, 'address'] != None:
            address = str(self.summary.loc[0, 'address']).strip().replace('{','').replace('}','').replace('[','').replace(']','').replace('  ',' ').strip()
        else:
            address = ''

        if name not in address:
            address = name + ', ' + address
        
        if (address == '') or (address == ', '):
            address = location

        geolocator = Nominatim(user_agent="location_app")

        try:
                loc = geolocator.geocode(address)
        except:
                loc = None
            
        if loc != None:
                
                self.summary.loc[0, 'address'] = loc.address

                if self.summary.loc[0, 'name'] == None:
                    self.summary.loc[0, 'name'] = loc.name
                
                if self.summary.loc[0, 'location'] == None:
                    self.summary.loc[0, 'location'] = loc.display_name

    def update_from_crossref(self, timeout = 60):

        """
        Looks up the Affiliation's CrossRef ID. If one is found, uses to update the Affiliation object.

        Parameters
        ----------
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        """

        uid = self.summary.loc[0,'crossref_id']
        if uid == None:
            uid = self.summary.loc[0,'uri']
            if uid == None:
                uid = ''

        res = lookup_funder(funder_id = uid, timeout = timeout) # type: ignore
        if len(res) > 0:
            self.import_crossref_result(res.loc[0]) # type: ignore

    def update_from_uri(self, timeout = 60):

        """
        Looks up the Affiliation's URI using the CrossRef API. If one is found, uses to update the Affiliation object.

        Parameters
        ----------
        timeout : int
            maximum time in seconds to wait for a response before aborting the CrossRef API call. Defaults to 60 seconds.
        """

        uid = self.summary.loc[0, 'uri']
        if uid == None:
            uid = self.summary.loc[0, 'crossref']
            if uid == None:
                uid = ''

        res = lookup_funder(funder_id = uid, timeout = timeout) # type: ignore
        if len(res) > 0:
            self.import_crossref_result(res.loc[0]) # type: ignore
   
class Affiliations(Entities):

    """
    This is an Affiliations object. It contains a collection of Affiliation objects and a summary of data about them.
    
    Parameters
    ----------
    affiliations_data : list or dict
        Optional: an iterable of affiliations data. Data on individual affiliations must be formatted as dictionaries.
    
    Attributes
    ----------
    summary : pandas.DataFrame
        a dataframe summarising the Affiliations collection's data.
    all : dict
        a dictionary storing formatted Affiliation objects.
    data : list
        a list of any unformatted data associated with Affiliation objects in the collection.
    """

    def __init__(self, affiliations_data = None):
        
        """
        Initialises Affiliations instance.
        
        Parameters
        ----------
        affiliations_data : list or dict
            Optional: an iterable of affiliations data. Data on individual affiliations must be formatted as dictionaries.
        """

        super().__init__()

        self.summary = pd.DataFrame(columns = 
                                [
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
        

        self.all = dict()

        self.data = []

        self.data.append(affiliations_data)

        if (type(affiliations_data) == list) and (type(affiliations_data[0]) == Affiliation):

            for a in affiliations_data:
                affil_details = a.summary.copy(deep=True)
                affil_id = affil_details.loc[0, 'affiliation_id']
                self.summary = pd.concat([self.summary, affil_details])
                self.all[affil_id] = a

            self.summary = self.summary.reset_index().drop('index',axis=1)

        else:

            if (type(affiliations_data) == list) and (type(affiliations_data[0]) == dict):

                for i in affiliations_data:
                    a = Affiliation.from_dict(i) # type: ignore
                    affil_id = a.summary.loc[0, 'affiliation_id']
                    affil_details = a.summary.copy(deep=True)
                    self.summary = pd.concat([self.summary, affil_details])
                    self.all[affil_id] = a

                self.summary = self.summary.reset_index().drop('index',axis=1)

            else:

                if type(affiliations_data) == dict:
                    
                    values = list(affiliations_data.values())

                    if type(values[0]) == Affiliation:

                        for a in affiliations_data.keys():
                            affil_id = a.summary.loc[0, 'affiliation_id']
                            affil_details = a.summary.copy(deep=True)
                            self.summary = pd.concat([self.summary, affil_details])
                            self.all[affil_id] = a

                        self.summary = self.summary.reset_index().drop('index',axis=1)
                
        self.update_ids()

    def __getitem__(self, key):
        
        """
        Retrieves Affiliations attribute using a key.
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
        Defines how Affiliations objects are represented in string form.
        """

        alphabetical = str(self.summary['name'].sort_values().to_list()).replace('[','').replace(']','')
        return alphabetical
    
    def __len__(self) -> int:

        """
        Returns the number of Affiliation objects in the Affiliations collection. Counts the number of Affiliation objects stored in the Affiliations.all dictionary.

        Returns
        -------
        result : int
            the number of Affiliation objects contained in the Affiliations.all dictionary.
        """

        return len(self.all.keys())

    def drop_empty_rows(self):

        """
        Drops rows that contain no data from Affiliations.summary dataframe.

        Returns
        -------
        self : Affiliations
            an Affiliations object.
        """

        ignore_cols = ['affiliation_id', 'address', 'email', 'other_links']

        df = self.summary.copy(deep=True)
        df['name'] = df['name'].replace('no_name_given', None)
        df = df.dropna(axis=0, how='all')
        drop_cols = [c for c in df.columns if c not in ignore_cols]
        df = df.dropna(axis=0, how='all', subset=drop_cols).reset_index().drop('index', axis=1)

        self.summary = df

        return self
    
    def remove_duplicates(self, drop_empty_rows = True, sync = False):

        """
        Removes duplicate Affiliation entries from the Affiliations collection.

        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to True.
        sync : bool
            whether to synchronise the Affiliations.summary dataframe with the Affiliations.all dictionary. Defaults to False.
        
        Returns
        -------
        self : Affiliations
            an Affiliations object.
        """

        if drop_empty_rows == True:
            self.drop_empty_rows()
        
        df = self.summary.copy(deep=True)
        df['uri'] = df['uri'].str.replace('http://', '', regex=False).str.replace('https://', '', regex=False).str.replace('wwww.', '', regex=False).str.replace('dx.', '', regex=False).str.replace('doi.org/', '', regex=False).str.strip('/')
        
        df = df.sort_values(by = ['uri', 'crossref_id', 'website', 'name', 'location', 'address']).reset_index().drop('index', axis=1)
        self.summary = deduplicate(self.summary)

        if sync == True:
            self.sync_summary(drop_duplicates=False, drop_empty_rows=False)

        return self

    def sync_all(self, drop_duplicates = False, drop_empty_rows=False):

        """
        Updates the Affiliations.summary dataframe using the Affiliation objects in the Affiliations.all dictionary.

        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        for i in self.all.keys():
            affil = self.all[i]
            affil.update_id()
            series = affil.summary.copy(deep=True).loc[0]
            all = self.summary.copy(deep=True).astype(str)
            indexes = all[all['affiliation_id'] == i].index.to_list()
            if len(indexes) > 0:
                affil_index = indexes[0]
                all_copy = self.summary.copy(deep=True)
                all_copy.loc[affil_index] = series
                self.summary = all_copy
        
        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

    def sync_summary(self, drop_duplicates = False, drop_empty_rows=False):

        """
        Updates all Affiliation objects in the Affiliations.all dictionary using the Affiliations.summary dataframe.

        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        self.update_ids(sync=False)

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        for i in self.summary.index:

            a_data = self.summary.loc[i]
            a_id = a_data['affiliation_id']

            if a_id != None:
                a = Affiliation.from_series(a_data) # type: ignore
                self.all[a_id] = a

            else:
                a_id = generate_affiliation_id(a_data)
                a_data['affiliation_id'] = a_id
                a = Affiliation.from_series(a_data) # type: ignore
                self.all[a_id] = a
        
        keys = list(self.all.keys())
        for key in keys:
            a_ids = self.summary['affiliation_id'].to_list()
            if key not in a_ids:
                del self.all[key]

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

    def sync(self, drop_duplicates = False, drop_empty_rows=False):
        
        """
        Synchronises the Affiliations.summary dataframe with the Affiliation objects in the Affiliations.all dictionary.
        
        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

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

    def merge(self, affiliations, drop_duplicates = False, drop_empty_rows=True):

        """
        Merges the Affiliations collection with another Affiliations collection.

        Parameters
        ----------
        affiliations : Affiliations
            the Affiliations collection to merge with.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        
        Returns
        -------
        self : Affiliations
            the merged Affiliations collection.
        """

        left = self.summary.copy(deep=True)
        right = affiliations.summary.copy(deep=True)
        
        merged = pd.concat([left, right])

        self.summary = merged.drop_duplicates(subset=['affiliation_id', 'name', 'location'], ignore_index=True)

        for i in affiliations.all.keys():
            if i not in self.all.keys():
                self.all[i] = affiliations.all[i]

        left_data = self.data
        right_data = affiliations.data

        if left_data == None:
                left_data = []
            
        if right_data == None:
                right_data = []

        if (type(left_data) == Affiliation) or (type(left_data) == str):
                left_data = [left_data]
            
        if (type(right_data) == Affiliation) or (type(right_data) == str):
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

    def add_affiliation(self, affiliation: Affiliation =  None, name: str = None, uri: str = None, crossref_id: int = None, data = None, use_api = True, drop_duplicates = False, drop_empty_rows=False): # type: ignore

        """
        Adds an Affiliation or affiliation data to the Affiliations collection.

        Parameters
        ----------
        affiliation : Affiliation
            an Affiliation object to add.
        uri : str
            a URI identifier to look up. Defaults to None.
        crossref_id : str
            a CrossRef ID to look up. Defaults to None.
        data : dict
            Optional: a dictionary containing affiliation data. Dictionary keys must match the names of columns in the Affiliations.summary dataframe.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        use_api : bool
            whether to update the Affiliation data using the CrossRef API. Defaults to False.
        """

        if type(affiliation) == str:

            affiliation = Affiliation(name = affiliation, use_api = use_api)

            affiliation = None # type: ignore

        if data is not None:

            if type(data) == dict:
                affiliation = Affiliation.from_dict(data=data, use_api = use_api) # type: ignore
            
            else:
                if type(data) == pd.Series:
                    affiliation = Affiliation.from_series(data=data) # type: ignore
                
                else:
                    if type(data) == pd.DataFrame:
                        affiliation = Affiliation.from_dataframe(data=data) # type: ignore


        if affiliation is None:
            affiliation = Affiliation(name=name, uri=uri, crossref_id=crossref_id, use_api = use_api)

        if use_api == True:
            affiliation.update_from_crossref()
            affiliation.update_address()

        affiliation.update_id()

        affiliation_id = str(affiliation.summary.loc[0, 'affiliation_id'])

        # if affiliation_id in self.summary['affiliation_id'].to_list():
        #     all_copy = self.summary.copy(deep=True).astype(str)
        #     id_count = len(all_copy[all_copy['affiliation_id'].str.contains(affiliation_id)]) # type: ignore
        #     affiliation_id = affiliation_id + f'#{id_count + 1}'
        #     affiliation.summary.loc[0, 'affiliation_id'] = affiliation_id

        self.summary = pd.concat([self.summary, affiliation.summary])
        self.summary = self.summary.reset_index().drop('index', axis=1)

        self.all[affiliation_id] = affiliation

        if data is None:
            data = affiliation.summary.to_dict(orient='index')
        
        self.data.append(data)

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        self.update_ids()

    def add_affiliations_list(self, affiliations_list: list, use_api: bool = False, drop_duplicates = False, drop_empty_rows=False):
        
        """
        Adds a list of Affiliation objects to the Affiliations collection.

        Parameters
        ----------
        affiliations_list : list[Affiliation]
            a list of Affiliation objects.
        use_api : bool
            whether to update the Affiliations data using the CrossRef API. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        for i in affiliations_list:
            if type(i) == Affiliation:
                self.add_affiliation(affiliation = i, use_api=use_api)
            else:
                if type(i) == dict:
                    affil = Affiliation.from_dict(i)  # type: ignore
                    self.add_affiliation(affiliation = affil, use_api=use_api)
                else:
                    if type(i) == pd.Series:
                        affil = Affiliation.from_series(i)  # type: ignore
                        self.add_affiliation(affiliation = affil, use_api=use_api)
        
        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        self.update_ids()

    def update_ids(self, sync=False, drop_duplicates = False, drop_empty_rows=False):

        """
        Updates affiliation IDs for all rows in the Affiliations.summary dataframe.

        Parameters
        ----------
        sync : bool
            whether to synchronise the Affiliations.summary dataframe with the Affiliation objects in the Affiliations.all dictionary. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        if sync == True:
            self.sync()

        for i in self.summary.index:
            
            data = self.summary.loc[i].copy(deep=True)
            old_id = self.summary.loc[i, 'affiliation_id']
            new_id = generate_affiliation_id(data)
            new_id = str(new_id)

            # if new_id in self.summary['affiliation_id'].to_list():
            #     df_copy = self.summary.copy(deep=True)
            #     df_copy = df_copy.astype(str)
            #     id_count = len(df_copy[df_copy['affiliation_id'].str.contains(new_id)])
            #     new_id = new_id + f'#{id_count + 1}'

            self.summary.at[i, 'affiliation_id'] = new_id
            if old_id in self.all.keys():
                self.all[new_id] = self.all[old_id]
                self.all[new_id].summary.loc[0, 'affiliation_id'] = new_id
                del self.all[old_id]

            else:
                affiliation = Affiliation.from_series(data) # type: ignore
                affiliation.summary.loc[0, 'affiliation_id'] = new_id
                self.all[new_id] = affiliation

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

    def update_addresses(self, sync=True, drop_duplicates = False, drop_empty_rows=False):

        """
        Updates all Affiliations' street addresses by looking up their names, locations, and/or existing addresses data using geopy.

        Parameters
        ----------
        sync : bool
            whether to synchronise the Affiliations.summary dataframe with the Affiliation objects in the Affiliations.all dictionary. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        if sync == True:
            self.sync(drop_duplicates=drop_duplicates,drop_empty_rows=drop_empty_rows)

        affiliation_ids = self.all.keys()

        for a in affiliation_ids:

            self.all[a].update_address()
            details = self.all[a].summary.loc[0]
            
            df_index = self.summary[self.summary['affiliation_id'] == a].index.to_list()[0]
            self.summary.loc[df_index] = details

            new_id = details['affiliation_id']
            if new_id != a:
                self.all[new_id] = self.all[a]
                del self.all[a]

        self.update_ids()

    def update_from_crossref(self, drop_duplicates = False, drop_empty_rows=False):

        """
        Looks up all Affiliations' CrossRef IDs and/or URIs using the CrossRef API. If found, uses to update the Affiliations collection.

        Parameters
        ----------
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        affiliation_ids = self.all.keys()

        for a in affiliation_ids:

            self.all[a].update_from_crossref()
            details = self.all[a].summary.loc[0]
            
            df_index = self.summary[self.summary['affiliation_id'] == a].index.to_list()[0]
            self.summary.loc[df_index] = details

            new_id = details['affiliation_id']
            if new_id != a:
                self.all[new_id] = self.all[a]
                del self.all[a]

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        self.update_ids()

    def import_crossref_ids(self, crossref_ids: list, drop_duplicates = False, drop_empty_rows=False):

        """
        Looks up a list of affiliations' CrossRef IDs and/or URIs using the CrossRef API. Adds any data found to the Affiliations collection.

        Parameters
        ----------
        crossref_ids : list[str]
            list containing affiliations' CrossRef IDs.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        for i in crossref_ids:

            auth = Affiliation.from_crossref(i) # type: ignore
            self.add_affiliation(affiliation = auth)

        if drop_empty_rows == True:
            self.drop_empty_rows()

        if drop_duplicates == True:
            self.remove_duplicates(drop_empty_rows=drop_empty_rows)

        self.update_ids()

    def from_crossref_ids(crossref_ids: list, drop_duplicates = False, drop_empty_rows=False): # type: ignore

        """
        Looks up a list of affiliations' CrossRef IDs and/or URIs using the CrossRef API. Returns all data found as an Affiliations object.

        Parameters
        ----------
        crossref_ids : list[str]
            list containing affiliations' CrossRef IDs.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.

        Returns
        -------
        funders : Funders
            a Funders object.
        """

        affiliations = Affiliations()
        affiliations.import_crossref_ids(crossref_ids, drop_empty_rows=drop_empty_rows, drop_duplicates=drop_duplicates)

        return affiliations

    def has_crossref(self):

        """
        Returns all rows in Affiliations.summary which contain CrossRef IDs.
        """

        return self.summary[~self.summary['crossref_id'].isna()]
    
    def has_uri(self):

        """
        Returns all rows in Affiliations.summary which contain URIs.
        """

        return self.summary[~self.summary['uri'].isna()]

    def from_affiliations_list(affiliations_list: list, use_api: bool = False, drop_duplicates = False, drop_empty_rows=False): # type: ignore

        """
        Reads a list of Affiliation objects and returns as an Affiliations object.

        Parameters
        ----------
        affiliations_list : list[Affiliation]
            a list of Affiliation objects.
        use_api : bool
            whether to update the Affiliations data using the CrossRef API. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        
        Returns
        -------
        affiliations : Affiliations
            an Affiliations object.
        """

        affiliations = Affiliations()
        affiliations.add_affiliations_list(affiliations_list, use_api=use_api, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        return affiliations
    
    def import_crossref_result(self, crossref_result: pd.DataFrame, use_api = False, drop_duplicates = False, drop_empty_rows=False):

        """
        Reads a pandas.DataFrame containing CrossRef API results and adds the data to the Affiliations collection.

        Parameters
        ----------
        crossref_result : pandas.Dataframe
            CrossRef API results.
        use_api : bool
            whether to update the Affiliations data using the CrossRef API. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        for i in crossref_result.index:
            
            data = crossref_result.loc[i]
            affil = Affiliation.from_crossref_result(crossref_result=data, use_api=use_api) # type: ignore
            self.add_affiliation(affiliation = affil, use_api=use_api, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        self.update_ids()

    def from_crossref_result(crossref_result: pd.DataFrame, use_api: bool = False, drop_duplicates = False, drop_empty_rows=False): # type: ignore

        """
        Reads a pandas.DataFrame containing CrossRef API results and returns as a Affiliations object.

        Parameters
        ----------
        crossref_result : pandas.Dataframe
            CrossRef API results.
        use_api : bool
            whether to update the Funders data using the CrossRef API. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.

        Returns
        -------
        affiliations : Affiliations
            a Affiliations object.
        """

        affiliations = Affiliations()
        affiliations.import_crossref_result(crossref_result, use_api=use_api, drop_duplicates=drop_duplicates, drop_empty_rows=drop_empty_rows)

        return affiliations

def format_affiliations(affiliation_data, use_api = False, drop_duplicates = False, drop_empty_rows=True):
        
        """
        Formats a collection of affiliations data as an Affiliations object.

        Parameters
        ----------
        affiliation_data : object
            a collection of affiliations data.
        use_api : bool
            whether to update the Affiliations data using the CrossRef API. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        """

        result = Affiliations()

        affil_type = type(affiliation_data)

        if (
            (affil_type != pd.DataFrame) 
            and (affil_type != pd.Series) 
            and (affil_type != Affiliations) 
            and (affil_type != Affiliation) 
            and (affil_type != list) 
            and (affil_type != dict)
            ):
            result = Affiliations()
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result

        if affil_type == Affiliations:
            result = affiliation_data
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result

        if affil_type == Affiliation:
            result.add_affiliation(affiliation=affiliation_data, use_api=use_api)
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result

        if affil_type == pd.Series:
            affiliation = Affiliation()
            affiliation.add_series(affiliation_data)
            result.add_affiliation(affiliation=affiliation, use_api=use_api)
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result

        if affil_type == pd.DataFrame:
            result.import_crossref_result(affiliation_data, use_api=use_api) # type: ignore
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result

        if affil_type == dict:
            affiliation = Affiliation.from_dict(affiliation_data) # type: ignore
            result.add_affiliation(affiliation = affiliation, use_api=use_api) # type: ignore
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result


        if (affil_type == list) and (len(affiliation_data) > 0) and (type(affiliation_data[0]) == Affiliation):
            result = Affiliations()
            result.add_affiliations_list(affiliation_data)
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
            return result
            

        if (affil_type == list) and (len(affiliation_data) > 0) and (type(affiliation_data[0]) == dict):

            for i in affiliation_data:
                affiliation = Affiliation.from_dict(i) # type: ignore
                result.add_affiliation(affiliation = affiliation, use_api=use_api) # type: ignore
            
            if drop_empty_rows == True:
                result.drop_empty_rows()
            if drop_duplicates == True:
                result.remove_duplicates(drop_empty_rows=drop_empty_rows)
    
            return result