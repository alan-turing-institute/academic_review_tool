from ..utils.basics import Iterator
from ..exporters.general_exporters import obj_to_folder

from typing import List, Dict
import copy


class Attr:
    
    """
    This is a general class of objects intended for use as attributes. It provides extra methods for ease of use.
    """
    
    def __init__(self):
        
        """
        Initialises Attr instance.
        """
    
    
    def copy(self):
        
        """
        Returns a copy of object.
        """
        
        return copy.deepcopy(self)
    
    def attributes(self):
        
        """
        Returns a list of attributes assigned to object.
        """
        
        return list(self.__dict__.keys())
    
    def __iter__(self):
        
        """
        Makes Attr objects iterable.
        """
        
        return Iterator(self)
    
    def get_name_str(self):
        
        """
        Returns the object's variable name as a string. 
        
        Notes
        -----
            * Searches global environment dictionary for objects sharing object's ID. Returns key if found.
            * If none found, searches local environment dictionary for objects sharing object's ID. Returns key if found.
        """
        
        for name in globals():
            if id(globals()[name]) == id(self):
                return name
        
        for name in locals():
            if id(locals()[name]) == id(self):
                return name
    
    
    def __getitem__(self, index):
        
        """
        Retrieves object attribute using its key.
        """
        
        return self.__dict__[index]
    
    def __setitem__(self, key, item):
        
        """
        Sets object attribute using its key.
        
        WARNING: will not allow user to set object's 'properties' attribute.
        """
        
        if key != 'properties':
            self.__dict__[key] = item
        
        
        
        
    def __delitem__(self, key):
        
        """
        Deletes object attribute using its key.
        
        WARNING: will not allow user to delete object's 'properties' attribute.
        """
        
        if key != 'properties':
            delattr(self, key)
        

    def hashable_list(self):
        
        """
        Converts object into a list of strings for hashing. Does not include object properties attribute.
        """
        
        return [str(i) for i in self]
    
    def __hash__(self):
        
        """
        Hashes object. Returns a unique integer.
        """
        
        self_tuple = tuple(self.hashable_list())
        return hash(self_tuple)

    # Methods to convert item to other object types
    
    def to_list(self):
        
        """
        Returns the object's attributes as a list. Excludes object properties attribute.
        """
        
        return [i for i in self]
    
    def to_dict(self):
        
        """
        Returns the object's attributes as a dictionary. Excludes object properties attribute.
        """
        
        output_dict = {}
        keys = [i for i in self.__dict__.keys()]
        for index in keys:
            output_dict[index] = self.__dict__[index]
        
        return output_dict
    
    def contents(self):
        
        """
        Returns the object's attributes as a list. Excludes object properties attribute.
        """
        
        contents = [i for i in self.__dict__.keys()]
        return contents
    
    def export_folder(self, folder_name = 'obj_name', folder_address = 'request_input', export_str_as = 'txt', export_dict_as = 'json', export_pandas_as = 'csv', export_network_as = 'graphML'):
        
        """
        Exports object's contents to a folder.
        
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
        
        obj_to_folder(self, folder_name = folder_name, folder_address = folder_address, export_str_as = export_str_as, export_dict_as = export_dict_as, export_pandas_as = export_pandas_as, export_network_as = export_network_as)

class AttrSet(Attr):

    """
    This is a collection of Attr objects.

    Notes
    -----
        * Subclass of Attr class.
        * Intended to used as a superclass for all collections of Attrs.
    """
    
    def __init__(self):
        
        """
        Initialises AttrSet instance.
        """

        # Retrieving methods and attributes from Attr superclass
        super().__init__()
    
    def __len__(self):
        
        """
        Returns the number of objects in the collection. Excludes collection's 'properties' attribute.
        """
        
        keys = [i for i in self.__dict__.keys() if i != 'properties']
        return len(keys)
    
    def contents(self):
        
        """
        Returns the collection's contents. Excludes collection's 'properties' attribute.
        """
        
        return [i for i in self.__dict__.keys() if i != 'properties']
    
    def ids(self):
        
        """
        Returns the names of all objects in the collection. Excludes collection's 'properties' attribute.
        """
        
        return self.contents()

    def hashable_list(self):
        
        """
        Converts collection into a list of strings for hashing. Excludes collection's 'properties' attribute.
        """
        
        return [str(i) for i in self]
    
    def __hash__(self):
        
        """
        Hashes collection. Returns a unique integer.
        """
        
        self_tuple = tuple(self.hashable_list())
        return hash(self_tuple)


    # Methods to convert object set to other object types
    
    def to_list(self):
        
        """
        Returns the collection as a list.  Excludes collection's 'properties' attribute.
        """
        
        return [i for i in self]
    
    def to_set(self):
        
        """
        Returns the collection as a set.  Excludes collection's 'properties' attribute.
        """
        
        return set(self.to_list())
    
    def to_dict(self):
        
        """
        Returns the collection as a dictionary.  Excludes collection's 'properties' attribute.
        """
        
        output_dict = {}
        contents = self.contents()
        for index in contents:
            output_dict[index] = self.__dict__[index].to_dict()
        
        return output_dict

    def delete_all(self):
        
        """
        Deletes all objects in collection.
        """
        
        # Retrieving attribute names
        objects = list(self.contents())
        
        # Deleting retrieved names
        for i in objects:
            delattr(self, i)
            
    def __getitem__(self, index):
        
        """
        Returns object in collection using an index or list of indexes.
        
        Allows for integer slicing, negative indexes, and passing lists as indexes.
        
        Parameters
        ----------
        index : slice, object, str or int
            an object slice, attribute (object), attribute name (str), or index position (int).
        
        Returns
        -------
        result : AttrSet or ReviewObject
            the result.
        """
        
        contents = self.contents()
        
        res = None
        
        # Logic for handling index slices
        if type(index) == slice:
            
            # Retrieving start index
            start = index.start
            
            # If none given, starts at first index (0)
            if start == None:
                start = 0
            
            # Raising error if start index isn't an integer
            if type(start) != int:
                raise TypeError(f'{str(type(self))} slices must be integers')
            
            # If start index is negative, creating start index 
            # by counting back from size of collection
            if start < 0:
                start = len(contents) + start
            
            # Retrieving stop index
            stop = index.stop
            
            # If none given, starts at stops at final item in collection
            if stop == None:
                stop = len(contents)
            
            # Raising error if stop index isn't an integer
            if type(stop) != int:
                raise TypeError(f'{str(type(self))} slices must be integers')
            
            # If stop index is negative, creating stop index 
            # by counting back from size of collection
            if stop < 0:
                stop = len(contents) + stop
            
            # Creating new empty collection
            sliced_set = copy.deepcopy(self)
            sliced_set.delete_all()
            
            # Iterating through indexes in range; using recursion to add item to sliced collection
            for i in range(start, stop):
                entity = self.__getitem__(i)
                entity_id = contents[i]
                sliced_set.__dict__[entity_id] = entity
            
            # Returning sliced collection
            res = sliced_set
        
        # Logic for integer, string, ReviewObject, list, set, and tuple indexes
        else:
            
            # Initialising variable for sub-indexing logic
            sub_index = None
            
            # Splitting into index and sub-index if iterable passed
            if (type(index) == list) or (type(index) == set) or (type(index) == tuple):
                sub_index = index[1] # type: ignore
                index = index[0] # type: ignore
            
            # Returning object if the index is a ReviewObject or ReviewObjectProperties
            if index in self.__dict__.values():
                res = index
            
            # For all other types of index
            else:
                # Returning object if the index is an object key
                if index in contents:
                    res = self.__dict__[index]
            
                # Otherwise checking if index is an integer and using to retrieve object key
                elif type(index) == int:
                    
                    # Raising error if index is out of range
                    if index not in range(0, len(contents)):
                        raise KeyError(f'{index} is out of range')
                    
                    # Retrieving key using index; using key to return result
                    obj = contents[index]
                    res = self.__dict__[obj]
            
            # If sub-index provided, using indexing to return attribute of result
            if sub_index != None:
                res = res[sub_index] # type: ignore
            
        # Checking if no result found
        if res == None:
            raise KeyError(f'"{index}" not found')
            
        return res
    
    
    def __setitem__(self, key, item):
        
        """
        Adding object to collection using a key name.
        
        WARNING: will not allow user to set collection's 'properties' attribute.
        """
        
        if key != 'properties':
            self.__dict__[key] = item
        
        
        
    def __delitem__(self, key):
        
        """
        Deletes object in collection using its key.
        
        WARNING: will not allow user to delete collection's 'properties' attribute.
        """
        
        if key != 'properties':
            delattr(self, key)
      