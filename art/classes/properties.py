
from ..utils.basics import Iterator
from datetime import datetime
from pathlib import Path

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
    
    def __init__(self, review_name = None, file_location = None):
        
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

        if file_location is not None:
            file_type = Path(file_location).suffix
            self.file_type = file_type
        else:
            self.file_type = None
    
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

    def update_file_type(self):

        """
        Updates the file_type attribute to the current file_location string's suffix.
        """

        file_path = self.file_location

        if file_path is not None:
            self.file_type = Path(file_path).suffix
        else:
            self.file_type = None