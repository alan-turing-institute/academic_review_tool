"""Basic classes and functions."""

from typing import List, Dict
import math
from os import stat_result
import os
import sys
import pickle

results_cols = [
                            'work_id',
                            'title',
                            'authors',
                            'date',
                            'source',
                            'type',
                            'editors',
                            'publisher',
                            'publisher_location',
                            'funder',
                            'keywords',
                            'abstract',
                            'description',
                            'extract',
                            'full_text',
                            'access_type',
                            'authors_data',
                            'author_count',
                            'author_affiliations',
                            'editors_data',
                            'citations',
                            'citation_count',
                            'citations_data',
                            'cited_by',
                            'cited_by_count',
                            'cited_by_data',
                            'recommendations',
                            'crossref_score',
                            'repository',
                            'language',
                            'doi',
                            'isbn',
                            'issn',
                            'pii',
                            'scopus_id',
                            'wos_id',
                            'pubmed_id',
                            'other_ids',
                            'link'
                                ]


def blockPrint():

    """
    Blocks command line interface from printing text via print() and similar functions. Follow with enablePrint() to allow printing.
    """

    sys.stdout = open(os.devnull, 'w')

def enablePrint():
    
    """
    Enables command line interface to print text via print() and similar functions. Must be used after blockPrint().
    """

    sys.stdout = sys.__stdout__

class Iterator:
    
    """
    Class to be invoked to enable objects to be iterated on.
    
    Parameters
    ----------
    obj : object
    """
    
    def __init__(self, obj):
        
        """
        Initialises iterator object.
        """
        
        self.obj = obj
        self.index_len = len(obj.__dict__.keys())
        self._current_index = 0    
    
    def __iter__(self):
        
        """
        Returns iterator object.
        """
        
        return self    
    
    def __next__(self):
        
        """
        Returns next item in iterator object.
        """
        
        attrs = list(self.obj.__dict__.keys())
        
        if self._current_index < self.index_len:
                attr_name = attrs[self._current_index]
                attr = self.obj.__dict__[attr_name]
                self._current_index += 1
                return attr
        
        raise StopIteration

def dict_to_str(item: dict) -> str:
    
    """
    Returns string representation of a dictionary's values.
    """
    
    return str(list(item.values()))

def inv_logit(value: float) -> float:
    
    """
    Returns the inverse logit of a numeric value.
    """
    
    output = math.exp(value) / (1 + math.exp(value))
    return output

def map_inf_to_1(number: float) -> float:
    
    """
    Maps a number to a range between 0 and 1, where 0 -> 0 and infinity -> 1.
    """

    return number / (1 + number)

def map_inf_to_0(number: float) -> float:
                     
    """
    Maps a number to a range between 0 and 1, where 0 -> 1 and infinity -> 0.
    """
    
    return 1 - map_inf_to_1(number)

def type_str(obj: object):
    
    """
    Returns the string representation of an object's type.
    """
                     
    type_str = str(type(obj)).replace('<', '').replace('>', '').replace('class ', '').replace("'", "").strip()
    
    return type_str

def stat_file_to_dict(self):
    
    """
    Converts a stat_result object to a dictionary.
    """
    
    dictionary = {}

    try:
        dictionary['st_mode'] = self.st_mode
    except:
        pass

    try:
        dictionary['st_ino'] = self.st_ino
    except:
        pass
    try:
        dictionary['st_dev'] = self.st_dev
    except:
        pass
    try:
        dictionary['st_nlink'] = self.st_nlink
    except:
        pass
    try:
        dictionary['st_uid'] = self.st_uid
    except:
        pass
    try:
        dictionary['st_gid'] = self.st_gid
    except:
        pass
    try:
        dictionary['st_size'] = self.st_size
    except:
        pass
    try:
        dictionary ['st_atime'] = self.st_atime
    except:
        pass
    try:
        dictionary ['st_mtime'] = self.st_mtime
    except:
        pass
    try:
        dictionary['st_ctime'] = self.st_ctime
    except:
        pass
    try:
        dictionary['st_atime_ns'] = self.st_atime_ns
    except:
        pass
    try:
        dictionary['st_mtime_ns'] = self.st_mtime_ns
    except:
        pass
    try:
        dictionary['st_ctime_ns'] = self.st_ctime_ns
    except:
        pass
    try:
        dictionary['st_birthtime'] = self.st_birthtime
    except:
        pass
    try:
        dictionary['st_birthtime_ns'] = self.st_birthtime_ns
    except:
        pass
    try:
        dictionary['st_blocks'] = self.st_blocks
    except:
        pass
    try:
        dictionary['st_blksize'] = self.st_blksize
    except:
        pass
    try:
        dictionary['st_rdev'] = self.st_rdev
    except:
        pass
    try:
        dictionary['st_flags'] = self.st_flags
    except:
        pass
    try:
        dictionary['st_gen'] = self.st_gen
    except:
        pass
    try:
        dictionary['st_fstype'] = self.st_fstype
    except:
        pass
    try:
        dictionary['st_rsize'] = self.st_rsize
    except:
        pass
    try:
        dictionary['st_creator'] = self.st_creator
    except:
        pass
    try:
        dictionary['st_type'] = self.st_type
    except:
        pass
    try:
        dictionary['st_file_attributes'] = self.st_file_attributes
    except:
        pass
    try:
        dictionary['st_reparse_tag'] = self.st_reparse_tag
    except:
        pass    
    
    return dictionary

def open_file(file_address: str = 'request_input'): # type: ignore

        """
        Reads saved ART files and returns as a Review object. Reviews must be formatted as .review or pickled .txt files.
        """

        if file_address == 'request_input':
            file_address = input('File address: ')
        
        if (file_address.endswith('.txt')) or (file_address.endswith('.review')):
            with open(file_address, 'rb') as f:
                review = pickle.load(f)

        return review

stat_result.to_dict = stat_file_to_dict # type: ignore