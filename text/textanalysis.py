"""Functions for analysing texts and strings"""

from typing import List, Dict, Tuple
import copy

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import Levenshtein as levenshtein
from Levenshtein import distance as lev


def cosine_sim(input_text_list: List[str], stopwords = 'english') -> np.float64:
    
    """
    Calculates the cosine similarity of two texts based on their word frequencies.
    
    Parameters
    ----------
    input_text_list : list
        list of strings to compare.
    stopwords : str
        name of stopwords dataset to use.
    
    Returns
    -------
    result : numpy.float64
        a float representing the count-vectorised cosine similarity of the inputted strings.
    
    Notes
    -----
    Takes a list of two texts.
    Removes stopwords based on the 'stopwords' argument.
    """
    
    # Initialising vectoriser object
    count_vectorizer = CountVectorizer(stop_words=stopwords)
    
    # Vectorising texts based on word frequency counts and transforming to a sparse matrix
    sparse_matrix = count_vectorizer.fit_transform(input_text_list)
    
    # Transforming sparse matrix to dense matrix
    doc_term_matrix = sparse_matrix.todense()
    
    # Creating dataframe from word vectors matrix
    df = pd.DataFrame(
       doc_term_matrix,
       columns = count_vectorizer.get_feature_names_out(),
       index = ['input1', 'input2'],
    )
    
    # Running cosine similarity algorithm on dataframe
    result = cosine_similarity(df, df)
    
    return result[0,1]

def normalised_levenshtein(first_string: str, second_string: str) -> float:
    
    """
    Calculates the normalised levenshtein distance between two strings.
    
    Parameters
    ----------
    first_string : str
        first string to compare.
    second_string : str
        second string to compare.
    
    Returns
    -------
    result : float
        a float representing the normalised Levenshtein similarity of the inputted strings.
    
    Notes
    -----
    Normalisation function: levenshtein distance divided by the length of the longest string.
    """
    
    # Checking types
    if type(first_string) != str:
        raise TypeError('Inputted strings must be of type "str"')
    
    if type(second_string) != str:
        raise TypeError('Inputted strings must be of type "str"')
    
    # Getting length of longest string
    len1 =  len(first_string)
    len2 = len(second_string)
    max_len = max(len1, len2)
    
    # Calculating levenshtein distance
    levenshtein = lev(first_string, second_string)
    
    # Normalising result
    normalised = levenshtein / max_len
    
    return normalised
    

def inverse_normalised_levenshtein(first_string: str, second_string: str) -> float:
    
    """
    Calculates the inverse of the normalised levenshtein distance between two strings.
    
    Parameters
    ----------
    first_string : str
        first string to compare.
    second_string : str
        second string to compare.
    
    Returns
    -------
    result : float
        a float representing the inverse normalised Levenshtein similarity of the inputted strings.
    
    Notes
    -----
    Normalisation function: levenshtein distance divided by the length of the longest string.
    """
    
    # Checking types
    if type(first_string) != str:
        raise TypeError('Inputted strings must be of type str')
    
    if type(second_string) != str:
        raise TypeError('Inputted strings must be of type str')
    
    # Calculating normalised levenshtein distance
    normalised_lev = normalised_levenshtein(first_string = first_string, second_string = second_string)
    
    # Calculating inverse
    inv_normalised = 1 - normalised_lev
    
    return inv_normalised