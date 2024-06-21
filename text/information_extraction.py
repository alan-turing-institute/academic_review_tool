from ..datasets import all_personal_names, first_names, last_names, nltk_names, countries_all, country_names, cities_all, cities_en, language_names, languages_en, language_codes, stopwords, languages_all, countries_zh, countries_ar, countries_es, countries_hi, countries_pt, countries_ru, countries_fr, nltk_words_list, nltk_webtext_words

from typing import List, Dict, Tuple
import copy
import pandas as pd

def extract_names(words: list, source: str = 'all_personal_names'):
    
    """
    Takes a list of words and returns all words that appear in a pre-defined list of names.
    
    Parameters
    ----------
    words : list, set, or tuple
        iterable of strings to analyse.
    source : str
        name of corpus of names to use for comparison.
    
    Returns
    -------
    result : list
        extracted names.
    
    Notes
    -----
        * 'all_personal_names' corpus includes first names and last names from a large number of languages. It features >1,000,000 names.
    """
    
    # Formatting words list and removing duplicates
    words = {i.strip('.').strip().lower() for i in words}
    
    
    
    # Retrieving specified name corpus and formatting it
    names_set = None
    
    if source == 'all_personal_names':
   
        global all_personal_names
        names_set = pd.Series(all_personal_names).str.lower().str.strip('.').to_list()
    
    if source == 'first_names':
        
        global first_names
        names_set = pd.Series(first_names).str.lower().str.strip('.').to_list()
    
    if source == 'last_names':
        
        global last_names
        names_set = set(pd.Series(last_names).str.lower().str.strip('.').to_list())
    
    if source == 'nltk_names':
        
        global nltk_names
        names_set = set(pd.Series(nltk_names).str.lower().str.strip('.').str.strip().to_list())
    
    if names_set == None:
        names_set = set(source)
    
    # Identifying intersection of names list and words list. Converting to a list.
    extracted_names = list(words.intersection(names_set))
    
    return extracted_names
    

def extract_countries(words, language = 'all'):
    
    """
    Takes a list of words and returns all words that appear in a pre-defined list of countries.
    
    Parameters
    ----------
    words : list, set, or tuple
        iterable of strings to analyse.
    language : str
        language of corpus of countries to use for comparison.
    
    Returns
    -------
    result : list
        extracted country names.
    
    Notes
    -----
        * 'all' corpus includes country names from a large number of languages.
    """
    
    # Formatting words list and removing duplicates
    words = {i.strip('.').strip().lower() for i in words}
    
    # Retrieving all country names corpus and formatting it
    if language == 'all':
        
        global countries_all
        countries_set = set(pd.Series(countries_all).str.lower().str.strip('.').to_list())
    
    # Retrieving country names corpus if a language is specified and formatting it
    else:
        global country_names
        countries = country_names[language]
        countries_set = set(pd.Series(countries).str.lower().str.strip('.').to_list())
    
    # Identifying intersection of country names corpus and words list. Converting to a list.
    extracted_countries = list(words.intersection(countries_set))
    
    return extracted_countries
    

def extract_cities(words, language = 'all'):
    
    """
    Takes a list of words and returns all words that appear in a pre-defined list of cities.
    
    Parameters
    ----------
    words : list, set, or tuple
        iterable of strings to analyse.
    language : str
        language of corpus of city names to use for comparison.
    
    Returns
    -------
    result : list
        extracted city names.
    
    Notes
    -----
        * 'all' corpus includes city names from a large number of languages.
    """
    
    # Formatting words list and removing duplicates
    words = {i.strip('.').strip().lower() for i in words}
    
    # Retrieving all city names corpus and formatting it
    if language == 'all':
        
        global cities_all
        cities_set = set(pd.Series(cities_all).str.lower().str.strip('.').to_list())
    
    # Retrieving city names corpus if the language is specified as English and formatting it
    if (language.lower() == 'en') or (language.lower() == 'english'):
        
        global cities_en
        cities = list(cities_en.keys())
        cities_set = set(pd.Series(cities).str.lower().str.strip('.').to_list())
    
    # Identifying intersection of city names corpus and words list. Converting to a list.
    extracted_cities = list(words.intersection(cities_set))
    
    return extracted_cities
    

def extract_languages(words, language = 'all'):
    
    """
    Takes a list of words and returns all words that appear in a pre-defined list of language names.
    
    Parameters
    ----------
    words : list, set, or tuple
        iterable of strings to analyse.
    language : str
        language of corpus of language names to use for comparison.
    
    Returns
    -------
    result : list
        extracted language names.
    
    Notes
    -----
        * 'all' corpus includes language names from a large number of languages.
    """
    
    # Formatting words list and removing duplicates
    words = {i.strip('.').strip().lower() for i in words}
    
    # Retrieving all language names corpus and formatting it
    if language == 'all':
        
        global languages_all
        langs_set = set(pd.Series(languages_all).str.lower().str.strip('.').to_list())
    
    # Retrieving language names corpus if the language is specified as English and formatting it
    if (language.lower() == 'en') or (language.lower() == 'english'):
        
        global languages_en
        langs = list(languages_en.values())
        langs_set = set(pd.Series(langs).str.lower().str.strip('.').to_list())
    
    # Identifying intersection of city names corpus and words list. Converting to a list.
    extracted_langs = list(words.intersection(langs_set))
    
    return extracted_langs
    