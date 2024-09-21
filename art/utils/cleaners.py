"""Functions for parsing, cleaning, and normalising data."""

# Importing packages
from .basics import blockPrint, enablePrint
from ..datasets import stopwords, html_stopwords 

from typing import List, Dict, Tuple
import copy
from datetime import datetime, date, timedelta
import numpy as np
import pandas as pd
from selectolax.parser import HTMLParser # type: ignore
import requests
# import requests_html <-- raising an error
# from requests_html import HTML

import nltk
from nltk.tokenize import word_tokenize, sent_tokenize # type: ignore

blockPrint()
nltk.download('punkt')
enablePrint()

def join_list_by_colon(item):

    """
    Takes list and returns as a string separated by a comma followed by a space. Used as input for Pandas.apply.
    """
    
    if type(item) == list:
        return ', '.join(item)
    
    else:
        return item

def join_df_col_lists_by_colon(dataframe):

    """
    Takes a Pandas DataFrame and converts lists to strings separated by a comma followed by a space.
    """

    for col in dataframe.columns:
        dataframe[col] = dataframe[col].apply(join_list_by_colon)
    
    return dataframe

def split_str_by_colon(item):
    
    """
    Splits a string into a list by semi-colons. Used as input for Pandas.apply.
    """

    if type(item) == str:
        return item.split(',')
    
    else:
        return item

def join_list_by_semicolon(item: list) -> str:
    
    """
    Joins the items of a list into a string connected by semicolons. 
    """
    
    if type(item) == list:
        
        return '; '.join(item)
    
    else:
        return item

def join_df_col_lists_by_semicolon(dataframe: pd.DataFrame) -> pd.DataFrame:
    
    """
    Replaces the lists in a Dataframe with strings connected by semicolons. 
    """
    
    for col in dataframe.columns:
        dataframe[col] = dataframe[col].apply(join_list_by_semicolon)
    
    return dataframe

def strip_list_str(list_item: List[str]) -> list:
    
    """
    Takes string. If it is formatted as a Python list, splits it and cleans it. Returns it as a list object.
    """
    
    if type(list_item) == str:
        
        list_series = pd.Series(list_item)
        list_series = list_series.str.replace('[', '', regex = False).str.replace(']', '', regex = False).str.replace('"', '', regex = False).str.replace("'", '', regex = False)
        list_series = list_series.replace(np.nan, None).replace('NaN', None).replace('nan', None).replace('none', None).replace('None', None)
        list_series = list_series.str.split(',')
        clean_list = list_series[0]
# 
        return clean_list
    
    else:
        raise TypeError('Item must be a string')

def text_cleaner(data: str, replace: List[str] = ['[', ']', '\n', '\\n', '\\\\']) -> str:
    
    """
    Takes string and removes inputted characters."
    """
    
    for character in replace:
            data = data.replace(character, '')
    
    return data

def remove_stopwords(
                    words_list: List[str], 
                     language: str = 'English', 
                     stopwords_list = 'all', 
                     min_length: int = 2, 
                     max_length: int = 50, 
                     ignore_case: bool = True, 
                     retain_order: bool = False
                    ) -> list:
    
    """
    Takes a sequence of strings and checks if the string is in a list of stopwords. Returns a list object with the strings removed.
    
    Users can select a list of stopwords from the stopwords object. Defaults to 'all' stopwords.
    Users can opt-in to retain the order of the words using the 'retain_order' parameter.
    """
    
    # Importing stopwords object into the environment
    global stopwords
    
    if stopwords_list in stopwords.keys():
        to_remove = stopwords[stopwords_list]
    
    else:
        
        if type(stopwords_list) == list:
            to_remove = stopwords_list
        
        else:
            if (language.lower() == 'english') or (language.lower() == 'en'):
                to_remove = stopwords['en']
    
    # Conducting some data cleaning
    to_remove = pd.Series(to_remove, dtype = object).str.lower().to_list()
    
    words_list_cleaned = pd.Series(words_list, dtype = object)
    words_list_cleaned = words_list_cleaned.str.replace('|', ' ', regex = False).str.replace('/', ' ', regex=False).str.replace('=', ' ', regex = False).str.replace('\n', ' ', regex = False).str.replace('?', ' ', regex=False).str.replace('!', ' ', regex=False).str.replace('“', ' ', regex=False).str.replace('”', ' ', regex=False).str.replace('"', ' ', regex=False).str.replace('"', ' ', regex=False).str.replace('#', ' ', regex=False).str.replace('[', '', regex=False).str.replace(']', '', regex=False).str.replace('(', '', regex=False).str.replace(')', '', regex=False).str.replace('%', ' ', regex=False)
    words_list_cleaned = words_list_cleaned.str.replace("'", '', regex=False).str.replace('\\', ' ', regex=False).str.replace('+', ' ', regex=False).str.replace('=', ' ').str.replace('-', ' ', regex=False).str.replace('/', ' ', regex=False).str.replace('_', ' ', regex=False)
    words_list_cleaned = words_list_cleaned.str.strip(' ').str.strip().str.replace('  ', ' ', regex=False).str.strip()
    
    if ignore_case == True:
        words_list_cleaned = words_list_cleaned.str.lower()
        
    words_list_cleaned = words_list_cleaned[(words_list_cleaned.str.len() >= min_length) & (words_list_cleaned.str.len() <= max_length) & (words_list_cleaned != '') & (words_list_cleaned != None) & (words_list_cleaned != np.nan)]
    words_list_cleaned = words_list_cleaned.to_list()
    
    words_list_joined = ','.join(words_list_cleaned)
    words_list_joined = words_list_joined.replace(' ', ',')
    words_list_cleaned = words_list_joined.split(',')
    
    
    # Converting lists into sets
    remove_words_set = set(to_remove)
    wordslist_set = set(words_list_cleaned)
    
    # Identifying words to be removed and retained
    to_remove = list(wordslist_set.intersection(remove_words_set))
    to_retain = list(wordslist_set.difference(remove_words_set))
    
    # Returning set difference if 'retain_order' parameter is set to False.
    if retain_order == False:
        result = to_retain
    
    else:
    
    # Creating list of strings *not* in the stopwords list. Takes advantage of Pandas' serialisation.
        words_df = pd.DataFrame(words_list_cleaned, columns = ['word'])
        words_df_lowered = words_df.copy(deep=True)
        words_df_lowered['word'] = words_df_lowered['word'].str.lower()
        
        drop_indexes = []
        for word in to_remove:
            indexes_to_remove = list(words_df_lowered[words_df_lowered['word'] == word].index)
            drop_indexes = drop_indexes + indexes_to_remove
            
        words_df = words_df.drop(drop_indexes)
        result_list = words_df['word'].to_list()
        result_joined = ','.join(result_list)
        result_joined = result_joined.replace(' ', ',')
        result = result_joined.split(',')
        
    return result
        
def html_words_cleaner(words_list: List[str], to_remove: List[str] = 'default', ignore_case: bool = True, retain_order: bool = True) -> list:
    
    """
    Takes a sequence of strings and checks if the string is in a list of html stopwords. Returns a list object with the strings removed.
    
    Users can select a list of stopwords from the stopwords object. Defaults to html stopwords.
    Users can opt-in to retain the order of the strings using the 'retain_order' parameter.
    """
    
    # By default, selecting the 'html' stopwords list
    if to_remove == 'default':
        global stopwords
        to_remove = 'html'
    
    # Conducting data cleaning
    words_joined = ','.join(words_list)
    words_joined = words_joined.replace('_', ',').replace('-', ',').replace('^', ',').replace('+', ',').replace('=', ',').replace('.', ',').replace(':', ',').replace('#', ',').replace(';', ',').replace('{', ',').replace('}', ',').replace('(', ',').replace(')', ',').replace('  ', ',').replace(' ', ',')
    split_words = words_joined.split(',')
    
    # Removing stopwords
    output_list = remove_stopwords(split_words, stopwords_list = to_remove, min_length = 2, max_length= 50, ignore_case= ignore_case, retain_order = retain_order)

    return output_list

def html_extract_all_words(html: str = 'request_input', to_remove: List[str] = 'default', ignore_case: bool = True, retain_order: bool = True) -> list:
    
    """
    Takes html as a string object and extracts words. Returns as a list object.
    """
    
    # By default, requests user to input html string
    if html == 'request_input':
        html = input('HTML to parse: ')
    
    # Tokenizing html
    html_tokenized = word_tokenize(html)
    
    # Cleaning html and extracting words
    html_words = html_words_cleaner(words_list = html_tokenized, to_remove = to_remove, ignore_case = ignore_case, retain_order = retain_order)
    
    return html_words

def text_splitter(data: str, parse_by: str = '.', replace: List[str] = ['\n', '\\n', '\\\\']) -> list:
    
    """
    Takes text as a string object, cleans it, and splits it into a set of strings depending on symbols given. Returns as a list object.
    """
    
    if type(data) == str:
        
        for character in replace:
            data = data.replace(character, parse_by)

        data = data.split(parse_by)
        
        split_data = []
        for element in data:
            if element != '':
                element = element.strip()
                split_data.append(element)

                
        return split_data
    
    
    else:
        return
    
def html_get_tags(html: str) -> list:
    
    """
    Takes html as a string object and extracts tags. Returns as a list.
    """
    
    # Cleaning and splitting data
    data = html.replace('>', '>\n')
    data_split = text_splitter(data, parse_by = '\n')
    
    
    # Extracting tags
    tags = []
    for item in data_split:
        
        if len(item) > 0:
            if (item[0] == '<') and (item[-1] == '>'):
                tags.append(item)
    
    
    tags = list(filter(None, tags))
    
    return tags

def get_text_selectolax(html:str) -> str:
    
    """
    Takes html as a string object and returns the text it contains.
    """
    
    
    tree = HTMLParser(html)

    if tree.body is None:
        return None

    for tag in tree.css('script'):
        tag.decompose()
    for tag in tree.css('style'):
        tag.decompose()

    text = tree.body.text(separator='\n')
    
    return text

def html_text_cleaner(text: str) -> str:
    
    """
    Takes html as a string; removes unwanted characters and strings.
    """
    
    def stripwords(item):
    
        item = item.replace('imprint ads', ' ').replace('javascript is not available', ' ').replace('javascript is disabled', ' ').replace('enable javascript', ' ').replace('supported browsers', ' ').replace('supported browser', ' ').replace('cookie policy', ' ').replace('/div', '').replace('layer layer', ' ').replace('function', ' ').replace('cookie', ' ').replace('page path', ' ').replace('true);', ' ').replace('ua-', ' ').replace('a llow', ' ').replace('allow', ' ').replace('d eveloper', ' ').replace('Developer', ' ').replace('developer', ' ').replace('placehol der', ' ').replace('anonymize ip', ' ').replace(';function', ' ').replace('push(arguments)', ' ').replace('datalayer', ' ').replace(':false', ' ').replace('ad personalization', ' ').replace('default', ' ').replace('page location', ' ').replace('placeholder', ' ').replace('config', ' ').replace('window.data', ' ').replace('window.datalayer', ' ').replace('window.dataLayer', ' ').replace('latest news', ' ').replace('Organization Structure', ' ').replace('Agenda', ' ').replace('\0Tweets', ' ').replace('mx-1', ' ').replace(' b ', ' ').replace('EN', ' ').replace('tooltip', ' ').replace('gtag', ' ').replace('id.', ' ').replace(':true', ' ').replace(' js ', ' ').replace('bottom=', ' ').replace('title=', ' ').replace('\\xa0', ' ').replace('xa0', ' ').replace('style=', ' ').replace('page_placeholder', ' ').replace('class=', ' ').replace('toggle', ' ').replace('data-', ' ').replace('a0', ' ').replace('\\x', ' ').replace('href=', ' ').replace("site map", ' ').replace("Sites Map", ' ').replace("sites map", ' ').replace('contact us', ' ').replace('Contact Us', ' ').replace('about us', ' ').replace('About Us', ' ').replace('sitemap', ' ')
        
        return item
    
    
    def stripchars(item):
        
        item = item.replace(' ); ', ' ').replace('); ', ' ').replace(' ) ', ' ').replace(' ( ', ' ').replace(' . ', ' ').replace(' ,', ' ').replace('>', ' ').replace('<', ' ').replace('\\t', ' ').replace('\n', ' ').replace('\\n', ' ').replace('\\\\n', ' ').replace('b\' ', ' ').replace("\'", ' ').replace('\\t', ' ').replace('\n', ' ').replace('\\n', ' ').replace('\\\\n', ' ').replace('b\' ', ' ').replace("\'", ' ').replace('\\', ' ').replace('|', ' ').replace('"', ' ').replace(']', ' ').replace('[', ' ').replace('=', ' ').replace(' ; ', ' ').replace(' ;', ' ').replace(' : ', ' ').replace('{', ' ').replace('}', ' ').replace('{}', ' ').replace('()', ' ').replace('_', ' ').replace(' s ', "'s ")

        return item
    
    def stripspace(item):
        
        item = item.replace('  ',' ').replace('   ', ' ').replace('     ', ' ').replace('       ', ' ')
        item = item.strip()
        
        return item
    
    text = stripwords(text)
    text = stripchars(text)
    text = stripspace(text)
    text = text.lower()
    text = stripwords(text)
    text = stripchars(text)
    text = stripspace(text)
    
    return text

def get_words_list(text: str, min_length = 2, lower = False) -> list:
    
    """
    Takes text as a string and returns a list of words.
    """
    
    # Removing leading and trailing whitespaces
    text = text.strip()
    
    # Replacing punctuation prior to splitting
    text = text.replace(' -', '').replace('!', '').replace('?', '').replace(',', '').replace('.', '').replace('(', '').replace(')', '').replace(':', '').replace(';', '')
    
    # Splitting string into list
    words_list = text.split(' ')
    
    # Removing empty strings and words shorter than the provided minimum length
    words_list_cleaned = [i for i in words_list if ((i != '') and (i != ' ') and (len(i) > min_length))]
    
    return words_list

def get_html_clean_text(html: str) -> str:
    
    """
    Takes html, extracts text, cleans it, and returns it as a string.
    """
    
    text = get_text_selectolax(html = html)
    text = html_text_cleaner(text)
    
    return text

def get_html_clean_words(html: str) -> list:
    
    """
    Takes html, extracts text, cleans it, and returns words contained as a string.
    """
    
    text = get_html_clean_text(html = html)
    words_list = get_words_list(text = text)
    
    return words_list

def list_remove_nones(list_item: list) -> list:
    
    """
    Removes None items from list.
    """
    
    return [i for i in list_item if i != None]

def none_list_to_empty_list(list_item: list) -> list:
    
    """
    Turns a list of None items into an empty list.
    """
    
    # Checking if object is a list
    if type(list_item) != list:
        raise TypeError('Item must be a list')
    
    # Returning empty list if list is a list of Nones
    if (len(list_item) == 1) and ((list_item[0] == None) or (list_item[0].lower() == 'none') or (str(list_item[0]) == 'NaT') or (list_item[0] == np.nan) or (list_item[0] == pd.NaT)):
        return list()
    
    else:
        return list_item

def nat_list_to_nones_list(list_item: list) -> list:
    
    """
    Replaces Numpy Not a Time (NaT) items in a list with Nones.
    """
    
    
    # Checking if object is a list
    if type(list_item) != list:
        raise TypeError('Item must be a list')
    
    
    # Replacing NaTs with Nones
    new_list = []
    
    for i in list_item:
        if type(i) == type(pd.NaT):
            new_list.append(None)
        else:
            new_list.append(i)
            
    # If list is (now) empty, returns a None object
    if (len(list_item) == 1) and (type(list_item[0]) == None):
        return None
    
    return new_list

def series_none_list_to_empty_lists(item):
    
    """
    Replaces None items in Pandas series with empty lists. To be use with pandas.apply().
    """
    
    try:
        return none_list_to_empty_list(item)
    except:
        return item

def empty_list_to_none(list_item: list):
    
    """
    Replaces empty list with None object.
    """
    
    # Checking if item is a list object
    if type(list_item) != list:
        raise TypeError('Item must be a list')
    
    if (len(list_item) == 1) and (list_item[0].lower() == 'none'):
        return None
    
    if len(list_item) == 0:
        return None
    
    else:
        return list_item

def empty_set_to_none(set_item: set):
    
    """
    Replaces empty set with None object.
    """
    
    if type(set_item) != set:
        raise TypeError('Item must be a set')
    
    if len(set_item) == 0:
        return None
    
    else:
        return set_item

def empty_dict_to_none(dict_item: dict):
    
    """
    Replaces empty dictionary with None object.
    """
    
    if type(dict_item) != dict:
        raise TypeError('Item must be a dictionary')
    
    if len(dict_item) == 0:
        return None
    
    else:
        return dict_item

def empty_to_none(item):
    
    """
    Replaces empty object with None object.
    """
    
    if type(item) == list:
        try:
            return empty_list_to_none(item)
        except:
            pass
    
    if type(item) == set:
        try:
            return empty_set_to_none(item)
        except:
            pass
    
    if type(item) == dict:
        try:
            return empty_dict_to_none(item)
        except:
            pass
    
    try:
        if len(item) == 0:
            return None
        else:
            return item
        
    except:
        return item
    
def parse_data(data, data_type: str, ignore_case: bool = True, stopwords: str = 'all', language: str = 'english', retain_word_order: bool = False) -> dict:
    
    """
    Parses string object based on an inputted datatype. Returns dictionary of parsed contents.
    """
    
    # Checking if data is None object. If so, returns an empty dictionary.
    if data == None:
        return {}
    
    # Checking the type of the data
    data_var_type = type(data)
    
    output = None
    
    # Processing data if datatype given is text
    if data_type == 'text':
        
        # Checking type
        if data_var_type != str:
            raise TypeError('Text data must be inputted as a string')
        
        # Lowering case for data cleaning purposes
        if ignore_case == True:
            data = data.lower()
        
        data_no_para = data.replace('\n', '')
        
        # Parsing data
        output = {
                'paragraphs': text_splitter(data, parse_by = '\n'),
                'sentences': sent_tokenize(data_no_para),
                'phrases': text_splitter(data, parse_by = '.', replace = [
                                                                            '\n',
                                                                            '(',
                                                                            ')',
                                                                            '['
                                                                            ']'
                                                                            '}'
                                                                            '{'
                                                                            '\\', 
                                                                            '/', 
                                                                            ',', 
                                                                            ';',
                                                                            ':',
                                                                            ' - ',
                                                                            ' -- ']
                                                                                ),
                'words': word_tokenize(data_no_para)}
#             text_splitter(data, parse_by = ' ', replace = [
#                                                                         '\n',
#                                                                         '(',
#                                                                         ')',
#                                                                         '['
#                                                                         ']'
#                                                                         '}'
#                                                                         '{'
#                                                                         '\\', 
#                                                                         '/', 
#                                                                         ',', 
#                                                                         ';',
#                                                                         ':',
#                                                                         ' - ',
#                                                                         ' -- ']
#                                                                                 ),
#                 }
        
        
    # Processing data if datatype given is html
    if (data_type == 'html') or (data_type == 'web code'):
            
            # Getting text if data is an html requests object
            if type(data) == requests_html.HTMLResponse:
                data = data.text
            
            # Lowering case for data cleaning purposes
            if ignore_case == True:
                data = data.lower()
            
            # Parsing data
            output = {}
            output['tags'] = html_get_tags(data)
            output['text'] = get_html_clean_text(data)
            output['words'] = get_html_clean_words(data)
            output['element_list'] = text_splitter(data, parse_by = '\n')
    
    # Extracting words and removing stopwords
    if output != None:
        if output['words'] != None:
            output['words'] = remove_stopwords(
                                                words_list = output['words'],
                                                language = language,
                                                stopwords_list = stopwords,
                                                ignore_case = ignore_case,
                                                retain_order = retain_word_order
                                              )
    
    return output

def parse_data_to_set(data, data_type: str, ignore_case: bool = True) -> set:
    
    """
    Parses string data object and returns a set.
    """
    
    # Parsing data
    parsed_data = parse_data(data=data, data_type=data_type, ignore_case = ignore_case)
    
    # Creating set from data.
    for key in parsed_data.keys():
        parsed_data[key] = set(parsed_data[key])
    
    return parsed_data

def is_int(string: str) -> bool:
    
    """
    Checks if a string can be converted successfully to an integer.
    """
    
    try:
        int(string)
        return True
    
    except:
        return False

def correct_int(string: str) -> int:
    
    """
    Converts a string to an integer if it can be converted successfully. Uses exception handling to avoid errors.
    """
    
    boolean = is_int(string)
    
    if boolean == True:
        return int(string)
    
    else:
        return

def is_float(string: str) -> bool:
    
    """
    Checks if a string can be converted successfully to a float.
    """
    
    try:
        float(string)
        return True
    
    except:
        return False

def correct_float(string: str):
    
    """
    Converts a string to a float if it can be converted successfully. Uses exception handling to avoid errors.
    """
    
    boolean = is_float(string)
    
    if boolean == True:
        return float(string)
    
    else:
        return

def date_checker(date: str, print_format: bool = False) -> tuple:
    
    """
    Checks if a string is a date. If successful, returns a tuple containing result and the date's format.
    """
    
    # defining formats
    date_formats = [
            '%y',
            '%Y',
            '%m %Y',
            '%m.%y',
            '%m.%Y',
            '%m/%y',
            '%m/%Y',
            '%m-%y',
            '%m-%Y',
            '%Y %m',
            '%y.%m',
            '%Y.%m',
            '%y/%m',
            '%Y/%m',
            '%y-%m',
            '%Y-%m',
            '%d-%m-%y',
            '%d/%m/%y',
            '%d.%m.%y',
            '%d-%m-%Y',
            '%d/%m/%Y',
            '%d.%m.%Y',
            '%m-%d-%y',
            '%m/%d/%y',
            '%m.%d.%y',
            '%m-%d-%Y',
            '%m/%d/%Y',
            '%m.%d.%Y',
            '%y-%m-%d',
            '%y/%m/%d',
            '%y.%m.%d',
            '%Y-%m-%d',
            '%Y/%m/%d',
            '%Y.%m.%d'
          ]
    
    date_format = ''
    
    for i in date_formats:

        # using try-except to check for truth value
        try:
            res = bool(datetime.strptime(date, i))
            if res == True:
                date_format = i
                if print_format == True:
                    print('Date format: ' + i)
                break
            else:
                continue

        except:
            res = False
            continue
    
    return (res, date, date_format)

def is_date(string: str, print_format: bool = False):
    
    """
    Checks if a string is a date.
    """
    
    if type(string) == datetime:
        return (True, string, None)
    
    if type(string) != str:
        raise TypeError('Input must be a string')
        
    string = string.strip()
    
    date_result = date_checker(date = string, print_format = print_format)
    
    if date_result[0] == False:

        if ('T' in string) or ('t' in string) or (': ' in string) or (',' in string) or (' ' in string):
            if ('T' in string) and ('UTC' not in string) and ('utc' not in string):
                string = string.split('T')[0].strip()

            else:
                if ('t' in string):
                    string = string.split('t')[0].strip()

                else:
                    if (': ' in string):
                        string = string.split(': ')[0].strip()
                    
                    else:
                        if ',' in string:
                            split = string.split(',')
                            if len(split) <=2:
                                string = split[0].strip()
                                
                            
                        elif ' ' in string:
                            split = string.split(' ')
                            if len(split) <=2:
                                string = split[0].strip()
                        
        date_result = date_checker(date = string, print_format = print_format)
    
    return date_result

def str_to_date(string: str, print_format: bool = False) -> datetime:
    
    """
    Converts date strings to datetime objects.
    """
    
    if type(string) == datetime:
        return string
    
    if type(string) != str:
        raise TypeError('Input must be a string')
    
    result = is_date(string, print_format = print_format)
    if result[0] == True:
    
        try:
            return datetime.strptime(result[1], result[2])

        except:
            raise TypeError('Input is not a valid date')
    
    else:
        raise TypeError('Input is not a valid date')

def time_checker(time: str, print_format: bool= False) -> tuple:
    
    """
    Checks if a string is a time.
    """
    
    # defining formats
    time_formats = [
            '%I%p',
            '%H%M',
            '%H %M',
            '%H/%M',
            '%H.%M',
            '%H:%M',
            '%H, %M',
            '%I%p%M',
            '%I%p %M',
            '%I%p, %M',
            '%H/%M/%S',
            '%H.%M.%S',
            '%H:%M:%S',
            '%H, %M, %S',
            '%H %M %S',
            '%I%p%M%S',
            '%I%p/%M/%S',
            '%I%p.%M.%S',
            '%I%p:%M:%S',
            '%I%p, %M, %S',
            '%I%p %M %S'
          ]
    
    time_format = ''
    
    if len(time) > 0:
        
        # Checking string against all time formats given
        for i in time_formats:

            # using try-except to check for truth value
            try:
                time_res = bool(datetime.strptime(time, i))
                if time_res == True:
                    time_format = i
                    if print_format == True:
                        print('Time format: ' + i)
                    break
                    
                else:
                    time_format = ''
                    continue

            except:
                time_format = ''
                time_res = False
                continue
    
    else:
        time_format = ''
        time_res = False
    
    return (time_res, time, time_format)

def utc_checker(utc_offset, print_format = False):
    
    """
    Checks if a string is in UTC datetime format.
    """
    
    # defining formats
    utc_formats = [
            '%z',
            '%Z',
            '%Z %z',
            '%Z%z',
            '%:z'
          ]
    
    utc_format = ''
    
    if len(utc_offset) > 0:
        
        # Checking string against all UTC datetime formats given
        for i in utc_formats:

            # using try-except to check for truth value
            try:
                utc_res = bool(datetime.strptime(utc_offset, i))
                if utc_res == True:
                    utc_format = i
                    if print_format == True:
                        print('UTC format: ' + i)
                    break
                else:
                    utc_format = ''
                    continue

            except:
                utc_format = ''
                utc_res = False
                continue
    
    else:
        utc_format = ''
        utc_res = False
    
    return (utc_res, utc_offset, utc_format)

def is_time(string: str, print_format: bool = False):
    
    """
    Checks if a string is a time, accounting for UTC formatting.
    """
    
    if type(string) == datetime:
        return (True, string, None)
    
    if type(string) != str:
        raise TypeError('Input must be a string')
    
    # Stripping leading and trailing whitespaces
    string = string.strip()
    
    # Checking if the string is a time using time checker
    time = string
    utc_offset = ''
    time_res = time_checker(time, print_format = print_format)
    
    # If the result is False, checking alternative formats.
    if time_res[0] == False:
        
        if ('T' in string) or ('t' in string) or (': ' in string) or (',' in string) or (' ' in string):
            if ('T' in string) and ('UTC' not in string) and ('utc' not in string):
                string = string.split('T')[1].strip()

            else:
                if ('t' in string):
                    string = string.split('t')[1].strip()

                else:
                    if (': ' in string):
                        string = string.split(': ')[1].strip()
                    
                    else:
                        if ',' in string:
                            split = string.split(',')
                            if len(split) <=2:
                                string = split[1].strip()
                            
                        elif ' ' in string:
                            split = string.split(' ')
                            if len(split) <=2:
                                string = split[1].strip()
    
        time = string
        utc_offset = ''

        time_res = time_checker(time, print_format = print_format)
    
    
    # If the result is False, checking alternative formats.
    if time_res[0] == False:
        
        if ('+' in string) or ('-' in string) or ('utc' in string.lower()):
        
            if ('UTC' in string) and (('+' in string) or ('-' in string)):
                string = string.replace('UTC', '')
            
            if ('UTC' in string):
                string = string.replace('UTC', '+')
            
            if ('utc' in string) and (('+' in string) or ('-' in string)):
                string = string.replace('utc', '')
            
            if ('utc' in string):
                string = string.replace('utc', '+')
            
            if '+' in string:
                    split = string.split('+')
                    time = split[0].strip()
                    utc_offset = '+' + split[1].strip()

            elif ('-' in string):
                    split = string.split('-')
                    time = split[0].strip()
                    utc_offset = '-' + split[1].strip()
        
        else:
            time = string
            utc_offset = ''
    
    
    # If the result is False, checking alternative formats and UTC formatting.
    
    time_res = time_checker(time, print_format = print_format)
    utc_res = utc_checker(utc_offset = utc_offset, print_format = print_format)
    
    if time_res[0] == True:
        new_time_str = time_res[1]
    else:
        new_time_str = time
    
    if utc_res[0] == True:
        new_utc_res = utc_res[1]
    else:
        new_utc_res = utc_offset
    
    new_str = new_time_str + new_utc_res
    new_format = time_res[2] + utc_res[2]
    
    if (time_res[0] == True) or (utc_res[0] == True):
        return (True, new_str, new_format)
    
    else:
        return (False, new_str, new_format)

def str_to_time(string: str, print_format: bool = False):
    
    """
    Converts time strings to time objects.
    """
    
    if type(string) == datetime:
        return string
    
    if type(string) != str:
        raise TypeError('Input must be a string')
    
    # Checking if the string is a time
    result = is_time(string, print_format = print_format)
    if result[0] == True:
    
        try:
            return datetime.strptime(result[1], result[2])

        except:
            raise TypeError('Input is not a valid time')
    
    else:
        raise TypeError('Input is not a valid time')
      
def is_datetime(string: str, print_format: bool = False) -> tuple:
    
    """
    Checks if string is a date, time, or date-time.
    """
    
    if type(string) == datetime:
        return (True, string, None)
    
    if type(string) != str:
        raise TypeError('Input must be a string')
    
    input_string = copy.deepcopy(string)
    
    # Checking if the string is a date or time
    date_res = is_date(string, print_format)
    time_res = is_time(string, print_format)
    
    if (date_res[0] == True) and (time_res[0] == False):
        return date_res
    
    if (date_res[0] == False) and (time_res[0] == True):
        return time_res
    
    if (date_res[0] == True) and (time_res[0] == True):
        return date_res
    
    else:
        return (False, input_string, None)
    
    
    
    # If the result is False, checking alternative formats and UTC formatting.
    if (date_res[0] == False) and (time_res[0] == False):
        
        if ('T' in string) or ('t' in string) or (': ' in string) or (' ' in string) or (',' in string):

            if ('T' in string):
                split = string.split('T')
                date_string = split[0].strip()
                time_string = split[1].strip()

            else:
                if ('T' in string):
                    split = string.split('T')
                    date_string = split[0].strip()
                    time_string = split[1].strip()

                else:
                    
                    
                    if (',' in string):
                        split = string.split(',')
                        date_string = split[0].strip()
                        time_string = split[1].strip()

                    else:
                        
                        if (': ' in string):
                            split = string.split(' ')
                            date_string = split[0].strip()
                            time_string = split[1].strip()
                        
                        elif (' ' in string):
                            split = string.split(' ')
                            date_string = split[0].strip()
                            time_string = split[1].strip()

        else:
            date_string = string
            time_string = ''

        date_res = is_date(date_string, print_format)
        time_res = is_time(time_string, print_format)
    
    
    
    
    if (date_res[0] == True) and (time_res[0] == True):
        new_str = date_res[1] + ' ' + time_res[1]
        new_format = date_res[2] + ' ' + time_res[2]
        return (True, new_str, new_format)
    
    else:
        return (False, input_string, None)

def is_date_or_time(string: str, print_format: bool = False) -> bool:
    
    """
    Checks if string is a date or time.
    """
    
    if type(string) == datetime:
        return (True, string, None)
    
    if type(string) != str:
        raise TypeError('Input must be a string')
    
    dt_res = is_datetime(string, print_format)
    
    if dt_res == False:
        date_res = is_date(string, print_format)[0]
        time_res = is_time(string, print_format)
    else:
        date_res = True
        time_res = True
    
    if print_format == True:
        print('\nIs date-time: ' + str(dt_res) + '\nContains date: ' + str(date_res) + '\nContains time: ' + str(time_res))
        
    if (date_res == True) or (time_res == True) or (dt_res == True):
        return True
    else:
        return False

def str_to_datetime(string: str, print_format:bool = False) -> datetime:
    
    """
    Converts date, time, or datetime strings to datetime objects.
    """
    
    if type(string) == datetime:
        return string
    
    if type(string) != str:
        raise TypeError('Input must be a string')
    
    # Checking if the item is a datetime
    dt_result = is_datetime(string, print_format = print_format)
    
    if dt_result[0] == True:
        return datetime.strptime(dt_result[1], dt_result[2])
    
    else:
        date_result = is_date(string, print_format = print_format)
        time_result = is_time(string, print_format = print_format)
    
        if (date_result[0] == True) and (time_result[0] == True):
            new_str = date_result[1] + ' ' + time_result[1]
            new_format = date_result[2] + ' ' + time_result[2]
            try:
                return datetime.strptime(new_str, new_format)

            except:
                raise TypeError('Input is not a valid date-time')
    
        else:
            if date_result[0] == True:
                
                try:
                    return datetime.strptime(date_result[1], date_result[2])

                except:
                    raise TypeError('Input is not a valid date-time')

            else:
                if time_result[0] == True:
                    try:
                        return datetime.strptime(time_result[1], time_result[2])

                    except:
                        raise TypeError('Input is not a valid date-time')
                
                else:
                    raise TypeError('Input is not a valid date-time')

def list_to_datetimes(item: list) -> list:
    
    """
    Converts a list of date or time strings into a list of datetime objects.
    """
    
    if (type(item) == list):
        item = pd.to_datetime(pd.Series(item, dtype=object)).to_list()
        return item
        
    else:
        return item

def series_to_datetimes(item: pd.Series) -> pd.Series:
    
    """
    Converts a Pandas series of dates into a series of datetime objects.
    """
    
    if type(item) == str:
        try:
            return str_to_datetime(item, False)
        except:
            return item
    
    else:
        if type(item) == list:
            try:
                return pd.to_datetime(item.astype(object))
            except:
                return item
        
        else: 
            return item

def correct_series_of_lists(series: pd.Series) -> pd.Series:
    
    """
    Converts a Pandas series of strings representing lists into a series of lists.
    """
    
    if type(series) != pd.Series:
        raise TypeError('Series must be a pandas Series object')
    
    index = list(series.index)
    for i in index:
        
        item = series.loc[i]
        
        if type(item) == str:
            item = item.replace('[', '').replace(']', '').replace('{', '').replace('}', '').replace('"', '').replace("'", "").strip().split(',')
            item = [i.strip() for i in item]

        series.at[i] = item
    
    return series

def merge_duplicate_ids(dataframe, merge_on: str):

    """
    Takes a DataFrame and merges rows with duplicate IDs.

    Parameters
    ----------
    dataframe : Results, References or pandas.DataFrame
        dataframe to process.
    merge_on : str
        name of column containing IDs to merge on.
    
    Returns
    -------
    dataframe : Results, References or pandas.DataFrame
        processed DataFrame.
    """

    df = dataframe.copy(deep=True)
    
    if merge_on in df.columns:

        # sorting index to prioritise entries with real values

        df = df.sort_values(merge_on)

        # Checking if there are valid IDs in the column (i.e., not None or NaN)
        dropna_indexes = df[merge_on].dropna().index

        if len(dropna_indexes) > 0:

            df_dropna = df.copy(deep=True).loc[dropna_indexes]

            try:
                ids = set(df_dropna[merge_on].to_list())
            except:
                ids = set(df_dropna[merge_on].astype(str).to_list())

            for i in ids:

                masked = df[df[merge_on]==i]
                masked_indexes = masked.index.to_list()
                masked = masked.reset_index().drop('index', axis=1)
                len_masked = len(masked)

                if len_masked > 0:

                    first_index = masked_indexes[0]
                    duplicate_indexes = masked_indexes[1:]
                    first_row = masked.loc[0]

                    for index in range(1, len_masked):

                        row = masked.loc[index]

                        for c in first_row.index:

                            data = first_row[c]
                            dtype = type(data)
                            dtype_str = str(dtype)
                            str_data = str(data)

                            if ((data is None) 
                                or (data is np.nan) 
                                or ((dtype == str) and (data == '')) 
                                or ((dtype == str) and (data == '[]'))
                                or ((dtype == str) and (data == '[]'))
                                or  ((dtype == str) and (data == 'None'))
                                or  ((dtype == str) and (data == 'none'))
                                or ((dtype == list) and (data == []))
                                or (str_data == '[]')
                                or (str_data == '{}')
                                ):
                                    first_row[c] = row[c]
                                    continue
                            
                            else:
                                if '.Results' in dtype_str:
                                    data2 = row[c]
                                    dtype2 = type(data2)
                                    dtype_str2 = str(dtype2)
                                    if (dtype2 == pd.DataFrame) or ('.Results' in dtype_str2):
                                        data2_copy = data2.copy(deep=True)
                                        first_row[c].add_dataframe(data2_copy)
                                        first_row[c].remove_duplicates()
                                    continue

                                if ('.Authors' in dtype_str) or ('.Funders' in dtype_str) or ('.Affiliations' in dtype_str):
                                    data2 = row[c]
                                    dtype2 = type(data2)
                                    dtype_str2 = str(dtype2)

                                    if ('.Authors' in dtype_str2) or ('.Funders' in dtype_str2) or ('.Affiliations' in dtype_str2):
                                        concat_df = pd.concat([data.all, data2.all])
                                        concat_df = deduplicate(concat_df)
                                        first_row[c].all = concat_df
                                    continue
  
                    try:
                        first_row = pd.Series(first_row)
                        df.loc[first_index] = first_row
                    except:
                        pass

                    df = df.drop(labels=duplicate_indexes)
        
        dataframe = df

    return dataframe

def merge_all_duplicate_ids(dataframe):

    """
    Takes a DataFrame and merges rows with duplicate bibliometric IDs. 

    Parameters
    ----------
    dataframe : Results, References or pandas.DataFrame
        dataframe to process.
    
    Returns
    -------
    dataframe : Results, References or pandas.DataFrame
        processed DataFrame.
    
    Notes
    -----
    Bibliometric identifiers used to check for duplicate records: DOI, ISBN, ISSN, URI, ORCID, CrossRef ID, Scopus, Web of Science, PubMed, address, URL, website.
    """

    id_names = ['doi', 'isbn', 'issn', 'uri', 'orcid', 'crossref_id', 'crossref', 'scopus_id', 'scopus', 'wos_id', 'wos', 'pubmed_id', 'address', 'link', 'website']
    df = dataframe.copy(deep=True)

    for i in id_names:
        if i in dataframe.columns:
            dataframe = merge_duplicate_ids(dataframe, merge_on = i)
    
    return dataframe

def deduplicate(dataframe):

    """
    Deduplicates custom ART DataFrames (Results, References, Authors.summary, Funders.summary, Affiliations.summary) using unique identifiers.
    """

    ignore_cols = ['work_id',
                   'author_id',
                   'funder_id',
                   'affiliation_id',
                   'alt_names',
                   'authors', 
                    'funder',
                    'given_name',
                    'affiliations',
                    'citations',
                    'citations_data',
                    'publications',
                    'address',
                    'work_count',
                    'tokens',
                    'other_links',
                    'website',
                    'link',
                    'other_links']

    df = dataframe.copy(deep=True)
    
    if 'doi' in df.columns:
        df['doi'] = df['doi'].str.replace('https://', '', regex = False).str.replace('http://', '', regex = False).str.replace('dx.', '', regex = False).str.replace('doi.org/', '', regex = False).str.replace('doi/', '', regex = False)

    # Creating dataframe without empty columns; converting to string to avoid errors
    df_dropna = df.dropna(axis=1).astype(str)

    # Converting strings to lowercase to improve matching
    for c in df_dropna.columns:
        df_dropna[c] = df_dropna[c].str.lower()

    # Removing duplicates
    col_subset = [c for c in df_dropna.columns if c not in ignore_cols]
    if len(col_subset) > 0:
        first_dedpublication = df_dropna.drop_duplicates(subset=col_subset)
    else:
        first_dedpublication = df_dropna
        
    first_dedpublication_index = first_dedpublication.index

    df2 = dataframe.loc[first_dedpublication_index]

    # Checking for duplicate UIDs; merging rows that share UIDs
    df3 = merge_all_duplicate_ids(df2)

    final_df = df3.reset_index().drop('index', axis=1)

    return final_df