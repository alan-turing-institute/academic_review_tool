"""Functions to load and parse PDFs"""

from ..utils.basics import results_cols
from ..utils.cleaners import is_datetime, str_to_datetime
from ..internet.webanalysis import is_url

from pathlib import Path
import random
import re
import io
import copy
import requests
import numpy as np
import pandas as pd
from pypdf import PdfReader
from nltk.tokenize import word_tokenize, sent_tokenize

def pdf_to_dict(file_path = None):
    
    """
    Reads PDF from file and outputs data as a dictionary.
    """
    
    # Requesting file address from user input if none provided
    if file_path == None:
        file_path = input('File path: ')
    
    # Reading PDF file
    pdf_file = PdfReader(file_path) 
    
    # Extracting metadata
    info = pdf_file.metadata
    
    # Iterating throigh pages and extracting text
    first_page_raw = pdf_file.pages[0]
    raw_text = []
    for i in pdf_file.pages:
        raw_text.append(i.extract_text())
    
    # Joining text list to make string
    full_text = ' \n '.join(raw_text)
    
    # Cleaning text
    full_text = re.sub(r"\s+[0-9]+\s", "", full_text).replace('\n ', ' <p> ').replace('\n', ' \n ').replace('  ', ' ')
    
    # Creating output dictionary 
    output_dict = {'metadata': info,
                  'raw': raw_text,
                  'first_page': first_page_raw,
                  'full_text': full_text}
    
    return output_dict

def pdf_url_to_dict(url = None):
    
    """
    Reads PDF from URL and outputs data as a dictionary.
    """
    
    # Requesting URL from user input if none provided
    if url == None:
        url = input('URL: ')
    
    # Setting browser headers for site request
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Windows; Windows x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36'}
    
    # Retrieving PDF data
    response = requests.get(url = url, headers=headers, timeout=120)
    on_fly_mem_obj = io.BytesIO(response.content)
    pdf_file = PdfReader(on_fly_mem_obj)
    
    # Extracting metadata
    info = pdf_file.metadata
    
    # Iterating throigh pages and extracting text
    first_page_raw = pdf_file.pages[0].extract_text()
    raw_text = []
    for i in pdf_file.pages:
        raw_text.append(i.extract_text())
    
    # Joining text list to make string
    full_text = ' \n '.join(raw_text)
    
    # Cleaning text
    full_text = re.sub(r"\s+[0-9]+\s", "", full_text).replace('\n ', ' <p> ').replace('\n', ' \n ').replace('  ', ' ')
    
    # Creating output dictionary
    output_dict = {'metadata': info,
                  'raw': raw_text,
                  'first_page': first_page_raw,
                  'full_text': full_text}
    
    return output_dict

def parse_pdf_text(input_data):
    
    """
    Parses text from PDF reader result.
    """

    if type(input_data) == str:
        text = input_data
    else:
        if (type(input_data) == dict) and ('full_text' in input_data.keys()):
            text = input_data['full_text']
        else:
            if type(input_data) == list:
                text = ' '.join(input_data)
            else:
                 raise TypeError('Inout data must be a string, dictionary of strings, or list of strings.')
    
    lines = text.split('\n')
    mean_length = pd.Series(lines).apply(len).describe()['mean']
    new_lines = []

    for i in range(0, len(lines)):
        length = len(lines[i])

        if length <= (mean_length / 5):
            if (length >= 2) and (i != 0):
                new_line = (lines[i-1] + lines[i])
                if lines[i-1] in new_lines:
                    new_lines.remove(lines[i-1])
                new_lines.append(new_line)

        else:
            if lines[i] not in new_lines:
                new_lines.append(lines[i])

    new_lines = pd.Series(new_lines).str.replace(' \\.', '.').str.replace(' ;', ';').str.replace(' :', ':').str.replace(' ,', ',').str.strip().to_list()
    

    text = ' '.join(new_lines[1:]).replace('<p>', '\n').replace('  ', '')
    if ('references\n' in text.lower()):
        refs_index = text.lower().find('references')
    
    else:
        if ('bibliography\n' in text.lower()):
            refs_index = text.lower().find('bibliography')
    
        else:
            refs_index = -1
    
    new_lines = pd.Series(new_lines).str.replace('<p>', '\n').to_list()
    
    output = {'top': new_lines[0:5],
             'main_body': text[:refs_index],
             'references': text[refs_index:]}
    
    return output

def parse_pdf_doi(input_data):
    
    """
    Parses DOI from PDF reader result.
    """

    if type(input_data) == str:
        text = input_data
        metadata = {}

    if (type(input_data) == dict) and ('first_page' in input_data.keys()):
        text = ' \\\\\\ '.join(input_data['raw'])
        metadata = input_data['metadata']
    
    
    if ('/DOI' in metadata.keys()):
        doi = metadata['/DOI']
        if (len(auth) > 2):
            return doi
            
    if ('/doi' in metadata.keys()):
        doi = metadata['/doi']
        if (len(auth) > 2):
            return doi
    
    if ('DOI' in metadata.keys()):
        doi = metadata['DOI']
        if (len(auth) > 2):
            return doi
        
    if ('doi' in metadata.keys()):
        doi = metadata['doi']
        if (len(auth) > 2):
            return doi
    
    else:
         
        if (
            ('DOI' in text)
            or ('doi.org' in text)
            or ('doi:' in text)
            or ('doi/' in text)
            ):
            
            text = text.replace('/ ', ':').replace(': ', ':').replace(' :', ':').replace('\n', ' ').replace(';', ' ').replace(',', ' ').replace('!', ' ').replace('|', ' ').replace('(', ' ').replace(')', ' ').replace('[', ' ').replace(']', ' ').replace('}', ' ').replace('{', ' ').replace('"', ' ').replace("'", ' ').replace('□', ' ').replace('“', ' ').replace('”', ' ').replace('+', ' ').replace('=', ' ').replace('*', ' ').replace('^', ' ').replace('$', ' ').replace('©', ' ').replace('   ', '').replace('   ', '')
            
            text_split = text.split(' ')
            text_split = [i for i in text_split if 'doi' in i.lower()]
            
            return text_split[0]
    
def parse_pdf_links(input_data):
    
    """
    Parses links from PDF reader result.
    """
    
    # Checking type of input data and defining variables for parsing
    if type(input_data) == str:
        text = input_data
        metadata = {}

    if (type(input_data) == dict) and ('first_page' in input_data.keys()):
        text = ' ~~~~~ '.join(input_data['raw'])
        metadata = input_data['metadata']
            
            # Cleaning text
    text = text.replace('\n', ' ').replace(' /', '/').replace(': ', ':').replace(' :', ':').replace('\n', ' ').replace('’', ' ').replace(';', ' ').replace(',', ' ').replace('|', ' ').replace('[', ' ').replace(']', ' ').replace('}', ' ').replace('{', ' ').replace('"', ' ').replace("'", ' ').replace('□', ' ').replace('“', ' ').replace('”', ' ').replace('^', ' ').replace('©', ' ').replace('   ', '').replace('  ', ' ')
    
    
            # Splitting text into strings
    text_split = text.split(' ')

     # Cleaning potential links
    text_cleaned = [i.strip().strip('[').strip(']').strip(')').strip('(').strip('Source:').strip('source:').strip('see:').strip('See:').strip('vSee:').strip('Abstract').strip('Guard.iii').strip(':') for i in text_split]
           
    # Adding https if missing
    text_cleaned_plus = []
    for i in text_cleaned:
        
        if i.startswith('www.'):
             i = 'https://' + i

        text_cleaned_plus.append(i)
            
            # Extracting potential links
    links_res = [
                            i for i in text_cleaned_plus if (is_url(i) == True)
                        ]
            
            # Removing repeats
    result = list(set(links_res))
            
    return result
    
def parse_pdf_authors(input_data):
    
    """
    Parses author details from PDF reader result.
    """
    
    # Checking type of input data and defining variables for parsing
    if type(input_data) == str:
        text = input_data
        metadata = {}

    if (type(input_data) == dict) and ('first_page' in input_data.keys()):
        text = input_data['first_page']
        metadata = input_data['metadata']
    
    # Creating output variable
    result = None
    
    # Checking for author metadata with key '/Author'
    if ('/Author' in metadata.keys()):
        
        # Retrieving metadata
        auth = metadata['/Author']
        
        # Cleaning metadata
        if (
            (auth != 'OscarWilde') 
            and (len(auth) > 2)
            ):
            result = auth.replace(" and ", ', ').replace('  ', '').replace('authors', '').replace('Authors', '').replace('author', '').replace('Author', '').strip()
        
        return result
    
    # Checking for author metadata with key 'Author'
    if ('Author' in metadata.keys()):
            
            # Retrieving metadata
            auth = str(metadata['Author'])
            
            # Cleaning metadata
            if (
                (auth != 'OscarWilde') 
                and (len(auth) > 2)
                ):
                result = auth.replace(" and ", ', ').replace('  ', '').replace('authors', '').replace('Authors', '').replace('author', '').replace('Author', '').strip()
            return result
        
    # Checking for author metadata with key '/author'
    if ('/author' in metadata.keys()):
        
                # Retrieving metadata
                auth = str(metadata['/author'])
                
                # Cleaning metadata
                if (
                    (auth != 'OscarWilde') 
                    and (len(auth) > 2)
                    ):
                    result = auth.replace(" and ", ', ').replace('  ', '').replace('authors', '').replace('Authors', '').replace('author', '').replace('Author', '').strip()
                return result
            
    # Checking for author metadata with key 'author'
    if ('author' in metadata.keys()):
        
                # Retrieving metadata
                auth = str(metadata['author'])
                
                # Cleaning metadata
                if (
                    (auth != 'OscarWilde') 
                    and (len(auth) > 2)
                    ):
                    result = auth.replace(" and ", ', ').replace('  ', '').replace('authors', '').replace('Authors', '').replace('author', '').replace('Author', '').strip()
                return result
    
    # If no author metadata found, tries to extract it from text
    else:
        
        # Checking if authors are named in text
        if ('author' in text.lower()) or ('by' in text.lower()):
            
                        # Splitting text into lines
                        fp_lines = text.split('\n')
                    
                        # Cleaning text to identify author name strings
                        cleaned_lines = []
                        for i in fp_lines:
                            i_lower = i.lower()
                            if (('author ' in i_lower) or ('author:' in i_lower) or ('authors' in i_lower) or ('by:' in i_lower) or ('et al.' in i_lower) or (': ' in i_lower)):
                                cleaned_lines.append(i.split(' .')[0])
                                
                        # Cleaning text to identify author name strings, round 2
                        split_lines = []
                        for line in cleaned_lines:
                            split_line = line.replace('&', ',').replace('|', '').replace('et al', 'et al.').replace(' ,', ',').split(': ')
                            try:
                                split_lines.append(split_line[1])

                            except:
                                split_lines.append(split_line[0])
                        
                        # Creating set of cleaned line segments
                        lines_set = set(split_lines)
                        
                        # First line in result is assumed to likely contain author names
                        try:
                            
                            # Cleaning text
                            result = list(lines_set)[0].replace(" and ", ', ').replace('  ', '').replace('authors', '').replace('Authors', '').replace('author', '').replace('Author', '').strip()

                        except:
                            None
        
            
        
        return result    

def parse_pdf_date(pdf_dict):
    
    """
    Parses date from PDF reader result.
    """
    
    # Checking type of input
    if (type(pdf_dict) != dict) or ('metadata' not in pdf_dict.keys()):
        return TypeError('Input must be a dictionary of data outputted by parsing PDF')
    
    # Retrieving metadata
    metadata = pdf_dict['metadata']
    
    # Checking for date metadata with key '/CreationDate'
    if ('/CreationDate' in metadata.keys()):
        date = metadata['/CreationDate']
    
    # Checking for date metadata with key '/Date'
    if ('/Date' in metadata.keys()):
            date = metadata['/Date']
            
    # Checking for date metadata with key 'CreationDate'
    if ('CreationDate' in metadata.keys()):
        date = metadata['CreationDate']
    
    # Checking for date metadata with key 'creationdate'
    if ('creationdate' in metadata.keys()):
        date = metadata['CreationDate']
    
    # Checking for date metadata with key 'Date'
    if ('Date' in metadata.keys()):
            date = metadata['Date']
            
    # Checking for date metadata with key 'date'
    if ('date' in metadata.keys()):
            date = metadata['Date']
    
    # Checking if the result is already a datetime object; if yes, returning it
    if is_datetime(date) != True:
        return date
    
    # Else, converting to datetime object
    else:
        
        # Cleaning string
        date = date.strip().replace('D:', '').replace('.', '').replace("'", '.').strip("'").strip('.').strip()

        try:
            date = str_to_datetime(date)
        except:
            pass

        return date

def parse_pdf_title(input_data):
    
    """
    Parses title from PDF reader result.
    """
    
    # Checking type of input data and defining variables for parsing
    if type(input_data) == str:
        text = input_data
        metadata = {}

    if (type(input_data) == dict) and ('first_page' in input_data.keys()):
        text = input_data['first_page']
        metadata = input_data['metadata']
    
    # Checking for title metadata with key '/Title'
    if ('/Title' in metadata.keys()):
        
        # Retrieving metadata
        title = metadata['/Title']
        
        # Cleaning metadata
        if (
            (title != '') 
            and (len(title) > 2)
            ):
            return title.replace('  ', ' ').replace('titles', '').replace('Titles', '').replace('title', '').replace('Title', '').strip()
    
    # Checking for title metadata with key 'Title'
    if ('Title' in metadata.keys()):
        
            # Retrieving metadata
            title =str(metadata['Title'])
            
            # Cleaning metadata
            if (
                (title != '') 
                and (len(title) > 2)
                ):
                return title.replace('  ', ' ').replace('titles', '').replace('Titles', '').replace('title', '').replace('Title', '').strip()
    
    # Checking for title metadata with key 'title'
    if ('title' in metadata.keys()):
        
            # Retrieving metadata
            title = str(metadata['title'])
            
            # Cleaning metadata
            if (
                (title != '') 
                and (len(title) > 2)
                ):
                return title.replace('  ', ' ').replace('titles', '').replace('Titles', '').replace('title', '').replace('Title', '').strip()
    
    # If no title metadata found, looking for title string in text
    else:
                
        output = None
        
        # Checking if 'title' is in text
        if ('title' in text.lower()) or ('name' in text.lower()):
                        
                        # Splitting text into lines
                        fp_lines = text.split('\n')
                        
                        # Removing lines which don't include 'name' or 'title'
                        cleaned_lines = []
                        for i in fp_lines:
                            if ('Title' in i) or ('title ' in i) or ('title:' in i) or ('titles' in i) or ('Titles' in i) or ('name' in i) or ('Name' in i):
                                cleaned_lines.append(i.split(' .')[0])
                        
                        # Splitting lines into phrases
                        split_lines = []
                        for line in cleaned_lines:
            
                            split_line = line.replace('|', '').replace('et al', 'et al.').split('.')
                            try:
                                split_lines.append(split_line[1])

                            except:
                                split_lines.append(split_line[0])
                        
                        # Removing duplicates
                        lines_set = set(split_lines)
                        
                        # Returning first line found, assuming this is more likely to be the title
                        try:
                            
                            # Cleaning string
                            output = list(lines_set)[0].replace('  ', ' ').replace('titles', '').replace('Titles', '').replace('title', '').replace('Title', '').strip()
                            
                        except:
                            output = None
        
        # If the result so far is None or very short, tries to join result with second and third lines
        if (output == None) or (len(output) <5):
            output = ' - '.join(parse_pdf_text(input_data)['top'][0:2])
            if 'authors' in output.lower():
                output = None
            
        return output
                
def parse_pdf_reader_dict(pdf_dict):
    
    """
    Parses dictionaries outputted by the PDF reader. Returns a dictionary containing parsed and formatted data.
    """

    try:
        pdf_dict['title'] = parse_pdf_title(pdf_dict)
    except:
        pdf_dict['title'] = None
    
    try:
        pdf_dict['authors'] = parse_pdf_authors(pdf_dict)
    except:
        pdf_dict['authors'] = None
    
    try:
        pdf_dict['date'] = parse_pdf_date(pdf_dict)
    except:
        pdf_dict['date'] = None
    
    try:
        pdf_dict['doi'] = parse_pdf_doi(pdf_dict)
    except:
        pdf_dict['doi'] = None
    
    try:
        pdf_dict['links'] = parse_pdf_links(pdf_dict)
    except:
        pdf_dict['links'] = None
    
    try:
        pdf_dict['text'] = parse_pdf_text(pdf_dict)
    except:
        pdf_dict['text'] = None
    
    return pdf_dict

def read_pdf(file_path: str = None) -> dict:
    
    """
    Reads and parses PDF file. Returns a dictionary of parsed and formatted data.
    """
    
    # Retrieving PDF date
    pdf_dict = pdf_to_dict(file_path = file_path)
    
    # Parsing PDF data
    output = parse_pdf_reader_dict(pdf_dict)
    output['link'] = None
    
    return output
    
def read_pdf_url(url = None):
    
    """
    Downloads and parses PDF file from a URL. Returns a dictionary of data.
    """
    
    # Retrieving PDF data
    pdf_dict = pdf_url_to_dict(url = url)
    
    # Parsing PDF data
    output = parse_pdf_reader_dict(pdf_dict)
    output['link'] = url
    
    return output

def select_pdf_reader(file_path):

    """
    Detects whether a file path directs to a valid internal address or URL, and reads the PDF using the appropriate reader function.
    """

    path = Path(file_path)

    if path.exists() == True:
         return read_pdf(file_path)
    
    else:
         return read_pdf_url(file_path)

def gen_pdf_id(pdf_dict):
     
    """
    Generates a unique identifier from a PDF reader result.
    """
    
    if (pdf_dict['title'] != None) and (pdf_dict['title'] != ''):
          pdf_id = pdf_dict['title'].replace(' ', '').replace('/', '').replace('.', '').replace('?', '').replace('-', '').replace('_', '').replace('!', '').lower().strip()
          rand_int = (random.randint(0,9)*len(pdf_id))+(random.randint(0,9)+len(pdf_id))
          pdf_id = pdf_id[:10] + str(rand_int)[:4]
    else:
        if (pdf_dict['link'] != None) and (pdf_dict['link'] != ''):
                pdf_id = pdf_dict['link'].replace('https', '').replace('http', '').replace('www.', '').replace(' ', '').replace('/', '').replace('.', '').replace('?', '').replace('-', '').replace('_', '').replace('!', '').lower().strip()
                rand_int = (random.randint(0,9)*len(pdf_id))+(random.randint(0,9)+len(pdf_id))
                pdf_id = pdf_id[:10] + str(rand_int)[:4]
        else:
             pdf_id = str(random.randint(0,1000))
    
    if (pdf_dict['authors'] != None) and (pdf_dict['authors'] != '') and (pdf_dict['authors'] != []) and (pdf_dict['authors'] != '[]'):
                  auths = str(pdf_dict['authors']).replace(' ', '').replace('/', '').replace('.', '').replace('?', '').replace('-', '').replace('_', '').replace('!', '').lower().strip()
                  auths_code = auths[:10]
                  pdf_id = auths_code + pdf_id
    
    return pdf_id

def read_pdf_to_table(file_path = None):
    
    """
    Reads and parses PDF file. Returns a Pandas DataFrame of parsed and formatted data.
    """

    if file_path == None:
        file_path = input('File path: ')
    
    parsed_dict = select_pdf_reader(file_path)
    
    pdf_id = gen_pdf_id(parsed_dict)

    global results_cols
    df = pd.DataFrame(dtype=object, columns = results_cols)
    
    df.loc[0, 'id'] = pdf_id
    df.loc[0, 'title'] = parsed_dict['title']
    df.at[0, 'authors'] = parsed_dict['authors']
    df.loc[0, 'date'] = parsed_dict['date']
    df.at[0, 'citations'] = parsed_dict['text']['references']
    df.at[0, 'citations_data'] = parsed_dict['links']
    df.loc[0, 'full_text'] = parsed_dict['full_text']
    df.loc[0, 'doi'] = parsed_dict['doi']
    df.loc[0, 'link'] = parsed_dict['link']
    
    df = df.astype(object).replace(np.nan, None)
    
    return df