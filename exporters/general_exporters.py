from ..utils.globaltools import get_var_name_str

import os
import copy
import json
import pickle
from random import Random

import pandas as pd
from igraph import Graph
from docx import Document

def export_obj(obj, file_name: str = 'obj_name', folder_address: str = 'request_input', export_str_as: str = 'txt', export_dict_as: str = 'json', export_pandas_as: str = 'csv', export_network_as: str = 'graphML'):
    
    """
    Exports objects to external files based on their type.
    """
    
    # Checking object type
    obj_type = type(obj)
    
    # If no file name given, defaults to using the object's variable string name
    if file_name == 'obj_name':
        file_name = get_var_name_str(obj)
    
    # Getting file name from user input
    if file_name == None:
        file_name = input('File name: ')
    
    # If file name is still None or is an empty string, assigns a random integer as file name
    if (file_name == '') or (file_name == None):
        file_name = Random().randint(0, 10**10)
        file_name = str(file_name)
    
    # Getting folder address from user input
    if folder_address == 'request_input':
        folder_address = input('Folder address: ')
    
    # Creating file address
    file_address = folder_address + '/' + file_name
    
    # Converting list, set, tuple, and numeric types to strings
    if (obj_type == list) or (obj_type == set) or (obj_type == tuple) or (obj_type == int) or (obj_type == float):
        obj = str(obj)
        obj_type = type(obj)
    
    # If object is a string, exporting object as a text file
    if obj_type == str:
        
        # Default: exports file as .txt format
        if export_str_as == 'txt':
        
            file_address = file_address + '.txt'
            with open(file_address, 'w', encoding='utf-8') as file:
                file.write(obj)
                file.close()

            return
        
        # Exporting file based on user input
        if (export_str_as == 'word') or (export_str_as == 'docx') or (export_str_as == '.docx'):
            
            file_address = file_address + '.docx'
            document = Document()
            document.add_paragraph(obj)
            document.save(file_address)
            
            return
    
    # If object is a dictionary, exporting object as a JSON file
    if obj_type == dict:
        
        file_address = file_address + '.json'
        obj_copy = copy.deepcopy(obj)
        
        for key in obj.keys():
            value = obj_copy[key]
            if (type(value) == dict) or (type(value) == pd.DataFrame) or (type(value) == pd.Series):
                str_version = str(value)
                del obj_copy[key]
                obj_copy[key] = str_version
        
        with open(file_address, "w") as file:
            json.dump(obj_copy, file)
            
        return
    
    # If object is a dateframe, exporting object as spreadsheet
    if (obj_type == pd.DataFrame) or (obj_type == pd.Series):
        
        # If export format selected is CSV, exporting .csv
        if (export_pandas_as == 'csv') or (export_pandas_as == 'CSV'):
            file_address = file_address + '.csv'
            return obj.to_csv(file_address)
        
        # If export format selected is Excel, exporting .xlsx
        if (export_pandas_as == 'excel') or (export_pandas_as == 'EXCEL') or (export_pandas_as == 'xlsx')or (export_pandas_as == '.xlsx'):
            file_address = file_address + '.xlsx'
            return obj.to_excel(file_address)
    
    # If object is a network, exporting object as a graph object
    if (obj_type == Graph) or ('CaseNetwork' in obj_typle):
        return export_network(obj, file_name = file_name, folder_address = folder_address, file_type = export_network_as)
    
    
    # For all other data types, exporting object as a .txt file based on its string representation
    else:
        obj = str(obj)
        file_address = file_address + '.txt'
        with open(file_address, 'w', encoding='utf-8') as file:
            file.write(obj)
            file.close()
        
        return

def obj_to_folder(obj, folder_name: str = 'obj_name', folder_address: str = 'request_input', export_str_as: str = 'txt', export_dict_as: str = 'json', export_pandas_as: str = 'csv', export_network_as: str = 'graphML'):
    
    """
    Exports objects as external folders.
    """
    
    # If the object is None, no folder created
    if obj is None:
        return
    
    # If no folder name given, defaults to using the object's variable string name
    if folder_name == 'obj_name':
        folder_name = get_var_name_str(obj)
    
    # Getting folder name from user input
    if folder_name == None:
        folder_name = input('Folder name: ')
        

    # If folder name is still None or is an empty string, assigns a random integer as a folder name
    if (folder_name == '') or (folder_name == None):
        folder_name = Random().randint(0, 10**10)
        folder_name = str(folder_name)
    
     # Getting folder address from user input
    if folder_address == 'request_input':
        folder_address = input('Folder address: ')
    
    # Creating folder address
    folder_name = folder_name.strip().replace('/', '_').replace(' ', '_')
    final_address = folder_address + '/' + folder_name
    final_address = final_address.strip().replace(' ', '_')
    
    # Creating folder
    os.mkdir(final_address)
    
    # If the item is a string, numeric, Pandas series or pandas.DataFrame, creates a file
    if (type(obj) == str) or (type(obj) == int) or (type(obj) == float) or (type(obj) == pd.Series) or (type(obj) == pd.DataFrame):
        folder_name = folder_name.strip().replace(' ', '_').replace('/', '_')
        export_obj(obj, file_name = folder_name, folder_address = final_address, export_str_as = export_str_as, export_dict_as = export_dict_as, export_pandas_as = export_pandas_as, export_network_as = export_network_as)
        return
    
    # If the object is iterable, creates a folder using recursion
    if (type(obj) == list) or (type(obj) == set) or (type(obj) == tuple):
        index = 0
        for i in obj:
            item_folder_name = folder_name + '_' + str(index)
            item_folder_name = item_folder_name.strip().replace(' ', '_').replace('/', '_')
            obj_to_folder(obj = i, folder_name = item_folder_name, folder_address = final_address)
            index += 1
        
        return
    
    # If the object is a dictionary, creates a folder with keys as filenames and values as files
    if type(obj) == dict:
        
        for key in obj.keys():
            
            key_folder_name = folder_name + '_' + key
            obj_to_folder(obj = obj[key], folder_name = key_folder_name, folder_address = final_address)
            
        return
    
    # If the object is a type from the case manager package, creates a folder using the .export_folder() method. This applies recursion.
    try:
        for key in obj.__dict__.keys():

            attr = obj.__dict__[key]

            if attr is not None:
                
                str_type = str(type(attr))
                
                if ('Properties' not in str_type
                    and (
                        ('.Project' in str_type)
                        or ('.Case' in str_type)
                        or ('.CaseData' in str_type)
                        or ('.CaseItem' in str_type)
                        or ('.CaseEntity' in str_type)
                        or ('.CaseEvent' in str_type)
                        or ('.CaseKeywords' in str_type)
                        or ('.CaseNetworkSet' in str_type)
                        or ('.CaseIndexes' in str_type)
                        or ('.CaseAnalytics' in str_type))
                   ):
                    attr.export_folder(folder_name = key, folder_address = final_address, export_str_as = export_str_as, export_dict_as = export_dict_as, export_pandas_as = export_pandas_as, export_network_as = export_network_as)

                else:

                    if (key == 'coinciding_data') and (type(attr) == dict):
                        obj_to_folder(attr, folder_name = key, folder_address = final_address, export_str_as = 'txt', export_dict_as = 'json', export_pandas_as = 'csv', export_network_as = 'graphML')


                    else:
                        export_obj(obj = obj.__dict__[key], file_name = key, folder_address = final_address)

        return
    
    # Error handling
    except Exception as e:
        raise e