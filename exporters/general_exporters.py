from .network_exporters import export_network

import os
import copy
import json
import pickle
from random import Random

import pandas as pd
import numpy as np
from igraph import Graph # type: ignore
from networkx.classes import Graph as NetworkX_Undir, DiGraph as NetworkX_Dir, MultiGraph as NetworkX_Multi # type: ignore
from docx import Document # type: ignore

def export_obj(obj, file_name = 'obj_name', folder_address: str = 'request_input', export_str_as: str = 'txt', export_dict_as: str = 'json', export_pandas_as: str = 'csv', export_network_as: str = 'graphML'):
    
    """
    Exports objects to external files based on their type.
    """
    
    # Checking object type
    obj_type = type(obj)
    
    # Getting file name from user input
    if (file_name is None) or (file_name is np.nan):
        file_name = input('File name: ')
    
    # If file name is still None or is an empty string, assigns a series of random integers as file name
    if (file_name == '') or (file_name is None) or (file_name is np.nan):
        file_name = str(Random().randint(1000, 9999)) + '_' + str(Random().randint(1000, 9999)) + '_' + str(Random().randint(1000, 9999))
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
        if export_str_as.lower().strip() == 'txt':
        
            file_address = file_address + '.txt'
            with open(file_address, 'w', encoding='utf-8') as file:
                file.write(obj)
                file.close()

            return
        
        # Exporting file based on user input
        if (export_str_as.lower().strip() == 'word') or (export_str_as.lower().strip() == 'docx') or (export_str_as.lower().strip() == '.docx'):
            
            file_address = file_address + '.docx'
            document = Document()
            document.add_paragraph(obj)
            document.save(file_address)
            
            return
    
    # If object is a dictionary, exporting object as a JSON file
    if obj_type == dict:
        
        file_address = file_address + '.json'
        obj_copy = copy.deepcopy(obj)
        
        for key in obj.keys(): # type: ignore
            value = obj_copy[key]
            if (type(value) == dict) or (type(value) == pd.DataFrame) or (type(value) == pd.Series):
                str_version = str(value)
                del obj_copy[key] # type: ignore
                obj_copy[key] = str_version # type: ignore
        
        with open(file_address, "w") as file:
            json.dump(obj_copy, file)
            
        return
    
    # If object is a dateframe, exporting object as spreadsheet
    if (obj_type == pd.DataFrame) or (obj_type == pd.Series):
        
        # If export format selected is CSV, exporting .csv
        if (export_pandas_as.lower().strip() == 'csv') or (export_pandas_as.lower().strip() == '.csv'):
            file_address = file_address + '.csv'
            return obj.to_csv(file_address) # type: ignore
        
        # If export format selected is Excel, exporting .xlsx
        if (export_pandas_as.lower().strip() == 'excel') or (export_pandas_as.lower().strip() == 'xlsx')or (export_pandas_as.lower().strip() == '.xlsx'):
            file_address = file_address + '.xlsx'
            return obj.to_excel(file_address) # type: ignore
    
    # If object is a network, exporting object as a graph object
    if (obj_type == Graph) or (obj_type == NetworkX_Undir) or (obj_type==NetworkX_Dir) or (obj_type==NetworkX_Multi) or ('Network' in str(obj_type)):
        return export_network(obj, file_name = file_name, folder_address = folder_address, file_type = export_network_as)
    
    
    # For all other data types, exporting object as a .txt file based on its string representation
    else:
        obj = str(obj)
        file_address = file_address + '.txt'
        with open(file_address, 'w', encoding='utf-8') as file:
            file.write(obj)
            file.close()
        
        return

def art_class_to_folder(obj, final_address, export_str_as, export_dict_as, export_pandas_as, export_network_as):

    obj_type_str = str(type(obj))

    if (('.Review' not in obj_type_str)
                        and ('.Results' not in obj_type_str)
                        and ('.ActivityLog' not in obj_type_str)
                        and ('.Entity' not in obj_type_str)
                        and ('.Entities' not in obj_type_str)
                        and ('.Author' not in obj_type_str)
                        and ('.Authors' not in obj_type_str)
                        and ('.Funder' not in obj_type_str)
                        and ('.Funders' not in obj_type_str)
                        and ('.Affiliation' not in obj_type_str)
                        and ('.Affiliations' not in obj_type_str)):
        return

    if (('.Results' in obj_type_str)
            or ('.ActivityLog' in obj_type_str)):
        obj.to_csv(final_address)
        return

    if (('.Review' in obj_type_str)
                        or ('.Entity' in obj_type_str)
                        or ('.Entities' in obj_type_str)
                        or ('.Author' in obj_type_str)
                        or ('.Authors' in obj_type_str)
                        or ('.Funder' in obj_type_str)
                        or ('.Funders' in obj_type_str)
                        or ('.Affiliation' in obj_type_str)
                        or ('.Affiliations' in obj_type_str)):

        os.mkdir(final_address)

        for key in obj.__dict__.keys():

            attr = obj.__dict__[key]

            if attr is not None:
                
                attr_str_type = str(type(attr))
                
                if (('.Review' in attr_str_type)
                        or ('.Results' in attr_str_type)
                        or ('.ActivityLog' in attr_str_type)
                        or ('.Entity' in attr_str_type)
                        or ('.Entities' in attr_str_type)
                        or ('.Author' in attr_str_type)
                        or ('.Authors' in attr_str_type)
                        or ('.Funder' in attr_str_type)
                        or ('.Funders' in attr_str_type)
                        or ('.Affiliation' in attr_str_type)
                        or ('.Affiliations' in attr_str_type)):
                    
                    attr.export_folder(folder_name = key, 
                                        folder_address= final_address, 
                                        export_str_as = export_str_as, 
                                        export_dict_as = export_dict_as,
                                        export_pandas_as = export_pandas_as,
                                        export_network_as=export_network_as)
                    
                    continue

                else:

                    if type(attr) == dict:
                        obj_to_folder(attr, 
                                        folder_address= final_address, 
                                        export_str_as = export_str_as, 
                                        export_dict_as = export_dict_as,
                                        export_pandas_as = export_pandas_as,
                                        export_network_as=export_network_as)
                        continue

                    else:
                        export_obj(attr, file_name = key, folder_address = final_address)
                        continue
            
    return

def obj_to_folder(obj, folder_name = 'request_input', folder_address: str = 'request_input', export_str_as: str = 'txt', export_dict_as: str = 'json', export_pandas_as: str = 'csv', export_network_as: str = 'graphML'):
    
    """
    Exports objects as external folders.
    """
    
    obj_type = type(obj)
    obj_type_str = str(type(obj))

    # If the object is None, no folder created
    if obj is None:
        return
    
    # Getting folder name from user input
    if (folder_name is None) or (folder_name is np.nan) or (folder_name =='request_input'):
        folder_name = input('Folder name: ')
        

    # If folder name is still None or is an empty string, assigns a random integer as a folder name
    if (folder_name == '') or (folder_name == None):
        folder_name = str(Random().randint(1000, 9999)) + '_' + str(Random().randint(1000, 9999)) + '_' + str(Random().randint(1000, 9999))
        folder_name = str(folder_name)
    
     # Getting folder address from user input
    if folder_address == 'request_input':
        folder_address = input('Folder address: ')
    
    # Creating folder address
    folder_name = folder_name.strip().replace('/', '_').replace(' ', '_')
    final_address = folder_address + '/' + folder_name
    final_address = final_address.strip()
    
    # Creating folder
    os.mkdir(final_address)
    
    # If the item is an ART class, recursively creates a folder using the .export_folder() method.
    if (('.Review' in obj_type_str)
                        or ('.Results' in obj_type_str)
                        or ('.ActivityLog' in obj_type_str)
                        or ('.Entity' in obj_type_str)
                        or ('.Entities' in obj_type_str)
                        or ('.Author' in obj_type_str)
                        or ('.Authors' in obj_type_str)
                        or ('.Funder' in obj_type_str)
                        or ('.Funders' in obj_type_str)
                        or ('.Affiliation' in obj_type_str)
                        or ('.Affiliations' in obj_type_str)):
        try:
            art_class_to_folder(obj = obj, final_address = final_address, export_str_as = export_str_as, export_dict_as = export_dict_as, export_pandas_as = export_pandas_as, export_network_as = export_network_as)
            return
        
        # Error handling
        except Exception as e:
            raise e

    # If the item is a string, numeric, Graph, Network, Pandas series or pandas.DataFrame, creates a file
    if (obj_type == str) or (obj_type == int) or (obj_type == float) or (obj_type == pd.Series) or (obj_type == pd.DataFrame) or (obj_type == Graph) or (obj_type == NetworkX_Undir) or (obj_type==NetworkX_Dir) or (obj_type==NetworkX_Multi) or ('Network' in obj_type_str):
        folder_name = folder_name.strip().replace(' ', '_').replace('/', '_')
        export_obj(obj, file_name = folder_name, folder_address = final_address, export_str_as = export_str_as, export_dict_as = export_dict_as, export_pandas_as = export_pandas_as, export_network_as = export_network_as)
        return
    
    # If the object is iterable, creates a folder using recursion
    if (obj_type == list) or (obj_type == set) or (obj_type == tuple):
        index = 0
        for i in obj:
            item_folder_name = folder_name + '_' + str(index)
            item_folder_name = item_folder_name.strip().replace(' ', '_').replace('/', '_')
            obj_to_folder(obj = i, folder_name = item_folder_name, folder_address = final_address)
            index += 1
        
        return
    
    

    # If the object is a dictionary, uses recursion to create a folder with keys as filenames and values as files
    if obj_type == dict:
        
        for key in obj.keys():
            
            key_folder_name = folder_name + '_' + key
            obj_to_folder(obj = obj[key], folder_name = key_folder_name, folder_address = final_address)
            
        return