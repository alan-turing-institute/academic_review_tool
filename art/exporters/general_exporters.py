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
    Exports objects to external files based on their type. Detects the object's type and selects a corresponding file type.

    Parameters
    ----------
    obj : object
        the object to export.
    file_name : str
        name for export file. Defaults to requesting from user input.
    folder_address : str
        directory path for folder to export to. Defaults to requesting from user input.
    export_str_as : str
        file type for saving string objects. Defaults to 'txt', i.e. a .txt file.
    export_dict_as : str
        file type for saving dictionary objects. Defaults to 'json', i.e. a JSON file.
    export_pandas_as : str
        file type for saving Pandas objects (e.g. Series and DataFrames). Defaults to 'csv', i.e. a CSV file.
    export_network_as : str
        file type for saving network and graph objects (e.g. Network, iGraph Graph, NetworkX). Defaults to 'graphML'.
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

def art_class_to_folder(obj, folder_name = 'request_input', folder_address: str = 'request_input', export_str_as: str = 'txt', export_dict_as: str = 'json', export_pandas_as: str = 'csv', export_network_as: str = 'graphML'):

    """
    Specialised function to export ART classes to external folders. Detects the object's type and selects a corresponding file type.

    Parameters
    ----------
    obj : object
        the object to export.
    folder_name : str
        name for export folder. Defaults to requesting from user input.
    folder_address : str
        directory path for folder to export to. Defaults to requesting from user input.
    export_str_as : str
        file type for saving string objects. Defaults to 'txt', i.e. a .txt file.
    export_dict_as : str
        file type for saving dictionary objects. Defaults to 'json', i.e. a JSON file.
    export_pandas_as : str
        file type for saving Pandas objects (e.g. Series and DataFrames). Defaults to 'csv', i.e. a CSV file.
    export_network_as : str
        file type for saving network and graph objects (e.g. Network, iGraph Graph, NetworkX). Defaults to 'graphML'.
    """

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
    folder_name = folder_name.strip().replace('/', '_').replace(' ', '_').replace('A:','').replace('F:','').replace('AUTH:','').strip()
    obj_address = str(folder_address) + '/' + str(folder_name)
    obj_address = obj_address.strip()

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
                        and ('.Affiliations' not in obj_type_str)
                        and ('.Networks') not in obj_type_str):
        return

    if (('.Results' in obj_type_str)
            or ('.ActivityLog' in obj_type_str)):
        obj_address = obj_address + '.csv'
        obj.to_csv(obj_address)
        return

    if (('.Review' in obj_type_str)
                        or ('.Entity' in obj_type_str)
                        or ('.Entities' in obj_type_str)
                        or ('.Author' in obj_type_str)
                        or ('.Authors' in obj_type_str)
                        or ('.Funder' in obj_type_str)
                        or ('.Funders' in obj_type_str)
                        or ('.Affiliation' in obj_type_str)
                        or ('.Affiliations' in obj_type_str)
                        or ('.Networks' in obj_type_str)):

        os.mkdir(obj_address)

        for key in obj.__dict__.keys():
            
            attr = obj.__dict__[key]
            attr_name = str(key).replace('A:','').replace('F:','').replace('AUTH:','')

            if (attr is not None) and (attr_name != 'data'):
                
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
                        or ('.Affiliations' in attr_str_type)
                        or ('.Networks' in attr_str_type)):
                    
                    art_class_to_folder(obj=attr,
                                        folder_name = attr_name, 
                                        folder_address= obj_address, 
                                        export_str_as = export_str_as, 
                                        export_dict_as = export_dict_as,
                                        export_pandas_as = export_pandas_as,
                                        export_network_as=export_network_as)
                    
                    continue

                else:

                    if (type(obj) == list) and (len(obj)>0) and ((type(obj[0]) == dict) or (type(obj[0]) == str) or (type(obj[0]) == dict) or (type(obj[0]) == set)or (type(obj[0]) == tuple)):
                        export_obj(obj, file_name = attr_name, folder_address = obj_address, export_str_as = export_str_as, export_dict_as = export_dict_as, export_pandas_as = export_pandas_as, export_network_as = export_network_as)
                        continue
                    
                    if (type(obj) == dict) and (len(obj.values())>0) and ((type(list(obj.values())[0]) == dict) or (type(list(obj.values())[0]) == list) or (type(list(obj.values())[0]) == str) or (type(list(obj.values())[0]) == set) or (type(list(obj.values())[0]) == tuple)):
                        export_obj(obj, file_name = attr_name, folder_address = obj_address, export_str_as = export_str_as, export_dict_as = export_dict_as, export_pandas_as = export_pandas_as, export_network_as = export_network_as)
                        continue
                    
                    else:
                    
                        obj_to_folder(obj=attr,
                                            folder_name = attr_name, 
                                            folder_address= obj_address, 
                                            export_str_as = export_str_as, 
                                            export_dict_as = export_dict_as,
                                            export_pandas_as = export_pandas_as,
                                            export_network_as=export_network_as)
                        
                        continue
            
    return

def obj_to_folder(obj, folder_name = 'request_input', folder_address: str = 'request_input', export_str_as: str = 'txt', export_dict_as: str = 'json', export_pandas_as: str = 'csv', export_network_as: str = 'graphML'):
    
    """
    Exports objects as external folders.

    Parameters
    ----------
    obj : object
        the object to export.
    folder_name : str
        name for export folder. Defaults to requesting from user input.
    folder_address : str
        directory path for folder to export to. Defaults to requesting from user input.
    export_str_as : str
        file type for saving string objects. Defaults to 'txt', i.e. a .txt file.
    export_dict_as : str
        file type for saving dictionary objects. Defaults to 'json', i.e. a JSON file.
    export_pandas_as : str
        file type for saving Pandas objects (e.g. Series and DataFrames). Defaults to 'csv', i.e. a CSV file.
    export_network_as : str
        file type for saving network and graph objects (e.g. Network, iGraph Graph, NetworkX). Defaults to 'graphML'.
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
    obj_address = str(folder_address) + '/' + str(folder_name)
    obj_address = obj_address.strip()
    
    
    # If the item is a custom ART class, recursively creates a folder using the art_class_to_folder() function.
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
                        or ('.Affiliations' in obj_type_str)
                        or ('.Networks' in obj_type_str)):
        try:
            art_class_to_folder(obj = obj, folder_name = folder_name, folder_address = folder_address, export_str_as = export_str_as, export_dict_as = export_dict_as, export_pandas_as = export_pandas_as, export_network_as = export_network_as)
            return
        
        # Error handling
        except Exception as e:
            raise e

    # If the item is a string, numeric, Graph, Network, Pandas series or pandas.DataFrame, creates a file rather than a folder
    if (obj_type == str) or (obj_type == int) or (obj_type == float) or (obj_type == pd.Series) or (obj_type == pd.DataFrame) or (obj_type == Graph) or (obj_type == NetworkX_Undir) or (obj_type==NetworkX_Dir) or (obj_type==NetworkX_Multi) or ('.Network' in obj_type_str):
        folder_name = folder_name.strip().replace(' ', '_').replace('/', '_')
        export_obj(obj, file_name = folder_name, folder_address = folder_address, export_str_as = export_str_as, export_dict_as = export_dict_as, export_pandas_as = export_pandas_as, export_network_as = export_network_as)
        return
    
    

    # If the object is iterable, creates a folder using recursion
    if (obj_type == list) or (obj_type == set) or (obj_type == tuple):

        if len(obj) >0:

            # Creating folder
            os.mkdir(obj_address)

            index = 0
            for i in obj:
                item_folder_name = str(folder_name) + '_' + str(index)
                item_folder_name = item_folder_name.strip().replace(' ', '_').replace('/', '_')
                obj_to_folder(obj = i, folder_name = item_folder_name, folder_address = obj_address)
                index += 1
        
        return
    
    

    # If the object is a dictionary, uses recursion to create a folder with keys as filenames and values as files
    if obj_type == dict:

        if len(obj.keys()) > 0:
        
            # Creating folder
            os.mkdir(obj_address)

            for key in obj.keys():
                
                key_folder_name = str(folder_name) + '_' + str(key)
                obj_to_folder(obj = obj[key], folder_name = key_folder_name, folder_address = obj_address)
            
        return