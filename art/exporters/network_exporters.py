"""Functions for exporting network objects to files"""

from typing import List, Dict, Tuple
import json
import igraph as ig # type: ignore
from igraph import Graph # type: ignore
from networkx.classes import Graph as NetworkX_Undir, DiGraph as NetworkX_Dir, MultiGraph as NetworkX_Multi # type: ignore

def export_network_to_kumu(network: Graph, folder_address: str = 'request_input', file_name: str = 'request_input'):
        
        """
        Exports network object (e.g. igraph.Graph, CaseNetwork, NetworkX) as a JSON file to be uploaded to Kumu.
        """
        
        # Requesting folder address from user input
        if folder_address == 'request_input':
            folder_address = input('Save to: ')

        # Requesting file name from user input
        if file_name == 'request_input':
            file_name = input('Save as: ')
        
        # Creating file address
        file_address = folder_address + '/' + file_name + '.json'
        
        # Initialising network blueprint variables
        kumu_blueprint = {}
        elements_list = []
        edge_list = []
        
        # Converting NetworkX objects to igraph objects
        if (
            (type(network) == NetworkX_Undir)
            or (type(network) == NetworkX_Dir)
            or (type(network) == NetworkX_Multi)
        ):
            network = Graph.from_networkx(network)
        
        # Setting 'type' attribute and handling potential errors (e.g. if 'type' has another name)
        if 'type' not in network.vs.attributes():
            network.vs['type'] = network.vs['type_1']
    
            if 'type_1' not in network.vs.attributes():
                network.vs['type'] = None
        
        # Setting network directed attribute
        if network.is_directed() == True:
            direction = "directed"

        else:
            direction = "mutual"
        
        # Creating list of vertices
        for vertex in network.vs():

            element_dict = {}
            element_dict["label"] = vertex["name"]
            element_dict["type"] = vertex["type"]

            elements_list.append(element_dict)

        # Creating edgelist
        for edge in network.es():
            edge_dict = {}
            edge_dict["from"] = network.vs[edge.source]["name"]
            edge_dict["to"] = network.vs[edge.target]["name"]
            edge_dict["direction"] = direction
    #         edge_dict["weight"] = edge["weight"]

            edge_list.append(edge_dict)

        # Creating dictionary
        kumu_blueprint["elements"] = elements_list
        kumu_blueprint["connections"] = edge_list

        
        # Exporting dictionary as JSON
        with open(file_address, "w") as outfile:
            json.dump(kumu_blueprint, outfile)

    

def export_network(network: Graph, file_name: str = 'request_input', folder_address: str = 'request_input', file_type: str = 'request_input'):
        
        """
        Exports network object (e.g. igraph.Graph, CaseNetwork, NetworkX) to one of a variety of graph file types. Defaults to .graphML.
        """
        
        # Requesting folder address from user input
        if folder_address == 'request_input':
            folder_address = input('Folder address: ')
        
        # Requesting file name from user input
        if file_name == 'request_input':
            file_name = input('Save as: ')
        
        # Requesting file type from user input
        if file_type == 'request_input':
            file_type = input('File type ("graphML", "kumu", GML", "LEDA", "lgl", "ncol", or "pajek"): ')
        
        # Converting NetworkX objects to igraph objects
        if (
            (type(network) == NetworkX_Undir)
            or (type(network) == NetworkX_Dir)
            or (type(network) == NetworkX_Multi)
        ):
            network = Graph.from_networkx(network)
        
        file_type = file_type.strip('.')
    
        # Writing GraphML file by default or if selected
        if (file_type == None) or (file_type == '') or (file_type.lower() == 'graphml'):

                file_address = folder_address + '/' + file_name + '.graphml'
                network.write_graphml(file_address)
        
        # Writing Kumu formatted JSON file if selected
        if (file_type.lower() == 'kumu') or (file_type.lower() == 'kumu blueprint'):
            
            export_network_to_kumu(network = network, folder_address = folder_address, file_name = file_name)
        
        # Writing GML file if selected
        if file_type.lower() == 'gml':

                file_address = folder_address + '/' + file_name + '.gml'
                network.write_gml(file_address)
        
        # Writing LEDA file if selected
        if file_type.lower() == 'leda':

                file_address = folder_address + '/' + file_name + '.lgr'
                network.write_leda(file_address)
        
        # Writing LGL file if selected
        if file_type.lower() == 'lgl':

                file_address = folder_address + '/' + file_name + '.lgl'
                network.write_lgl(file_address)
        
        # Writing NCOL file if selected
        if file_type.lower() == 'ncol':

                file_address = folder_address + '/' + file_name + '.ncol'
                network.write_ncol(file_address)
        
        # Writing Pajek file if selected
        if file_type.lower() == 'pajek':

                file_address = folder_address + '/' + file_name + '.net'
                network.write_pajek(file_address)

                