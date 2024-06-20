from igraph import Graph
from networkx.classes import Graph as NetworkX_Undir, DiGraph as NetworkX_Dir, MultiGraph as NetworkX_Multi
import numpy as np
import pandas as pd

def generate_urls_network(urls_dict: dict) -> Graph:
        
        """
        Returns a directed network representing if/how items embed hyperlinks to one another.
        
        Parameters
        ----------
        urls_dict : dict
            a dictionary with the following structure:
                * keys: URLs
                * values: links associated with the URLs
        """

        
        # Creating list of all URLs in source dictionary
        all_urls = []
        for i in urls_dict.keys():
            all_urls.append(i)
            all_urls = all_urls + urls_dict[i]
        
        # Removing repeats
        all_urls = set(all_urls)
        
        # Removing None and empty string URLs
        all_urls = [i for i in all_urls if ((i != None) and (i != ''))]
        
        # Initialising network
        g = Graph(n=len(all_urls), directed=True, vertex_attrs={'name': all_urls})
        
        
        # Adding edges by iterating through vertices and retrieving links associated
        for vertex in g.vs:
            
            # Getting vertex url
            url = vertex['name']
            
            # Ignoring None and empty string URL addresses
            if (url != None) and (len(url) > 0):
                
                # Retrieving vertex index
                v_index = vertex.index
                
                # Retrieving vertex links
                if url in urls_dict.keys():
                    v_links = urls_dict[url]
                else:
                    v_links = []
                
                # If the vertex has links associated, finds vertices those links direct to
                if len(v_links) > 0:
                    
                    for link in v_links:
                        end_index = g.vs.find(name = link).index
                        
                        # Adding edges between linked vertices
                        Graph.add_edges(g, 
                                        [(v_index, end_index)], 
                                        attributes={
                                           'name': f'{url} -> {link}'})
                    
        return g

def colinks_out(links_network: Graph) -> pd.DataFrame:
    
    """
    Conducts out-bound colink analysis. Returns a pandas.DataFrame.
    
    Notes
    -----
    Is able to take igraph.Graph, CaseNetwork, and NetworkX objects.
    """
    
    # Converting NetworkX objects to igraph objects
    if (
            (type(links_network) == NetworkX_Undir)
            or (type(links_network) == NetworkX_Dir)
            or (type(links_network) == NetworkX_Multi)
        ):
            links_network = Graph.from_networkx(links_network)
    
    # Retrieving URLs list
    urls = links_network.vs['name']
    
    # Initialising dictionary to store tuples of co-linked URLs
    colink_tuples_dict = {}
    
    # Iterating through vertices to identify pairs of co-linked URLs
    for v in links_network.vs:
        
        # Retrieving vertex URL
        url = v['name']
        
        # Creating list of URLS linked by selected URL 
        children = v.successors()
        names = [v['name'] for v in children]
        
        # Creating list of all combinations of linked URLs
        # Stored as a list of tuples
        tuples = list(itertools.combinations(names, 2))
        tuples = [tuple(set(i)) for i in tuples]
        
        # Iterating through linked URLs combinations and adding to dictionary of colinks
        for pair in tuples:
            
            # Adding to dictionary of colinks
            if pair not in colink_tuples_dict.keys():
                colink_tuples_dict[pair] = {'Co-link to': [], 'Frequency': 0}

            colink_tuples_dict[pair]['Co-link to'].append(url)
            
            # Adding combination to count
            colink_tuples_dict[pair]['Frequency'] += 1

    # Creating and formatting output dataframe
    df = pd.DataFrame.from_dict(colink_tuples_dict).T
    df.index.name = 'Names'
    df = df.sort_values('Frequency', ascending=False)
    
    return df

def colinks_in(links_network: Graph) -> pd.DataFrame:
    
    """
    Conducts in-bound colink analysis. Returns a pandas.DataFrame.
    
    Notes
    -----
    Is able to take igraph.Graph, CaseNetwork, and NetworkX objects.
    """
    
    # Converting NetworkX objects to igraph objects
    if (
            (type(links_network) == NetworkX_Undir)
            or (type(links_network) == NetworkX_Dir)
            or (type(links_network) == NetworkX_Multi)
        ):
            links_network = Graph.from_networkx(links_network)
    
    # Retrieving URLs list
    urls = links_network.vs['name']
    
    # Initialising dictionary to store tuples of co-linking URLs
    coupling_dict = {}
    
    # Iterating through vertices to identify pairs of co-linking URLs
    for v in links_network.vs:
        
        # Retrieving vertex URL
        url = v['name']
        
        # Creating list of URLS linking to selected URL 
        parents = v.predecessors()
        names = [v['name'] for v in parents]
        
        # Creating list of all combinations of linking URLs
        # Stored as a list of tuples
        tuples = list(itertools.combinations(names, 2))
        tuples = [tuple(set(i)) for i in tuples]
        
         # Adding to dictionary of colinking URLs
        if url not in coupling_dict.keys():
            coupling_dict[url] = {'Co-linked by': [], 'Frequency': 0}
        
        coupling_dict[url]['Co-linked by'] = list(set(coupling_dict[url]['Co-linked by'] + tuples))
        
        # Counting and storing number of times selected URL is co-linked to
        coupling_dict[url]['Frequency'] = len(coupling_dict[url]['Co-linked by'])
    
    # Creating and formatting output dataframe
    df = pd.DataFrame.from_dict(coupling_dict).T
    df.index.name = 'Name'
    df = df.sort_values('Frequency', ascending=False)
    
    return df