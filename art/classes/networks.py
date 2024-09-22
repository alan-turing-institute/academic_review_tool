
from networkx import degree
from ..networks.network_functions import colinks_in, colinks_out

from ..exporters.network_exporters import export_network, export_network_to_kumu

from .attrs import Attr, AttrSet

from typing import List, Dict, Tuple
import numpy as np
import pandas as pd
import igraph as ig # type: ignore
from igraph import Graph # type: ignore
from networkx.classes import Graph as NetworkX_Undir, DiGraph as NetworkX_Dir, MultiGraph as NetworkX_Multi # type: ignore
import matplotlib.pyplot as plt # type: ignore

class Network(Graph):
    
    """
    This is a modified igraph.Graph object. It provides additional analytics methods and functionality for review management.
    
    Parameters
    ----------
    graph : igraph.Graph or NetworkX object 
        An existing network object to be used.
    n : int
        The number of vertices. Can be omitted, the default is
        zero. Note that if the edge list contains vertices with indexes
        larger than or equal to M{m}, then the number of vertices will
        be adjusted accordingly.
    edges : edgelist
        The edge list where every list item is a pair of
        integers. If any of the integers is larger than M{n-1}, the number
        of vertices is adjusted accordingly. C{None} means no edges.
    directed : bool
        Whether the graph should be directed
    graph_attrs : dict
        The attributes of the graph as a dictionary.
    vertex_attrs : dict
        The attributes of the vertices as a dictionary.
        The keys of the dictionary must be the names of the attributes; the
        values must be iterables with exactly M{n} items where M{n} is the
        number of vertices.
    edge_attrs : dict
        The attributes of the edges as a dictionary. The
        keys of the dictionary must be the names of the attributes; the values
        must be iterables with exactly M{m} items where M{m} is the number of
        edges.
    """
        
    
    def __init__(self, graph = None, n = 0, edges = None, directed = False, graph_attrs = {}, vertex_attrs = {}, edge_attrs = {}, __ptr = None, obj_name = None, parent_obj_path = None): # type: ignore
        
        """
        Initialises a Network. Converts igraph and NetworkX objects if provided.
        
        Parameters
        ----------
        graph : igraph.Graph or NetworkX object 
            An existing network object to be used.
        n : int
            The number of vertices. Can be omitted, the default is
            zero. Note that if the edge list contains vertices with indexes
            larger than or equal to M{m}, then the number of vertices will
            be adjusted accordingly.
        edges : edgelist
            The edge list where every list item is a pair of
            integers. If any of the integers is larger than M{n-1}, the number
            of vertices is adjusted accordingly. C{None} means no edges.
        directed : bool
            Whether the graph should be directed
        graph_attrs : dict
            The attributes of the graph as a dictionary.
        vertex_attrs : dict
            The attributes of the vertices as a dictionary.
            The keys of the dictionary must be the names of the attributes; the
            values must be iterables with exactly M{n} items where M{n} is the
            number of vertices.
        edge_attrs : dict
            The attributes of the edges as a dictionary. The
            keys of the dictionary must be the names of the attributes; the values
            must be iterables with exactly M{m} items where M{m} is the number of
            edges.
        """
        
        if (
            (type(graph) == NetworkX_Undir)
            or (type(graph) == NetworkX_Dir)
            or (type(graph) == NetworkX_Multi)
        ):
             graph = Graph.from_networkx(graph)
        
        if graph != None:
            
            directed = graph.is_directed()
            n = len(graph.vs)
            vs_attrs = graph.vs.attributes()
            edges = graph.to_tuple_list()
            es_attrs = graph.es.attributes()
            g_attrs = graph.attributes()
            
        super().__init__(n = n, edges = edges, directed = directed, graph_attrs = graph_attrs, vertex_attrs = vertex_attrs, edge_attrs = edge_attrs)
        
        if graph != None:
            
            for attr in g_attrs:
                self[attr] = graph[attr]
            
            for attr in vs_attrs:
                self.vs[attr] = graph.vs[attr]
            
            for attr in es_attrs:
                self.es[attr] = graph.es[attr]

    def __repr__(self):

        """
        Defines how Network objects are represented in string form.
        """

        if self.is_directed() == True:
            dir = 'Directed'
        else:
            dir = 'Undirected'
        
        if self.is_bipartite() == True:
            bi = 'bipartite '
        else:
            bi = ''

        vertices = list(self.vs.indices)
        vs_len = len(vertices)
        
        if vs_len > 5:
            vertices = vertices[:3]
            vertices = str(vertices)
            vertices = vertices.strip('[').strip(']') + '...'
        else:
            vertices = str(vertices)
            vertices = vertices.strip('[').strip(']')
        

        edges = list(self.es.indices)
        es_len = len(edges)
       
        if es_len > 5:
            edges = edges[:3]
            edges = str(edges)
            edges = edges.strip('[').strip(']') + '...'
        else:
            edges = str(edges)
            edges = edges.strip('[').strip(']')
       

        output = f'{dir} {bi}network with {vs_len} vertices and {es_len} edges\n'
        output = f'{(len(output)-1)*"-"}\n' + output + f'{(len(output)-1)*"-"}\n\n'
        output = output + f'Vertices\n--------\n{vertices}\n\n'
        output = output + f'Edges\n-----\n{edges}\n'

        return output
    
    def is_weighted(self) -> bool:

        """
        Checks whether edges have a 'weight' attribute. Returns True if yes; False if no.
        """

        return 'weight' in self.es.attributes()

    def degrees(self):
        
        """
        Returns the network's degree distributions as a Pandas DataFrame.
        """
        
        isdir = self.is_directed()

        if isdir == False:
            degrees_dataframe = pd.DataFrame(columns = ['vertex', 'degree'])
            total_degrees = Network.degree(self, mode = 'all')
        else:

            degrees_dataframe = pd.DataFrame(columns = ['vertex', 'total_degree', 'in_degree', 'out_degree'])
            total_degrees = Network.degree(self, mode = 'all')
            in_degrees = Network.degree(self, mode = 'in')
            out_degrees = Network.degree(self, mode = 'out')

        index = 0
        for v in self.vs:
            if 'name' in v.attributes().keys():
                item = v['name']
            else:
                item = v.index
            
            degrees_dataframe.loc[index, 'vertex'] = item

            if isdir == False:
                degrees_dataframe.loc[index, 'degree'] = total_degrees[index]
                degrees_dataframe = degrees_dataframe.sort_values('degree', ascending=False)
            else:
                degrees_dataframe.loc[index, 'total_degree'] = total_degrees[index]
                degrees_dataframe.loc[index, 'in_degree'] = in_degrees[index]
                degrees_dataframe.loc[index, 'out_degree'] = out_degrees[index]
                degrees_dataframe = degrees_dataframe.sort_values('total_degree', ascending=False)
            index += 1
        
        degrees_dataframe.index.name = 'vertex_id'

        return degrees_dataframe

    
    def degrees_stats(self):

        """
        Returns frequency statistics for the network's degree distributions in a Pandas DataFrame.
        """
        
        deg_df = self.degrees()

        if deg_df is not None:
            cols = deg_df.columns.to_list()
            cols.remove('vertex')
            stats_df = pd.DataFrame(columns=cols)
            for c in cols:
                stats_df[c] = deg_df[c].describe()
            return stats_df
        else:
            return None
    
    def betweenness_dataframe(self):
        
        """
        Returns the network's betweenness centrality distribution as a Pandas DataFrame.
        """
        
        df = pd.DataFrame(columns = ['vertex', 'betweenness'])

        betweenness = Network.betweenness(self)
        index = 0
        for v in self.vs:
            if 'name' in v.attributes().keys():
                item = v['name']
            else:
                item = v.index
            df.loc[index] = [item, betweenness[index]]
            index += 1
        
        df.index.name = 'vertex_id'
        df = df.sort_values('betweenness', ascending=False)

        return df

    def betweenness_stats(self):
        
        """
        Returns frequency statistics for vertex betweenness centralities.
        """
        
        df = self.betweenness_dataframe()

        if df is not None:
                return df['betweenness'].describe()
        else:
            return None
    
    def eigencentralities_dataframe(self):
        
        """
        Returns the network's eigenvector centrality distribution as a Pandas DataFrame.
        """
        
        if self.is_directed() == True:
            return pd.DataFrame(dtype=object)
        
        else:

            df = pd.DataFrame(columns = ['vertex', 'eigencentrality'])

            eigencentrality = Network.eigenvector_centrality(self)
            index = 0
            for v in self.vs:
                if 'name' in v.attributes().keys():
                    item = v['name']
                else:
                    item = v.index
                df.loc[index] = [item, eigencentrality[index]]
                index += 1
            
            df.index.name = 'vertex_id'
            df = df.sort_values('eigencentrality', ascending=False)

            return df


    def eigencentralities_stats(self):
        
        """
        Returns frequency statistics for vertex eigenvector centralities.
        """
        
        df = self.eigencentralities_dataframe()

        if df is not None:
                return df['eigencentrality'].describe() # type: ignore
        else:
            return None
    
    def authority_scores_dataframe(self):
        
        """
        Returns the network's authority scores distribution as a Pandas DataFrame.
        """
        
        df = pd.DataFrame(columns = ['vertex', 'authority_score'])

        authority_scores = Network.authority_score(self)
        index = 0
        for v in self.vs:
            if 'name' in v.attributes().keys():
                item = v['name']
            else:
                item = v.index
            df.loc[index] = [item, authority_scores[index]]
            index += 1
        
        df.index.name = 'vertex_id'
        df = df.sort_values('authority_score', ascending=False)

        return df


    def authority_scores_stats(self):
        
        """
        Returns frequency statistics for vertex authority scores.
        """
        
        df = self.authority_scores_dataframe()

        if df is not None:
                return df['authority_score'].describe()
        else:
            return None

    def hub_scores_dataframe(self):

        """
        Returns the network's hub scores distribution as a Pandas DataFrame.
        """
        
        df = pd.DataFrame(columns = ['vertex', 'hub_score'])

        hub_scores = Network.hub_score(self)
        index = 0
        for v in self.vs:
            if 'name' in v.attributes().keys():
                item = v['name']
            else:
                item = v.index
            df.loc[index] = [item, hub_scores[index]]
            index += 1
        
        df.index.name = 'vertex_id'
        df = df.sort_values('hub_score', ascending=False)

        return df


    def hub_scores_stats(self):

        """
        Returns frequency statistics for vertex hub scores.
        """
        
        df = self.hub_scores_dataframe()

        if df is not None:
                return df['hub_score'].describe()
        else:
            return None
    
    def coreness_dataframe(self):
        
        """
        Returns the network's coreness scores distribution as a Pandas DataFrame.
        """
        
        df = pd.DataFrame(columns = ['vertex', 'coreness'])

        coreness = Network.coreness(self)
        index = 0
        for v in self.vs:
            if 'name' in v.attributes().keys():
                item = v['name']
            else:
                item = v.index
            df.loc[index] = [item, coreness[index]]
            index += 1
        
        df.index.name = 'vertex_id'
        df = df.sort_values('coreness', ascending=False)

        return df


    def coreness_stats(self):
        
        """
        Returns frequency statistics for coreness hub scores.
        """
        
        df = self.coreness_dataframe()

        if df is not None:
                return df['coreness'].describe()
        else:
            return None
    

    def community_detection(self, algorithm='fastgreedy'):
        
        """Identifies communities in the network. Gives the option of using different algorithms.
        
        Parameters
        ----------
        algorithm : str
            name of community detection algorithm. Options: 
            1. betweenness
            2. fastgreedy
            3. eigenvector
            4. spinglass
            5. walktrap
        """

        if (algorithm == None) or (algorithm == ''):
            algorithm = input('Algorithm must be specified. Options: 1. betweenness, 2. fastgreedy, 3. eigenvector, 4. spinglass, 5. walktrap.:')
        
        if algorithm == 'betweenness':
            return self.community_edge_betweenness()
        
        if algorithm == 'fastgreedy':
            return self.community_fastgreedy()
        
        if algorithm == 'eigenvector':
            return self.community_leading_eigenvector()
        
        if algorithm == 'spinglass':
            return self.community_spinglass()
        
        if algorithm == 'walktrap':
            return self.community_walktrap()


    
    def weighted_density(self):
        
        """Returns the weighted density for network."""
        
        # Checks if network is weighted

        if self.is_weighted() == False:
            return self.density()

        else:
            # Gets the weighted edge count

            weighted_edge_count = 0
            for edge in self.es:
                weighted_edge_count += edge['weight']


            # Calculating the sum of the possible maximally weighted edges.
            # Comparison network varies depending on the network inputted.

            complete_edge_count = None
                
            if ('coinciding_words' in self.contents()) and (self == self.coinciding_words):
                complete_edge_count = self.full_words_network.ecount()
            
            if ('coinciding_information' in self.contents()) and (self == self.coinciding_information):
                complete_edge_count = self.full_info_network.ecount()

            if ('items_information_shared' in self.contents()) and (self == self.items_information_shared):
                all_comparisons = self['all_information_comparisons']
                all_unions = all_comparisons['Intersect size'] + all_comparisons['Difference size']
                complete_edge_count = all_unions.sum()

            if ('items_metadata_shared' in self.contents()) and (self == self.items_metadata_shared):
                all_comparisons = self['all_metadata_comparisons']
                all_unions = all_comparisons['Intersect size'] + all_comparisons['Difference size']
                complete_edge_count = all_unions.sum()


            # For networks where weight is a number =<1, assumes that the maximum weight is 1. 
            # The maximum weighted count is therefore the number of edges.

            if (complete_edge_count == None) and (self.is_directed() == True):
                try:
                    complete_edge_count = (self.full_items_network.ecount())*2
                except:
                    return

            if (complete_edge_count == None) and (self.is_directed() == False):
                try:
                    complete_edge_count = self.full_items_network.ecount()
                except:
                    return

            # Calculates the weighted density
            if complete_edge_count > 0: # type: ignore
                weighted_density = weighted_edge_count / complete_edge_count # type: ignore
            else:
                weighted_density = np.nan

            return weighted_density

    
    def weighted_degrees(self):

        """Returns the network's weighted degree distributions as a Pandas Dataframe."""
        
        # Checks if network is directed
        isdir = self.is_directed()
        
        # Checks if network is weighted

        if 'weight' not in self.es.attributes():

            degrees_dataframe = self.degrees()

            if isdir == False:
                degrees_dataframe = degrees_dataframe.rename(columns={'degree':'weighted_degree'})
            else:
                degrees_dataframe = degrees_dataframe.rename(columns={'total_degree':'weighted_total_degree', 'in_degree': 'weighted_in_degree', 'out_degree': 'weighted_out_degree'})

            return degrees_dataframe
    
        else:
            if isdir == False:
                cols = ['vertex', 'weighted_degree']
                directions = {'all': 'weighted_degree'}
            else:
                cols = ['vertex', 'weighted_total_degree', 'weighted_in_degree', 'weighted_out_degree']
                directions = {'all': 'weighted_total_degree', 'in': 'weighted_in_degree', 'out': 'weighted_out_degree'}

            degrees_dataframe = pd.DataFrame(columns = cols)

            index = 0
            for vertex in self.vs:
                weighted_degree = 0
                if 'name' in vertex.attributes().keys():
                    degrees_dataframe.loc[index, 'vertex'] = vertex['name']
                else:
                    degrees_dataframe.loc[index, 'vertex'] = vertex.index
                for d in directions.keys():
                    colname = directions[d]
                    incident_edges = (Network.incident(self, vertex, mode=d))
                    for edge in incident_edges:
                        weight = self.es[edge]['weight']
                        weighted_degree += weight
                    degrees_dataframe.loc[index, colname] = weighted_degree          
                index += 1

            if isdir == False:
                degrees_dataframe = degrees_dataframe.sort_values('weighted_degree', ascending=False)
            else:
                degrees_dataframe = degrees_dataframe.sort_values('weighted_total_degree', ascending=False)

            degrees_dataframe.index.name = 'vertex_id'

            return degrees_dataframe

        
    def weighted_degrees_stats(self):

        """
        Returns frequency statistics for the weighted degree distributions as a Pandas DataFrame.
        """

        df = self.weighted_degrees()

        if df is not None:
            cols = df.columns.to_list()
            cols.remove('vertex')
            stats_df = pd.DataFrame(columns=cols)
            for c in cols:
                stats_df[c] = df[c].describe()
            return stats_df
        else:
            return None

        
    def degree_distributions(self, weighted = False, direction = 'all'):

        """
        Returns either the weighted or unweighted degree distributions as a Pandas Dataframe.
        """

        isdir = self.is_directed()
        directions = {'all': 'weighted_total_degree', 'in': 'weighted_in_degree', 'out': 'weighted_out_degree'}

        if weighted == True:
            degrees_frame = self.weighted_degrees()
        else:
            degrees_frame = self.degrees()

        cols = degrees_frame.columns.to_list()
        cols.remove('vertex')

        if isdir == False:
            col = cols[0]
        else:
            col = directions[direction]
        
        freq_table = degrees_frame[col].value_counts()


        dist_frame = pd.DataFrame({col:freq_table.index, 'frequency':freq_table.values}).sort_values(col, ascending=False)
        return dist_frame
            
    

    def all_centralities(self):

        """
        Calculates all centrality measures for network. Returns as a Pandas Dataframe.
        """
        
        is_directed = self.is_directed()
        
        try:
            degrees = self.degrees().set_index('vertex').sort_index()
        except:
            degrees = pd.DataFrame()
            degrees.index.name = 'vertex'
        
        try:
            weighted_degrees = self.weighted_degrees().set_index('vertex').sort_index()
        except:
            weighted_degrees = pd.DataFrame()
            weighted_degrees.index.name = 'vertex'
        
        try:
            eigencents = self.eigencentralities_dataframe().set_index('vertex').sort_index() # type: ignore
        except:
            eigencents = pd.DataFrame()
            eigencents.index.name = 'vertex'
        
        try:
            betweenness = self.betweenness_dataframe().set_index('vertex').sort_index()
        except:
            betweenness = pd.DataFrame()
            betweenness.index.name = 'vertex'
        
        try:
            auths = self.authority_scores_dataframe().set_index('vertex').sort_index()
        except:
            auths = pd.DataFrame()
            auths.index.name = 'vertex'
        
        try:
            hubs = self.hub_scores_dataframe().set_index('vertex').sort_index()
        except:
            hubs = pd.DataFrame()
            hubs.index.name = 'vertex'
        
        combined = weighted_degrees.join(
                                            degrees
                                        ).join(
                                            betweenness
                                        ).join(
                                            eigencents
                                        ).join(
                                            auths
                                        ).join(
                                            hubs)
        
        if is_directed == True:
            sort_by = ['weighted_total_degree', 'total_degree', 'betweenness']
        else:
            sort_by = ['weighted_degree', 'degree', 'betweenness', 'eigencentrality', 'authority_score','hub_score']
        
        if combined.index.dtype == 'float64':
            combined.index = combined.index.astype(int)

        return combined.sort_values(sort_by, ascending=False)

    
    def get_neighbours(self,  vertex_name = 'request_input'):
        
        """Returns vertex neighbours as a Pandas Dataframe."""
        
        if vertex_name == 'request_input':
            vertex_name = input('Vertex name or index: ')

        # Get vertex
        if 'name' in self.vs.attributes():
            vertex = self.vs.find(name = vertex_name)
        else:
            vertex = self.vs[vertex_name]

        # Get vertex neighbours in a Pandas DataFrame

        df = pd.DataFrame(columns = ['vertex_id', 'vertex_name'])

        neighbours = vertex.neighbors()
        
        index = 0
        for neighbour in neighbours:
            neighbour_id = neighbour.index
            neighbour_name = neighbour['name']
            df.loc[index] = [neighbour_id, neighbour_name]
            index += 1

        df = df.set_index('vertex_id')

        return df


    def get_degree(self, vertex_name = 'request_input', direction = 'all'):
        
        """Returns the number of other vertices a vertex is tied to. I.e., the size, or len(), of neighbours()."""
        
        
        if vertex_name == 'request_input':
            vertex_name = input('Vertex name: ')
        
        # Get vertex
        if 'name' in self.vs.attributes():
            vertex = self.vs.find(name = vertex_name)
        else:
            vertex = self.vs[vertex_name]

        degree = len(vertex.neighbors())
        degree = int(degree)
        
        return degree


    def get_weighted_degree(self, vertex_name = 'request_input', direction = 'all'):
        
        """Returns the number of other vertices a vertex is tied to. I.e., the size, or len(), of neighbours()."""
        
        if vertex_name == 'request_input':
            vertex_name = input('Vertex name: ')

        df = self.weighted_degrees(direction = direction)
        masked = df[df['vertex'] == vertex_name]
        degree = int(masked['weighted_degree']) # type: ignore

        return degree

    
    def get_betweenness(self, vertex_name = 'request_input', direction = 'all'):
        
        """
        Returns a vertex's betweenness centrality score.
        """
        
        if vertex_name == 'request_input':
            vertex_name = input('Vertex name: ')

        betweenness = self.vs.find(name = vertex_name).betweenness()
        betweenness = float(betweenness)

        return betweenness

    
    def colinks(self, direction = 'out'):
        
        """
        Runs a colink analysis on the network. Returns a Pandas DataFrame.
        
        Parameters
        ----------
        direction : str 
            The direction for colink analysis.
            -> 'in': counts instances where to vertices connect to the same vertex.
            -> 'out': counts number of times a given pair of vertices are both connected to.
        """
        
        if direction == 'out':
            res = colinks_out(links_network = self) # type: ignore
        
        if direction == 'in':
            res = colinks_in(links_network = self) # type: ignore
        
        return res
    
    def visualise(self, vertex_names = True, edge_weights = False, weight_by = 'weight'):
        
        """
        Plots a network visualisation.
        
        Parameters
        ----------
        vertex_names : bool
            whether to show vertex names.
        edge_weights : bool
            whether to show edge weights.
        weight_by : str
            edge attribute to use as weight. Defaults to 'weight'.
        """
        
        networkx_obj = self.to_networkx()
        network_obj = Graph.from_networkx(networkx_obj)

        fig, ax = plt.subplots()

    #     if network == 'corroboration':
    #         edge_weights = True

        if (edge_weights == False) and (vertex_names == False):
            ig.plot(network_obj, 
                    target=ax
                   )
        elif (edge_weights == False) and (vertex_names == True):
            ig.plot(network_obj, 
                    target=ax,
                    vertex_label = network_obj.vs['name']
                   )
        elif (edge_weights == True) and (vertex_names == True):
            ig.plot(network_obj, 
                    target=ax,
                    vertex_label = network_obj.vs['name'],
                    edge_label = network_obj.es[weight_by]
                   )
        else:
             ig.plot(network_obj, 
                    target=ax,
                    edge_label = network_obj.es[weight_by]
                   )

    # Methods for exporting network to external files
    
    def to_igraph(self) -> Graph:

        """Returns the Network as an igraph Graph object."""

        is_dir = self.is_directed()

        g_attrs = self.attributes()

        vs_len = len(self.vs)
        v_attrs = self.vs.attributes()

        edgelist = self.get_edgelist()
        e_attrs = self.es.attributes()

        g = Graph(n=vs_len, edges=edgelist, directed=is_dir)

        for a in g_attrs:
            g[a] = self[a]

        for a in v_attrs:
            g.vs[a] = self.vs[a]
        
        for a in e_attrs:
            g.es[a] = self.es[a]
        
        return g

    def export_network_to_kumu(self, file_name: str = 'request_input', folder_address = 'request_input'):
        
        """
        Exports network to a Kumu blue-print formatted .JSON file.
        """
        
        if file_name == 'request_input':
            file_name = input('File name: ')

        if folder_address == 'request_input':
            folder_address = input('Save to: ')
        
        export_network_to_kumu(network = self, folder_address = folder_address, file_name = file_name)

    
    def export_network(self, file_name: str = 'request_input', folder_address = 'request_input', file_type = 'request_input'):
        
        """
        Exports network to one of a variety of graph file types. Defaults to .graphML.
        """
        
        if file_name == 'request_input':
            file_name = input('File name: ')

        if folder_address == 'request_input':
            folder_address = input('Folder address: ')

        if file_type == 'request_input':
            file_type = input('File type ("graphML", "kumu", GML", "LEDA", "lgl", "ncol", or "pajek"): ')

        export_network(network = self, file_name = file_name, folder_address = folder_address, file_type = file_type)

  

class Networks(AttrSet):
    
    """This is a collection of Networks.
    
    Notes
    -----
        * Subclass of AttrSet class.
        * Intended to assigned to all Review objects.
    """
    
    def __init__(self):
        
        """
        Initialises Networks instance.
        
        Parameters
        ----------
        obj_name : str
            ID used for network set.
        parent_obj_path : str
            if network set is an attribute, object path of object is attribute of. Defaults to None.
        """
        
        # Inheriting methods and attributes from AttrSet class
        super().__init__()
        
    
    
    def __repr__(self):
        
        """
        Defines how Networks objects are represented in string form.
        """
        
        string_repr = f'\nNetworks: {self.__len__()}\n- - - - - - - - - - - -\n\n'
        network_names = self.contents()
        
        for name in network_names:
            vertices = len(self.__dict__[name].vs)
            edges = len(self.__dict__[name].es)
            string_repr = string_repr + f'{name}: {vertices} vertices, {edges} edges\n\n' 
        
        return string_repr
    
    
    # Methods for retreiving networks
    
    def get_network(self, network = 'request_input'):
        
        """
        Returns a network if given its name.
        """
        
        if network == 'request_input':
            network = input('Network name: ')
        
        return self.__dict__[network]
    
    
    # Methods for deleting networks
    
    def delete_all(self):
        
        """
        Deletes all networks in collection.
        """
        
        networks = list(self.contents())
        
        for n in networks:
            delattr(self, n)
    
    
    # Methods for creating network visualisations
            
    def visualise(self, network = 'request_input', vertex_names = True, edge_weights = False, weight_by = 'weight'):
        
        """
        Plots a network visualisation.
        
        Parameters
        ----------
        network : str
            name of network to visualise.
        vertex_names : bool
            whether to show vertex names.
        edge_weights : bool
            whether to show edge weights.
        weight_by : str
            edge attribute to use as weight. Defaults to 'weight'.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        network_obj = self.get_network(network)

        fig, ax = plt.subplots()

    #     if network == 'corroboration':
    #         edge_weights = True

        if (edge_weights == False) and (vertex_names == False):
            ig.plot(network_obj, 
                    target=ax
                   )
        elif (edge_weights == False) and (vertex_names == True):
            ig.plot(network_obj, 
                    target=ax,
                    vertex_label = network_obj.vs['name']
                   )
        elif (edge_weights == True) and (vertex_names == True):
            ig.plot(network_obj, 
                    target=ax,
                    vertex_label = network_obj.vs['name'],
                    edge_label = network_obj.es[weight_by]
                   )
        else:
             ig.plot(network_obj, 
                    target=ax,
                    edge_label = network_obj.es[weight_by]
                   )
    
    
    # Methods for calculating network analytics
    
    def average_path_length(self, network = 'request_input', directed = True, unconn = True, weights = None):
        
        """
        Calculates the average path length in a network.
       
        Parameters
        ----------
        network : str
            Name of network to analyse. Defaults to requesting from user input.
        directed : bool
            Whether to consider directed paths in case of a
            directed graph. Ignored for undirected graphs.
            unconn: what to do when the graph is unconnected. If C{True},
            the average of the geodesic lengths in the components is
            calculated. Otherwise for all unconnected vertex pairs,
            a path length equal to the number of vertices is used.
            weights : list, set, tuple, or str edge weights to be used. Can be a sequence or iterable or
            even an edge attribute name.

        Returns
        -------
        result : int
            The average path length in the graph
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        return self.get_network(network).average_path_length(directed = directed, unconn = unconn, weights = weights)

    def diameter(self, network = 'request_input', unconn = True, weights = None):
        
        """
        Calculates the diameter of the graph.
       
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        directed : bool
            whether to consider directed paths.
        unconn : bool
            if C{True} and the graph is unconnected, the
            longest geodesic within a component will be returned. If
            C{False} and the graph is unconnected, the result is the
            number of vertices if there are no weights or infinity
            if there are weights.
        weights : list, set, tuple, or str edge
            weights to be used. Can be a sequence or iterable or
            even an edge attribute name.
      
        Returns
        -------
        result
            the diameter
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        return self.get_network(network).diameter(unconn = unconn, weights = weights)

    def density(self, network = 'request_input', loops = False):
        
        """
        Calculates the density of the graph.
       
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        loops : bool
            whether to take loops into consideration. If C{True},
            the algorithm assumes that there might be some loops in the graph
            and calculates the density accordingly. If C{False}, the algorithm
            assumes that there can't be any loops.
        
        Returns
        -------
        result
            the density of the graph.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        return self.get_network(network).density(loops = loops)

    
    def reciprocity(self, network = 'request_input', ignore_loops=True, mode='default'):
        
        """
        Reciprocity defines the proportion of mutual connections in a
        directed graph. It is most commonly defined as the probability
        that the opposite counterpart of a directed edge is also included
        in the graph. This measure is calculated if C{mode} is C{"default"}.

        Prior to igraph 0.6, another measure was implemented, defined as
        the probability of mutual connection between a vertex pair if we
        know that there is a (possibly non-mutual) connection between them.
        In other words, (unordered) vertex pairs are classified into three
        groups: (1) disconnected, (2) non-reciprocally connected and (3)
        reciprocally connected. The result is the size of group (3), divided
        by the sum of sizes of groups (2) and (3). This measure is calculated
        if C{mode} is C{"ratio"}.
            
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        ignore_loops : bool
            whether loop edges should be ignored.
        mode : str
            the algorithm to use to calculate the reciprocity; see
            above for more details.
        
        Returns
        -------
        result
            the reciprocity of the graph
        """
        if network == 'request_input':
            network = input('Network name: ')

        return self.get_network(network).reciprocity(ignore_loops=ignore_loops, mode=mode)


    def degrees_df(self, network = 'request_input', direction = 'all'):
        
        """
        Calculates the degree distribution of the network. Returns a Pandas DataFrame.
        
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        direction : str
            which  edge directions to analyse.
        
        Returns
        -------
        result : pandas.DataFrame
            a Pandas DataFrame containing the degree distribution of the graph.
        """
        
        if network == 'request_input':
                network = input('Network name: ')

        network_obj = self.get_network(network)
        degrees_df = pd.DataFrame(columns = ['vertex', 'degree'])
        degrees = Network.degree(network_obj, mode = direction)

        index = 0
        for item in network_obj.vs['name']:
            degrees_df.loc[index] = [item, degrees[index]]
            index += 1
        
        degrees_df.index.name = 'vertex_id'
        degrees_df = degrees_df.sort_values('degree', ascending=False)

        return degrees_df

    
    def degrees_stats(self, network = 'request_input', direction = 'all'):
        
        """
        Calculates frequency statistics for the degree distribution of the network.
        
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        direction : str
            which  edge directions to analyse.
        
        Returns
        -------
        result : pandas.DataFrame
            a Pandas DataFrame of frequency statistics for the degree distribution of the
            graph.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        df = self.degrees_df(network = network, direction = direction)

        if df is not None:
            return df['degree'].describe()
        else:
            return None
    
    def betweenness_df(self, network = 'request_input', vertices=None, directed=True, cutoff=None, weights=None, sources=None, targets=None):
        
        """
            Calculates or estimates the betweenness of vertices in a network. Returns a Pandas DataFrame.

            Also supports calculating betweenness with shortest path length cutoffs or
            considering shortest paths only from certain source vertices or to certain
            target vertices.

            Parameters
            ----------
            network : str
                name of network to analyse. Defaults to requesting from user input.
            vertices
                the vertices for which the betweennesses must be returned.
                If C{None}, assumes all of the vertices in the graph.
            directed : bool
                whether to consider directed paths.
            cutoff : float
                if it is an integer, only paths less than or equal to this
                length are considered, effectively resulting in an estimation of the
                betweenness for the given vertices. If C{None}, the exact betweenness is
                returned.
            weights : list, set, tuple, or str
                edge weights to be used. Can be a sequence or iterable or
                 even an edge attribute name.
            sources
                the set of source vertices to consider when calculating
                shortest paths.
            targets
                the set of target vertices to consider when calculating
                shortest paths.
         
            Returns
            -------
            result : pandas.DataFrame
                the (possibly cutoff-limited) betweenness of the given vertices in a Pandas DataFrame.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        network_obj = self.get_network(network)

        df = pd.DataFrame(columns = ['vertex', 'betweenness'])

        betweenness = Network.betweenness(network_obj, vertices=vertices, directed=directed, cutoff=cutoff, weights=weights, sources=sources, targets=targets)
        index = 0
        for item in network_obj.vs['name']:
            df.loc[index] = [item, betweenness[index]]
            index += 1
        
        df.index.name = 'vertex_id'
        df = df.sort_values('betweenness', ascending=False)

        return df


    def betweenness_stats(self, network = 'request_input', vertices=None, directed=True, cutoff=None, weights=None, sources=None, targets=None):
        
        """
            Returns frequency statistics for the betweenness of vertices in a network. Returns a Pandas DataFrame.

            Parameters
            ----------
            network : str
                name of network to analyse. Defaults to requesting from user input.
            vertices
                the vertices for which the betweennesses must be returned.
                If C{None}, assumes all of the vertices in the graph.
            directed : bool
                whether to consider directed paths.
            cutoff : float
                if it is an integer, only paths less than or equal to this
                 length are considered, effectively resulting in an estimation of the
                 betweenness for the given vertices. If C{None}, the exact betweenness is
                 returned.
            weights : list, set, tuple, or str
                edge weights to be used. Can be a sequence or iterable or
                even an edge attribute name.
            sources
                the set of source vertices to consider when calculating
                shortest paths.
            targets
                the set of target vertices to consider when calculating
                shortest paths.
         
            Returns
            -------
            result : pandas.DataFrame
                frequency statistics for betweenness of the given vertices in a Pandas DataFrame.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        df = self.betweenness_df(network = network, vertices=vertices, directed=directed, cutoff=cutoff, weights=weights, sources=sources, targets=targets)

        if df is not None:
                return df['betweenness'].describe()
        else:
            return None
    
    
    def eigencentralities_df(self, network = 'request_input', scale=True, weights=None, return_eigenvalue=False):
        
        """
        Calculates the eigenvector centralities of the vertices in a graph. Returns a Pandas DataFrame.
       
        Eigenvector centrality is a measure of the importance of a node in a
        network. It assigns relative scores to all nodes in the network based
        on the principle that connections from high-scoring nodes contribute
        more to the score of the node in question than equal connections from
        low-scoring nodes. In practice, the centralities are determined by calculating
        eigenvector corresponding to the largest positive eigenvalue of the
        adjacency matrix. In the undirected case, this function considers
        the diagonal entries of the adjacency matrix to be twice the number of
        self-loops on the corresponding vertex.
       
        In the directed case, the left eigenvector of the adjacency matrix is
        calculated. In other words, the centrality of a vertex is proportional
        to the sum of centralities of vertices pointing to it.
       
        Eigenvector centrality is meaningful only for connected graphs.
        Graphs that are not connected should be decomposed into connected
        components, and the eigenvector centrality calculated for each separately.
       
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        directed : bool
            whether to consider edge directions in a directed
            graph. Ignored for undirected graphs.
        scale : bool
            whether to normalize the centralities so the largest
            one will always be 1.
        weights : list, set, tuple, or str
            edge weights given as a list or an edge attribute. If
            C{None}, all edges have equal weight.
        return_eigenvalue : bool
            whether to return the actual largest
            eigenvalue along with the centralities
        arpack_options : ARPACKOptions
            an L{ARPACKOptions} object that can be used
            to fine-tune the calculation. If it is omitted, the module-level
            variable called C{arpack_options} is used.
         
        Returns
        -------
        result : pandas.DataFrame
            the eigenvector centralities in a Pandas DataFrame.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        network_obj = self.get_network(network)
        
        if network_obj.is_directed() == True:
            return np.nan
        
        else:

            df = pd.DataFrame(columns = ['vertex', 'eigencentrality'])

            eigencentrality = Network.eigenvector_centrality(network_obj, scale=scale, weights=weights, return_eigenvalue=return_eigenvalue)
            index = 0
            for item in network_obj.vs['name']:
                df.loc[index] = [item, eigencentrality[index]]
                index += 1
            
            df.index.name = 'vertex_id'
            df = df.sort_values('eigencentrality', ascending=False)

            return df


    def eigencentralities_stats(self, network = 'request_input'):
        
        """
        Returns frequency statistics for the eigenvector centralities of the vertices in a graph.
       
        Eigenvector centrality is a measure of the importance of a node in a
        network. It assigns relative scores to all nodes in the network based
        on the principle that connections from high-scoring nodes contribute
        more to the score of the node in question than equal connections from
        low-scoring nodes. In practice, the centralities are determined by calculating
        eigenvector corresponding to the largest positive eigenvalue of the
        adjacency matrix. In the undirected case, this function considers
        the diagonal entries of the adjacency matrix to be twice the number of
        self-loops on the corresponding vertex.
       
        In the directed case, the left eigenvector of the adjacency matrix is
        calculated. In other words, the centrality of a vertex is proportional
        to the sum of centralities of vertices pointing to it.
       
        Eigenvector centrality is meaningful only for connected graphs.
        Graphs that are not connected should be decomposed into connected
        components, and the eigenvector centrality calculated for each separately.
       
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        directed : bool
            whether to consider edge directions in a directed
            graph. Ignored for undirected graphs.
        scale : bool
            whether to normalize the centralities so the largest
            one will always be 1.
        weights : list, set, tuple, or str
            edge weights given as a list or an edge attribute. If
            C{None}, all edges have equal weight.
        return_eigenvalue : bool
            whether to return the actual largest
            eigenvalue along with the centralities
        arpack_options : ARPACKOptions
            an L{ARPACKOptions} object that can be used
            to fine-tune the calculation. If it is omitted, the module-level
            variable called C{arpack_options} is used.
         
        Returns
        -------
        result : pandas.Series
            frequency statistics for eigenvector centralities in a Pandas DataFrame.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        df = self.eigencentralities_df(network = network)

        if df is not None:
                return df['eigencentrality'].describe() # type: ignore
        else:
            return None
    
    def authority_scores_df(self, network = 'request_input', weights=None, scale=True, return_eigenvalue=False):
        
        """
        Calculates Kleinberg's authority score for the vertices of the network. Returns a Pandas DataFrame.
        
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        weights : list, set, tuple, or str
            edge weights to be used. Can be a sequence or iterable or
            an edge attribute name.
        scale : bool
            whether to normalize the scores so that the largest one is 1.
        arpack_options : ARPACKOptions
            an L{ARPACKOptions} object used to fine-tune
            the ARPACK eigenvector calculation. If omitted, the module-level
            variable called C{arpack_options} is used.
        return_eigenvalue : bool
            whether to return the largest eigenvalue.
        
        Returns
        -------
        result : pandas.DataFrame
            the authority scores as a Pandas DataFrame.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        network_obj = self.get_network(network)

        df = pd.DataFrame(columns = ['vertex', 'authority_score'])

        authority_scores = Network.authority_score(network_obj, weights=weights, scale=scale, return_eigenvalue=return_eigenvalue)
        index = 0
        for item in network_obj.vs['name']:
            df.loc[index] = [item, authority_scores[index]]
            index += 1
        
        df.index.name = 'vertex_id'
        df = df.sort_values('authority_score', ascending=False)

        return df


    def authority_scores_stats(self, network = 'request_input', weights=None, scale=True, return_eigenvalue=False):
        
        """
        Returns frequency statistics for Kleinberg's authority score for the vertices of the network. Returns a Pandas DataFrame.
        
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        weights : list, set, tuple, or str
            edge weights to be used. Can be a sequence or iterable or
            an edge attribute name.
        scale : bool
            whether to normalize the scores so that the largest one is 1.
        arpack_options : ARPACKOptions
            an L{ARPACKOptions} object used to fine-tune
            the ARPACK eigenvector calculation. If omitted, the module-level
            variable called C{arpack_options} is used.
        return_eigenvalue : bool
            whether to return the largest eigenvalue.
        
        Returns
        -------
        result : pandas.DataFrame
            frequency statistics for authority scores as a Pandas DataFrame.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        df = self.authority_scores_df(network = network, weights=weights, scale=scale, return_eigenvalue=return_eigenvalue)

        if df is not None:
                return df['authority_score'].describe()
        else:
            return None

    def hub_scores_df(self, network = 'request_input', weights=None, scale=True, return_eigenvalue=False):
        
        """
        Calculates Kleinberg's hub score for the vertices of the graph. Returns a Pandas DataFrame.
       
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        weights : list, set, tuple, or str
            edge weights to be used. Can be a sequence or iterable or
            an edge attribute name.
        scale : bool 
            whether to normalize the scores so that the largest one is 1.
        arpack_options : ARPACKOptions
            an L{ARPACKOptions} object used to fine-tune
            the ARPACK eigenvector calculation. If omitted, the module-level
            variable called C{arpack_options} is used.
        return_eigenvalue : bool
            whether to return the largest eigenvalue.
       
        Returns
        -------
        result : pandas.DataFrame
            the hub scores as a Pandas DataFrame.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        network_obj = self.get_network(network)
        
        df = pd.DataFrame(columns = ['vertex', 'hub_score'])

        hub_scores = Network.hub_score(network_obj, weights=weights, scale=scale, return_eigenvalue=return_eigenvalue)
        index = 0
        for item in network_obj.vs['name']:
            df.loc[index] = [item, hub_scores[index]]
            index += 1
        
        df.index.name = 'vertex_id'
        df = df.sort_values('hub_score', ascending=False)

        return df


    def hub_scores_stats(self, network = 'request_input', weights=None, scale=True, return_eigenvalue=False):
        
        """
        Returns frequency statistisc for Kleinberg's hub score for the vertices of the graph. Returns a Pandas DataFrame.
       
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        weights : list, set, tuple, or str
            edge weights to be used. Can be a sequence or iterable or
            an edge attribute name.
        scale : bool
            whether to normalize the scores so that the largest one
            is 1.
        arpack_options : ARPACKOptions
            an L{ARPACKOptions} object used to fine-tune
            the ARPACK eigenvector calculation. If omitted, the module-level
            variable called C{arpack_options} is used.
        return_eigenvalue : bool 
            whether to return the largest eigenvalue.
       
        Returns
        -------
        result : pandas.DataFrame
            frequency statistics for hub scores as a Pandas DataFrame.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        df = self.hub_scores_df(network = network, weights=weights, scale=scale, return_eigenvalue=return_eigenvalue)

        if df is not None:
                return df['hub_score'].describe()
        else:
            return None
    
    def coreness_df(self, network = 'request_input', mode='all'):
        
        """
            Finds the coreness (shell index) of the vertices of the network. Returns a Pandas DataFrame.
       
            The M{k}-core of a graph is a maximal subgraph in which each vertex
            has at least degree k. (Degree here means the degree in the
            subgraph of course). The coreness of a vertex is M{k} if it
            is a member of the M{k}-core but not a member of the M{k+1}-core.
       
            Parameters
            ----------
            network : str
                name of network to analyse. Defaults to requesting from user input.
            mode : str
                whether to compute the in-corenesses (C{"in"}), the
                out-corenesses (C{"out"}) or the undirected corenesses (C{"all"}).
                Ignored and assumed to be C{"all"} for undirected graphs.
            
            Returns
            -------
            result : pandas.DataFrame
                the corenesses for each vertex.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        network_obj = self.get_network(network)

        df = pd.DataFrame(columns = ['vertex', 'coreness'])

        coreness = Network.coreness(network_obj, mode=mode)
        index = 0
        for item in network_obj.vs['name']:
            df.loc[index] = [item, coreness[index]]
            index += 1
        
        df.index.name = 'vertex_id'
        df = df.sort_values('coreness', ascending=False)

        return df


    def coreness_stats(self, network = 'request_input'):
        
        """
            Returns frequency statistics for the coreness (shell index) values of the vertices of the network.
       
            The M{k}-core of a graph is a maximal subgraph in which each vertex
            has at least degree k. (Degree here means the degree in the
            subgraph of course). The coreness of a vertex is M{k} if it
            is a member of the M{k}-core but not a member of the M{k+1}-core.
       
            Parameters
            ----------
            network : str
                name of network to analyse. Defaults to requesting from user input.
            mode : str
                whether to compute the in-corenesses (C{"in"}), the
                out-corenesses (C{"out"}) or the undirected corenesses (C{"all"}).
                Ignored and assumed to be C{"all"} for undirected graphs.
            
            Returns
            -------
            result : pandas.Series
                frequency statistics for corenesses values.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        df = self.coreness_df(network = network)

        if df is not None:
                return df['coreness'].describe()
        else:
            return None
    

    def community_detection(self, network = 'request_input', algorithm='fastgreedy'):
        
        """
        Identifies network communities. Gives the option of using different algorithms.
        
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        algorithm : str
            the community detection algorithm to use.
        
        Notes
        -----
        'algorithm' options:
            * betweenness
            * fastgreedy
            * eigenvector
            * spinglass
            * spinglass
            * walktrap
        """

        if network == 'request_input':
            network = input('Network name: ')

        network_obj = self.get_network(network)

        if (algorithm == None) or (algorithm == ''):
            algorithm = input('Algorithm must be specified. Options: 1. betweenness, 2. fastgreedy, 3. eigenvector, 4. spinglass, 5. walktrap.:')
        
        if algorithm == 'betweenness':
            return network_obj.community_edge_betweenness()
        
        if algorithm == 'fastgreedy':
            return network_obj.community_fastgreedy()
        
        if algorithm == 'eigenvector':
            return network_obj.community_leading_eigenvector()
        
        if algorithm == 'spinglass':
            return network_obj.community_spinglass()
        
        if algorithm == 'walktrap':
            return network_obj.community_walktrap()


    def components(self, network = 'request_input', mode='strong'):
        
        """
        Calculates the (strong or weak) connected components for
        a given graph.
       
        Parameters
        ----------
        network : str
            name of network to analyse. Defaults to requesting from user input.
        mode : str
            must be either C{"strong"} or C{"weak"}, depending on the
            connected components being sought. Optional, defaults to C{"strong"}.
        
        Returns
        -------
        result : VertexClustering
            a L{VertexClustering} object
        """

        if network == 'request_input':
            network = input('Network name: ')

        network_obj = self.get_network(network)

        return network_obj.components(mode=mode)


    def decompose(self, network = 'request_input', mode='strong', maxcompno=None, minelements=1):
        
        """
            Decomposes the graph into subgraphs.
       
            Parameters
            ----------
            mode : str
                must be either C{"strong"} or C{"weak"}, depending on
                the clusters being sought. Optional, defaults to C{"strong"}.
            maxcompno : int
                maximum number of components to return.
                C{None} means all possible components.
            minelements : int
                minimum number of vertices in a component.
                By setting this to 2, isolated vertices are not returned
                as separate components.
         
            Returns
            -------
            result : list
                a list of the subgraphs. Every returned subgraph is a
                copy of the original.
        """

        if network == 'request_input':
            network = input('Network name: ')

        network_obj = self.get_network(network)

        return network_obj.decompose(mode=mode, maxcompno=maxcompno, minelements=minelements)
    
    
    
    def weighted_degrees_df(self, network = 'request_input', direction = 'all'):

        """Calculates a network's weighted degrees and returns a Pandas DataFrame."""
        
        if network == 'request_input':
                network = input('Network name: ')

        network_obj = self.get_network(network)

        # Checks if network is weighted

        if 'weight' not in network_obj.es.attributes():
            degrees_df = self.degrees_df(network = network, direction = direction)
            degrees_df['weighted_degree'] = degrees_df['degree']
            degrees_df = degrees_df.drop('degree', axis = 1)
            return degrees_df
            

        else:

            degrees_df = pd.DataFrame(columns = ['vertex', 'weighted_degree'])

            index = 0
            for vertex in network_obj.vs:
                weighted_degree = 0
                incident_edges = (Network.incident(network_obj, vertex))
                for edge in incident_edges:
                    weight = network_obj.es[edge]['weight']
                    weighted_degree += weight
                degrees_df.loc[index] = [vertex['name'], weighted_degree]            
                index += 1

            degrees_df = degrees_df.sort_values('weighted_degree', ascending=False)

            return degrees_df

        
    def weighted_degrees_stats(self, network = 'request_input', direction = 'all'):
        
        """
        Returns frequency statistics for the network's weighted degree distribution.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        df = self.weighted_degrees_df(network = network, direction = direction)

        if df is not None:
            return df['weighted_degree'].describe()

        
    def degree_distribution(self, network = 'request_input', weighted = False, direction = 'all'):
        
        """
        Returns the network's weighted or unweighted degree distribution as a Pandas DataFrame.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        if weighted == True:
            degrees_frame = self.weighted_degrees_df(network = network, direction = direction)
            freq_table = degrees_frame['weighted_degree'].value_counts()
            dist_frame = pd.DataFrame({'weighted_degree':freq_table.index, 'counts':freq_table.values})

            return dist_frame

        else:
            degrees_frame = self.degrees_df(network = network, direction = direction)
            freq_table = degrees_frame['degree'].value_counts()
            dist_frame = pd.DataFrame({'degree':freq_table.index, 'counts':freq_table.values})

            return dist_frame
            
    

    def all_centralities(self, network = 'request_input'):
        
        """
        Calculates all centrality measures for network. Returns as a Pandas DataFrame.
        """
        
        if network == 'request_input':
                network = input('Network name: ')
        
        network_obj = self.get_network(network)
        return network_obj.all_centralities()

    
    def get_neighbours(self,  network = 'request_input', vertex_name = 'request_input'):
        
        """Returns vertex neighbours as a Pandas DataFrame"""
        
        if network == 'request_input':
            network = input('Network name: ')

        if vertex_name == 'request_input':
            vertex_name = input('Vertex name: ')

        network_obj = self.get_network(network)

        # Get vertex
        vertex = network_obj.vs.find(name = vertex_name)

        # Get vertex neighbours in a Pandas DataFrame

        df = pd.DataFrame(columns = ['vertex_id', 'vertex_name'])

        neighbours = vertex.neighbors()
        
        index = 0
        for neighbour in neighbours:
            neighbour_id = neighbour.index
            neighbour_name = neighbour['name']
            df.loc[index] = [neighbour_id, neighbour_name]
            index += 1

        df = df.set_index('vertex_id')

        return df


    def get_degree(self, network = 'request_input', vertex_name = 'request_input', direction = 'all'):
        
        """Returns the number of other vertices a vertex is tied to. I.e., the size, or len(), of neighbours()."""
        
        if network == 'request_input':
            network = input('Network name: ')

        if vertex_name == 'request_input':
            vertex_name = input('Vertex name: ')

        degree = len(self.get_network(network).vs.find(name = vertex_name).neighbors())
        degree = int(degree)
        
        return degree


    def get_weighted_degree(self, network = 'request_input', vertex_name = 'request_input', direction = 'all'):
        
        """Returns the number of other vertices a vertex is tied to. I.e., the size, or len(), of neighbours()."""
        
        if network == 'request_input':
            network = input('Network name: ')

        if vertex_name == 'request_input':
            vertex_name = input('Vertex name: ')

        df = self.weighted_degrees_df(network = network, direction = direction)
        masked = df[df['vertex'] == vertex_name]
        degree = int(masked['weighted_degree']) # type: ignore

        return degree

    
    def get_item_all_degrees(self, item_id = 'request_input', weighted = False):
        
        """
        Returns a Pandas DataFrame of degrees for all vertices representing an item. Takes an item ID.
        """
        
        if item_id == 'request_input':
            item_id = input('Item ID: ')

        networks = [
                    'items_words_shared', 
                    'items_words_similarity', 
                    'items_information_shared', 
                    'items_information_similarity',
                    'items_metadata_shared',
                    'items_metadata_similarity',
    #                 'items_links',
                    'items_references', 
                    'items_contents'
                    ]

        series = pd.Series(index = networks, dtype = object)

        for network in networks:

            if weighted == True:
                result = self.get_weighted_degree(network = network, vertex_name = item_id)
            else:
                result = self.get_degree(network = network, vertex_name = item_id)

            series[network] = int(result)

        return series



    
    def get_betweenness(self, network = 'request_input', vertex_name = 'request_input', direction = 'all'):
        
        """
        Returns a vertex's betweenness centrality score.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        if vertex_name == 'request_input':
            vertex_name = input('Vertex name: ')

        betweenness = self.get_network(network).vs.find(name = vertex_name).betweenness()
        betweenness = float(betweenness)

        return betweenness

    
    def get_item_all_betweenness(self, item_id = 'request_input'):
        
        """
        Returns betweenness centrality scores for an item's vertices in all networks.
        """
        
        if item_id == 'request_input':
            item_id = input('Item ID: ')

        networks = [
                    'items_words_shared', 
                    'items_words_similarity', 
                    'items_information_shared', 
                    'items_information_similarity',
                    'items_metadata_shared',
                    'items_metadata_similarity',
    #                 'items_links',
                    'items_references', 
                    'items_contents'
                    ]

        series = pd.Series(index = networks, dtype = object)

        for network in networks:
            
            result = self.get_betweenness(network = network, vertex_name = item_id)
            series[network] = float(result)

        return series




    def get_eigencentrality(self, network = 'request_input', vertex_name = 'request_input', direction = 'all'):
        
        """
        Returns the eigenvector centrality score for a vertex.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        if vertex_name == 'request_input':
            vertex_name = input('Vertex name: ')
        
        network_obj = self.get_network(network)
        vertex_id = network_obj.vs.find(name = vertex_name).index
        
        all_eigencentralities = network_obj.eigenvector_centrality()
        eigencentrality = all_eigencentralities[vertex_id]
        eigencentrality = float(eigencentrality)

        return eigencentrality

    def get_hub_score(self, network = 'request_input', vertex_name = 'request_input', direction = 'all'):
        
        """
        Returns the hub score for a vertex.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        if vertex_name == 'request_input':
            vertex_name = input('Vertex name: ')

        network_obj = self.get_network(network)
        vertex_id = network_obj.vs.find(name = vertex_name).index
        
        all_scores = network_obj.hub_score()
        hub_score = all_scores[vertex_id]
        hub_score = float(hub_score)

        return hub_score

    
    def get_authority_score(self, network = 'request_input', vertex_name = 'request_input', direction = 'all'):
        
        """
        Returns the authority score for a vertex.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        if vertex_name == 'request_input':
            vertex_name = input('Vertex name: ')

        network_obj = self.get_network(network)
        vertex_id = network_obj.vs.find(name = vertex_name).index
        
        all_scores = network_obj.authority_score()
        authority_score = all_scores[vertex_id]
        authority_score = float(authority_score)

        return authority_score

    
    def get_pagerank(self, network = 'request_input', vertex_name = 'request_input', direction = 'all'):
        
        """
        Returns the Pagerank score for a vertex.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        if vertex_name == 'request_input':
            vertex_name = input('Vertex name: ')

        network_obj = self.get_network(network)
        vertex_id = network_obj.vs.find(name = vertex_name).index
        
        all_pageranks = network_obj.pagerank()
        pagerank = all_pageranks[vertex_id]
        pagerank = float(pagerank)

        return pagerank

    
    # Methods for exporting networks to external files
    
            
    def export_network_to_kumu(self, network = 'request_input', folder_address = 'request_input'):
        
        """
        Exports network to a Kumu blue-print formatted .JSON file.
        """
        
        if network == 'request_input':
            network = input('Network name: ')
        
        if folder_address == 'request_input':
            folder_address = input('Save to: ')
        
        network_obj = self.get_network(network)
        export_network_to_kumu(network = network_obj, folder_address = folder_address, file_name = network)

    
    def export_network(self, network = 'request_input', folder_address = 'request_input', file_type = 'request_input'):
        
        """
        Exports network to one of a variety of graph file types. Defaults to .graphML.
        """
        
        if network == 'request_input':
            network = input('Network name: ')

        network_obj = self.get_network(network)
        
        if folder_address == 'request_input':
            folder_address = input('Folder address: ')

        if file_type == 'request_input':
            file_type = input('File type ("graphML", "kumu", GML", "LEDA", "lgl", "ncol", or "pajek"): ')

        export_network(network = network_obj, file_name = network, folder_address = folder_address, file_type = file_type)

                
    def export_all_networks(self, folder_address = 'request_input', file_type = 'request_input'):
        
        """
        Exports all networks in the collection to one of a variety of graph file types. Defaults to .graphML.
        """
        
        if folder_address == 'request_input':
            folder_address = input('Folder address: ')

        if file_type == 'request_input':
            file_type = input('File type (Options: "graphML", "kumu", "GML", "LEDA", "lgl", "ncol", or "pajek"): ')


        if type(folder_address) != str:
            raise TypeError('Folder address must be a string')

        for network in self.contents():
            self.export_network(network = network, folder_address = folder_address, file_type = file_type)

