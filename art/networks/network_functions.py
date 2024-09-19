from ..utils.basics import results_cols

from igraph import Graph # type: ignore
from networkx.classes import Graph as NetworkX_Undir, DiGraph as NetworkX_Dir, MultiGraph as NetworkX_Multi # type: ignore
import numpy as np
import pandas as pd

import itertools

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
    Is able to take igraph.Graph, Network, and NetworkX objects.
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
    Is able to take igraph.Graph, Network, and NetworkX objects.
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



def generate_coauthors_network(coauthors_dict: dict) -> Graph:
        
        """
        Returns an undirected network representing how authors co-publish with one another.
        
        Parameters
        ----------
        coauthors_dict : dict
            a dictionary with the following structure:
                * keys: author IDs
                * values: Pandas DataFrames containing details on co-authors.
        
        Returns
        -------
        g : Graph
            an iGraph Graph object representing the co-authorship network.
        """

        
        # Creating list of all unique author_ids in source dictionary
        all_auths = list(set(coauthors_dict.keys()))
        
        
        # Initialising network
        g = Graph(n=len(all_auths), directed=False, vertex_attrs={'name': all_auths})
        
        
        # Adding edges by iterating through vertices and retrieving links associated
        for auth_id in all_auths:
            
            # Ignoring None and empty string vertex IDs
            if (auth_id != None) and (len(auth_id) > 0):
                
                # Adding vertex if none in graph
                if auth_id not in g.vs['name']:
                    g.add_vertex(name=auth_id)

                # Getting vertex
                vertex = g.vs.find(name=auth_id)
            
                # Retrieving vertex index
                v_index = vertex.index
                
                # Retrieving co-author ids
                v_edges = coauthors_dict[auth_id]['author_id'].to_list()
                
                # If the vertex has links associated, finds vertices those links direct to
                if len(v_edges) > 0:
                    
                    for author in v_edges:

                        if author not in g.vs['name']:
                            g.add_vertex(name=author)

                        end_index = g.vs.find(name = author).index
                        df_index = coauthors_dict[auth_id][coauthors_dict[auth_id]['author_id'] == author].index.to_list()[0]
                        weight = coauthors_dict[auth_id].loc[df_index, 'frequency']
                        
                        # Adding edges between linked vertices
                        Graph.add_edges(g, 
                                        [(v_index, end_index)], 
                                        attributes={
                                           'name': f'{auth_id} <-> {author}',
                                           'weight': weight
                                           })
        
        g = g.simplify(combine_edges='first')

        return g

def generate_funders_network(funders_dict: dict) -> Graph:
        
        """
        Returns an undirected network representing how funders co-publish with one another.
        
        Parameters
        ----------
        funders_dict : dict
            a dictionary with the following structure:
                * keys: funder IDs
                * values: Pandas DataFrames containing details on co-funders.
        
        Returns
        -------
        g : Graph
            an iGraph Graph object representing the co-funder network.
        """

        
        # Creating list of all unique funder_ids in source dictionary
        all_funders = list(set(funders_dict.keys()))
        
        
        # Initialising network
        g = Graph(n=len(all_funders), directed=False, vertex_attrs={'name': all_funders})
        
        
        # Adding edges by iterating through vertices and retrieving links associated
        for funder_id in all_funders:
            
            
            # Ignoring None and empty string vertex IDs
            if (funder_id != None) and (len(funder_id) > 0):
                
                if funder_id not in g.vs['name']:
                    g.add_vertex(name=funder_id)

                # Getting vertex id
                vertex = g.vs.find(name = funder_id)

                # Retrieving vertex index
                v_index = vertex.index
                
                # Retrieving co-funder ids
                v_edges = funders_dict[funder_id]['funder_id'].to_list()
                
                # If the vertex has links associated, finds vertices those links direct to
                if len(v_edges) > 0:
                    
                    for f in v_edges:

                        if f not in g.vs['name']:
                            g.add_vertex(name=f)

                        end_index = g.vs.find(name = f).index
                        df_index = funders_dict[funder_id][funders_dict[funder_id]['funder_id'] == f].index.to_list()[0]
                        weight = funders_dict[funder_id].loc[df_index, 'frequency']
                        
                        # Adding edges between linked vertices
                        Graph.add_edges(g, 
                                        [(v_index, end_index)], 
                                        attributes={
                                           'name': f'{funder_id} <-> {f}',
                                           'weight': weight
                                           })
        
        g = g.simplify(combine_edges='first')

        return g

def generate_citations_network(citations_dict: dict) -> Graph:
        
        """
        Returns an undirected network representing how publications cite one another.
        
        Parameters
        ----------
        citations_dict : dict
            a dictionary with the following structure:
                * keys: work IDs
                * values: Pandas DataFrames containing details on cited works.
        
        Returns
        -------
        g : Graph
            an iGraph Graph object representing the citation network.
        """

        
        # Creating list of all unique author_ids in source dictionary
        all_ids = list(set(citations_dict.keys()))
        
        
        # Initialising network
        g = Graph(n=len(all_ids), directed=True, vertex_attrs={'name': all_ids})
        
        
        # Adding edges by iterating through vertices and retrieving links associated
        for work_id in all_ids:
            
            # Ignoring None and empty string vertex IDs
            if (work_id != None) and (len(work_id) > 0):
                
                if work_id not in g.vs['name']:
                    g.add_vertex(name=work_id)

                # Getting vertex object
                vertex = g.vs.find(name = work_id)
                
                # Retrieving vertex index
                v_index = vertex.index
                
                refs_obj = citations_dict[work_id]
                if (refs_obj is None) or (type(refs_obj) == float ) or (refs_obj is np.nan) or (type(refs_obj) == str):
                     continue
                
                if (refs_obj is not None) and (type(refs_obj) != float ) and (refs_obj is not np.nan) and (type(refs_obj) != str):
                    if 'update_work_ids' in refs_obj.__dir__():
                        refs_obj.update_work_ids()

                    if 'copy' in refs_obj.__dir__():
                        df = refs_obj.copy(deep=True)
                        
                    else:
                         df = refs_obj
                    
                    if 'astype' in df.__dir__():
                            df = df.astype(str)

                else:
                    df = pd.DataFrame(columns=results_cols, dtype=object)

                if type(df) == float:
                     df = pd.DataFrame(columns=results_cols, dtype=object)

                # Retrieving citations work ids
                df_index = df.index.to_list()
                
                # If the vertex has links associated, finds vertices those links direct to
                if len(df_index) > 0:
                    
                    for i in df_index:

                        citation_data = refs_obj.loc[i] # type: ignore
                        citation = citation_data['work_id']

                        if citation not in g.vs['name']:
                             g.add_vertex(name=citation)

                        end_index = g.vs.find(name = citation).index

                        for i in citation_data.index.to_list():
                             data = citation_data[i]
                             g.vs[end_index][i] = data
                        
                        # Adding edges between linked vertices
                        Graph.add_edges(g, 
                                        [(v_index, end_index)], 
                                        attributes={
                                           'name': f'{work_id} -> {citation}'
                                           })
        
        g = g.simplify(combine_edges='first')

        return g

def generate_author_works_network(author_works_dict: dict) -> Graph:

    """
        Returns an undirected bipartite network representing the relationships between authors and publications.
        
        Parameters
        ----------
        author_works_dict : dict
            a dictionary with the following structure:
                * keys: work IDs
                * values: Pandas DataFrames containing details on authors.
        
        Returns
        -------
        g : Graph
            an iGraph Graph object.
    """

    work_ids = author_works_dict.keys()
    work_ids = [i for i in work_ids if (i != None) and (len(i) > 0)]
    works_len = len(work_ids)
    init_types = [False]*works_len

    g = Graph.Bipartite(types=init_types, edges=[], directed=False)
    g.vs['name'] = work_ids
    g.vs['category'] = ['publication']*works_len

    for work_id in work_ids:

        # Getting vertex object
        vertex = g.vs.find(name = work_id)

        v_index = vertex.index

        data = author_works_dict[work_id]

        if type(data) == dict:
             data = pd.DataFrame.from_dict(data, orient='index').T
        
        if type(data) != pd.DataFrame:
             continue
        
        if 'author_id' not in data.columns:
            continue
            
        auth_ids = data['author_id'].to_list()
        auth_ids = [i.split('#')[0].strip() for i in auth_ids if type(i) == str]

        for a in auth_ids:

            if a not in g.vs['name']:
                g.add_vertex(name=a, type=True)
                
            author_vertex = g.vs.find(name = a)
            author_index = author_vertex.index
            author_vertex['category'] = 'author'

            # Adding edges between linked vertices
            Graph.add_edges(g, 
                                        [(v_index, author_index)], 
                                        attributes={
                                           'name': f'{work_id} <-> {a}'
                                           })
    
    g = g.simplify(combine_edges='first')

    return g

def generate_author_affils_network(author_affils_dict: dict) -> Graph:

    """
        Returns an undirected bipartite network representing the relationships between authors and their affiliated organisations.
        
        Parameters
        ----------
        author_affils_dict : dict
            a dictionary with the following structure:
                * keys: work IDs
                * values: Pandas DataFrames containing details on authors.
        
        Returns
        -------
        g : Graph
            an iGraph Graph object.
    """

    author_ids = author_affils_dict.keys()
    author_ids_stripped = [i.split('#')[0].strip() for i in author_ids if ((type(i) == str) and (len(i) > 0))]
    auths_len = len(author_ids_stripped)
    init_types = [False]*auths_len

    g = Graph.Bipartite(types=init_types, edges=[], directed=False)
    g.vs['name'] = author_ids_stripped
    g.vs['category'] = ['author']*auths_len

    for auth_id in author_ids:
        
        auth_id_stripped = auth_id.split('#')[0].strip()

        # Getting vertex object
        vertex = g.vs.find(name = auth_id_stripped)

        v_index = vertex.index

        data = author_affils_dict[auth_id]
        
        if type(data) == dict:
             data = pd.DataFrame.from_dict(data, orient='index').T
        
        if type(data) != pd.DataFrame:
             continue

        if 'affiliation_id' not in data.columns:
            continue
            
        affil_ids = data['affiliation_id'].to_list()
        affil_ids = [i.split('#')[0].strip() for i in affil_ids if (type(i) == str and len(i) > 0)]

        for a in affil_ids:

            if a not in g.vs['name']:
                g.add_vertex(name=a, type=True)
                
            affil_vertex = g.vs.find(name = a)
            affil_index = affil_vertex.index
            affil_vertex['category'] = 'affiliation'

            # Adding edges between linked vertices
            Graph.add_edges(g, 
                                        [(v_index, affil_index)], 
                                        attributes={
                                           'name': f'{auth_id_stripped} <-> {a}'
                                           })
    
    g = g.simplify(combine_edges='first')

    return g

def generate_funder_works_network(funder_works_dict: dict) -> Graph:

    """
        Returns an undirected bipartite network representing the relationships between funders and publications.
        
        Parameters
        ----------
        author_works_dict : dict
            a dictionary with the following structure:
                * keys: work IDs
                * values: Pandas DataFrames containing details on funders.
        
        Returns
        -------
        g : Graph
            an iGraph Graph object.
    """

    work_ids = funder_works_dict.keys()
    work_ids = [i for i in work_ids if (i != None) and (len(i) > 0)]
    works_len = len(work_ids)
    init_types = [False]*works_len

    g = Graph.Bipartite(types=init_types, edges=[], directed=False)
    g.vs['name'] = work_ids
    g.vs['category'] = ['publication']*works_len

    for work_id in work_ids:

        # Getting vertex object
        vertex = g.vs.find(name = work_id)

        v_index = vertex.index

        data = funder_works_dict[work_id]

        if type(data) == dict:
             data = pd.DataFrame.from_dict(data, orient='index').T
        
        if type(data) != pd.DataFrame:
             continue

        if 'funder_id' not in data.columns:
            continue
            
        f_ids = data['funder_id'].to_list()
        f_ids = [i.split('#')[0].strip() for i in f_ids if type(i) == str]

        for f in f_ids:

            if f not in g.vs['name']:
                g.add_vertex(name=f, type=True)
                
            funder_vertex = g.vs.find(name = f)
            funder_index = funder_vertex.index
            funder_vertex['category'] = 'funder'

            # Adding edges between linked vertices
            Graph.add_edges(g, 
                                        [(v_index, funder_index)], 
                                        attributes={
                                           'name': f'{work_id} <-> {f}'
                                           })
    
    g = g.simplify(combine_edges='first')

    return g

def cocitation_dict(citation_network) -> dict:
    
    """
    Generates a dictionary representing co-citations from a citation network.
    
    Notes
    -----
    Is able to take igraph.Graph, Network, and NetworkX objects.
    """

    # Converting NetworkX objects to igraph objects
    if (
            (type(citation_network) == NetworkX_Undir)
            or (type(citation_network) == NetworkX_Dir)
            or (type(citation_network) == NetworkX_Multi)
        ):
            citation_network = Graph.from_networkx(citation_network)
    
    # Retrieving URLs list
    ids = citation_network.vs['name']
    
    # Initialising dictionary to store tuples of co-linked URLs
    cocitation_tuples_dict = {}
    
    # Iterating through vertices to identify pairs of co-linked URLs
    for v in citation_network.vs:
        
        # Retrieving vertex URL
        work_id = v['name']
        
        # Creating list of URLS linked by selected URL 
        children = v.successors()
        child_ids = [v['name'] for v in children]
        
        # Creating list of all combinations of linked URLs
        # Stored as a list of tuples
        tuples = list(itertools.combinations(child_ids, 2))
        tuples = [tuple(set(i)) for i in tuples]
        
        # Iterating through linked URLs combinations and adding to dictionary of colinks
        for pair in tuples:
            
            # Adding to dictionary of colinks
            if pair not in cocitation_tuples_dict.keys():
                cocitation_tuples_dict[pair] = {'Co-cited by': [], 'Frequency': 0}

            cocitation_tuples_dict[pair]['Co-cited by'].append(work_id)
            
            # Adding combination to count
            cocitation_tuples_dict[pair]['Frequency'] = len(set(cocitation_tuples_dict[pair]['Co-cited by']))
    
    return cocitation_tuples_dict

def generate_cocitation_network(citation_network):
    
    """
    Generates a co-citation network from a citation network.
    
    Notes
    -----
    Is able to take igraph.Graph, Network, and NetworkX objects.
    """
    
    
    cocitations = cocitation_dict(citation_network)
    v_len = len(citation_network.vs)
    v_attr_keys = citation_network.vs.attributes()

    v_attrs = {}
    for a in v_attr_keys:
        v_attrs[a] = citation_network.vs[a]
    
    
    cocitation_graph = Graph(n=v_len, directed=False, vertex_attrs=v_attrs, edge_attrs={'name':[], 'weight': [], 'cocited_by': []})

    for k in cocitations.keys():

        v1_id = k[0]
        v2_id = k[1]
        freq = cocitations[k]['Frequency']
        cocited_by = cocitations[k]['Co-cited by']

        if v1_id not in cocitation_graph.vs['name']:
              cocitation_graph.add_vertex(name=v1_id)
        
        if v2_id not in cocitation_graph.vs['name']:
              cocitation_graph.add_vertex(name=v2_id)
        
        v1 = cocitation_graph.vs.find(name=v1_id)
        v1_index = v1.index

        v2 = cocitation_graph.vs.find(name=v2_id)
        v2_index = v2.index

        if (cocitation_graph.are_connected(v1_index, v2_index) == False) and (cocitation_graph.are_connected(v2_index, v1_index) == False):
             Graph.add_edges(cocitation_graph, 
                                        [(v1_index, v2_index)], 
                                        attributes={
                                           'name': f'{v1_id} <-> {v2_id}',
                                           'weight': freq,
                                           'cocited_by': cocited_by
                                           })
        
        else:
             edgelist = list(cocitation_graph.es.select(_between= ([v1_index], [v2_index])))

             if len(edgelist) > 0:

                edge = edgelist[0]
                edge_index = edge.index

                old_cocited_by = cocitation_graph.es[edge_index]['cocited_by']
                new_cocited_by = old_cocited_by + cocited_by
                new_cocited_by = list(set(new_cocited_by))
                new_freq = len(new_cocited_by)

                cocitation_graph.es[edge_index]['cocited_by'] = new_cocited_by
                cocitation_graph.es[edge_index]['weight'] = new_freq
    
    # cocitation_graph = cocitation_graph.simplify()

    for v in citation_network.vs:

        work_id = v['name']
        if work_id not in cocitation_graph.vs['name']:
              cocitation_graph.add_vertex(name=work_id)
        
        attrs = citation_network.vs.attributes()
        cocit_v = cocitation_graph.vs.find(name=work_id)

        for a in attrs:
             cocit_v[a] = v[a]
             
    return cocitation_graph

def bibcoupling_dict(citation_network):
    
    """
    Generates a dictionary representing bibliometric coupling from a citation network.
    
    Notes
    -----
    Is able to take igraph.Graph, Network, and NetworkX objects.
    """
    
    # Converting NetworkX objects to igraph objects
    if (
            (type(citation_network) == NetworkX_Undir)
            or (type(citation_network) == NetworkX_Dir)
            or (type(citation_network) == NetworkX_Multi)
        ):
            citation_network = Graph.from_networkx(citation_network)
    
    # Initialising dictionary to store tuples of co-citing works
    bc_dict = {}
    
    # Iterating through vertices to identify pairs of co-citing works
    for v in citation_network.vs:
        
        # Retrieving work ID
        work_id = v['name']
        
        # Creating list of IDs citing the selected work 
        parents = v.predecessors()
        p_ids = [i['name'] for i in parents]
        
        # Creating list of all combinations of citing works
        # Stored as a list of tuples
        tuples = list(itertools.combinations(p_ids, 2))
        tuples = [tuple(set(i)) for i in tuples]

        for t in tuples:

            if t not in bc_dict.keys():
                bc_dict[t] = {'co-cite': [], 'frequency': 0}
            
            bc_dict[t]['co-cite'].append(work_id)
            bc_dict[t]['frequency'] = len(bc_dict[t]['co-cite'])
    
    
    
    return bc_dict


def generate_bibcoupling_network(citation_network):
    
    """
    Generates a bibliometric coupling network from a citation network.
    
    Notes
    -----
    Is able to take igraph.Graph, Network, and NetworkX objects.
    """
    
    bc_dict = bibcoupling_dict(citation_network)
    
    v_count = len(citation_network.vs)
    v_attr_keys = citation_network.vs.attributes()

    v_attrs = {}
    for a in v_attr_keys:
        v_attrs[a] = citation_network.vs[a]
    
    g = Graph(n=v_count, directed=False, vertex_attrs=v_attrs)

    for k in bc_dict.keys():
    
        v1_id = k[0]
        v2_id = k[1]
        freq = bc_dict[k]['frequency']
        cocite = bc_dict[k]['co-cite']

        if v1_id not in g.vs['name']:
              g.add_vertex(name=v1_id)
        
        if v2_id not in g.vs['name']:
              g.add_vertex(name=v2_id)
        
        v1 = g.vs.find(name=v1_id)
        v1_index = v1.index

        v2 = g.vs.find(name=v2_id)
        v2_index = v2.index

        if (g.are_connected(v1_index, v2_index) == False) and (g.are_connected(v2_index, v1_index) == False):
             Graph.add_edges(g, 
                                        [(v1_index, v2_index)], 
                                        attributes={
                                           'name': f'{v1_id} <-> {v2_id}',
                                           'weight': freq,
                                           'both_cite': cocite
                                           })
        
        else:
             edgelist = list(g.es.select(_between= ([v1_index], [v2_index])))

             if len(edgelist) > 0:

                edge = edgelist[0]
                edge_index = edge.index

                old_cociting = g.es[edge_index]['both_cite']
                new_cociting = old_cociting + cocite
                new_cociting = list(set(new_cociting))
                new_freq = len(new_cociting)

                g.es[edge_index]['both_cite'] = new_cociting
                g.es[edge_index]['weight'] = new_freq
    
    
    
    return g

