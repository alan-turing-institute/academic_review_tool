from ..utils.basics import results_cols
from ..importers.crossref import references_to_df
from .results import Results

import pandas as pd
import numpy as np


class References(Results):

    """
    This is a References DataFrame. It is a modified Pandas Dataframe object designed to store citations, references, and links associated with a published work.
    
    Parameters
    ----------
    dataframe : pandas.DataFrame
        a Pandas DataFrame to convert to a Results DataFrame. Defaults to None.
    index : list
        list of indices for Results DataFrame. Defaults to an empty list.

    Attributes
    ----------
    T : pandas.DataFrame
    _AXIS_LEN : int
    _AXIS_ORDERS : list
    _AXIS_TO_AXIS_NUMBER : dict
    _HANDLED_TYPES : tuple
    __annotations__ : dict
    __array_priority__ : int
    _attrs : dict
    _constructor : type
    _constructor_sliced : type
    _hidden_attrs : frozenset
    _info_axis : pandas.Index
    _info_axis_name : str
    _info_axis_number : int
    _internal_names : set
    _is_homogeneous_type : bool
    _is_mixed_type : bool
    _is_view : bool
    _item_cache : dict
    _metadata : list
    _series : dict
    _stat_axis : pandas.Index
    _stat_axis_name : str
    _stat_axis_number : int
    _typ : str
    _values : numpy.ndarray
    attrs : dict
    axes : list
    columns : pandas.Index
    dtypes : pandas.Series
    empty : bool
    flags : pandas.Flags
    index : pandas.Index
    ndim : int
    shape : tuple
    size : numpy.int64
    values : numpy.ndarray

    Columns
    -------
    * **work_id**: a unique identifier assigned to each result.
    * **title**: the result's title.
    * **authors**: any authors associated with the result.
    * **date**: any date(s) or year(s) associated with the result.
    * **source**: the name of the journal, conference, book, website, or other publication in which the result is contained (if any).
    * **type**: result type (e.g. article, chapter, book, website).
    * **editors**:  any authors associated with the result.
    * **publisher**: the name of the result's publisher (if any).
    * **publisher_location**: any locations or addresses associated with the result's publisher.
    * **funder**:  any funders associated with the result.
    * **keywords**:  any keywords associated with the result.
    * **abstract**: the result's abstract (if available).
    * **description**: the result's abstract (if available).
    * **extract**: the result's extract (if available).
    * **full_text**: the result's full text (if available).
    * **access_type**: the result's access type (e.g. open access, restricted access)
    * **authors_data**: unformatted data on any authors associated with the result.
    * **author_count**: the number of authors associated with the result.
    * **author_affiliations**: any affiliations associated with the result's authors.
    * **editors_data**: unformatted data on any editors associated with the result.
    * **citations**: any citations/references/links associated with the result.
    * **citation_count**: the number of citations/references/links associated with the result.
    * **citations_data**: unformatted data on any citations/references/links associated with the result.
    * **cited_by**: a list of publications that cite/reference/link to the result.
    * **cited_by_count**: the number of publications that cite/reference/link to the result.
    * **cited_by_data**: unformatted data on publications that cite/reference/link to the result.
    * **recommendations**: data on recommended publications associated with the result.
    * **crossref_score**: a bibliometric score assigned by CrossRef (if available).
    * **repository**: the repository from which the result was retrieved (if available).
    * **language**: the language(s) of the result.
    * **doi**: the Digital Object Identifier (DOI) assigned to the result.
    * **isbn**: the International Standard Book Number (ISBN) assigned to the result.
    * **issn**: the International Standard Serial Number (ISSN) assigned to the result or its source.
    * **pii**: any Publisher Item Identifiers (PII) assigned to the result.
    * **scopus_id**: the Scopus identifier assigned to the result.
    * **wos_id**: the Web of Science (WoS) identifier assigned to the result.
    * **pubmed_id**: the PubMed Identifier (PMID) assigned to the result.
    * **other_ids**: any other identifiers assigned to the result.
    * **link**: a URL or other link to the result.
    """

    def __init__(self, dataframe = None, index = []):
        
        """
        Initialises References instance.
        
        Parameters
        ----------
        dataframe : pandas.DataFrame
            a Pandas DataFrame to convert to a Results DataFrame. Defaults to None.
        index : list
            list of indices for Results DataFrame. Defaults to an empty list.
        """

        
        # Inheriting methods and attributes from References class
        super().__init__()
            
        self.replace(np.nan, None)
    
    def __repr__(self):

        """
        Defines how References objects are represented in string form.
        """

        return f'References object containing {len(self)} items'

    def from_dataframe(dataframe): # type: ignore
        
        """
        Converts a Pandas DataFrame to a ReReferencesults object.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            a Pandas DataFrame to convert to a References object.
        drop_duplicates : bool
            whether to remove duplicated rows. Defaults to False.
        drop_empty_rows : bool
            whether to remove rows which do not contain any data. Defaults to False.
        
        Returns
        -------
        results_table : References
            a References object.
        """

        dataframe = dataframe.copy(deep=True).reset_index().drop('index', axis=1)
        results_table = References(index = dataframe.index)
        results_table.columns = results_table.columns.astype(str).str.lower().str.replace(' ', '_')
        dataframe.columns = dataframe.columns.astype(str).str.lower().str.replace(' ', '_')

        for c in dataframe.columns:
            results_table[c] = dataframe[c]
        
        return results_table



def is_formatted_reference(item):

    """
    Returns True if the object is a Reference instance; else, returns False.
    """

    if type(item) == References:
        return True
    else:
        return False


def format_references(references_data, add_work_ids = True, update_from_doi = False):

    """
        Formats a collection of citations, references, and/or links as a References object.

        Parameters
        ----------
        references_data : object
            a collection of citations, references, and/or links.
        add_work_ids : bool
            whether to assign unique identifiers (work IDs). Defaults to True.
        update_from_doi : bool
            whether to update the References data using the CrossRef API. Defaults to False.
    """

    refs = References()

    if type(references_data) == References:
        refs = references_data
        refs.refs_count = None

    
    if (type(references_data) != References) and (type(references_data) != str) and (type(references_data) != list) and (type(references_data) != dict):
        refs.refs_count = None
        return refs

    if (type(references_data) == list) and (len(references_data) > 0) and (type(references_data[0]) == dict):
        
        if 'count' in references_data[0].keys():
            count = references_data[0]['count']
        else:
            count = None

        df = references_to_df(references_data, update_from_doi = update_from_doi)
        df.replace({np.nan: None})
        refs = References.from_dataframe(df) # type: ignore
        refs.refs_count = count

        if 'db' in refs.columns:
            refs = refs.drop('db', axis=1)
        
        if 'count' in refs.columns:
            refs = refs.drop('count', axis=1)
        
        refs = refs.dropna(axis=0, how='all').reset_index().drop('index', axis=1)
        refs = References.from_dataframe(dataframe=refs) # type: ignore

        if add_work_ids == True:
            refs.generate_work_ids() # type: ignore
    
    if (type(references_data) == list) and (len(references_data) > 0) and (type(references_data[0]) == str):
        df = pd.DataFrame(columns=results_cols, dtype=object)
        df['link'] = pd.Series(references_data, dtype=object)
        df.replace({np.nan: None})

        refs = References.from_dataframe(df) # type: ignore
        if add_work_ids == True:
            refs.generate_work_ids()

        refs.refs_count = None


    return refs

