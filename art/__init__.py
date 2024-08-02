
from .importers.crossref import lookup_doi, lookup_dois, lookup_journal, lookup_journals, search_journals, get_journal_entries, search_journal_entries, lookup_funder, lookup_funders, search_funders, get_funder_works, search_funder_works
from .importers.crossref import search_works as search_crossref
from .importers.wos import search as search_wos
from .importers.scopus import search as search_scopus, lookup as lookup_scopus
from .importers.orcid import lookup_orcid, search as search_orcid
from .importers.search import search as api_search
from .importers import pdf, orcid, crossref, scopus, jstor, wos
from .classes import Results, References, Author, Authors, Funder, Funders, Affiliation, Affiliations, Review
from .classes.networks import Network, Networks
from .classes.citation_crawler import academic_scraper as scrape

import pickle

 def open_file(file_address: str = 'request_input'): # type: ignore

        if file_address == 'request_input':
            file_address = input('File address: ')
        
        if (file_address.endswith('.txt')) or (file_address.endswith('.review')):
            with open(file_address, 'rb') as f:
                review = pickle.load(f)
        
            return review # type: ignore