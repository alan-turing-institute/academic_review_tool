
"""
==========================
Academic Review Tool (ART)
==========================

Version: 1.1.0

The Academic Review Tool (ART) is a package for performing academic reviews and bibliometric analyses in Python. 
It offers capabilities for discovering, retrieving, and analysing academic literature at scale. 
ART accesses records from Crossref, Web of Science, Scopus, Orcid, and more.

ART's functionalities include:
* Searching for works using keywords, dates, authors, funders, and other information.
* Searching for authors and their works.
* Searching for funders and their funded works.
* Looking up DOIs, ISBNs, ISSNs, ORCID IDs, URLs, and other unique identifiers.
* Scraping academic repositories and websites.
* Citation and weblink crawling.
* Citation and coauthorship analysis.
* Generating networks representing:
    ** Citations and references
    ** Coauthors
    ** Cofunders
    ** And more...

ART uses the following APIs:
* Crossref
* Web of Science (Starter API)
* Scopus
* ORCID
* Geopy / Nominatim
"""

from .utils.basics import open_file as open
from .importers.crossref import lookup_doi, lookup_dois, lookup_journal, lookup_journals, search_journals, get_journal_entries, search_journal_entries, lookup_funder, lookup_funders, search_funders, get_funder_works, search_funder_works
from .importers.crossref import search_works as search_crossref
# from .importers.wos import search as search_wos
from .importers.scopus import search as search_scopus, lookup as lookup_scopus
from .importers.orcid import lookup_orcid, search as search_orcid
from .importers.search import search as api_search
# from .importers import pdf, orcid, crossref, scopus, jstor, wos
from .classes import Results, References, Author, Authors, Funder, Funders, Affiliation, Affiliations, Review
from .classes.networks import Network, Networks
from .classes.citation_crawler import academic_scraper as scrape