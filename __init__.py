
from .importers.crossref import lookup_doi, lookup_dois, lookup_journal, lookup_journals, search_journals, get_journal_entries, search_journal_entries, lookup_funder, lookup_funders, search_funders, get_funder_works, search_funder_works
from .importers.crossref import search_works as search_crossref
from .importers.scopus import search as search_scopus
from .importers.orcid import lookup_orcid, search as search_orcid
from .importers import pdf, orcid, crossref, scopus, jstor
from .classes import Results, References, Author, Authors, Funder, Funders, Affiliation, Affiliations, Review
from .classes.networks import Network, Networks
from .classes.citation_crawler import academic_scraper as scrape