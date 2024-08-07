"""
Useful datasets for reference and analysis

Notes
-----
Corpus extracted from names_dataset module + NLTK module. Stored locally for efficiency. See https://pypi.org/project/names-dataset/ for source of first and last names; nltk.corpus.names for source of nltk names
"""
import json
from pathlib import Path

here = str(Path(__file__).parent)

with open(f'{here}/names/all_personal_names.txt', 'r', encoding='utf-8') as file:
    all_personal_names = file.read()
    file.close()
all_personal_names = all_personal_names.replace("'", "").split(', ')

with open(f'{here}/names/first_names.txt', 'r', encoding='utf-8') as file:
    first_names = file.read()
    file.close()
first_names = first_names.replace("'", "").split(', ')

with open(f'{here}/names/last_names.txt', 'r', encoding='utf-8') as file:
    last_names = file.read()
    file.close()
last_names = last_names.replace("'", "").split(', ')

with open(f'{here}/names/nltk_names.txt', 'r', encoding='ascii') as file:
    nltk_names = file.read()
    file.close()
nltk_names = nltk_names.replace("'", "").split(', ')

# Corpus extracted from country_list module. Stored locally for efficiency.

with open(f'{here}/countries/countries_all.txt', 'r', encoding='utf-8') as file:
    countries_all = file.read()
    file.close()
countries_all = countries_all.replace("'", "").split(', ')

with open(f'{here}/countries/country_names.json', 'r', encoding='utf-8') as file:
    country_names = json.load(file)
    file.close()

# Corpus extracted from geonamescache module. Stored locally for efficiency.
with open(f'{here}/cities/cities_all.txt', 'r', encoding='utf-8') as file:
    cities_all = file.read()
    file.close()
cities_all = cities_all.replace("'", "").split(', ')

with open(f'{here}/cities/cities_en.json', 'r', encoding='ascii') as file:
    cities_en = json.load(file)
    file.close()

# Corpus extracted from language_data and langcodes modules. Stored locally for efficiency.

with open(f'{here}/languages/language_names.json', 'r', encoding='ascii') as file:
    language_names = json.load(file)
    file.close()

with open(f'{here}/languages/languages_en.json', 'r', encoding='ascii') as file:
    languages_en = json.load(file)
    file.close()

with open(f'{here}/languages/language_codes.txt', 'r', encoding='ascii') as file:
    language_codes = file.read()
    file.close()

from .stopwords.stopwords import stopwords_dict as stopwords
html_stopwords = stopwords['html']
en_stopwords = stopwords['en']

# Corpus extracted from language_data and langcodes modules. Stored locally for efficiency.

from nltk import download


# Importing NLTK's Word Lists corpus as an NLTK text

try:
    from nltk.corpus import words as nltk_wordlists
    nltk_wordlists.words()
except:
    download('words')
    from nltk.corpus import words as nltk_wordlists


# Importing Swadesh corpus as an NLTK text

try:
    from nltk.corpus import swadesh as nltk_swadesh
    nltk_swadesh.words()
except:
    download('swadesh')
    from nltk.corpus import swadesh as nltk_swadesh

    
# Importing NLTK's Web Text corpus
try:
    from nltk.corpus import webtext as nltk_webtext
    nltk_webtext.words()
except:
    download('webtext')
    from nltk.corpus import webtext as nltk_webtext
      
    
# Importing WordNet 3.0 corpus

try:
    from nltk.corpus import wordnet as nltk_wordnet
    nltk_wordnet.words()
except:
    download('wordnet')
    from nltk.corpus import wordnet as nltk_wordnet

# Creating useful dictionaries of countries in major languages

countries_zh = country_names['zh']
countries_ar = country_names['ar']
countries_es = country_names['es']
countries_hi = country_names['hi']
countries_pt = country_names['pt']
countries_ru = country_names['ru']
countries_fr = country_names['fr']

languages_all = []
for l in language_names.values():
    languages_all = languages_all + list(l.values())

languages_all = list(set(languages_all))


# Extracting NLTK Words Lists as a list of words
nltk_words_list = nltk_wordlists.words()

# Extracting NLTK Web Text  as a list of words
nltk_webtext_words = nltk_webtext.words()