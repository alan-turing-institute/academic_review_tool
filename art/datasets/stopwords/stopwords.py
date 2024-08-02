from nltk import download
import pandas as pd

# Importing Stopwords corpus as an NLTK text
try:
    from nltk.corpus import stopwords as nltk_stopwords
    nltk_stopwords.words()
except:
    download('stopwords')
    from nltk.corpus import stopwords as nltk_stopwords
    
nltk_stopwords = list(nltk_stopwords.words())

with open('/Users/jhancock/Documents/Tool_dev/Investigative_data_analyser/Development/Current/idea/datasets/stopwords/en_stopwords.txt', 'r') as file:
    en_stopwords = file.read()
    file.close()
en_stopwords = en_stopwords.replace("'", "").split(', ')

en_stopwords_lower = pd.Series(en_stopwords).str.lower().to_list()
en_stopwords = list(set(en_stopwords_lower + en_stopwords))

with open('/Users/jhancock/Documents/Tool_dev/Investigative_data_analyser/Development/Current/idea/datasets/stopwords/html_stopwords.txt', 'r') as file:
    html_stopwords = file.read()
    file.close()
html_stopwords = html_stopwords.replace("'", "").split(', ')

html_stopwords_lower = pd.Series(html_stopwords).str.lower().to_list()
html_stopwords = list(set(html_stopwords_lower + html_stopwords))

combined = list(set(nltk_stopwords + en_stopwords))

stopwords_dict = {
                'nltk': nltk_stopwords,
                'en': en_stopwords,
                'en+nltk': combined,
                'html': html_stopwords
            }

all_stopwords = []
for key in stopwords_dict.keys():
    all_stopwords = all_stopwords + stopwords_dict[key]

stopwords_dict['all'] = all_stopwords