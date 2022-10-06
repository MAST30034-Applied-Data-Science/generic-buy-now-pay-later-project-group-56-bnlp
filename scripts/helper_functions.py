'''
This script defines the function to assign 
categories and sub-categories for the merchant 
for the etl script.
'''

# Importing libraries
import nltk
from nltk.corpus import wordnet
from nltk.stem.wordnet import WordNetLemmatizer

# define 
wordnet_lemmatizer = WordNetLemmatizer()
industry_dict = {'agriculture': ['farmer', 'nurseries', 'flower', 'garden',
                                'lawn'],
                'arts_and_recreation': ['art', 'musician', 'artist',
                                        'performer', 'gambling', 'casino',
                                        'craft'],
                'info_media_and_telecommunications': ['magazine', 'book', 
                                                'newspaper', 'information', 
                                                'technology', 'digital', 
                                                'telecommunication', 'online', 
                                                'computer', 'radio', 'tv'],
                'rental_hiring_and_real_estate': ['rent', 'machine', 
                                                'machinery'],
                'retail_and_wholesale_trade': ['supply', 'supplier', 'shop',
                                            'food', 'clothing', 'equipment', 
                                            'footwear', 'textiles', 
                                            'accessories', 'furniture', 'fuel', 
                                            'cosmetic', 'pharmaceuticals']}
industry_lst = ['rental_hiring_and_real_estate', 'retail_and_wholesale_trade', 
                'agriculture', 'arts_and_recreation',  
                'info_media_and_telecommunications']

retail_dict = {'food_retailing': ['food', 'grocery', 'liquor', 'poultry',
                                'lawn'],
                'household_goods_retailing': ['furniture', 'textile', 
                                            'houseware', 'electrical', 
                                            'electronic', 'computer', 
                                            'digital'],
                'clothing_footwear__personal_accessory_retailing': 
                ['clothing', 'footwear', 'accessories', 'furniture', 
                'cosmetic', 'watch', 'jewellery'],
                'department_stores': ['store', 'department']}


def get_synonyms(words):

    '''
    Takes a word and return its synomn defined in the dictionary
    '''

    synonyms = []

    for word in words:
        for synset in wordnet.synsets(word):
            for lemma in synset.lemmas():
                synonyms.append(lemma.name())

    return synonyms

def subcategory(data):

    '''
    Takes data and return subcategory related to the data
    '''

    tokens = nltk.word_tokenize(data)
    lemmen_words = [wordnet_lemmatizer.lemmatize(word, pos="v") 
                    for word in tokens if word != ',']

    for subcategory in retail_dict.keys():

        synonyms = get_synonyms(retail_dict[subcategory])

        if (len(set(lemmen_words).intersection(set(synonyms))) != 0):
            return subcategory

    return 'others_retailing'

def assign_category(data):

    '''
    Takes data and return category related to the data
    '''

    tokens = nltk.word_tokenize(data)
    lemmen_words = [wordnet_lemmatizer.lemmatize(word, pos="v") 
                    for word in tokens if word != ',']

    for category in industry_lst:

        synonyms = get_synonyms(industry_dict[category]) 

        if (category == 'retail_and_wholesale_trade'):

            if ((len(set(lemmen_words).intersection(set(synonyms))) != 0 ) or 
                ('goods' in set(lemmen_words))):
                return category
        
        else:

            if (len(set(lemmen_words).intersection(set(synonyms))) != 0):
                return category

    return 'others'

def assign_subcategory(data, category):

    '''
    Takes data and checks if category is 'retail_and_wholesale_trade' 
    then return the subcategory related to the data
    '''

    if (category == 'retail_and_wholesale_trade'):
        return subcategory(data)