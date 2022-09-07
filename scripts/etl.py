""" 
This script transforms the raw data into clean data
and saves it under 'data/curated' directory
"""

#Importing required libraries
from pyspark.sql import SparkSession, functions as F
import pandas as pd
import nltk
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('omw-1.4')
from nltk.corpus import wordnet
from nltk.stem.wordnet import WordNetLemmatizer

# Create a spark session
spark = (
    SparkSession.builder.appName("BNPL Project")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)

# Loading all data sets
merchants = spark.read.parquet("./data/tables/tbl_merchants.parquet")
consumer = spark.read.csv("./data/tables/tbl_consumer.csv", sep = '|', header=True)
userdetails = spark.read.parquet("./data/tables/consumer_user_details.parquet")
transaction_batch1 = spark.read.parquet("./data/tables/transactions_20210228_20210827_snapshot/")
transaction_batch2 = spark.read.parquet("./data/tables/transactions_20210828_20220227_snapshot/")
# transaction_batch3 = spark.read.parquet("./data/tables/<insert_folder_name>_snapshot/")

"""
Renaming columns, cleaning column
and add columns accordingly
"""

# Merchant data
merchants = merchants.withColumnRenamed("name", "merchant_name")\
                    .withColumn("tags", F.expr("substring(tags, 3, length(tags) - 4)"))

merchants = merchants.withColumn('tag', F.split(F.regexp_replace('tags', '\), \(|], \[', ";"), ";").getItem(0))\
                .withColumn('revenue', F.split(F.regexp_replace('tags', '\), \(|], \[', ";"), ";").getItem(1))\
                .withColumn('rate', F.split(F.regexp_replace('tags', '\), \(|], \[', ";"), ";").getItem(2))\
                .withColumn('rate', F.split('rate', ': ').getItem(1))

merchants = merchants.withColumn("tag", F.lower(F.col("tag")))
merchants = merchants.withColumn("tag", F.regexp_replace(F.col("tag"), " +", " "))

merchants = merchants.select("merchant_name", "merchant_abn", "tag", "revenue", "rate")

#function to assign category
wordnet_lemmatizer = WordNetLemmatizer()
industry_dict = {'agriculture': ['farmer', 'nurseries', 'flower', 'garden', 'lawn'], \
                'arts_and_recreation': ['art', 'musician', 'artist', 'performer', 'gambling', 'casino', 'craft'],\
                'info_media_and_telecommunications': 
                ['magazine', 'book', 'newspaper', 'information', 'technology', 'digital', 'telecommunication', 'online', 'computer', 'radio', 'tv'],\
                'rental_hiring_and_real_estate': ['rent', 'machine', 'machinery'],\
                'retail_and_wholesale_trade': 
                ['supply', 'supplier', 'shop', 'food', 'clothing', 'equipment', 'footwear', 'textiles', 'accessories', 'furniture', 
                'fuel', 'cosmetic', 'pharmaceuticals']}
industry_lst = ['rental_hiring_and_real_estate', 'retail_and_wholesale_trade', 'agriculture', 'arts_and_recreation', 
                'info_media_and_telecommunications']

def get_synonyms(words):

    synonyms = []

    for word in words:
        for synset in wordnet.synsets(word):
            for lemma in synset.lemmas():
                synonyms.append(lemma.name())

    return synonyms

def assign_category(data):

    tokens = nltk.word_tokenize(data)
    lemmen_words = [wordnet_lemmatizer.lemmatize(word, pos="v") for word in tokens if word != ',']

    for category in industry_lst:

        synonyms = get_synonyms(industry_dict[category]) 

        if (category == 'retail_and_wholesale_trade'):

            if ((len(set(lemmen_words).intersection(set(synonyms))) != 0 ) or ('goods' in set(lemmen_words))):
                return category
        
        else:

            if (len(set(lemmen_words).intersection(set(synonyms))) != 0):
                return category

    return 'others'

merchants_pd = merchants.toPandas()
merchants_pd['category'] = merchants_pd['tag'].apply(assign_category)
 
# Consumer Data
consumer = consumer.select("state", "postcode", "gender", "consumer_id")

# Transaction Data (merging transaction batches together)
transactions = transaction_batch1.union(transaction_batch2)
transactions = transactions.withColumn('dollar_value', F.round('dollar_value',2))

# Merging all dataset into one dataset
result = transactions.join(userdetails, on="user_id", how="left")
result = result.join(consumer, on="consumer_id", how="left")
result = result.join(spark.createDataFrame(merchants_pd), on="merchant_abn", how="left")

# Loading data
result.write.mode('overwrite').parquet('./data/curated/process_data.parquet')

