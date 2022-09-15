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
#different locations
merchants = spark.read.parquet("./data/tables/tbl_merchants.parquet")
merchants_frud_prob = spark.read.csv("./data/tables/merchant_fraud_probability.csv", sep = ',', header=True)
consumer = spark.read.csv("./data/tables/tbl_consumer.csv", sep = '|', header=True)
consumer_frud_prob = spark.read.csv("./data/tables/consumer_fraud_probability.csv", sep = ',', header=True)
userdetails = spark.read.parquet("./data/tables/consumer_user_details.parquet")
transaction_batch1 = spark.read.parquet("./data/tables/transactions_20210228_20210827_snapshot/")
transaction_batch2 = spark.read.parquet("./data/tables/transactions_20210828_20220227_snapshot/")
population = spark.read.option("header", True).csv('./data/tables/population_data.csv')
# transaction_batch3 = spark.read.parquet("./data/tables/<insert_folder_name>_snapshot/")

"""
Renaming columns, cleaning column
and add columns accordingly
"""

# External dataset 1: Estimated Region Population by SA2 Districts, 2021

# Function to pre-process population data
from pyspark.sql.types import IntegerType, LongType

def population_preprocess(data):

    cols_to_keep = ['sa2_maincode_2016', 'sa2_name_2016', 'erp_2021']
    population_df = data.select(*cols_to_keep)

    population_df = population_df \
                    .withColumn("erp_2021", F.col('erp_2021').cast(IntegerType()))

    population_df = population_df \
                    .withColumnRenamed('sa2_name_2016', 'suburb') \
                    .withColumnRenamed('erp_2021', 'estimated_region_population_2021')
                    
    return population_df

population = population_preprocess(population)

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

#function to assign category and subcategories
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

retail_dict = {'food_retailing': ['food', 'grocery', 'liquor', 'poultry', 'lawn'],
                'household_goods_retailing': ['furniture', 'textile', 'houseware', 'electrical', 'electronic', 'computer', 'digital'],
                'clothing_footwear__personal_accessory_retailing':  
                ['clothing', 'footwear', 'accessories', 'furniture', 'cosmetic', 'watch', 'jewellery'],
                'department_stores': ['store', 'department']}

def get_synonyms(words):

    synonyms = []

    for word in words:
        for synset in wordnet.synsets(word):
            for lemma in synset.lemmas():
                synonyms.append(lemma.name())

    return synonyms

def subcategory(data):
    tokens = nltk.word_tokenize(data)
    lemmen_words = [wordnet_lemmatizer.lemmatize(word, pos="v") for word in tokens if word != ',']

    for subcategory in retail_dict.keys():

        synonyms = get_synonyms(retail_dict[subcategory]) 

        if (len(set(lemmen_words).intersection(set(synonyms))) != 0):
            return subcategory

    return 'others_retailing'

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

def assign_subcategory(data, category):
    if (category == 'retail_and_wholesale_trade'):
        return subcategory(data)


merchants_pd = merchants.toPandas()
merchants_pd['category'] = merchants_pd['tag'].apply(assign_category)
merchants_pd['subcategory'] = merchants_pd.apply(
                                lambda row: assign_subcategory(
                                    row['tag'], row['category']), axis = 1)
 
# Merchant fraud Data
merchants_frud_prob = merchants_frud_prob.withColumnRenamed('merchant_abn', 'abn')\
                                        .withColumnRenamed('order_datetime', 'datetime')\
                                        .withColumnRenamed('fraud_probability', 'merchant_fraud_probability')

# Consumer Data
consumer = consumer.select("state", "postcode", "gender", "consumer_id")

#Consumer Fraud Data
consumer_frud_prob = consumer_frud_prob.withColumnRenamed('user_id', 'user')\
                                        .withColumnRenamed('order_datetime', 'user_datetime')\
                                        .withColumnRenamed('fraud_probability', 'user_fraud_probability')

# Transaction Data (merging transaction batches together)
transactions = transaction_batch1.union(transaction_batch2)
transactions = transactions.withColumn('dollar_value', F.round('dollar_value',2))

# Merging all dataset into one dataset
result = transactions.join(userdetails, on="user_id", how="left")
result = result.join(consumer, on="consumer_id", how="left")
result = result.join(spark.createDataFrame(merchants_pd), on="merchant_abn", how="left")
result = result.join(merchants_frud_prob, (result["merchant_abn"] == merchants_frud_prob["abn"]) &
                    (result["order_datetime"] == merchants_frud_prob["datetime"]), how= 'left')\
                    .drop('abn', 'datetime')
result = result.join(consumer_frud_prob, (result["user_id"] == consumer_frud_prob["user"]) &
                    (result["order_datetime"] == consumer_frud_prob["user_datetime"]), how= 'left')\
                    .drop('user', 'user_datetime')


# Loading data
result.write.mode('overwrite').parquet('./data/curated/process_data.parquet')

