""" 
This script transforms the raw data into clean data
and saves it under 'data/curated' directory
"""

#Importing required libraries

from decouple import config
from pyspark.sql import SparkSession, Window, functions as F
import nltk
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('omw-1.4')
from nltk.corpus import wordnet
from nltk.stem.wordnet import WordNetLemmatizer

import geopandas
import folium
import io
import os
import requests
import zipfile
import pandas as pd
from urllib.request import urlretrieve
from owslib.wfs import WebFeatureService

# import functions from other scripts
from etl_ext_datasets_funcs import etl_income
from etl_ext_datasets_funcs import etl_population
from etl_ext_datasets_funcs import join_ext_with_master
from helper_functions import *
from fraud_model_funcs import get_fraud_df, get_models
from pyspark.ml.regression import GBTRegressionModel

# argparse
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--path", help="path to the datasets")
parser.add_argument("--output", help="path to output file")
parser.add_argument("--models", help="path to the models folder")
parser.add_argument("--train_model", help="1 for training the model, 0 for using pre-trained")
args = parser.parse_args()
input_path = args.path
output_path = args.output
model_path = args.models
is_train = args.train_model


# Create a spark session
spark = (
    SparkSession.builder.appName("BNPL Project")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)

# Loading all data sets
merchants = spark.read.parquet(input_path + "/tbl_merchants.parquet")
merchants_fraud_prob = spark.read.csv(input_path + 
    "/merchant_fraud_probability.csv", sep = ',', header=True)
consumer = spark.read.csv(input_path + "/tbl_consumer.csv", 
    sep = '|', header=True)
consumer_fraud_prob = spark.read.csv(input_path + 
    "/consumer_fraud_probability.csv", sep = ',', header=True)
userdetails = spark.read.parquet(input_path + 
    "/consumer_user_details.parquet")
transaction_batch1 = spark.read.parquet(input_path + 
    "/transactions_20210228_20210827_snapshot/")
transaction_batch2 = spark.read.parquet(input_path + 
    "/transactions_20210828_20220227_snapshot/")
transaction_batch3 = spark.read.parquet(input_path +
    "/transactions_20220228_20220828_snapshot/")


# EXTRACT ALL EXTERNAL DATASETS

# Location of root directory.
root_dir = input_path + '/'

# Create "external_datasets" folder under the root directory where all the 
# external data will be stored.
external_data_dir = 'external_datasets'

# Prevent error if directory already exists.
if not os.path.exists(root_dir + external_data_dir):
    os.makedirs(root_dir + external_data_dir)

path = root_dir + external_data_dir + '/'


# 1a) Postcode and SA2 data:
url = "https://www.matthewproctor.com/Content/postcodes/" + \
    "australian_postcodes.csv"
r = requests.get(url)
target_dir = path + 'postcode_SA2_data.csv'

with open(target_dir, 'wb') as outfile:
    outfile.write(r.content)
    outfile.close()


# 1b) Total income 2014-2019 excel file:
url = "https://www.abs.gov.au/statistics/labour/" + \
    "earnings-and-working-conditions/personal-income-australia/" + \
    "2014-15-2018-19/6524055002_DO001.xlsx"
r = requests.get(url)
target_dir = path + 'income_data.xlsx'

with open(target_dir, 'wb') as outfile:
    outfile.write(r.content)
    outfile.close()


# 1c) Australian state shapefiles:
url = "https://www.abs.gov.au/statistics/standards/" + \
    "australian-statistical-geography-standard-asgs-edition-3/" + \
    "jul2021-jun2026/access-and-downloads/digital-boundary-files/" + \
    "STE_2021_AUST_SHP_GDA2020.zip"
target_dir = path + 'state_data.zip'
urlretrieve(url, target_dir)

# unzip state_data.zip
with zipfile.ZipFile(target_dir,"r") as zip_ref:
    zip_ref.extractall(path + "state_data")


# 1d) Australian post-code shapefiles:
url = "https://www.abs.gov.au/statistics/standards/" + \
    "australian-statistical-geography-standard-asgs-edition-3/" + \
    "jul2021-jun2026/access-and-downloads/digital-boundary-files/" + \
    "POA_2021_AUST_GDA94_SHP.zip"
target_dir = path + 'postcode_data.zip'
urlretrieve(url, target_dir)

# unzip state_data.zip
with zipfile.ZipFile(target_dir,"r") as zip_ref:
    zip_ref.extractall(path + "postcode_data")


# 2) API call:
# Set up API connection.


WFS_USERNAME = config('username', default='')
WFS_PASSWORD = config('password', default='')

# WFS_USERNAME = 'xrjps'
# WFS_PASSWORD = 'Jmf16l4TcswU3Or7'
WFS_URL='https://adp.aurin.org.au/geoserver/wfs'

adp_client = WebFeatureService(url=WFS_URL,username=WFS_USERNAME, 
    password=WFS_PASSWORD, version='2.0.0')

# Extract population 2001-2021 data and store into external dataset 
# folder directory.
response = adp_client.getfeature(typename="datasource-AU_Govt_ABS-" + \
    "UoM_AURIN_DB_3:abs_regional_population_sa2_2001_2021", 
    outputFormat='csv')
target_dir = path + 'population_data.csv'

out = open(target_dir, 'wb')
out.write(response.read())
out.close


# read in processed external datasets
# ED1: Estimated Region Population by SA2 Districts, 2021
population = etl_population(path) 
# ED2: Income and age statistics by SA2 region
income = etl_income(path) 

"""
Renaming columns, cleaning column
and add columns accordingly
"""

# Merchant data
merchants = merchants.withColumnRenamed("name", "merchant_name")\
                    .withColumn("tags", 
                        F.expr("substring(tags, 3, length(tags) - 4)"))

merchants = merchants.withColumn('tag', F.split(F.regexp_replace('tags', 
                    '\), \(|], \[', ";"), ";").getItem(0))\
                .withColumn('revenue', F.split(F.regexp_replace('tags', 
                    '\), \(|], \[', ";"), ";").getItem(1))\
                .withColumn('rate', F.split(F.regexp_replace('tags', 
                    '\), \(|], \[', ";"), ";").getItem(2))\
                .withColumn('rate', F.split('rate', ': ').getItem(1))

merchants = merchants.withColumn("tag", F.lower(F.col("tag")))
merchants = merchants.withColumn("tag", F.regexp_replace(F.col("tag"), 
    " +", " "))

merchants = merchants.select("merchant_name", "merchant_abn", "tag", 
    "revenue", "rate")
merchants_pd = merchants.toPandas()
merchants_pd['category'] = merchants_pd['tag'].apply(assign_category)
merchants_pd['subcategory'] = merchants_pd.apply(
                                lambda row: assign_subcategory(
                                    row['tag'], row['category']), axis = 1)

# Merchant fraud Data
merchants_fraud_prob = merchants_fraud_prob.withColumnRenamed('merchant_abn', 
                                            'abn')\
                                        .withColumnRenamed('order_datetime', 
                                            'datetime')\
                                        .withColumnRenamed(
                                            'fraud_probability', 
                                            'merchant_fraud_probability')

# Consumer Data
consumer = consumer.select("state", "postcode", "gender", "consumer_id")

# Consumer Fraud Data
consumer_fraud_prob = consumer_fraud_prob.withColumnRenamed('user_id', 
                                            'user')\
                                        .withColumnRenamed('order_datetime', 
                                            'user_datetime')\
                                        .withColumnRenamed(
                                            'fraud_probability', 
                                            'user_fraud_probability')

# Transaction Data (merging transaction batches together)
transaction_join1 = transaction_batch1.union(transaction_batch2)
transaction_join2 = transaction_join1.union(transaction_batch3)
transactions = transaction_join2.withColumn('dollar_value', 
    F.round('dollar_value',2))

# Merging all dataset into one dataset
result = transactions.join(userdetails, on="user_id", how="left")
result = result.join(consumer, on="consumer_id", how="left")
result = result.join(spark.createDataFrame(merchants_pd), 
    on="merchant_abn", how="left")
result = result.join(merchants_fraud_prob, 
                    (result["merchant_abn"] == merchants_fraud_prob["abn"]) &
                    (result["order_datetime"] == 
                        merchants_fraud_prob["datetime"]), how= 'left')\
                    .drop('abn', 'datetime')
result = result.join(consumer_fraud_prob, (result["user_id"] == 
                        consumer_fraud_prob["user"]) &
                    (result["order_datetime"] == 
                        consumer_fraud_prob["user_datetime"]), how= 'left')\
                    .drop('user', 'user_datetime')

# join external datasets with master
result = join_ext_with_master(income, population, result)

# Pre processing steps on the joined dataset

# merchant_abn, consumer_id and user_id should be positive numbers
result = result.filter(F.col('merchant_abn') > 0)
result = result.filter(F.col('consumer_id') > 0)
result = result.filter(F.col('user_id') > 0)

# dollar_value should be positive
result = result.filter(F.col('dollar_value') > 0)

# Remove NULL values for order_id, order_datetime, state, merchant_name
# and tag
result = result.filter(F.col('order_id').isNotNull())
result = result.filter(F.col('order_datetime').isNotNull())
result = result.filter(F.col('state').isNotNull())
result = result.filter(F.col('merchant_name').isNotNull())
result = result.filter(F.col('tag').isNotNull())

# postcode should be between 200 and 9999 inclusive
result = result.filter(F.col('postcode').cast("integer") >= 200)
result = result.filter(F.col('postcode').cast("integer") <= 9999)

# gender should be Male, Female or Undisclosed
result = result.filter((F.col("gender") == "Male")|
    (F.col("gender") == "Female")|(F.col("gender") == "Undisclosed"))

# revenue level should be a, b, c, d or e
result = result.filter((F.col("revenue") == "a")|
    (F.col("revenue") == "b")|(F.col("revenue") == "c")|
    (F.col("revenue") == "d")|(F.col("revenue") == "e"))

# rate should be between 0 and 1
result = result.withColumn("rate", F.col("rate").cast("double"))
result = result.withColumn("rate", F.round(F.col("rate")/100, 4))
result = result.filter((F.col("rate") >= 0)&(F.col("rate") <= 1))


# drop duplicate orders
result = result.dropDuplicates(['order_id'])

# the probabilities must be between 0 and 1 and be floats.
result = result.withColumn('merchant_fraud_probability', 
    F.round(F.col('merchant_fraud_probability').cast('double')/100, 2))
result = result.withColumn('user_fraud_probability', 
    F.round(F.col('user_fraud_probability').cast('double')/100, 2))

# get models
if is_train == '1':
    # train the models
    uf_model, mf_model = get_models(result, model_path)
else:
    uf_model = GBTRegressionModel.load(model_path + '/user_fraud_model')
    mf_model = GBTRegressionModel.load(model_path + '/merchant_fraud_model')

# model fraudulent transactions and remove them from df
result = get_fraud_df(result,
                      uf_model=uf_model,
                      mf_model=mf_model).drop('isfraud')

# Writing data
print('Writing processed data to file...')
result.write.mode('overwrite').parquet(output_path + '/process_data.parquet')