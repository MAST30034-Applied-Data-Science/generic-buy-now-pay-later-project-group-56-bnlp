# import required libraries
from pyspark.sql import SparkSession, functions as F
import pandas as pd

import os
import argparse

# argparse
try: 
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help="Path to Processed Data")
    parser.add_argument("--N", help="Number of Merchants per Category")
    args = parser.parse_args()
    N = int(args.N)
except:
    N = 10

print('Loading Data...')
print('_' * 30)

# Create a spark session
spark = (
    SparkSession.builder.appName("BNPL Project")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.logConf", "false") 
    .getOrCreate()
)

# read in data
sdf = spark.read.parquet(args.path)

# select specified columns
cols = ['merchant_name', 'user_id', 'dollar_value', 'order_datetime',\
    'rate', 'category', 'subcategory', \
    'estimated_region_population_2021_sum',	\
    'persons_earners_2018-19_sum', 'mean_earnings_2018-19_avg', \
    'sum_earnings_2018-19_sum',	'median_earnings_2018-19_avg', \
    'med_age_earners_2018-19_avg']
sdf = sdf.select(cols)

def topNmerchants(sdf, categories, N):
    '''
    ranks the top N merchants from the given categories
    '''
    merchants = {}
    income_total = {}
    income_risk = {}
    counts = {}

    for category in categories:
        tdf = sdf.where(F.col('category') == category)
        tdf = tdf.groupBy('merchant_name').agg(
            F.sum('dollar_value').alias('income_total'),
            F.stddev('dollar_value').alias('income_deviation'),
            F.mean('rate').alias('rate'),
            F.count('merchant_name').alias('count_merchant'))\
        .withColumn('income_total', F.col('income_total') * F.col('rate'))\
        .withColumn('income_deviation', 
            F.col('income_deviation') * F.col('rate'))\
        .orderBy(F.col('count_merchant').desc(), 
            F.col('income_total').desc(), F.col('income_deviation').asc())\
        
        # save result as a list
        tdf = tdf.limit(N)
        merchants[category] = tdf.select('merchant_name')\
            .toPandas()['merchant_name'].to_list()
        
        # assuming that all income is Normally distributed
        income_total[category] = tdf.agg({'income_total': 'sum'})\
            .first()['sum(income_total)']
        income_risk[category] = tdf.agg({'income_deviation': 'sum'})\
            .first()['sum(income_deviation)']
        counts[category] = tdf.agg({'count_merchant': 'sum'})\
            .first()['sum(count_merchant)']

    return pd.DataFrame(merchants), income_total, income_risk, counts

categories = ['retail_and_wholesale_trade', 'rental_hiring_and_real_estate',\
    'arts_and_recreation', 'info_media_and_telecommunications', 'others']

print('Ranking...')
print('_' * 30)

# run ranking system
merchants, total, risk, counts = topNmerchants(sdf, categories, N=N)

os.system('clear')

# print results
print('\t' * 9, f'Top {N} Merchants per Category\n')
print(merchants)