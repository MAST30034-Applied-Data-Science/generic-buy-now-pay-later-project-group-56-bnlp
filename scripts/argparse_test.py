import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--path", help="path to the datasets")

args = parser.parse_args()
# should be "generic-buy-now-pay-later-project-group-56-bnlp/data/tables/"
merchant_path = args.path

from pyspark.sql import SparkSession
spark = (
    SparkSession.builder.appName("BNPL Project")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)

merchantdf = spark.read.parquet(merchant_path + "tbl_merchants.parquet")
merchantdf.show(1, vertical=True)