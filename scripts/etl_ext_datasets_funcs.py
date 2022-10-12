# import
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructType, IntegerType

# Create a spark session
spark = (
    SparkSession.builder.appName("BNPL_EXTDS_ETL")
    .config("spark.sql.repl.eagerEval.enabled", True)
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)

# define file names
INCOME_SDF_PATH = 'income_by_sa2.parquet'
POSTCODE_SDF_PATH = 'postcode_SA2_data.csv'
POPULATION_PATH = 'population_data.csv'

# define subset of postcode needed
POSTCODES_SUBSET = ['postcode', 'SA2_MAINCODE_2016']


def load_income_from_csv(path):
    '''
    saves the income csv file from the given path as a spark dataframe
    '''

    # manually create the schema to deal with duplicate column names
    schema = StructType() \
        .add("SA2", StringType(), True) \
        .add("SA2_NAME", StringType(), True) \
        .add("persons_earners_2014-15", StringType(), True) \
        .add("persons_earners_2015-16", StringType(), True) \
        .add("persons_earners_2016-17", StringType(), True) \
        .add("persons_earners_2017-18", StringType(), True) \
        .add("persons_earners_2018-19", StringType(), True) \
        .add("med_age_earners_2014-15", StringType(), True) \
        .add("med_age_earners_2015-16", StringType(), True) \
        .add("med_age_earners_2016-17", StringType(), True) \
        .add("med_age_earners_2017-18", StringType(), True) \
        .add("med_age_earners_2018-19", StringType(), True) \
        .add("sum_earnings_2014-15", StringType(), True) \
        .add("sum_earnings_2015-16", StringType(), True) \
        .add("sum_earnings_2016-17", StringType(), True) \
        .add("sum_earnings_2017-18", StringType(), True) \
        .add("sum_earnings_2018-19", StringType(), True) \
        .add("median_earnings_2014-15", StringType(), True) \
        .add("median_earnings_2015-16", StringType(), True) \
        .add("median_earnings_2016-17", StringType(), True) \
        .add("median_earnings_2017-18", StringType(), True) \
        .add("median_earnings_2018-19", StringType(), True) \
        .add("mean_earnings_2014-15", StringType(), True) \
        .add("mean_earnings_2015-16", StringType(), True) \
        .add("mean_earnings_2016-17", StringType(), True) \
        .add("mean_earnings_2017-18", StringType(), True) \
        .add("mean_earnings_2018-19", StringType(), True)

    # read in csv conforming to custom schema
    income_sdf = spark.read.format("csv") \
        .option("header", False) \
        .schema(schema) \
        .load(path + 'income_data_raw.csv')
    # remove header
    income_sdf = income_sdf.where(income_sdf['SA2'] != "SA2")

    return income_sdf

def write_inc_to_pq(income_sdf, path):
    '''
    saves the income spark dataframe as a parquet file
    '''
    income_sdf.write.mode('overwrite').parquet(path + INCOME_SDF_PATH)
    print("Wrote to ", path + INCOME_SDF_PATH)
    pass

def read_postcodes(path):
    '''
    reads the postcode-SA2 correspondence file from the given path 
    and saves it as a spark dataframe
    '''

    postcodes = spark.read.options(header=True) \
        .csv(path + POSTCODE_SDF_PATH)

    # select useful subset for linking
    postcodes = postcodes.select(*POSTCODES_SUBSET)
    # rename clashing col names
    postcodes = postcodes.withColumnRenamed('SA2_MAINCODE_2016', 'sa2_code')
    return postcodes

def read_population(path):
    '''
    reads the population file from the given path and
    returns it as a spark dataframe
    '''
    population = spark.read.option("header", True)\
        .csv(path + POPULATION_PATH)
    return population

def etl_income(path):
    '''
    reads the income data from the given path and
    aggregrates it by postcode. Returns a spark dataframe
    '''

    # get sdfs
    postcodes = read_postcodes(path)
    income_sdf = load_income_from_csv(path) 

    # join income and postcode dataset by SA2 maincode
    inc_joined = postcodes \
        .join(income_sdf, \
            postcodes['sa2_code'] == income_sdf['SA2'], "left") \
        .na.drop() \
        .distinct()

    # n.b postcodes can have multiple SA2 codes & SA2 codes 
    # can have multiple postcodes
    # aggregate by postcode ->
    agg_by_postcode_income = inc_joined \
        .groupBy('postcode') \
        .agg(
        F.sum('persons_earners_2018-19')\
            .alias('persons_earners_2018-19_sum'),
        F.mean('mean_earnings_2018-19')\
            .alias('mean_earnings_2018-19_avg'),
        F.sum('sum_earnings_2018-19')\
            .alias('sum_earnings_2018-19_sum'),
        F.mean('median_earnings_2018-19')\
            .alias('median_earnings_2018-19_avg'),
        F.mean('med_age_earners_2018-19')\
            .alias('med_age_earners_2018-19_avg')
    ) \
        .orderBy('postcode')

    return agg_by_postcode_income

def etl_population(path):
    '''
    runs ETL for the population dataset from the given path
    '''
    # read in the population/postcode data from file
    pop_sdf = read_population(path)
    postcodes = read_postcodes(path)

    # preprocess the df
    pop_sdf = population_preprocess(pop_sdf)

    # join the population data with postcodes
    population_joined = pop_sdf.join(postcodes,
                                     pop_sdf['sa2_maincode_2016'] \
                                        == postcodes['sa2_code'],
                                     "left") \
                                .na.drop() \
                                .distinct()

    # aggregate population data by postcode
    agg_by_postcode_population = population_joined \
        .groupBy('postcode') \
        .agg(
        F.sum('estimated_region_population_2021').\
            alias('estimated_region_population_2021_sum'),
    ) \
        .orderBy('postcode')

    return agg_by_postcode_population

def population_preprocess(data):
    '''
    preprocesses the population dataset
    '''

    # select columns
    cols_to_keep = ['sa2_maincode_2016', 'sa2_name_2016', 'erp_2021']
    population_df = data.select(*cols_to_keep)

    # cast erp_2021 as integer
    population_df = population_df \
        .withColumn("erp_2021", F.col('erp_2021').cast(IntegerType()))

    # rename columns
    population_df = population_df \
        .withColumnRenamed('sa2_name_2016', 'suburb') \
        .withColumnRenamed('erp_2021', 'estimated_region_population_2021')

    # remove negative population values
    population_df = population_df\
        .filter(F.col('estimated_region_population_2021') > 0)

    return population_df

def join_ext_with_master(income_sdf, pop_sdf, transactions):
    '''
    joins the income dataset with consumer transactions dataset
    '''

    # rename postcode cols before joining
    pop_sdf = pop_sdf.withColumnRenamed('postcode', 'postcode_pset')
    income_sdf = income_sdf.withColumnRenamed('postcode', 'postcode_iset')

    pop_sdf = pop_sdf\
        .withColumn('postcode_pset', \
            pop_sdf['postcode_pset'].cast('int'))
    income_sdf = income_sdf\
        .withColumn('postcode_iset', \
            income_sdf['postcode_iset'].cast('int'))

    transactions = transactions\
        .withColumn('postcode', transactions['postcode'].cast('int'))

    # join datasets
    transactions_ = transactions \
        .join(pop_sdf, \
            transactions['postcode'] == pop_sdf['postcode_pset'],\
            how='left') \
        .drop('postcode_pset') \
        .join(income_sdf, \
            transactions['postcode'] == income_sdf['postcode_iset'],\
            how='left') \
        .drop('postcode_iset')
    return transactions_