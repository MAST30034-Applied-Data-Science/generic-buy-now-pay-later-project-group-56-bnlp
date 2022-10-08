import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.regression import GBTRegressor
import numpy as np
import time

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

# Create a spark session
spark = (
    SparkSession.builder.appName("Predict fraud probabilities")
    .config("spark.sql.repl.eagerEval.enabled", True)
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)

# these are our predictors
base_features = {'dollar_value',
                 'gender',
                 'revenue',
                 'rate'}

target_features = {'user_fraud_probability', 'merchant_fraud_probability'}
subset = base_features.union(target_features)

categorical_cols = {'gender','revenue'}
continous_cols = {'dollar_value', 'rate'}

cols = ["user_fraud_prediction", "merchant_fraud_prediction"]

gbt_uf = GBTRegressor(labelCol='user_fraud_probability',
                      featuresCol="features",
                      maxIter=20,
                      maxDepth=10,
                      seed=56,
                      lossType='squared',
                      weightCol='weights',
                      predictionCol='user_fraud_prediction')


gbt_mf = GBTRegressor(labelCol='merchant_fraud_probability',
                      featuresCol="features",
                      maxIter=20,
                      maxDepth=10,
                      seed=56,
                      lossType='squared',
                      weightCol='weights',
                      predictionCol='merchant_fraud_prediction')


def get_models(data):
    # get the weightings of the data
    ud = get_weights(data, 'user_fraud_probability', 10)
    md = get_weights(data, 'merchant_fraud_probability', 5)

    # vectorize the dsets
    ud_v = get_dummy(ud,
                     list(categorical_cols),
                     list(continous_cols),
                     labelCol='user_fraud_probability')
    md_v = get_dummy(md,
                 list(categorical_cols),
                 list(continous_cols),
                 labelCol='merchant_fraud_probability')

    (ufTrainData, ufTestData) = ud_v.randomSplit([0.9, 0.1])
    (mfTrainData, mfTestData) = md_v.randomSplit([0.9, 0.1])

    uf_model = gbt_uf.fit(ufTrainData)
    mf_model = gbt_mf.fit(mfTrainData)


    return uf_model, mf_model


# returns vectorized dataset for the required columns
def vectorize_data(data):
    start = time.time()
    data_v = get_dummy(data,
                     continuousCols=list(continous_cols),
                     categoricalCols=list(categorical_cols),
                     master=True)

    print("Took {} seconds to vectorize {} rows".format(time.time() - start, data.count()))
    return data_v


# predicts frauds using the built models
def predict_frauds(data, uf_model, mf_model):
    # predict user probs from feature column
    data_ = mf_model.transform(data)
    data_ = uf_model.transform(data_)

    # drop features column
    data_ = data_.drop('features')
    return data_

# create isfraudcols for user and merchant
def find_frauds(data):

    # generate quantile bounds for each column
    bounds = {
        c: dict(
            zip(["q1", "q3"], data.approxQuantile(c, [0.25, 0.75], 0.01))
        )
        for c in cols
    }

    for c in bounds:
        iqr = bounds[c]['q3'] - bounds[c]['q1']
        bounds[c]['lower'] = bounds[c]['q1'] - (iqr * 1.5)
        bounds[c]['upper'] = bounds[c]['q3'] + (iqr * 1.5)

    # for each column -> if fraud outside bounds == 1 else 0
    data_ = data.select(
        "*",
        *[
            F.when(
                F.col(c).between(bounds[c]['lower'], bounds[c]['upper']),
                0
            ).otherwise(1).alias(c+"_out")
            for c in cols
        ]
    )

    # if they are fraud for both merchant and user == 1 else 0
    data_ = data_.select(
        "*",
        *[
            F.when(
                (F.col('user_fraud_prediction_out') != 1) | (F.col('merchant_fraud_prediction_out') != 1),
                0
            ).otherwise(1).alias('isfraud')
        ]
    )

    # drop unneeded cols
    data_ = data_.drop(*['user_fraud_prediction_out', 'merchant_fraud_prediction_out'])
    return data_


def get_weights(df1, label, n):

    # drop rows with missing label
    df = df1.na.drop(subset=[label]).toPandas()

    # find bins
    df['bin'] = pd.cut(df[label], n)

    # get weights and create mapper
    d = df.groupby('bin').count()[label].map(np.log)
    d = d.sum() / d
    mapper = d.to_dict()

    # create weights col mapped from bins
    df['weights'] = df['bin'].map(lambda x: mapper[x])
    weighted_df = spark.createDataFrame(df.drop('bin', axis=1))
    return weighted_df



def get_dummy(df,categoricalCols,continuousCols,labelCol=None, master=False):

    indexers = [ StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c))
                 for c in categoricalCols ]

    # default setting: dropLast=True
    encoders = [ OneHotEncoder(inputCol=indexer.getOutputCol(),
                               outputCol="{0}_encoded".format(indexer.getOutputCol()))
                 for indexer in indexers ]

    assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders]
                                          + continuousCols, outputCol="features")

    pipeline = Pipeline(stages=indexers + encoders + [assembler])

    model=pipeline.fit(df)
    data_ = model.transform(df)

    if master:
        for indexer in indexers:
            data_ = data_.drop(indexer.getOutputCol())
        for encoder in encoders:
            data_ = data_.drop(encoder.getOutputCol())
        return data_
    else:
        return data_.select('features',labelCol, 'weights')


def get_fraud_df(data):
    # get models
    uf_model, mf_model = get_models(data)

    # vectorize df
    data_v = vectorize_data(data)
    data_v = predict_frauds(data_v, uf_model, mf_model)
    data_v = find_frauds(data_v)

    data_fr = data_v.where(data_v['isfraud'] == 0)
    return data_fr


