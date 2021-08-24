import sys
import datetime, time
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def get_null_perc(spark, df, null_cols):
    """ Get null/empty percentage for columns

    Args:
        spark (Spark): SparkSession object
        df (DataFrame): dataframe to perform null/empty analysis on
        null_cols (List): list of columns that need to be considered for analysis

    Returns:
        DataFrame: dataframe with null check analysis
    """
    schema = StructType([ \
        StructField("Column",StringType(),True), \
        StructField("NullPercentage",StringType(),True)
      ])
    emptyRDD = spark.sparkContext.emptyRDD()
    resultdf = spark.createDataFrame(emptyRDD, schema=schema)
    
    for x in null_cols:
    	df_null_count = df.select(F.col(x)).filter(F.col(x).isNull() | (F.col(x) == '')).count()
    	df_null = spark.createDataFrame([[x, str(df_null_count*100.0/df.count()) + '%' ]],schema=schema)
    	resultdf = resultdf.union(df_null)

    return resultdf

def get_summary_numeric(df, numeric_cols):
    """ Get Summary for numeric columns

    Args:
        df (DataFrame): dataframe to perform analysis on
        numeric_cols (List): list of columns that need to be considered for analysis

    Returns:
        DataFrame: dataframe with summary analysis
    """

    return df.select(numeric_cols).summary()

def get_distinct_counts(spark, df, aggregate_cols):
    """ Get distinct count for columns

    Args:
        spark (Spark): SparkSession object
        df (DataFrame): dataframe to perform distinct count analysis on
        aggregate_cols (List): list of columns that need to be considered for analysis

    Returns:
        DataFrame: dataframe with distinct count analysis
    """
    schema = StructType([ \
        StructField("Column",StringType(),True), \
        StructField("DistinctCount",StringType(),True)
      ])
    
    emptyRDD = spark.sparkContext.emptyRDD()
    resultdf = spark.createDataFrame(emptyRDD, schema=schema)
    
    for x in aggregate_cols:
    	df_distinct_count = df.select(F.col(x)).distinct().count()
    	df_distinct = spark.createDataFrame([[x, str(df_distinct_count)]],schema=schema)
    	resultdf = resultdf.union(df_distinct)

    return resultdf

def get_distribution_counts(spark, df, aggregate_cols):
    """ Get Distribution Counts for columns

    Args:
        spark (Spark): SparkSession object
        df (DataFrame): dataframe to perform null/empty analysis on
        aggregate_cols (List): list of columns that need to be considered for analysis

    Returns:
        Array: Array of objects with dataframes
    """
    result = []
    for i in aggregate_cols:
    	result.append(df.groupby(F.col(i)).count().sort(F.col("count").desc()))
    ###
    
    return result

def get_mismatch_perc(spark, df, data_quality_cols_regex):
    """ Get Mismatch Percentage for columns

    Args:
        spark (Spark): SparkSession object
        df (DataFrame): dataframe to perform null/empty analysis on
        data_quality_cols_regex (Dictionary): Dictionary of columns/regex-expression for data quality analysis

    Returns:
        DataFrame: DataFrame with data quality analysis
    """
    schema = StructType([ \
        StructField("Column",StringType(),True), \
        StructField("MismatchPercentage",StringType(),True)
      ])
    
    emptyRDD = spark.sparkContext.emptyRDD()
    resultdf = spark.createDataFrame(emptyRDD, schema=schema)
    
    
    for key, value in data_quality_cols_regex.items():
    	df_regex_not_like_count = df.select(F.col(key)).filter(~F.col(key).rlike(value)).count()
    	df_regex_not_like = spark.createDataFrame([[key, str(df_regex_not_like_count*100.0/df.count()) + '%']],schema=schema)
    	resultdf = resultdf.union(df_regex_not_like)
    
    return resultdf