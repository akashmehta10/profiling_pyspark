import sys
import datetime, time
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from profile_lib_v2 import get_null_perc, get_summary_numeric, get_distinct_counts, get_distribution_counts, get_mismatch_perc

conf = SparkConf()
conf.set('set hive.vectorized.execution', 'true')
conf.set('set hive.vectorized.execution.enabled', 'true')
conf.set('set hive.cbo.enable', 'true')
conf.set('set hive.compute.query.using.stats', 'true')
conf.set('set hive.stats.fetch.column.stats','true')
conf.set('set hive.stats.fetch.partition.stats', 'true')
conf.set('spark.cleaner.referenceTracking.cleanCheckpoints', 'true')

spark = SparkSession.builder.appName("scd_driver_program").config(conf=conf).enableHiveSupport().getOrCreate()
spark.sql('set hive.exec.dynamic.partition=True')
spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')

schema = StructType([
		 	StructField(name='id', dataType=IntegerType(), nullable=False),
		 	StructField(name='first_name', dataType=StringType(), nullable=True),
		 	StructField(name='last_name', dataType=StringType(), nullable=True),
		 	StructField(name='gender', dataType=StringType(), nullable=True),
		 	StructField(name='age', dataType=IntegerType(), nullable=True),
		 	StructField(name='education', dataType=StringType(), nullable=True),
		 	StructField(name='occupation', dataType=StringType(), nullable=True),
		 	StructField(name='income', dataType=IntegerType(), nullable=True),
		 	StructField(name='street_address', dataType=StringType(), nullable=True),
		 	StructField(name='city', dataType=StringType(), nullable=True),
		 	StructField(name='state', dataType=StringType(), nullable=True)
		 	])

df = spark.read.format('csv').option("header", False).schema(schema).load("/tmp/profile.csv")
null_cols = ['gender','age','education','occupation','income','street_address','city','state']
numeric_cols = ['age', 'income']
aggregate_cols = ['occupation','state']
data_quality_cols_regex = {'age': '^[0-99]{1,2}$', 'first_name': '^[a-zA-Z]*$', 'gender': '^M(ale)?$|^F(emale)?$'}
result_limit = 10
# List of commonly used regex: https://digitalfortress.tech/js/top-15-commonly-used-regex/

### 1. NULL Checks
resultdf = get_null_perc(spark, df, null_cols)
print("NULL/Empty Percentage for Columns")
resultdf.show(result_limit, False)

###2. Summary, Average, Standard Deviation, Percentiles for Numeric Columns
resultdf = get_summary_numeric(df, numeric_cols)
print("Summary for Numeric Columns")
resultdf.show(result_limit, False)

###3. Distinct Count
print("Distinct Counts for Aggregate Columns")
resultdf = get_distinct_counts(spark, df, aggregate_cols)
resultdf.show(result_limit, False)

###4. Distribution Count
print("Distribution Count for Aggregate Columns")
result = get_distribution_counts(spark, df, aggregate_cols)
for i in result:
	print("======== Distribution for - " + i.columns[0] + " ========")
	i.show(result_limit, False)

###5. Data Quality
print("Data Quality Issue Percentage for Columns")
resultdf = get_mismatch_perc(spark, df, data_quality_cols_regex)
resultdf.show(result_limit, False)