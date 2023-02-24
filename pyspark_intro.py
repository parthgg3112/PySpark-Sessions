import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practise').getOrCreate()
print(spark)
df_pyspark = spark.read.csv('test_1.csv',header=True)
print(df_pyspark.show())
print(df_pyspark.printSchema())