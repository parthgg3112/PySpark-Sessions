import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Dataframe').getOrCreate()
df = spark.read.csv('test_1.csv',header=True,inferSchema=True)
df = df.withColumn('Age after 5 years',df['Age']+5)
print(df.select(['Name','Age','Age after 5 years']).show())
df = df.drop('Age after 5 years')
print(df.show())
df = df.withColumnRenamed('Name', 'FirstName')
print(df.show())