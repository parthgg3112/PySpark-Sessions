import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Filter_Operations').getOrCreate()
df = spark.read.csv('test_1.csv',header=True,inferSchema=True)
print(df.show())

#filter df on age condition
print(df.filter(df['age']<=25).show()) #can be written as df.filter("age<=25")

#filter df on multiple condition
print(df.filter((df['age']<=25) & (df['Salary']<25000)).show())