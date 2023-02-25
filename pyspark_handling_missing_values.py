import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Handling_Missing_Values').getOrCreate()
df = spark.read.csv('test_2.csv',header=True,inferSchema=True)
print(df.show())
df_drop_name = df.drop('name')
print(df_drop_name.show())
df_drop_na_rows = df.na.drop()
print(df_drop_na_rows.show())

#how parameter in na.drop()
print(df.na.drop(how='all').show())
print(df.na.drop(how='any').show())

#thresh parameter in na.drop()
print(df.na.drop(how='any',thresh=3).show())

#subset parameter in na.drop()
print(df.na.drop(how='any',subset=['Name']).show())

#replacing values in df
df_replace_nan = df.na.fill(1) #gets filled for all columns if inferSchema = False , else based on the datatype of fill value
print(df_replace_nan.show())


#handling missing values using imputer 
from pyspark.ml.feature import Imputer

imputer = Imputer(
    inputCols=['age', 'Experience', 'Salary'], 
    outputCols=["{}_imputed".format(c) for c in ['age', 'Experience', 'Salary']]
    ).setStrategy("median")


print(imputer.fit(df).transform(df).show())