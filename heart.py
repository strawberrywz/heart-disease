from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from struc import heart_schema

spark = SparkSession.builder.getOrCreate()


heart_df = spark.read.csv('heart_data.csv', header=True, schema=heart_schema)               # reading the csv file

heart_df.createOrReplaceTempView('heart_data')                                              # create a temp view
df = spark.sql('select * from heart_data')                                                  # select using sql syntax 
df = df.drop('heart_disease')                                                               # drop to only keep the features
df = df.drop('index')

input_list = df.columns                                                                     # store column names/columns

df.show()                                                                                   # to compare the df before and after

features_assembler = VectorAssembler(inputCols=input_list, outputCol='features')            # features will be our new column

df = features_assembler.transform(heart_df)

working_df = df.select('features', 'heart_disease')

train, test = working_df.randomSplit([0.7, 0.3])                                            # train the data

regressor = LinearRegression(featuresCol='features', labelCol='heart_disease')
model = regressor.fit(train)
prediction = model.transform(train)

prediction.show()