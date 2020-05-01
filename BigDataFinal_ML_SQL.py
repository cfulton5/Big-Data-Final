from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
import pyspark.sql.functions as sqlFunction
import pandas as pandaFunction
from pyspark.ml.feature import *
from pyspark.ml.tuning import *
from pyspark.ml.classification import *

def isBad(murderrate):
    if murderrate < 5.5:
        return 0
    else:
        return 1

columns = (
        'year',
        'population',
        'violent crime',
        'violent crime rate',
        'rape',
        'rape rate',
        'robbery',
        'robbery rate',
        'aggravated assault',
        'aggravated assault rate',
        'property theft',
        'property theft rate',
        'burglary',
        'burglary rate',
        'larceny-theft',
        'larceny-theft rate',
        'motor vehicle theft',
        'motor vehicle theft rate',
        'murder and manslaughter rate'
        )

crimeStats = pandaFunction.read_csv("crime.csv", delimiter=',', names=columns)          #using data from https://ucr.fbi.gov/crime-in-the-u.s/2014/crime-in-the-u.s.-2014/tables/table-1 and other years
crimeStats['oh no'] = crimeStats['murder and manslaughter rate'].apply(isBad)
sqlDataFrame = spark.createDataFrame(crimeStats)
modifiedColumns = (
            'year',
            'population',
            'violent crime',
            'violent crime rate',
            'rape',
            'rape rate',
            'robbery',
            'robbery rate',
            'aggravated assault',
            'aggravated assault rate',
            'property theft',
            'property theft rate',
            'burglary',
            'burglary rate',
            'larceny-theft',
            'larceny-theft rate',
            'motor vehicle theft',
            'motor vehicle theft rate'
            )

newData = VectorAssembler(inputCols=modifiedColumns, outputCol="modifiedColumns")
matrixifiedData = newData.transform(sqlDataFrame)
scaleIt=StandardScaler().setInputCol("modifiedColumns").setOutputCol("modifiedColumnsScaled")
matrixifiedData=scaleIt.fit(matrixifiedData).transform(matrixifiedData)

dataSet1, dataSet2 = matrixifiedData.randomSplit([0.50, 0.50], seed=12345)
regressorTime = LogisticRegression(labelCol="oh no", featuresCol="modifiedColumnsScaled", maxIter=100)
testingModel=regressorTime.fit(dataSet1)
predictingTime=testingModel.transform(dataSet2)
testTime = predictingTime.withColumn('predicted correctly', sqlFunction.when(sqlFunction.col('oh no') == sqlFunction.col('prediction'), 1).otherwise(0))

predictingTime.select("oh no","prediction").show()
testTime.groupby("predicted correctly").count().show()
