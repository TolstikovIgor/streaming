#export PYSPARK_PYTHON=python3
#export SPARK_KAFKA_VERSION=0.10
# pyspark2 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --num-executors 1 --executor-memory 512m
# https://towardsdatascience.com/machine-learning-with-pyspark-and-mllib-solving-a-binary-classification-problem-96396065d2aa
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, CountVectorizer, StringIndexer, IndexToString

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()

users_known = spark.sql("""
select count(*) as c, sum(items_count) as s1, max(items_count) as ma1, min(items_count) as mi1,
sum(price) as s2, max(price) as ma2, min(price) as mi2 ,u.gender, u.age, u.user_id, u.segment 
from sint_sales.sales_known s join sint_sales.users_known u 
where s.user_id = u.user_id 
group by u.user_id, u.gender, u.age, u.segment""")


categoricalColumns = ['gender', 'age']
stages = []
for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index').setHandleInvalid("keep")
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"]).setHandleInvalid("keep")
    stages += [stringIndexer, encoder]

label_stringIdx = StringIndexer(inputCol = 'segment', outputCol = 'label').setHandleInvalid("keep")
stages += [label_stringIdx]

numericCols = ['c' ,'s1', 'ma1', 'mi1','s2', 'ma2', 'mi2']
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features").setHandleInvalid("keep")
stages += [assembler]

lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
stages += [lr]

label_stringIdx_fit = label_stringIdx.fit(users_known)
indexToStringEstimator = IndexToString().setInputCol("prediction").setOutputCol("category").setLabels(  label_stringIdx_fit.labels)

stages +=[indexToStringEstimator]

pipeline = Pipeline().setStages(stages)
pipelineModel = pipeline.fit(users_known)

pipelineModel.write().overwrite().save("my_LR_model")

###
pipelineModel.transform(users_known).show() #посчитать процент сходимости
