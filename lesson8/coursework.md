# 8. Финальный проект по модулю Потоковая обработка данных
```
Есть некий магазин, требуется в режиме реального времени, на основании скомпилированной нейросети определять тип покупателя на основании его персональных данных users_known и его корзины sales_known(метка known — известные). Те данные которые находятся в процесинге тип которых предсказали помечаются users_unknown и их корзины помечаются sales_unknown
```
# Запускаю
hive

# Подключаюсь к базе  sint_sales, смотрю таблицы
use sint_sales;
show tables;

# В другом окне терминала запускаю spark packages с kafka и cassandra connector
/opt/spark-2.4.8/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2

# Импортирую необходимые пакеты
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, CountVectorizer, StringIndexer, IndexToString

# Создаю пустой DataFrame
my_df = spark.createDataFrame( range( 1 , 200000 ), IntegerType())

# Создаю датасет
items_df = my_df.select(F.col("value").alias("order_id"), \
                        F.round( (F.rand()*49999)+1 ).alias("user_id").cast("integer"), \
                        F.round( (F.rand()*9)+1).alias("items_count").cast("integer")). \
    withColumn("price", (F.col("items_count")* F.round( (F.rand()*999)+1)).cast("integer") ). \
    withColumn("order_date", F.from_unixtime(F.unix_timestamp(F.current_date()) + (F.lit(F.col("order_id")*10))))

# В третьем окне терминала смотрю свою директорию в hdfs и удаляю ранее созданные папки parquet_data, my_LR_model8
hdfs dfs -ls
hdfs dfs -rm -f -r parquet_data
hdfs dfs -rm -f -r my_LR_model8
hdfs dfs -ls

# Во втором окне терминала записываю и сохраняю данные в  виде parquet
items_df.write.format("parquet").option("path", "parquet_data/sales").saveAsTable("sint_sales.sales", mode="overwrite")

# Смотрю что записалось
items_df.show()

# В третьем окне терминала проверяю и создаю директорию parquet_data
hdfs dfs -ls parquet_data
hdfs dfs -mkdir parquet_data
hdfs dfs -ls

# В первом окне терминала делаю выборку 1 строки из sales
select * from sales limit 1;

# Во втором окне терминала создаю items_df  sint_sales
items_df=spark.table("sint_sales.sales")
Создаю таблицу users
spark.sql("""create table sint_sales.users
	(user_id int,
	gender string,
	age string,
	segment string)
	stored as parquet location 'parquet_data/users'""")

# В третьем окне терминала смотрю директорию parquet_data
hdfs dfs -ls parquet_data

# В первом окне терминала смотрю таблицы
show tables;

# Во втором окне терминала  провожу нормализация по полу, группирую и поместил это все в таблицу users
spark.sql("""insert into sint_sales.users
	select user_id, case when pmod( user_id, 2 )=0 then 'M' else 'F' end, 
	case when pmod(user_id, 3 )=0 then 'young' when pmod(user_id, 3 )=1 then 'midage' else 'old' end ,
	case when s>23 then 'happy' when s>15 then 'neutral' else 'shy' end
	from (
	select sum(items_count) s, user_id from sint_sales.sales group by user_id ) t""")

# В первом окне терминала делаю выборку одной записи из таблицы  users
select * from users limit 1;

# Во втором окне терминала делаю выборку и создаю таблицы для известных/неизвестных пользователей и покупок
spark.sql("""create table sint_sales.users_known stored as parquet location 'parquet_data/users_known' as 
	select * from sint_sales.users where user_id < 30000
	""")
spark.sql("""create table sint_sales.users_unknown stored as parquet location 'parquet_data/users_unknown' as 
	select user_id, gender, age from sint_sales.users where user_id >= 30000
	""")
spark.sql("""create table sint_sales.sales_known stored as parquet location 'parquet_data/sales_known' as
	select * from sint_sales.sales where user_id < 30000
	""")
spark.sql("""create table sint_sales.sales_unknown stored as parquet location 'parquet_data/sales_unknown' as
	select * from sint_sales.sales where user_id >= 30000
	""")

# В первом окне терминала смотрю таблицы
show tables;

# Делаю выборку по 1 записи из таблиц ales_known и  sales_unknown
select * from sales_known limit 1;
select * from sales_unknown limit 1;

# Во втором окне терминала беру объект sales загружаю в df
items_df=spark.table("sint_sales.sales")

# Делаю  join всех покупок и покупателей кое-то кол-во статистических данных, по ключу соединил, сгруппировал и агрегировал, заполнил  gender,  age,  segment
df = spark.sql("""
	select count(*) as c, sum(items_count) as s1, max(items_count) as ma1, min(items_count) as mi1,
	sum(price) as s2, max(price) as ma2, min(price) as mi2 ,u.gender, u.age, u.user_id, u.segment 
	from sint_sales.sales_known s join sint_sales.users_known u 
	where s.user_id = u.user_id 
	group by u.user_id, u.gender, u.age, u.segment""")

# Делаю просмотр sales_unknown
sales_unknown = spark.table("sint_sales.sales_unknown")
sales_unknown.show()

# Делаю чтение в  parquet  sint_sales и parquet_data в бд  sales_unknown
sales_unknown = spark.read.parquet("/apps/spark/warehouse/sint_sales.db/sales_unknown")
sales_unknown = spark.read.parquet("parquet_data/sales_unknown")

# Смотрю записи в sales_unknown и запускаю kafka_brokers
sales_unknown.show()
kafka_brokers = 'bigdataanalytics-worker-3:6667'

# В третьем окне терминала смотрю лист с topics
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper bigdataanalytics-worker-3:2181 —list

# Удалю старые topics и создаю новый sales_unknown
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --topic tolstykov_les8 --zookeeper bigdataanalytics-worker-3:2181
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --topic tolstykov_les5 --zookeeper bigdataanalytics-worker-3:2181
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --topic tolstykov_les4 --zookeeper bigdataanalytics-worker-3:2181
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --topic tolstykov_les3 --zookeeper bigdataanalytics-worker-3:2181
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --topic tolstykov_les4_sink --zookeeper bigdataanalytics-worker-3:2181
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --delete --topic sales_unknown --zookeeper bigdataanalytics-worker-3:2181
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --topic sales_unknown --zookeeper bigdataanalytics-worker-3:2181 --partitions 1 --replication-factor 1

# Во втором окне терминала запускаю запись  bootstrap.servers
sales_unknown.selectExpr("cast (null as string) as key", "cast (to_json(struct(*)) as string) as value"). \
    write.format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("topic", "sales_unknown"). \
    save()

# Запускаю запись в  cassandra
users_unknown = spark.table("sint_sales.users_unknown")
users_unknown.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="users_unknown", keyspace="keyspace1") \
    .mode("append")\
    .save()

# В третьем окне терминала запускаю  cassandra и смотрю базы
cqlsh
select keyspace_name, table_name from system_schema.tables where keyspace_name = 'keyspace1';

# Смотрю базы и таблицы
select keyspace_name, table_name from system_schema.tables;

# Переключаюсь на  keyspace1 и создаю таблицу  users_unknown
use keyspace1;
CREATE TABLE users_unknown( user_id int, gender text, age text, c int, s1 int,  ma1 int, mi1 int, s2 int, ma2 int, mi2 int, segment text, primary key (user_id));

# Делаю выборку из  users_unknown
select * from users_unknown;

# Во втором  окне терминала запускаю чтения  parquet с созданием временных представлений известных покупок и известных пользователей
spark.read.parquet("parquet_data/sales_known").createOrReplaceTempView("sales_known")
spark.read.parquet("parquet_data/users_known").createOrReplaceTempView("users_known")

# Выборка sint_sales по 10 строк из  sales_known и  users_known
spark.sql("select * from sint_sales.sales_known").show(10, False)
spark.sql("select * from sint_sales.users_known").show(10, False)

# Считаю сегмент зависимым от количества покупок клиента, суммы всех купленых товаров клиента, максимального числа купленных товаров клиента, минимального числа купленных товаров клиента, суммы потраченных рублей клиента, максимально потраченных рублей клиента, минимально потраченных рублей клиента
users_known = spark.sql("""
	select count(*) as c, sum(items_count) as s1, max(items_count) as ma1, min(items_count) as mi1,
	sum(price) as s2, max(price) as ma2, min(price) as mi2 ,u.gender, u.age, u.user_id, u.segment 
	from sint_sales.sales_known s join sint_sales.users_known u 
	where s.user_id = u.user_id 
	group by u.user_id, u.gender, u.age, u.segment""")

# Подготовка модели машиннго обучения.
df = users_known
df.show()

# Беру колонки которые будет обрабатывать  stringIndexer, потом  encoder
categoricalColumns = ['gender', 'age']
stages = []
for categoricalCol in categoricalColumns:
	stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
	encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
	stages += [stringIndexer, encoder]
stages

# Определяю label, является  segment, т. е. та колонка которую буду предсказывать
label_stringIdx = StringIndexer(inputCol = 'segment', outputCol = 'label')
stages += [label_stringIdx]

# Колонки с численными значениями кол-во записей, покупок, общее, макс, мин, общий счет, макс чек, мин чек, преобразование в вектор
numericCols = ['c' ,'s1', 'ma1', 'mi1','s2', 'ma2', 'mi2']
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

# Добавляю LR в поле stages, переименование колонки label в  prediction, то что предсказываю
lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
stages += [lr]
label_stringIdx_fit = label_stringIdx.fit(df)
indexToStringEstimator = IndexToString().setInputCol("prediction").setOutputCol("category").setLabels(  label_stringIdx_fit.labels)
stages +=[indexToStringEstimator]

# Собираю  Pipeline, соединение, запись, сохранение
pipeline = Pipeline().setStages(stages)
pipelineModel = pipeline.fit(df)

# Сохраняю модель на HDFS
pipelineModel.write().overwrite().save("my_LR_model8")

# В четвертом окне терминала
hdfs dfs -ls

# Во втором окне терминала разделил датасет на train и test 70/30
train, test = df.randomSplit([0.7, 0.3], seed = 2018)
print("Training Dataset Count: " + str(train.count()))
print("Test Dataset Count: " + str(test.count()))

# трансформация  test, просмотр
pipelineModel.transform(test).show(100)

pipelineModel.transform(test).select("segment", "label", "probability", "prediction", "category").show(1)
pipelineModel.transform(test).select("segment", "label", "probability", "prediction", "category").show(10)

stages

for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
stages += [stringIndexer, encoder]
pipeline = Pipeline(stages = stages)
pipelineModel = pipeline.fit(df)
df = pipelineModel.transform(df)
cols = df.columns
selectedCols = ['label', 'features'] + cols
df = df.select(selectedCols)
df.printSchema()

train, test = df.randomSplit([0.7, 0.3], seed = 2018)
print("Training Dataset Count: " + str(train.count()))
print("Test Dataset Count: " + str(test.count()))

Делаю обучение модели
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
lrModel = lr.fit(train)

lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
stages += [lr]
label_stringIdx_fit = label_stringIdx.fit(users_known)
indexToStringEstimator = IndexToString().setInputCol("prediction").setOutputCol("category").setLabels(  label_stringIdx_fit.labels)
stages +=[indexToStringEstimator]

# Объединяю в новый pipeline, устанавливаю этапы, получаею pipelineModel, получаю скомпилированную модель
pipeline = Pipeline().setStages(stages)
pipelineModel = pipeline.fit(users_known)

# Сохраняю модель на HDFS
pipelineModel.write().overwrite().save("my_LR_model8")
pipelineModel.transform(test).select("segment", "category").show(100)

# Выполняю метод  transform на вход подаю users_known (надо подавать test)
pipelineModel.transform(users_known).select("segment", "category").show(100)
