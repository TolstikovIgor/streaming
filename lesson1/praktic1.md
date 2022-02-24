## Урок 1. Spark Streaming. Тестовые стримы, чтение файлов в реальном времени.

https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

1. Подключаемся к серверу

```bash
ssh BD_274_ashadrin@185.241.193.174 -i ~/.ssh/id_rsa_gb_spark
```
 
2\. Запускаем спарк-приложение

```bash
[BD_274_ashadrin@bigdataanalytics-head-0 ~]$ pyspark
```

3\. Далее пробуем выполнить команды из файла

```python
from pyspark.sql import functions as F

raw_rate = spark \
    .readStream \
    .format("rate") \
    .load()

raw_rate.printSchema()
```

    root
     |-- timestamp: timestamp (nullable = true)
     |-- value: long (nullable = true)

```python
raw_rate.isStreaming
```

    True

Будем писать наш стрим в консоль с интервалом 30 секунд

```python
stream = raw_rate \
    .writeStream \
    .trigger(processingTime='30 seconds') \
    .format("console") \
    .options(truncate=False) \
    .start()
```

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +---------+-----+
    |timestamp|value|
    +---------+-----+
    +---------+-----+
    
    -------------------------------------------                                     
    Batch: 1
    -------------------------------------------
    +-----------------------+-----+
    |timestamp              |value|
    +-----------------------+-----+
    |2020-12-06 17:37:44.125|0    |
    |2020-12-06 17:37:45.125|1    |
    |2020-12-06 17:37:46.125|2    |
    |2020-12-06 17:37:47.125|3    |
    |2020-12-06 17:37:48.125|4    |
    |2020-12-06 17:37:49.125|5    |
    |2020-12-06 17:37:50.125|6    |
    |2020-12-06 17:37:51.125|7    |
    |2020-12-06 17:37:52.125|8    |
    |2020-12-06 17:37:53.125|9    |
    |2020-12-06 17:37:54.125|10   |
    |2020-12-06 17:37:55.125|11   |
    |2020-12-06 17:37:56.125|12   |
    |2020-12-06 17:37:57.125|13   |
    |2020-12-06 17:37:58.125|14   |
    +-----------------------+-----+
    
    -------------------------------------------
    Batch: 2
    -------------------------------------------
    +-----------------------+-----+
    |timestamp              |value|
    +-----------------------+-----+
    |2020-12-06 17:37:59.125|15   |
    |2020-12-06 17:38:00.125|16   |
    |2020-12-06 17:38:01.125|17   |
    |2020-12-06 17:38:02.125|18   |
    |2020-12-06 17:38:03.125|19   |
    |2020-12-06 17:38:04.125|20   |
    |2020-12-06 17:38:05.125|21   |
    |2020-12-06 17:38:06.125|22   |
    |2020-12-06 17:38:07.125|23   |
    |2020-12-06 17:38:08.125|24   |
    |2020-12-06 17:38:09.125|25   |
    |2020-12-06 17:38:10.125|26   |
    |2020-12-06 17:38:11.125|27   |
    |2020-12-06 17:38:12.125|28   |
    |2020-12-06 17:38:13.125|29   |
    |2020-12-06 17:38:14.125|30   |
    |2020-12-06 17:38:15.125|31   |
    |2020-12-06 17:38:16.125|32   |
    |2020-12-06 17:38:17.125|33   |
    |2020-12-06 17:38:18.125|34   |
    +-----------------------+-----+
 
```python
stream.stop()
```

Параметры стрима

```python
stream.explain()
```

    == Physical Plan ==
    WriteToDataSourceV2 org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter@163ded53
    +- Scan ExistingRDD[timestamp#42,value#43L]

```python
stream.lastProgress
```

    {u'stateOperators': [], u'name': None, u'timestamp': u'2020-12-06T17:38:30.000Z', u'processedRowsPerSecond': 129.87012987012986, u'inputRowsPerSecond': 1.0, u'numInputRows': 30, u'batchId': 2, u'sources': [{u'description': u'RateSource[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=2]', u'endOffset': 45, u'processedRowsPerSecond': 129.87012987012986, u'inputRowsPerSecond': 1.0, u'numInputRows': 30, u'startOffset': 15}], u'durationMs': {u'queryPlanning': 10, u'getOffset': 0, u'addBatch': 147, u'getBatch': 20, u'walCommit': 52, u'triggerExecution': 231}, u'runId': u'0148ec24-e0d8-4320-aa81-75912227107e', u'id': u'c6d6c44e-e111-41e9-b0a5-c8b4de5096ed', u'sink': {u'description': u'org.apache.spark.sql.execution.streaming.ConsoleSinkProvider@6a441f3e'}}


```python
stream.status
```

    {u'message': u'Stopped', u'isTriggerActive': False, u'isDataAvailable': False}

Добавим метод для вывода стрима в консоль.

```python
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .start()
```

Добавим фильтр к стриму

```python
filtered_rate = raw_rate \
    .filter( F.col("value") % F.lit("2") == 0 )
```

Смотрим результат

```python
out = console_output(filtered_rate, 5)
```

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +---------+-----+
    |timestamp|value|
    +---------+-----+
    +---------+-----+
    
    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +-----------------------+-----+
    |timestamp              |value|
    +-----------------------+-----+
    |2020-12-06 17:46:26.177|0    |
    |2020-12-06 17:46:28.177|2    |
    +-----------------------+-----+
    
    -------------------------------------------
    Batch: 2
    -------------------------------------------
    +-----------------------+-----+
    |timestamp              |value|
    +-----------------------+-----+
    |2020-12-06 17:46:30.177|4    |
    |2020-12-06 17:46:32.177|6    |
    +-----------------------+-----+

```python
out.stop()
```

Добавим к стриму ещё одну колонку

```python
extra_rate = filtered_rate \
    .withColumn("my_value",
                F.when((F.col("value") % F.lit(10) == 0), F.lit("jubilee"))
                    .otherwise(F.lit("not yet")))

out = console_output(extra_rate, 5)
```

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +---------+-----+--------+
    |timestamp|value|my_value|
    +---------+-----+--------+
    +---------+-----+--------+
    
    -------------------------------------------                                     
    Batch: 1
    -------------------------------------------
    +-----------------------+-----+--------+
    |timestamp              |value|my_value|
    +-----------------------+-----+--------+
    |2020-12-06 17:57:34.206|0    |jubilee |
    +-----------------------+-----+--------+
    
    -------------------------------------------
    Batch: 2
    -------------------------------------------
    +-----------------------+-----+--------+
    |timestamp              |value|my_value|
    +-----------------------+-----+--------+
    |2020-12-06 17:57:36.206|2    |not yet |
    |2020-12-06 17:57:38.206|4    |not yet |
    +-----------------------+-----+--------+
    
    -------------------------------------------
    Batch: 3
    -------------------------------------------
    +-----------------------+-----+--------+
    |timestamp              |value|my_value|
    +-----------------------+-----+--------+
    |2020-12-06 17:57:40.206|6    |not yet |
    |2020-12-06 17:57:42.206|8    |not yet |
    +-----------------------+-----+--------+

```python
out.stop()
```

Метод для прекращения всех активных стримов

```python
def killAll():
    for active_stream in spark.streams.active:
        print("Stopping %s by killAll" % active_stream)
        active_stream.stop()
```

Проверим как он отработает, если объект вывода стрима в консоль не сохранить в переменную.

```python
console_output(extra_rate, 5)
```

    <pyspark.sql.streaming.StreamingQuery object at 0x7fd5a0fac890>
    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +---------+-----+--------+
    |timestamp|value|my_value|
    +---------+-----+--------+
    +---------+-----+--------+
    
    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +-----------------------+-----+--------+
    |timestamp              |value|my_value|
    +-----------------------+-----+--------+
    |2020-12-06 17:58:33.967|0    |jubilee |
    +-----------------------+-----+--------+
    
    -------------------------------------------
    Batch: 2
    -------------------------------------------
    +-----------------------+-----+--------+
    |timestamp              |value|my_value|
    +-----------------------+-----+--------+
    |2020-12-06 17:58:35.967|2    |not yet |
    |2020-12-06 17:58:37.967|4    |not yet |
    +-----------------------+-----+--------+

Принудительно завершаем стрим. Вывод в консоль прекращается.

```python
killAll()
```

######FILE SOURCE HDFS
hdfs dfs -mkdir input_csv_for_stream
hdfs dfs -ls
hdfs dfs -put *.csv input_csv_for_stream

#требует схему:
schema = StructType() \
    .add("product_category_name", StringType()) \
    .add("product_category_name_english", StringType())

#все разом
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path="input_csv_for_stream", header=True) \
    .load()

out = console_output(raw_files, 5)
out.stop()

#по одному
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path="input_csv_for_stream", header=True, maxFilesPerTrigger=1) \
    .load()

out = console_output(raw_files, 5)
out.stop()

#так же добавляем свою колонку
extra_files = raw_files \
    .withColumn("spanish_length", F.length(F.col("product_category_name"))) \
    .withColumn("english_length", F.length(F.col("product_category_name_english")))

out = console_output(extra_files, 5)
out.stop()


    Stopping <pyspark.sql.streaming.StreamingQuery object at 0x7fd5a0faca50> by killAll

4\. Закрываем подключение к кластеру

```python
exit()
```

```bash
[BD_274_ashadrin@bigdataanalytics-head-0 ~]$ exit
logout
Connection to 185.241.193.174 closed.
``` 
