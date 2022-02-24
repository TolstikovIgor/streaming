## Урок 3. Spark Streaming. Чтение Kafka.

##### Задание 1. Повторить чтение файлов со своими файлами со своей схемой.


1\.1\. Подключаемся к серверу

```bash
 ssh BD_274_ashadrin@89.208.223.141 -i ~/.ssh/id_rsa_gb_spark
```

1\.2\. Подготовка файлов и папок. 

Создадим папку `input_csv_for_stream` на HDFS, из которой стрим будет читать файлы.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -mkdir input_csv_for_stream
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -ls
Found 2 items
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-06 19:57 .sparkStaging
drwxr-xr-x   - BD_274_ashadrin BD_274_ashadrin          0 2020-12-14 09:46 input_csv_for_stream 
```

Создадим локальную директорию `for_stream`, откуда будем копировать файлы на HDFS.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ mkdir for_stream
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ ls
for_stream
```

Скопируем подготовленные файлы на удаленный сервер с помощью команды `scp`. Эта команда запускается на локальном компьютере, а не на удалённом сервере.  

```bash
scp -i ~/.ssh/id_rsa_gb_spark -r /usa_president BD_274_ashadrin@89.208.223.141:~/for_stream
```

Проверяем что файлы загрузились.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ ls  for_stream/usa_president/
usa_president_1.csv  usa_president_2.csv  usa_president_3.csv  usa_president_4.csv  usa_president_5.csv
```

Файлы содержат список президентов США, максимум 10 записей в файле. Столбцы файлов `President`, `Took office`, `Left office`.

1\.3\. Запускаем `pyspark`. 

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ pyspark --master local[1]
```

1\.4\. Инициализация стрима. 

В командной строке `pyspark` импортируем нужные методы и определяем функцию `console_output` для вывода стрима в консоль. 

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .start()
```

Определяем схему наших файлов.
 
```python
schema = StructType() \
    .add("President", StringType()) \
    .add("Took office", StringType()) \
    .add("Left office", StringType())
``` 
 
Создаём стрим чтения из файла (с параметром `.format("csv")`). В `options` указываем папку на HDFS, из которой будут читаться файлы.

```python
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path="input_csv_for_stream", header=True) \
    .load()
```

Запускаем стрим. 

```python
out = console_output(raw_files, 5)
```

1\.5\. Тестирование стрима.
 
В соседнем терминале подключаемся к удалённому серверу `worker-0` и переходим в каталог с загруженными файлами.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ cd for_stream/usa_president/
[BD_274_ashadrin@bigdataanalytics-worker-0 usa_president]$ ll
total 20
-rw-rw-r-- 1 BD_274_ashadrin BD_274_ashadrin 391 дек 14 10:43 usa_president_1.csv
-rw-rw-r-- 1 BD_274_ashadrin BD_274_ashadrin 399 дек 14 10:43 usa_president_2.csv
-rw-rw-r-- 1 BD_274_ashadrin BD_274_ashadrin 416 дек 14 10:43 usa_president_3.csv
-rw-rw-r-- 1 BD_274_ashadrin BD_274_ashadrin 403 дек 14 10:43 usa_president_4.csv
-rw-rw-r-- 1 BD_274_ashadrin BD_274_ashadrin 209 дек 14 10:43 usa_president_5.csv
```

Копируем один файл на HDFS

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 usa_president]$ hdfs dfs -put usa_president_1.csv input_csv_for_stream
```

В первом терминале наблюдаем чтение данных.


    -------------------------------------------                                     
    Batch: 0
    -------------------------------------------
    +----------------------+-----------+-----------+
    |President             |Took office|Left office|
    +----------------------+-----------+-----------+
    |George Washington     |30/04/1789 |4/03/1797  |
    |John Adams            |4/03/1797  |4/03/1801  |
    |Thomas Jefferson      |4/03/1801  |4/03/1809  |
    |James Madison         |4/03/1809  |4/03/1817  |
    |James Monroe          |4/03/1817  |4/03/1825  |
    |John Quincy Adams     |4/03/1825  |4/03/1829  |
    |Andrew Jackson        |4/03/1829  |4/03/1837  |
    |Martin Van Buren      |4/03/1837  |4/03/1841  |
    |William Henry Harrison|4/03/1841  |4/04/1841  |
    |John Tyler            |4/04/1841  |4/03/1845  |
    +----------------------+-----------+-----------+

Во втором терминале скопируем все оставшиеся файлы на HDFS.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 usa_president]$ hdfs dfs -put usa_president_* input_csv_for_stream
put: 'input_csv_for_stream/usa_president_1.csv': File exists
```

В первом терминале видим что все непрочитанные ранее файлы считались.

    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +---------------------------+-----------+-----------+
    |President                  |Took office|Left office|
    +---------------------------+-----------+-----------+
    |Chester A. Arthur          |19/09/1881 |4/03/1885  |
    |Grover Cleveland           |4/03/1885  |4/03/1889  |
    |Benjamin Harrison          |4/03/1889  |4/03/1893  |
    |Grover Cleveland (2nd term)|4/03/1893  |4/03/1897  |
    |William McKinley           |4/03/1897  |14/9/1901  |
    |Theodore Roosevelt         |14/9/1901  |4/3/1909   |
    |William Howard Taft        |4/3/1909   |4/03/1913  |
    |Woodrow Wilson             |4/03/1913  |4/03/1921  |
    |Warren G. Harding          |4/03/1921  |2/8/1923   |
    |Calvin Coolidge            |2/8/1923   |4/03/1929  |
    |Herbert Hoover             |4/03/1929  |4/03/1933  |
    |Franklin D. Roosevelt      |4/03/1933  |12/4/1945  |
    |Harry S. Truman            |12/4/1945  |20/01/1953 |
    |Dwight D. Eisenhower       |20/01/1953 |20/01/1961 |
    |John F. Kennedy            |20/01/1961 |22/11/1963 |
    |Lyndon B. Johnson          |22/11/1963 |20/1/1969  |
    |Richard Nixon              |20/1/1969  |9/8/1974   |
    |Gerald Ford                |9/8/1974   |20/01/1977 |
    |Jimmy Carter               |20/01/1977 |20/01/1981 |
    |Ronald Reagan              |20/01/1981 |20/01/1989 |
    +---------------------------+-----------+-----------+
    only showing top 20 rows

Завершаем стрим командой `out.stop()`. 

1\.6\. Попробуем запустить стрим с другими опциями. 

Параметр `maxFilesPerTrigger` определяет сколько файлов будет прочитано в одном батче. При этом, если необработанных файлов меньше чем `maxFilesPerTrigger`, то они не будут прочитаны и батч не появится.

```python
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path="input_csv_for_stream",
             header=True,
             maxFilesPerTrigger=1) \
    .load()
```

Так же добавим свою колонку `Days in power`, в которой посчитаем сколько дней каждый президент провёл у власти.

```python
extra_files = raw_files \
    .withColumn("Days in power", F.datediff(
        F.to_date(F.col("Left office"), "dd/MM/yyyy"),
        F.to_date(F.col("Took office"), "dd/MM/yyyy")
    ))
```

Запускаем стрим 

```python
out = console_output(extra_files, 5)
```

Так как все файлы уже записаны на HDFS, то они сразу считываются. В консоли наблюдаем 5 батчей (по числу файлов на HDFS). Тут приведу только последний батч. 

    -------------------------------------------
    Batch: 4
    -------------------------------------------
    +-----------------+-----------+-----------+-------------+
    |President        |Took office|Left office|Days in power|
    +-----------------+-----------+-----------+-------------+
    |George H. W. Bush|20/01/1989 |20/01/1993 |1461         |
    |Bill Clinton     |20/01/1993 |20/01/2001 |2922         |
    |George W. Bush   |20/01/2001 |20/01/2009 |2922         |
    |Barack Obama     |20/01/2009 |20/01/2017 |2922         |
    |Donald J. Trump  |20/01/2017 |null       |null         |
    +-----------------+-----------+-----------+-------------+


1\.7\. Завершение первой части.

Закрываем стрим и выходим из консоли `pyspark`.

```python
out.stop()
exit()
```

Удаляем файлы из HDFS. Локально пока оставим, может пригодятся. 

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ hdfs dfs -rm -r input_csv_for_stream
20/12/14 11:48:10 INFO fs.TrashPolicyDefault: Moved: 'hdfs://bigdataanalytics-head-0.novalocal:8020/user/BD_274_ashadrin/input_csv_for_stream' to trash at: hdfs://bigdataanalytics-head-0.novalocal:8020/user/BD_274_ashadrin/.Trash/Current/user/BD_274_ashadrin/input_csv_for_stream
```

##### Задание 2. Создать свой топик/топики, загрузить туда через консоль осмысленные данные с kaggle. Лучше в формате json. Много сообщений не нужно, достаточно штук 10-100. Прочитать свой топик так же, как на уроке.


2\.1\. Аналогично второму уроку создадим топик `shadrin_iris`. 

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --create --topic shadrin_iris --zookeeper bigdataanalytics-worker-0.novalocal:2181 --partitions 1 --replication-factor 2 --config retention.ms=-1
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic "shadrin_iris".
```

В одном терминале запустим `console-producer` для записи данных в топик.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-console-producer.sh --topic shadrin_iris --broker-list bigdataanalytics-worker-0.novalocal:6667
```

Во втором терминале запустим `console-consumer` чтобы проконтролировать запись.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-console-consumer.sh --topic shadrin_iris --bootstrap-server bigdataanalytics-worker-0.novalocal:6667
```

Для записи выбран классический датасет ирисов. У топика не ограниченное время жизни, так что данные в партиции затираться не будут. Проверим как можно читать из Кафки. В этой секции все прочитанные данные будем выводить в консоль.

2\.2\. Переходим в консоль pyspark. 

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ export SPARK_KAFKA_VERSION=0.10
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ pyspark --master local[1] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2
```

`export SPARK_KAFKA_VERSION=0.10` - устанавливаем временную (на сессию) переменную окружения с версией Кафки. 

В параметрах запуска `--master local[1]` определяет что Spark будет запущен на одной ноде. Параметр `--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2` определяет с какой версией Кафки мы будем работать.
 
 0.10 - версия Кафки;
 
 2.11 - версия языка Scala, на котрой написан Spark;
 
 2.3.2 - версия Spark, которая запускается на ноде.
  
  При этом в Spark подтягивается указанный пакет из Maven репозитория. Надеюсь тут нигде не напутал с определениями.
 
 После загрузки пакета определяем базовые функции. 

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, FloatType

kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()
``` 

2\.3\. Чтение  батчевом режиме.

```python
raw_data = spark.read. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "shadrin_iris"). \
    option("startingOffsets", "earliest"). \
    option("endingOffsets", """{"shadrin_iris":{"0":20}}"""). \
    load()

raw_data.show(100)
```


    +----+--------------------+------------+---------+------+--------------------+-------------+
    | key|               value|       topic|partition|offset|           timestamp|timestampType|
    +----+--------------------+------------+---------+------+--------------------+-------------+
    |null|[7B 22 73 65 70 6...|shadrin_iris|        0|     0|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|     1|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|     2|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|     3|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|     4|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|     5|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|     6|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|     7|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|     8|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|     9|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    10|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    11|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    12|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    13|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    14|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    15|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    16|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    17|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    18|2020-12-14 23:41:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    19|2020-12-14 23:41:...|            0|
    +----+--------------------+------------+---------+------+--------------------+-------------+

Параметры чтения:

`endingOffsets` - последнее значение `offset`, до которого нужно прочитать сообщения. Есть только при батчевом чтении (`spark.read`). В стриминговом чтении (`spark.readStream`) финальный оффсет задать нельзя, он всегда равен `latest`. 

Формат: `{"shadrin_iris":{"0":20}}`. Здесь `shadrin_iris` - топик; `0` - партиция, на которую накладывается ограничение; `20` - `offset` записи, до которой нужно прочитать партицию. Запись с `offset=20` прочитана не будет.  
 
`startingOffsets` - значение `offset` в партиции, с которого будут читаться сообщения. Есть в стриминговом и батчевом режимах чтения. Возможные значения `latest`, `earliest`, или строка `{"shadrin_iris":{"0":10}}` в формате, аналоичном `endingOffsets`.


2\.4\. Стриминговое чтение из Кафки.

```python
raw_data = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "shadrin_iris"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "5"). \
    load()
    
out = console_output(raw_data, 10)
```

Наблюдаем батчи по 5 записей раз в 10 секунд (`maxOffsetsPerTrigger = 5`). Всего 150 записей, 30 батчей.

    -------------------------------------------
    Batch: 14
    -------------------------------------------
    +----+--------------------+------------+---------+------+--------------------+-------------+
    | key|               value|       topic|partition|offset|           timestamp|timestampType|
    +----+--------------------+------------+---------+------+--------------------+-------------+
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    70|2020-12-15 00:21:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    71|2020-12-15 00:21:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    72|2020-12-15 00:21:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    73|2020-12-15 00:21:...|            0|
    |null|[20 20 7B 22 73 6...|shadrin_iris|        0|    74|2020-12-15 00:21:...|            0|
    +----+--------------------+------------+---------+------+--------------------+-------------+

```python
out.stop()
```

Так же попробовал читать с различными начальными отступами. Результат аналогичный.

2\.5\. Парсинг сообщений.

Посмотрим в каком формате в Кафке хранятся сообщения. 

```python
raw_data.printSchema()
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)
```

`value` это всегда либо бинарный код, либо строка.

Определяем структуру данных нашего исходного датасета.

```python
schema = StructType() \
    .add("sepalLength", FloatType()) \
    .add("sepalWidth", FloatType()) \
    .add("petalLength", FloatType()) \
    .add("petalWidth", FloatType()) \
    .add("species", StringType())
    
    
value_iris = raw_data \
    .select(
        F.from_json(F.col("value").cast("String"), schema).alias("value"), 
        "offset"
    )
    
value_iris.printSchema()
```

    root
     |-- value: struct (nullable = true)
     |    |-- sepalLength: float (nullable = true)
     |    |-- sepalWidth: float (nullable = true)
     |    |-- petalLength: float (nullable = true)
     |    |-- petalWidth: float (nullable = true)
     |    |-- species: string (nullable = true)
     |-- offset: long (nullable = true)


Выбрали само значение и отступ. Видим что в `value` подтянулась наша схема. 

Поднимем значения в `value` на уровень выше.

```python
parsed_iris = value_iris.select("value.*", "offset")
parsed_iris.printSchema()
```

    root
     |-- sepalLength: float (nullable = true)
     |-- sepalWidth: float (nullable = true)
     |-- petalLength: float (nullable = true)
     |-- petalWidth: float (nullable = true)
     |-- species: string (nullable = true)
     |-- offset: long (nullable = true)


```python
out = console_output(parsed_iris.withColumn(
    'foo', 
    F.col("sepalLength") / F.col("petalLength")
), 30)
```

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +-----------+----------+-----------+----------+-------+------+------------------+
    |sepalLength|sepalWidth|petalLength|petalWidth|species|offset|               foo|
    +-----------+----------+-----------+----------+-------+------+------------------+
    |        5.1|       3.5|        1.4|       0.2| setosa|     0| 3.642857136775036|
    |        4.9|       3.0|        1.4|       0.2| setosa|     1| 3.500000127724241|
    |        4.7|       3.2|        1.3|       0.2| setosa|     2|3.6153846012770066|
    |        4.6|       3.1|        1.5|       0.2| setosa|     3| 3.066666603088379|
    |        5.0|       3.6|        1.4|       0.2| setosa|     4|3.5714286322496385|
    +-----------+----------+-----------+----------+-------+------+------------------+


```python
out.stop()
```

2\.6\. Чекпоинты

```python
def console_output_checkpointed(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .option("truncate",False) \
        .option("checkpointLocation", "shadrin_iris_console_checkpoint") \
        .start()

out = console_output_checkpointed(parsed_iris, 5)
out.stop()
```
    
    -------------------------------------------
    Batch: 5
    -------------------------------------------
    +-----------+----------+-----------+----------+-------+------+
    |sepalLength|sepalWidth|petalLength|petalWidth|species|offset|
    +-----------+----------+-----------+----------+-------+------+
    |5.0        |3.2       |1.2        |0.2       |setosa |35    |
    |5.5        |3.5       |1.3        |0.2       |setosa |36    |
    |4.9        |3.6       |1.4        |0.1       |setosa |37    |
    |4.4        |3.0       |1.3        |0.2       |setosa |38    |
    |5.1        |3.4       |1.5        |0.2       |setosa |39    |
    +-----------+----------+-----------+----------+-------+------+
    
При следующем чтении топика с помощью метода `console_output_checkpointed` чтение начнется со следующей записи. Метаданные о чекпойнте хранятся в папке `shadrin_iris_console_checkpoint` на HDFS.

```python
out = console_output_checkpointed(parsed_iris, 5)
```

    -------------------------------------------
    Batch: 6
    -------------------------------------------
    +-----------+----------+-----------+----------+-------+------+
    |sepalLength|sepalWidth|petalLength|petalWidth|species|offset|
    +-----------+----------+-----------+----------+-------+------+
    |5.0        |3.5       |1.3        |0.3       |setosa |40    |
    |4.5        |2.3       |1.3        |0.3       |setosa |41    |
    |4.4        |3.2       |1.3        |0.2       |setosa |42    |
    |5.0        |3.5       |1.6        |0.6       |setosa |43    |
    |5.1        |3.8       |1.9        |0.4       |setosa |44    |
    +-----------+----------+-----------+----------+-------+------+
    
    
```python
out.stop()
```

