## Урок 2. Kafka. Архитектура.

<details>
    <summary>Термины и определения по мотивам лекции</summary>
    
    Kafka - распределённая key-value система транспорта сообщений (альтернатива - rabbitmq), гарантирующая хронологический порядок сообщения внутри одной партиции.
    Broker - сервер кафки на дата-ноде (демон).
    Zookeeper - распределённая key-value база с супер консистентностью, но низкой пропускной способностью для хранения конфигурации брокеров.
    Topic - таблица в кафке с параметрами партиционирования, репликации и retention time.
    Partition - лог сообщений с возможностью только вставки сообщения в конец и без возможности изменить уже записанные сообщения. Несколько партиций в топике позволяют снизить нагрузку на консьюмеры (параллелизм).
    Replicas - копия партиции на соседнем брокере (соседней дата-ноде, соседней стойке), которая заполняется при вставке сообщения и вычитывается только в случае падения лидера топика.
    Retention time - гарантированное время хранения сообщения в топике. Может быть изменено после создания топика.
    Max size - максимальный объем партиции, по достижении которого из партиции удаляются сообщения из начала очереди. Может быть изменено после создания топика.
    Offset - абсолютный порядковый номер сообщения в текущей партиции, хранимый в течение жизни топика.
    Producer - сервер, который поставляет сообщения в топик.
    Acks - параметр подтверждеине записи сообщения от брокера продьюссеру. 
        Acks = -1 - без ожидания подтверждения от брокера;
        Acks = 0 - с ожиданием подтверждения записи сообщения в партицию;
        Acks = 1 - с ожиданием подтверждения записи сообщения в партицию и реплики;
    Consumer - сервис, который читает сообщения из партиции. По-умолчанию партиция вычитывается с конца в будущее. Порядок вычитки партиции определяется параметром offset ("-1" - earliest). Партиции вычитываются целиком, друг за другом. После вычитки сообщение из очереди не удаляется.
    Consumer group - группа консьюмеров, которые вычитывают данные из топика параллельно. Параллелизм достигается за счет того что каждый консьюмер читает свою партицию. Каждый консьюмер, не включенный в группу, вычитывает топик целиком. Разные консьюмеры из группы могут вычитывать одну партицию.
    At most once, At least once, Exactly once - настройки тракта сообщений, определяющие тип проверки сообщений на дубли.
        At most once - один раз записали в топик и не проверяем что записалось;
        At least once - записали хотя бы одно сообщение в топик, возможно больше (обычно стоит эта настройка); 
        Exactly once - записали только одно сообщение, без дублей;
</details>


##### Создать свои топик в kafka. Поиграться с retention time, console-consumer, console-producer.

1\. Подключаемся к серверу

```bash
ssh BD_274_ashadrin@89.208.223.141 -i ~/.ssh/id_rsa_gb_spark
```
 
2\. Создаем топик.

```bash
[BD_274_ashadrin@bigdataanalyti cs-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --create --topic shadrin_les2 --zookeeper bigdataanalytics-worker-0.novalocal:2181 --partitions 3 --replication-factor 2 --config retention.ms=-1
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic "shadrin_les2".

```

3\. Изменяем время хранения данных на 1 минуту.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --zookeeper bigdataanalytics-worker-0.novalocal:2181 --alter --config retention.ms=60000 --topic shadrin_les2
WARNING: Altering topic configuration from this script has been deprecated and may be removed in future releases.
         Going forward, please use kafka-configs.sh for this functionality
Updated config for topic "shadrin_les2".
```

4\. В первом терминале создаём консьюмер, который вычитыват сообщения из топика и пишет их в консоль.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-console-consumer.sh --topic shadrin_les2 --bootstrap-server bigdataanalytics-worker-0.novalocal:6667
```

5\. Во втором терминале создаем провайдер, который читает ввод с консоли и записывет сообщения в топик.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-console-producer.sh --topic shadrin_les2 --broker-list bigdataanalytics-worker-0.novalocal:6667
>message 1
>message 2
>message 3
>message 4
>message 5
>message 6
>
```

6\. В первом терминале наблюдаем пришедшие данные.

```bash
message 1
message 2
message 3
message 4
message 5
message 6
```

7\. Далее в этом терминале завершим скрипт чтения сообщений (ctrl+C) и пересоздадим консьюмер, который вычитыват сообщения из самого начала топика (флаг `--from-beginning`). Наблюдаем что вычитаны все не удалённые по `retention-time` сообщения с начала жизни топика. И каждая партиция вычитывается целиком (порядок сообщений в рамках топика не хронологический). 

```bash
^CProcessed a total of 6 messages
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-console-consumer.sh --topic shadrin_les2 --from-beginning --bootstrap-server bigdataanalytics-worker-0.novalocal:6667
message 1
message 4
message 3
message 6
message 2
message 5
```

Видим что запись в партиции происходит последовательно в каждую партицию (1-2-3-1-2-3 и т.д.). А вычитываются партиции целиком, но не обязательно в том порядке, в котором происходила запись. В нашем случае порядок чтения из партиции был 1-3-2.

8\. Спустя некоторое время в этом же терминале пересоздаем консьюмер с флагом `--from-beginning`. Старые сообщения удалены из топика и не вычитываются (`retention.ms=60000`). В консоли наблюдаем только новые сообщения.

```bash
^CProcessed a total of 6 messages
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-console-consumer.sh --topic shadrin_les2 --from-beginning --bootstrap-server bigdataanalytics-worker-0.novalocal:6667
message 7
```

9\. В конце удаляем топик.

```bash
[BD_274_ashadrin@bigdataanalytics-worker-0 ~]$ /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --zookeeper bigdataanalytics-worker-0.novalocal:2181 --delete --topic shadrin_les2
Topic shadrin_les2 is marked for deletion.
Note: This will have no impact if delete.topic.enable is not set to true.
```
