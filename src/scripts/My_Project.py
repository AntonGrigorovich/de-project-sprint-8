#1 Действиме 1 - Подготовить инфраструктуру в telegram боте запросить Модуль 8: Потоковая обработка данных (проект)
#1.1 Открыть visual studio и поменять HostName

#Шаг 1. Проверить работу потока.
#################################
##Инфраструктура и файл в топик##
#################################

#1.2 Запустить виртуальную машину по данному хосту
#1.3 Установить докер и запустить Attach Shell
#2 Создать в директории  /data/ txt-файл first_message который мы будем отправлять в брокер
#2.1 Проверить, есть ли директория /data внутри контейнера: 
#cd / (выйти в корень), 
#ls -la (проверить содержимое)
#2.2 Перейти в папку /data (cd /data) и создать текстовый файл (touch first_message.txt) 
#2.3 Перейти в текстовый редактор (vi first_message.txt) 
#2.4 Открыть режим редактирования (e) 
#2.5 Вставить скопированные строки, не выходя из режима INSERT (через shift) 
#first_message:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant.ru","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516} 
#2.6 Режи ввода команд shift + ":" - обязательно на буквах, не цифрах
#2.6.1 Выйти из режима INSERT (ESC) и сохранить изменения (:wq)
#2.7 Залить данные в свой топик
#kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username="de-student" -X sasl.password="ltcneltyn" -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt -t antongrigo -K: -P -l /data/first_message.txt
#2.7 Вызвать привычный jupyter notebook --allow-root (Порядок имеет значение)

#################################
###№Postgresql, таблица, данные##
#################################
#1 - создать или запустить postgresql в docker "sprint8_project"
#docker run --name sprint8_project -p 5432:5432 -e POSTGRES_USER=jovyan -e POSTGRES_PASSWORD=jovyan -d postgres:13.3
#2 - Создать таблицу в базе public 
#CREATE TABLE public.subscribers_restaurants (id serial4 NOT NULL, client_id varchar NOT NULL, restaurant_id varchar NOT NULL, CONSTRAINT pk_id PRIMARY KEY (id));
#INSERT INTO public.subscribers_restaurants(client_id, restaurant_id) VALUES ('223e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000');
#INSERT INTO public.subscribers_restaurants(client_id, restaurant_id) VALUES ('323e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000');
#INSERT INTO public.subscribers_restaurants(client_id, restaurant_id) VALUES ('423e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000');
#INSERT INTO public.subscribers_restaurants(client_id, restaurant_id) VALUES ('523e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000');
#INSERT INTO public.subscribers_restaurants(client_id, restaurant_id) VALUES ('623e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000');
#INSERT INTO public.subscribers_restaurants(client_id, restaurant_id) VALUES ('723e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000');
#INSERT INTO public.subscribers_restaurants(client_id, restaurant_id) VALUES ('823e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000');
#INSERT INTO public.subscribers_restaurants(client_id, restaurant_id) VALUES ('923e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174001');
#INSERT INTO public.subscribers_restaurants(client_id, restaurant_id) VALUES ('923e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174001');
#INSERT INTO public.subscribers_restaurants(client_id, restaurant_id) VALUES ('023e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000');
#SELECT * FROM public.subscribers_restaurants




#######################
#Работаем по шагам 2-7#
#######################

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import current_date
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# необходимые библиотеки для интеграции Spark с Kafka и Postgres
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# создаём SparkSession с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и Postgres
spark = SparkSession.builder \
    .appName("RestauranttSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()


truststore_location = "/etc/security/ssl"
truststore_pass = "de_sprint_8"
 
#Шаг 2. Прочитать данные об акциях из Kafka.    
df = (spark.read
      .format('kafka')
      .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
      .option('kafka.security.protocol', 'SASL_SSL')
      .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
      .option('kafka.sasl.jaas.config','org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";')
      #.option('kafka.ssl.truststore.location', truststore_location)
      #.option('kafka.ssl.truststore.password', truststore_pass)
      .option("subscribe", "antongrigo")
      .load())

#Шаг 4. Преобразование JSON в датафейм.(задать структуру)
schema = StructType([
    StructField("restaurant_id", StringType()),
    StructField("adv_campaign_id", StringType()),
    StructField("adv_campaign_content", StringType()),
    StructField("adv_campaign_owner", StringType()),
    StructField("adv_campaign_owner_contact", StringType()),
    StructField("adv_campaign_datetime_start", StringType()),
    StructField("adv_campaign_datetime_end", StringType()),
    StructField("datetime_created", StringType())
])
#Шаг 4. Преобразование JSON в датафейм. (выполнить преобразование)
df = (df
      .withColumn('value', f.col('value').cast(StringType()))
      .withColumn('key', f.col('key').cast(StringType()))
      .withColumn('event', f.from_json(f.col('value'), schema))
      .selectExpr('event.*', '*').drop('event')
      )
 
df.printSchema()
df.select('restaurant_id', 'adv_campaign_id', 'adv_campaign_content', 'adv_campaign_owner', 'adv_campaign_owner_contact', 'adv_campaign_datetime_start', 'adv_campaign_datetime_end', 'datetime_created').show(truncate=False)


#Шаг 3. Прочитать данные о подписчиках из Postgres.
# находим всех пользователей с подпиской на рестораны
subscribers_restaurants_df = spark.read \
                    .format('jdbc') \
                    .option("url", "jdbc:postgresql://localhost:5432/de") \
                    .option('driver', 'org.postgresql.Driver') \
                    .option("dbtable", "subscribers_restaurants") \
                    .option("user", "jovyan") \
                    .option("password", "jovyan") \
                    .load()

subscribers_restaurants_df.show()

#Шаг 5. Провести JOIN потоковых и статичных данных.
subscribers_feedback = df.join(subscribers_restaurants_df, [df.restaurant_id == subscribers_restaurants_df.restaurant_id], 'inner').drop(subscribers_restaurants_df.restaurant_id).select('id', 'restaurant_id', 'adv_campaign_id', 'adv_campaign_content', 'adv_campaign_owner', 'adv_campaign_owner_contact', 'adv_campaign_datetime_start', 'adv_campaign_datetime_end', 'datetime_created', 'client_id', current_date().alias("trigger_datetime_created"))

#Шаг 6. Отправить результаты JOIN в Postgres для аналитики фидбэка (subscribers_feedback).

#Записать
subscribers_feedback.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/de") \
        .option('driver', 'org.postgresql.Driver') \
        .option("dbtable", "subscribers_feedback") \
        .option("user", "jovyan") \
        .option("password", "jovyan") \
        .save()

#Прочитать(проверить)
subscribers_feedback_examination = spark.read \
                    .format('jdbc') \
                    .option("url", "jdbc:postgresql://localhost:5432/de") \
                    .option('driver', 'org.postgresql.Driver') \
                    .option("dbtable", "subscribers_feedback") \
                    .option("user", "jovyan") \
                    .option("password", "jovyan") \
                    .load()

subscribers_feedback_examination.show()

#Шаг 7. Отправить данные, сериализованные в формат JSON, в Kafka для push-уведомлений. + #Шаг 8. Персистентность датафрейма.

#сохранить как json в памяти
df_join.write.mode('overwrite').json("json_from_df.json")

#прочитать из памяти сохранённый json, сохранив в датафрейм
df_json = spark.read.json("json_from_df.json")
df_json.show()

#Запускаем стриминг
df_json
      .writeStream
      .outputMode("append")
      .foreachBatch(foreach_batch_function)
      .start()
      .awaitTermination()

