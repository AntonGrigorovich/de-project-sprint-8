
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
spark_config = {
   "spark.app.name": "RestauranttSubscribeStreamingService",
   "spark.sql.session.timeZone": "UTC",
   "spark.jars.packages": spark_jars_packages
}

spark = SparkSession.builder \
   .configAll(spark_config) \
   .getOrCreate() 


truststore_location = "/etc/security/ssl"
truststore_pass = "de_sprint_8"
 
#Шаг 2. Прочитать данные об акциях из Kafka.    
kafka_options = {
   "kafka.bootstrap.servers": "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091",
   "kafka.security.protocol": "SASL_SSL",
   "kafka.sasl.mechanism": "SCRAM-SHA-512",
   "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn"',
   "subscribe": "antongrigo"
}

df = spark.read.format("kafka").options(**kafka_options).load() 
      
      
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

