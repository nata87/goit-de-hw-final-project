from kafka import KafkaProducer
from config import kafka_config
from datetime import datetime
from pyspark.sql import SparkSession


# MySQL configs
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
jdbc_driver = "com.mysql.cj.jdbc.Driver"
jdbc_table = "olympic_dataset.athlete_event_results"

# Kafka configs
kafka_bootstrap_servers = kafka_config['bootstrap_servers']
kafka_user = kafka_config['username']
kafka_password = kafka_config['password']
kafka_security_protocol = "SASL_PLAINTEXT"
kafka_sasl_mechanism = kafka_config['sasl_mechanism']
kafka_sasl_jaas_config = f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_user}\" password=\"{kafka_password}\";"


topic = "nata_athlete_event_results"

spark = SparkSession.builder \
    .appName("AthleteEventResultsProducer") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read MySQL
df = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver=jdbc_driver,
    dbtable=jdbc_table,
    user=jdbc_user,
    password=jdbc_password
).load()

# Write to Kafka
df.selectExpr("CAST(athlete_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config).option("topic", topic).save()

print("Successfully loaded data to Kafka topic")


