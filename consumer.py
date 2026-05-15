
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, avg, current_timestamp, to_json, struct, broadcast, round as spark_round
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import uuid
from config import kafka_config



jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_bio"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
jdbc_properties = {
    "user": jdbc_user,
    "password": jdbc_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}


spark = SparkSession.builder \
    .appName("ReadAthleteBio") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


df_bio = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable=jdbc_table,
    user=jdbc_user,
    password=jdbc_password
).load()

df_bio.show(10)


df_bio = df_bio.withColumn("height", col("height").cast(FloatType()))
df_bio = df_bio.withColumn("weight", col("weight").cast(FloatType()))


df_bio_filtered = df_bio.filter(
    (col("height").isNotNull()) & (col("weight").isNotNull())
)

df_bio_filtered.show(10)

EVENT_SCHEMA = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("timestamp", StringType(), True),
])

kafka_bootstrap_servers = kafka_config['bootstrap_servers']
kafka_user = kafka_config['username']
kafka_password = kafka_config['password']
kafka_security_protocol = "SASL_PLAINTEXT"
kafka_sasl_mechanism = kafka_config['sasl_mechanism']
kafka_sasl_jaas_config = f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_user}\" password=\"{kafka_password}\";"

# Read stream from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "nata_athlete_event_results") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "50000") \
    .option("failOnDataLoss", "false") \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
    .option("kafka.group.id", f"spark-{uuid.uuid4()}") \
    .load()
# Parse Kafka messages
events_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json("json_str", EVENT_SCHEMA).alias("data")) \
    .select("data.*") \
    .withColumn("athlete_id", col("athlete_id").cast(IntegerType())) \
    .withColumn("ingest_ts", current_timestamp())

# Join and aggregate
joined_df = events_stream.join(broadcast(df_bio_filtered), on="athlete_id")

agg_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        spark_round(avg("height"), 3).alias("avg_height"),
        spark_round(avg("weight"), 3).alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )

# Write batch
def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty — skipping")
        return

    print(f"Batch {batch_id}: writing {batch_df.count()} rows")

    batch_df.withColumn("value", to_json(struct(*batch_df.columns))) \
        .select("value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", "nata_athlete_aggregation") \
        .option("kafka.security.protocol", kafka_security_protocol) \
        .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
        .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
        .save()

    batch_df.write.jdbc(url=jdbc_url, table="nata_athlete_aggregation", mode="append", properties=jdbc_properties)

# Start stream
agg_df.writeStream \
    .foreachBatch(process_batch).outputMode("update").trigger(processingTime="15 seconds").option("checkpointLocation", "./checkpoints/agg_stream").start().awaitTermination()
