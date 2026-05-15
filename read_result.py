from pyspark.sql import SparkSession

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "nata_athlete_aggregation"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

spark = SparkSession.builder \
    .appName("ReadAthleteBio") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df_result = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable=jdbc_table,
    user=jdbc_user,
    password=jdbc_password
).load()

df_result.createOrReplaceTempView("enriched")
spark.sql("SELECT * FROM enriched").show(truncate=False)
