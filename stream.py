from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, lit, expr, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("Kafka Sensor Alerts").getOrCreate()

spark.sparkContext.setLogLevel("OFF")
sensor_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "building_sensors_vp5") \
    .option("startingOffsets", "latest") \
    .load()

sensor_data = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), sensor_schema).alias("data")) \
    .select("data.*")

aggregated_stream = sensor_data \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds")
    ).agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg")
    )

alert_conditions = spark.read.csv(
    "alerts_conditions.csv",
    header=True,
    inferSchema=True
).withColumnRenamed("temperature_max", "t_max") \
 .withColumnRenamed("temperature_min", "t_min") \
 .withColumnRenamed("humidity_max", "h_max") \
 .withColumnRenamed("humidity_min", "h_min")

alert_conditions = alert_conditions \
    .withColumn("t_max", expr("NULLIF(t_max, -999)")) \
    .withColumn("t_min", expr("NULLIF(t_min, -999)")) \
    .withColumn("h_max", expr("NULLIF(h_max, -999)")) \
    .withColumn("h_min", expr("NULLIF(h_min, -999)"))

alert_stream = aggregated_stream.crossJoin(alert_conditions) \
    .filter(
        (col("t_avg") > col("t_max")) | (col("t_avg") < col("t_min")) |
        (col("h_avg") > col("h_max")) | (col("h_avg") < col("h_min"))
    ).select(
        struct(
            col("window.start").alias("start"),
            col("window.end").alias("end")
        ).alias("window"),
        col("t_avg"),
        col("h_avg"),
        col("code"),
        col("message"),
        lit("current_timestamp").alias("timestamp")
    )


alert_stream.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "alerts_output_vp5") \
    .option("checkpointLocation", "./checkpoints") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
