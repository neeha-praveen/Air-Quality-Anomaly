from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import avg, stddev, when, col, lit, window, to_timestamp, first
import requests

spark = SparkSession.builder \
    .appName("AirQualityStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema of incoming JSON
schema = StructType([
    StructField("city", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("pm25", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("co", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("so2", DoubleType()),
    StructField("o3", DoubleType()),
    StructField("timestamp", StringType())
])

# Read Kafka Stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "air_quality_raw") \
    .option("startingOffsets", "latest") \
    .load()

# Convert binary Kafka value → string
json_df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON
parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp("timestamp")
)

# parsed_df also needs a watermark before a stream-stream join
parsed_df = parsed_df.withWatermark("timestamp", "1 hour")

result_df = parsed_df \
    .groupBy(
        window(col("timestamp"), "10 minutes", "2 minute"),
        col("city")
    ) \
    .agg(
        avg("pm25").alias("mean_pm25"),
        stddev("pm25").alias("std_pm25"),
        first("pm25").alias("pm25"),            # <-- raw reading, not avg
        first("timestamp").alias("timestamp")  # <-- carry timestamp through
    ) \
    .withColumn(
        "z_score",
        when(col("std_pm25") != 0,
             (col("pm25") - col("mean_pm25")) / col("std_pm25"))
        .otherwise(lit(0.0))
    ) \
    .withColumn(
        "anomaly",
        when(col("z_score") > 2, True).otherwise(False)
    )

def write_to_es(batch_df, batch_id):
    batch_df = batch_df.withColumn("timestamp", col("timestamp").cast("timestamp"))  # ← add this
    batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "true") \
        .option("es.resource", "air_quality_index") \
        .option("es.mapping.date.rich", "false") \
        .option("es.field.read.as.array.include", "") \
        .mode("append") \
        .save()

mapping = {
    "mappings": {
        "properties": {
            "timestamp": {"type": "date"},
            "city":      {"type": "keyword"},
            "pm25":      {"type": "float"},
            "mean_pm25": {"type": "float"},
            "std_pm25":  {"type": "float"},
            "z_score":   {"type": "float"},
            "anomaly":   {"type": "boolean"}
        }
    }
}
requests.put("http://localhost:9200/air_quality_index", json=mapping)

query = result_df.select(
    "city",
    "pm25",
    "mean_pm25",
    "std_pm25",
    "z_score",
    "anomaly",
    "timestamp"
).writeStream \
.foreachBatch(write_to_es) \
.option("checkpointLocation", "C:/bda_project/checkpoints/air_quality") \
.outputMode("update") \
.start()

# Silence the state store noise specifically                                        
log4j = spark._jvm.org.apache.log4j
log4j.LogManager.getLogger("org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider").setLevel(log4j.Level.ERROR)

query.awaitTermination()