from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import avg, stddev, when, col, lit, window, to_timestamp, first, date_format
import requests

spark = SparkSession.builder \
    .appName("AirQualityStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Create index with correct mapping BEFORE Spark writes anything ─
mapping = {
    "mappings": {
        "properties": {
            "timestamp":    {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
            "city":         {"type": "keyword"},
            "pm25":         {"type": "float"},
            "mean_pm25":    {"type": "float"},
            "std_pm25":     {"type": "float"},
            "mean_pm10":    {"type": "float"},
            "mean_no2":     {"type": "float"},
            "mean_so2":     {"type": "float"},
            "mean_o3":      {"type": "float"},
            "mean_co":      {"type": "float"},
            "z_score":      {"type": "float"},
            "anomaly":      {"type": "boolean"},
            "aqi_pm25":     {"type": "float"},
            "aqi_category": {"type": "keyword"}
        }
    }
}
resp = requests.put("http://localhost:9200/air_quality_index", json=mapping)
print(f"Index creation: {resp.status_code} {resp.text}")

# ── Schema of incoming JSON ────────────────────────────────────────
schema = StructType([
    StructField("city",      StringType()),
    StructField("lat",       DoubleType()),
    StructField("lon",       DoubleType()),
    StructField("pm25",      DoubleType()),
    StructField("pm10",      DoubleType()),
    StructField("co",        DoubleType()),
    StructField("no2",       DoubleType()),
    StructField("so2",       DoubleType()),
    StructField("o3",        DoubleType()),
    StructField("timestamp", StringType())
])

# ── Read Kafka Stream ──────────────────────────────────────────────
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "air_quality_raw") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

parsed_df = parsed_df \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withWatermark("timestamp", "1 hour")

# ── Windowed aggregation ───────────────────────────────────────────
result_df = parsed_df \
    .groupBy(
        window(col("timestamp"), "10 minutes", "2 minutes"),
        col("city")
    ) \
    .agg(
        avg("pm25").alias("mean_pm25"),
        stddev("pm25").alias("std_pm25"),
        avg("pm10").alias("mean_pm10"),
        avg("no2").alias("mean_no2"),
        avg("so2").alias("mean_so2"),
        avg("o3").alias("mean_o3"),
        avg("co").alias("mean_co"),
        first("pm25").alias("pm25"),
        first("timestamp").alias("timestamp")
    )

# ── Z-score anomaly detection ──────────────────────────────────────
result_df = result_df \
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

# ── Real CPCB AQI calculation ──────────────────────────────────────
result_df = result_df \
    .withColumn(
        "aqi_pm25",
        when(col("mean_pm25") <= 30,  (col("mean_pm25") / 30) * 50)
        .when(col("mean_pm25") <= 60,  50  + ((col("mean_pm25") - 30)  / 30)  * 50)
        .when(col("mean_pm25") <= 90,  100 + ((col("mean_pm25") - 60)  / 30)  * 100)
        .when(col("mean_pm25") <= 120, 200 + ((col("mean_pm25") - 90)  / 30)  * 100)
        .when(col("mean_pm25") <= 250, 300 + ((col("mean_pm25") - 120) / 130) * 100)
        .otherwise(                    400 + ((col("mean_pm25") - 250) / 130) * 100)
    ) \
    .withColumn(
        "aqi_category",
        when(col("aqi_pm25") <= 50,  "Good")
        .when(col("aqi_pm25") <= 100, "Satisfactory")
        .when(col("aqi_pm25") <= 200, "Moderate")
        .when(col("aqi_pm25") <= 300, "Poor")
        .when(col("aqi_pm25") <= 400, "Very Poor")
        .otherwise("Severe")
    )

# ── Write to Elasticsearch ─────────────────────────────────────────
def write_to_es(batch_df, batch_id):
    batch_df = batch_df.withColumn(
        "timestamp",
        date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )
    batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "true") \
        .option("es.resource", "air_quality_index") \
        .option("es.mapping.date.rich", "false") \
        .mode("append") \
        .save()

# ── Start streaming query ──────────────────────────────────────────
query = result_df.select(
    "city",
    "pm25",
    "mean_pm25",
    "std_pm25",
    "mean_pm10",
    "mean_no2",
    "mean_so2",
    "mean_o3",
    "mean_co",
    "z_score",
    "anomaly",
    "aqi_pm25",
    "aqi_category",
    "timestamp"
).writeStream \
 .foreachBatch(write_to_es) \
 .option("checkpointLocation", "C:/bda_project/checkpoints/air_quality") \
 .outputMode("update") \
 .start()

# Silence state store noise
log4j = spark._jvm.org.apache.log4j
log4j.LogManager.getLogger(
    "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
).setLevel(log4j.Level.ERROR)

query.awaitTermination()