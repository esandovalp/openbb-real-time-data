from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType

# Definir schema del mensaje
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("ticker", StringType()) \
    .add("price", DoubleType()) \
    .add("volume", IntegerType())

# Crear SparkSession
spark = SparkSession.builder \
    .appName("KafkaStockStream") \
    .getOrCreate()

# Leer desde Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "acciones") \
    .option("startingOffsets", "latest") \
    .load()

# Transformar el valor en JSON
df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Agregaciones por ticker cada 1 minuto
agg = df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("ticker")
    ) \
    .agg(
        {"price": "avg", "volume": "sum"}
    ) \
    .withColumnRenamed("avg(price)", "avg_price") \
    .withColumnRenamed("sum(volume)", "total_volume")

# Escribir en consola
agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Escribir en parquet
agg.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/parquet/") \
    .option("checkpointLocation", "data/parquet/_checkpoints") \
    .start() \
    .awaitTermination()