from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, min, max, sum
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaStockStats") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema del JSON
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("ticker", StringType()) \
    .add("price", DoubleType()) \
    .add("volume", IntegerType())

# Leer desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "acciones") \
    .option("startingOffsets", "latest") \
    .load()

# Parsear JSON y convertir timestamp
df = df_kafka.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select(
        col("data.timestamp").cast(TimestampType()).alias("timestamp"),
        col("data.ticker"),
        col("data.price"),
        col("data.volume")
    )

# Ventana de 1 minuto
windowed = df \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("ticker")
    ) \
    .agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        sum("volume").alias("total_volume")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("ticker"),
        col("avg_price"),
        col("min_price"),
        col("max_price"),
        col("total_volume")
    )

# Mostrar resultados por consola
query = windowed.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Mostrar resultados por consola
query = windowed.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Guardar resultados agregados en archivos Parquet
query = windowed.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "data/output_parquet") \
    .option("checkpointLocation", "data/checkpoint") \
    .start()


query.awaitTermination()

