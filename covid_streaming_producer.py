from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, sum as _sum, when, round as _round
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

spark = SparkSession.builder.appName("CovidStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Esquema de los mensajes JSON que envía el productor
schema = StructType([
    StructField("id_caso", IntegerType()),
    StructField("departamento", StringType()),
    StructField("edad", IntegerType()),
    StructField("sexo", StringType()),
    StructField("estado", StringType()),
    StructField("timestamp", TimestampType()),
])

# Leer el flujo desde Kafka
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "casos_covid")
      .load())

# Convertir el valor (bytes) a columnas
casos = df.select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*")

# Estadísticas por ventana de 1 minuto y departamento
stats = (casos
         .groupBy(window(col("timestamp"), "1 minute"), col("departamento"))
         .agg(count("*").alias("casos"),
              _round(avg("edad"), 1).alias("edad_promedio"),
              _sum(when(col("estado") == "FALLECIDO", 1).otherwise(0)).alias("fallecidos"))
         .orderBy(col("window").desc(), col("casos").desc()))

# Mostrar resultados en consola
query = (stats.writeStream
         .outputMode("complete")
         .format("console")
         .option("truncate", False)
         .start())

query.awaitTermination()