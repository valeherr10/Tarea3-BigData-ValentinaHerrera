import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, when, count, sum, avg, round

# 1. CREAR LA SESIÓN DE SPARK
spark = SparkSession.builder.appName("AnalisisCovidColombia").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. CARGAR EL DATASET
print("\n--- 1. Cargando el dataset ---")
df = spark.read.csv("file:///home/vboxuser/covid_colombia.csv", header=True, inferSchema=True)
print("Registros cargados:", df.count())
df.printSchema()

# 3. LIMPIEZA
print("\n--- 2. Limpieza de datos ---")
df = df.dropDuplicates(["id_de_caso"])                                     # quitar casos repetidos
df = df.withColumn("departamento", upper(trim(col("departamento_nom"))))   # texto en mayúsculas y sin espacios
df = df.withColumn("sexo", upper(trim(col("sexo"))))
df = df.withColumn("estado", upper(trim(col("estado"))))
df = df.dropna(subset=["departamento", "edad", "sexo"])                    # quitar filas vacías
df = df.filter((col("edad") >= 0) & (col("edad") <= 110))                  # quitar edades imposibles
print("Registros después de limpiar:", df.count())

# 4. TRANSFORMACIÓN
print("\n--- 3. Transformación ---")
df = df.withColumn("grupo_edad",
        when(col("edad") < 20, "0-19")
        .when(col("edad") < 40, "20-39")
        .when(col("edad") < 60, "40-59")
        .when(col("edad") < 80, "60-79")
        .otherwise("80+"))
df = df.withColumn("fallecido", when(col("estado") == "FALLECIDO", 1).otherwise(0))
df.select("departamento", "edad", "grupo_edad", "sexo", "estado", "fallecido").show(5)

# 5. ANÁLISIS EXPLORATORIO
print("\n--- 4. Análisis exploratorio ---")
print("Top 10 departamentos con más casos:")
por_depto = df.groupBy("departamento").agg(count("*").alias("casos")).orderBy(col("casos").desc())
por_depto.show(10)

print("Casos y letalidad por grupo de edad:")
por_edad = (df.groupBy("grupo_edad")
              .agg(count("*").alias("casos"), sum("fallecido").alias("fallecidos"))
              .withColumn("letalidad_%", round(col("fallecidos") / col("casos") * 100, 2))
              .orderBy("grupo_edad"))
por_edad.show()

print("Casos por sexo:")
por_sexo = df.groupBy("sexo").agg(count("*").alias("casos"))
por_sexo.show()

print("Edad promedio de los casos:")
df.select(round(avg("edad"), 1).alias("edad_promedio")).show()

# 6. VISUALIZACIÓN
print("\n--- 5. Visualización ---")
carpeta = os.path.expanduser("~/resultados_tarea3")
os.makedirs(carpeta, exist_ok=True)

p = por_depto.limit(10).toPandas()
plt.figure(figsize=(10, 6))
plt.barh(p["departamento"][::-1], p["casos"][::-1])
plt.title("Top 10 departamentos con más casos de COVID-19")
plt.xlabel("Casos")
plt.tight_layout()
plt.savefig(carpeta + "/1_departamentos.png")

p = por_edad.toPandas()
plt.figure(figsize=(8, 5))
plt.bar(p["grupo_edad"], p["letalidad_%"], color="firebrick")
plt.title("Letalidad (%) por grupo de edad")
plt.ylabel("% de casos fallecidos")
plt.tight_layout()
plt.savefig(carpeta + "/2_letalidad_edad.png")

p = por_sexo.toPandas()
plt.figure(figsize=(6, 6))
plt.pie(p["casos"], labels=p["sexo"], autopct="%1.1f%%")
plt.title("Casos por sexo")
plt.savefig(carpeta + "/3_sexo.png")

print("Gráficos guardados en", carpeta)
spark.stop()