# Tarea3-BigData-ValentinaHerrera

Análisis en batch con PySpark del conjunto de datos Casos positivos de COVID-19 en Colombia (Instituto Nacional de Salud, Datos Abiertos Colombia, código gt2j-8ykr).

# Problema

Identificar cómo se distribuyeron los casos de COVID-19 en Colombia por departamento, edad y sexo, y qué grupos tuvieron mayor letalidad.

# Qué hace el script tarea3_batch.py
1. Carga el CSV en un DataFrame de Spark.
2. Limpieza: elimina casos duplicados, estandariza el texto (mayúsculas, sin espacios), quita filas vacías y edades imposibles.
3. Transformación: crea la columna grupo_edad y la columna fallecido (1 o 0).
4. Análisis exploratorio: top 10 departamentos con más casos, casos y letalidad por grupo de edad, casos por sexo y edad promedio.
5. Visualización: guarda 3 gráficos PNG (matplotlib) en ~/resultados_tarea3.

# Requisitos
- Apache Spark 3.5.x con Python 3 (máquina virtual del curso)
- pip install matplotlib pandas kafka-python

# Ejecución

# 1. Descargar una muestra de 100.000 registros
wget -O covid_colombia.csv 'https://www.datos.gov.co/resource/gt2j-8ykr.csv?$limit=100000'

# 2. Ejecutar el análisis
spark-submit tarea3_batch.py

# 3. Ver resultados
ls ~/resultados_tarea3

# Procesamiento en tiempo real (Kafka + Spark Streaming)
- covid_producer.py: lee los casos reales del CSV descargado de datos.gov.co y los envía uno por uno (cada 0,5 s) al topic casos_covid, simulando su llegada en tiempo real.
- covid_streaming_consumer.py: lee el topic con Spark Structured Streaming y calcula, por ventanas de 1 minuto y por departamento, el número de casos, la edad promedio y los fallecidos.




Valentina Herrera Rocha – UNAD, Ingeniería de Sistemas, 2026
