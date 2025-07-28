# Databricks notebook source
# MAGIC %md
# MAGIC 🚀 Proyecto: Análisis de Retrasos de Vuelos con PySpark en Databricks

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

# Ruta interna de Databricks: reemplázala por la tuya si reproduces el proyecto
df = spark.read.csv(
    "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv", 
    header=True, 
    inferSchema=True
)

df.show(5)

# COMMAND ----------

df.printSchema()   
#Ver el esquema representa conocer el nombre de las columnas que forman el df y e tipo de dato de cada uno de ellas

# COMMAND ----------

print("numero de registro",df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC  Paso 2: Limpieza de datos (Extract & Transform)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#Quitar los valores nulos en las columnas
df_clean = df.dropna(subset=["date","delay","distance","origin","destination"])

print("Verificar si existian valores nulos , la cantidad de filas deberia ser menor a 1391578",df_clean.count())

# Filtrar solo vuelos con distancias > 0 y retrasos no negativos

df_clean = df_clean.filter((col("distance") > 0) & (col("delay") >= 0))


# COMMAND ----------

# MAGIC %md
# MAGIC Paso 3: Análisis Exploratorio

# COMMAND ----------

#Retraso promedio por aeropuerto de origen

df_clean.groupBy("origin").avg("delay").orderBy("avg(delay)",ascending = False).show(10)


# COMMAND ----------

# Top 10 rutas con más retrasos
df_clean.groupBy("origin", "destination").avg("delay").orderBy("avg(delay)", ascending=False).show(10)

# COMMAND ----------

from pyspark.sql.functions import substring
#Número de vuelos por mes

df_by_month = df_clean.withColumn("month", substring(col("date"), 5, 2))
df_by_month.groupBy("month").count().orderBy('month').show(10)
                                    

# COMMAND ----------

from pyspark.sql.functions import when

df_enriched = df_clean.withColumn(
    "delay_category",
    when(col("delay") == 0, "On time")
    .when((col("delay") > 0) & (col("delay") <= 30), "Slight delay")
    .when((col("delay") > 30) & (col("delay") <= 120), "Moderate delay")
    .otherwise("Severe delay")
)

df_enriched.select("origin", "destination", "delay", "delay_category").show(10)


# COMMAND ----------

output_path = "dbfs:/FileStore/flights_etl_results"

df_enriched.write.mode("overwrite").parquet(output_path)
print(f"Resultados guardados en: {output_path}")
