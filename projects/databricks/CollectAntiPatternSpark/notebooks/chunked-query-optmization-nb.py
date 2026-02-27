# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame
import time

# COMMAND ----------

# DBTITLE 1,SparkSession
spark = SparkSession.builder.appName("chunking").getOrCreate()

# COMMAND ----------

# DBTITLE 1,DTS Ventas
# Cargamos el dataset de ventas

ventas_df = spark.read.csv('/Volumes/workspace/opt/mnt/mockdata_ventas.csv',
                    header=True,
                    inferSchema=True)
ventas_df.display()

# COMMAND ----------

# DBTITLE 1,DTS Forecast
# Cargamos el dataset de forecast

forecast_df = spark.read.csv('/Volumes/workspace/opt/mnt/mockdata_forecast.csv',
                    header=True,
                    inferSchema=True)
forecast_df.display()

# COMMAND ----------

# DBTITLE 1,Dataframe Index
# Indexamos las descripciones

w = Window.orderBy("descripcion")

descripciones_idx = ventas_df.select("descripcion") \
    .union(forecast_df.select("descripcion")) \
    .distinct() \
    .withColumn("desc_idx", F.row_number().over(w))

descripciones_idx.display()

# COMMAND ----------

# DBTITLE 1,Funcion de Transformacion
def transformacion(ventas: DataFrame, forecast: DataFrame) -> DataFrame:
    df_final = ventas.join(forecast, ["mes", "descripcion"], how="left")
    return df_final

# COMMAND ----------

# DBTITLE 1,Loop por Descripcion
# Loop por descripcion.
descripciones = ventas_df.select("descripcion") \
    .union(forecast_df.select("descripcion")) \
    .distinct().collect()
resultado = None

start = time.perf_counter()
for row in descripciones:
    df_frcst = forecast_df.filter(F.col("descripcion")==row.descripcion) \
                        .groupBy("mes", "descripcion") \
                        .agg(F.sum("frcst_hl").alias("frcst_hl"))

    df_ventas = ventas_df.filter(F.col("descripcion")==row.descripcion) \
                        .groupBy("mes", "descripcion") \
                        .agg(F.sum("ventas_hl").alias("ventas_hl"))

    df = transformacion(df_ventas, df_frcst)
    if resultado is None:
        resultado = df
    else:
        resultado = resultado.unionByName(df)
end = time.perf_counter()
print(f"Tiempo de ejecucion: {end-start:.6f} segundos")
resultado.display()

# COMMAND ----------

# DBTITLE 1,Loop por Chunk
# Configuracion
chunk_size = 10
max_idx = descripciones_idx.agg(F.max("desc_idx")).collect()[0][0]

# Loop por chunk no por descripcion.
for ini in range(1, max_idx + 1, chunk_size):
    fin = ini + chunk_size - 1

    desc_chunk = descripciones_idx.filter(
                (F.col("desc_idx")>= ini) & (F.col("desc_idx") <= fin)) \
                .select("descripcion")

    df_frcst_chunk = forecast_df.join(desc_chunk, "descripcion") \
                        .groupBy("mes", "descripcion") \
                        .agg(F.sum("frcst_hl").alias("frcst_hl"))
    #df_frcst_chunk.display()
    df_ventas_chunk = ventas_df.join(desc_chunk, "descripcion") \
                        .groupBy("mes", "descripcion") \
                        .agg(F.sum("ventas_hl").alias("ventas_hl"))
    #df_ventas_chunk.display()
    start = time.perf_counter()
    df = transformacion(df_ventas_chunk, df_frcst_chunk)
    end = time.perf_counter()
    print(f"Tiempo de ejecucion: {end-start:.6f} segundos")
    df.display()