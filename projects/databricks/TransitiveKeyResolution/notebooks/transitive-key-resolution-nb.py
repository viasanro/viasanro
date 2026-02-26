# Databricks notebook source
# MAGIC %md
# MAGIC **Resolución Transitiva de Claves 1:N**

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,DTS Ventas
data = [("A1", "202401", "100"),
        ("A2", "202401", "200")]

columns = ["sku_a", "mes", "venta"]

ventas = spark.createDataFrame(data, columns)
ventas.show()

# COMMAND ----------

# DBTITLE 1,DTS Productos
data = [("A1", "B10"),
        ("A1", "B11"),
        ("A2", "B20")]

columns = ["sku_a", "sku_b"]

productos = spark.createDataFrame(data, columns)
productos.show()

# COMMAND ----------

# DBTITLE 1,DTS Forecast
data = [("B10", "202401", "90"),
        ("B11", "202401", "110"),
        ("B20", "202401", "210")]

columns = ["sku_b", "mes", "forecast"]

forecast = spark.createDataFrame(data, columns)
forecast.show()

# COMMAND ----------

# DBTITLE 1,ID Aux
ventas = ventas.withColumn("id_mono", F.monotonically_increasing_id())
ventas.show()

# COMMAND ----------

# DBTITLE 1,Join con Tabla Transitiva
ventas_transitiva = ventas.join(productos, "sku_a", how="left")
ventas_transitiva.show()

# COMMAND ----------

# DBTITLE 1,Join con Forecast
joined_df = ventas_transitiva.join(forecast, ["sku_b", "mes"], how="left")
joined_df.show()

# COMMAND ----------

# DBTITLE 1,Collect
grouped_df = joined_df.groupBy("id_mono", "sku_a", "mes", "venta") \
                    .agg(F.collect_list(F.struct("sku_b","forecast")).alias("opciones"))

grouped_df.show(truncate=False) 

# COMMAND ----------

# DBTITLE 1,Evaluación del Schema
grouped_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Supongamos regla:**<br>
# MAGIC _Elegir el forecast más cercano a la venta_<br>
# MAGIC > Para A1:<br>
# MAGIC Venta = 100<br>
# MAGIC Opciones = 90 y 110 <br>
# MAGIC Ambos están a distancia 10 (empate) <br>
# MAGIC Supongamos que elegimos el mayor.

# COMMAND ----------

# DBTITLE 1,Explode
expanded_df = grouped_df.selectExpr(
    "id_mono",
    "sku_a",
    "mes",
    "venta",
    "explode(opciones) as opcion"
)
expanded_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Evaluación del Schema
expanded_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Aplicar Regla de Negocio
window = Window.partitionBy("id_mono") \
               .orderBy(F.abs(F.col("opcion.forecast").cast("int") - F.col("venta").cast("int")) \
                        ,F.col("opcion.forecast").cast("int").desc())

final_df = expanded_df.withColumn("rn", F.row_number().over(window)) \
                .filter("rn = 1") \
                .drop("rn", "id_mono")

final_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > ¿Qué resolvimos? <br>
# MAGIC
# MAGIC - La apertura 1→N <br>
# MAGIC - La ambigüedad del mapeo <br>
# MAGIC - Sin duplicar métricas <br>
# MAGIC -  Conservando la granularidad original