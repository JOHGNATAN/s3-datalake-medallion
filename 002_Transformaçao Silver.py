# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName('Transformação Silver Data') \
    .getOrCreate()

bronze_path = '/mnt/aws_mount/bronze/vendas'
silver_path = '/mnt/aws_mount/silver/vendas'

df_bronze = spark.read.format('parquet').load(bronze_path)

df_silver = df_bronze.withColumn("Data", to_date(col("Data"), "yyyy-MM-dd")) \
                     .withColumn("Email", lower(expr("regexp_replace(split(EmailNome, ':')[0], '[()]', '')"))) \
                     .withColumn("Nome", expr("split(split(EmailNome, ':')[1], ', ')")) \
                     .withColumn("Nome", expr("concat(Nome[1], ' ', Nome[0])")) \
                     .withColumn("Cidade", expr("split(Cidade, ',')[0]")) \
                     .withColumn("PrecoUnitario", format_number(col("PrecoUnitario"), 2)) \
                     .withColumn("CustoUnitario", format_number(col("CustoUnitario"), 2)) \
                     .withColumn("TotalVendas", format_number(col("PrecoUnitario") * col("Unidades"),2))\
                     .drop("EmailNome")\
                     .drop("IdCampanha")   
                     

df_bronze.unpersist()

#display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gravar transformações Silver
# MAGIC
# MAGIC Particionamento por ano e mês para otimizar consultas baseadas em data, com recomendação de tamanho de arquivo em formato Parquet

# COMMAND ----------

# simulando grande quantidade de dados, por isso o partitionBy, o que melhora na hora de realizar consultas por mês/ano
df_silver.withColumn('Ano', year('Data')) \
            .withColumn('Mes', month('Data')) \
            .write.option('maxRecordsPerFile', 50000) \
            .mode('overwrite') \
            .partitionBy('Ano', 'Mes') \
            .parquet(silver_path)

# Limpando a Memória
df_silver.unpersist()

# COMMAND ----------

import gc

gc.collect()

spark.catalog.clearCache()
