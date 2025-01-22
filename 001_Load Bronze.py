# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

lz_income_data = '/mnt/aws_mount/landingzone/vendas/processar'
lz_outcome_data = '/mnt/aws_mount/landingzone/vendas/processado'
bronze_path = '/mnt/aws_mount/bronze/vendas'

spark = SparkSession.builder \
    .appName('Load Bronze Data') \
    .getOrCreate()


df_vendas = spark.read.option("header", "true") \
                    .option("inferschema", "true").csv(lz_income_data) \
                    .withColumn("filename", regexp_extract(input_file_name(), "([^/]+)$", 0))

distinct_filenames = df_vendas.select("filename").distinct()

#display(distinct_filenames)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Salvar/Persistir dados na camada Bronze
# MAGIC
# MAGIC Os dados serão salvos de forma particionada **Ano e Mês**

# COMMAND ----------

# simulando grande quantidade de dados, por isso o partitionBy, o que melhora na hora de realizar consultas por mês/ano
df_vendas.withColumn('Ano', year('Data')) \
            .withColumn('Mes', month('Data')) \
            .write.mode('overwrite') \
            .partitionBy('Ano','Mes').parquet(bronze_path)

df_vendas.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mover os arquivos processados para pasta processado

# COMMAND ----------

# Verifica se há arquivos distintos na coluna 'filename'
if distinct_filenames.select("filename").distinct().count() > 0:
    # Coleta os nomes dos arquivos distintos
    filenames = distinct_filenames.select("filename").distinct().collect()

    # Move cada arquivo do caminho de origem para o caminho de destino

    for row in filenames:
        try:
            src_path = row["filename"]
            dbutils.fs.cp(lz_income_data + "/" + src_path, lz_outcome_data)
            print('Arquivo copiado com sucesso!')
            dbutils.fs.rm(lz_income_data + "/" + src_path)
        except Exception as e:
            display(f"Ocorreu um erro:{e}")

else:
    print('Nenhum arquivo encontrado!')
