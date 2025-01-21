# Databricks notebook source
# Create DataBase
spark.sql('create database if not exists lhdw_vendas')

#Use DataBase
spark.sql('use lhdw_vendas')

# COMMAND ----------

delta_path = '/mnt/lhdw/gold/vendas_delta'
files = dbutils.fs.ls(delta_path)

# Cria uma lista com os nomes e caminhos dos arquivos/pastas
arquivos_pastas = [(file.name, file.path) for file in files]

# Cria um DataFrame com os resultados
df = spark.createDataFrame(arquivos_pastas, ['Nome', 'Caminho'])

for i in df.collect():
    table_name = i['Nome'].replace('/', '')
    path = i['Caminho']
    print(f'Creating table: {table_name} at path: {path}')
    spark.sql(f"""
              create table if not exists {table_name}
              using delta
              location '{path}'
              """)

# COMMAND ----------

spark.sql('show tables').show()
