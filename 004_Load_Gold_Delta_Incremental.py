# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Creating a SparkSession with the required configuration for Delta Lake
spark = SparkSession.builder \
    .appName('Load Gold Delta Incremental') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()


# COMMAND ----------

# Delta Lake's Storage path

silver_path = '/mnt/aws_mount/silver/vendas'
gold_path = '/mnt/aws_mount/gold/vendas_delta' 
gold_fato_path = '/mnt/aws_mount/gold/vendas_delta/Fato_Vendas'


# COMMAND ----------


max_data_vendas = spark.read.format('delta').load(gold_fato_path) \
                            .selectExpr('max(DataVenda) as MaxDataVenda') \
                            .collect()[0]['MaxDataVenda']

#display(max_data_vendas)

#Load Silver Data filtering by max data got on max_data_vendas
df_silver = spark.read.format('parquet').load(silver_path) \
                        .filter(f'Data > "{max_data_vendas}"')


#display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando Dimensão Produto

# COMMAND ----------

# Nome da tabela destino

tb_destino = 'Dim_Produto'

dim_produto_df = df_silver.select(
    'IDProduto', 'Produto', 'Categoria'
).dropDuplicates()

dim_produto_df = dim_produto_df.withColumn('Sk_produto', monotonically_increasing_id()+1) \
                                .withColumn('data_atualizacao', current_timestamp())

#display(dim_produto_df)

dim_produto_df.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(f'{gold_path}/{tb_destino}')

#display(dim_produto_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando Dimensão Categoria

# COMMAND ----------

tb_destino = 'Dim_Categoria'

dim_categoria_df = df_silver.select(
    'Categoria'
).dropDuplicates()

dim_categoria_df = dim_categoria_df.withColumn('Sk_categoria', monotonically_increasing_id()+1) \
                                    .withColumn('data_atualizacao', current_timestamp())

#display(dim_categoria_df)

dim_categoria_df.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(f'{gold_path}/{tb_destino}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando Dimensão Segmento

# COMMAND ----------

tb_destino = 'Dim_Segmento'

dim_segmento_df = df_silver.select(
    'Segmento'
).dropDuplicates()

dim_segmento_df = dim_segmento_df.withColumn('Sk_segmento', monotonically_increasing_id()+1) \
                                .withColumn('data_atualizacao', current_timestamp())

#display(dim_segmento_df)

dim_segmento_df.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(f'{gold_path}/{tb_destino}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando Dimensão Fabricante

# COMMAND ----------

tb_destino = 'Dim_Fabricante'

dim_fabricante_df = df_silver.select(
    'IDFabricante','Fabricante'
).dropDuplicates()

dim_fabricante_df = dim_fabricante_df.withColumn('Sk_fabricante', monotonically_increasing_id()+1) \
                                    .withColumn('data_atualizacao', current_timestamp())

#display(dim_fabricante_df)

dim_fabricante_df.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(f'{gold_path}/{tb_destino}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando Dimensão Geografia

# COMMAND ----------

tb_destino = 'Dim_Geografia'

dim_geografia_df = df_silver.select(
    'Cidade', 'Estado', 'Regiao', 'Distrito', 'Pais', 'CodigoPostal'
).dropDuplicates()

dim_geografia_df = dim_geografia_df.withColumn('Sk_categoria', monotonically_increasing_id()+1) \
                                    .withColumn('data_atualizacao', current_timestamp())

#display(dim_geografia_df)

dim_geografia_df.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(f'{gold_path}/{tb_destino}')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando Dimensão Cliente

# COMMAND ----------

tb_destino = 'Dim_Cliente'

dim_cliente_df = df_silver.select(
    'IDCliente', 'Nome', 'Email', 'Cidade', 'Estado', 'Regiao', 'Distrito', 'Pais', 'CodigoPostal'
).dropDuplicates()

dim_cliente_with_sk_df = dim_cliente_df.alias('cliente') \
    .join(dim_geografia_df.alias('geografia'),
          (col('cliente.Cidade') == col('geografia.Cidade'))&
          (col('cliente.Estado') == col('geografia.Estado'))&
          (col('cliente.Regiao') == col('geografia.Regiao'))&
          (col('cliente.Distrito') == col('geografia.Distrito'))&
          (col('cliente.Pais') == col('geografia.Pais'))&
          (col('cliente.CodigoPostal') == col('geografia.CodigoPostal')),
          'left') \
              .select('cliente.IDCliente', 'cliente.Nome', 'cliente.Email', 'geografia.Sk_categoria')


dim_cliente_with_sk_df = dim_cliente_with_sk_df.withColumn('Sk_cliente', monotonically_increasing_id()+1) \
                                                .withColumn('data_atualizacao', current_timestamp())

dim_cliente_with_sk_df = dim_cliente_with_sk_df.select('cliente.IDCliente', 'cliente.Nome', 'cliente.Email', 'geografia.Sk_categoria', 'Sk_cliente', 'data_atualizacao')

#display(dim_cliente_with_sk_df)

dim_cliente_with_sk_df.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(f'{gold_path}/{tb_destino}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando Tabela Fato

# COMMAND ----------

# Nome destino
tb_destino = 'Fato_Vendas'

fato_vendas_df = df_silver.alias('s') \
    .join(broadcast(dim_produto_df.select('IDProduto', 'Sk_produto').alias('dprod')),'IDProduto') \
    .join(broadcast(dim_categoria_df.select('Categoria', 'Sk_categoria').alias('dcat')),'Categoria') \
    .join(broadcast(dim_segmento_df.select('Segmento', 'Sk_segmento').alias('dseg')),'Segmento') \
    .join(broadcast(dim_fabricante_df.select('Fabricante', 'Sk_fabricante').alias('dfab')),'Fabricante') \
    .join(broadcast(dim_cliente_with_sk_df.select('IDCliente', 'Sk_cliente').alias('dcli')),'IDCliente') \
    .select(
        col('s.Data').alias('DataVenda'),
        'Sk_produto',
        'Sk_categoria',
        'Sk_segmento',
        'Sk_fabricante',
        'Sk_cliente',
        'Unidades',
        col('s.PrecoUnitario'),
        col('s.CustoUnitario'),
        col('s.TotalVendas'),
        current_timestamp().alias('data_atualizacao')
    )
#display(fato_vendas_df)

fato_vendas_df.withColumn('Ano', year('DataVenda')) \
            .withColumn('Mes', month('DataVenda')) \
            .write.format('delta') \
            .mode('append') \
            .option('mergeSchema', 'true') \
            .option('maxRecordsPerFile', 1000000) \
            .partitionBy('Ano', 'Mes') \
            .save(f'{gold_path}/{tb_destino}')

# COMMAND ----------

from pyspark.sql.functions import sum, col

resultado = spark.read.format('delta').load(gold_fato_path) \
                                        .groupBy('Ano') \
                                        .agg(sum('TotalVendas').alias('SomaTotalVendas')) \
                                        .orderBy(col('Ano'), col('SomaTotalVendas').desc())

test = resultado.select('Ano').distinct()

#display(test)

# Pequena validação para garantir que as tabelas foram carregadas
if test.filter(col('Ano') > 2011).count() > 0:
    print('OK')
else:
    print('As tabelas não foram carregadas')



# COMMAND ----------

# MAGIC %md
# MAGIC # Limpeza de memória

# COMMAND ----------

import gc

gc.collect()
spark.catalog.clearCache()
