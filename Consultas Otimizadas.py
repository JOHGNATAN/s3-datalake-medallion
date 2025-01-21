# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName('Consulta Otimizada') \
                    .getOrCreate()


# Leia as tabelas >> Fato e categoria
vendas_df = spark.read.format('delta').load('/mnt/lhdw/gold/vendas_delta/Fato_Vendas')
categoria_df = spark.read.format('delta').load('/mnt/lhdw/gold/vendas_delta/Dim_Categoria')

#  Usar o Broadcast na tabela categoria
categoria_df = broadcast(categoria_df)

# Realize o join entre as tabelas
joined_df = vendas_df.join(categoria_df, vendas_df.Sk_categoria == categoria_df.Sk_categoria)

# Agrupe por Categoria, Ano e calcule a soma total de vendas

resultado = joined_df.groupBy('Categoria', 'Ano') \
                    .agg(sum('TotalVendas').alias('SomaTotalVendas')) \
                    .orderBy('Ano', desc('SomaTotalVendas'))

display(resultado)
