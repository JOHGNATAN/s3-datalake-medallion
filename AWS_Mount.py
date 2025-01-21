# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
                    .appName('test') \
                    .getOrCreate()
                    
secrets = spark.read.format('csv')\
                    .option('inferschema', 'true')\
                    .option('header', 'true')\
                    .load('/FileStore/secret_key/Databricks_access_accessKeys.csv')

access_key = secrets.select('Access key ID').take(1)[0]['Access key ID']

secret_key = secrets.select('Secret access key').take(1)[0]['Secret access key']

encoded_secret_key = secret_key.replace('/', '%2F')

aws_bucket_name = 'files-tobe-processed'
mount_name = 'aws_mount/landingzone/vendas/processar'

try:
    dbutils.fs.mount(f's3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}', f'/mnt/{mount_name}')
    print(f'A Bucket: "{aws_bucket_name}" was mounted at: {mount_name}')

except Exception as e:
    print(f'An error occured: {e}')

# COMMAND ----------

# Criando diretorios no DBFS

dbutils.fs.mkdirs("/mnt/aws_mount/landingzone/vendas/processado")
dbutils.fs.mkdirs("/mnt/aws_mount/bronze")
dbutils.fs.mkdirs("/mnt/aws_mount/silver")
dbutils.fs.mkdirs("/mnt/aws_mount/gold")


# COMMAND ----------

# Limpando memória
 
import gc

gc.collect()
spark.catalog.clearCache()

# COMMAND ----------

#dbutils.fs.unmount('/mnt/aws_mount/landingzone/vendas/processar')
#dbutils.fs.rm("/mnt/aws_mount/landingzone/vendas/processado", recurse = True)
#dbutils.fs.rm("/mnt/aws_mount/landingzone/vendas", recurse = True)
#dbutils.fs.rm("/mnt/aws_mount/landingzone", recurse = True)
#dbutils.fs.rm("/mnt/aws_mount/bronze", recurse = True)
#dbutils.fs.rm("/mnt/aws_mount/silver", recurse = True)
#dbutils.fs.rm("/mnt/aws_mount/gold", recurse = True)
