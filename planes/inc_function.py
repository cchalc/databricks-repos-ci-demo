# Databricks notebook source
# MAGIC %md 
# MAGIC # add python file

# COMMAND ----------

# MAGIC %fs ls /FileStore/python

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC cat /dbfs/FileStore/python/common.py

# COMMAND ----------

spark.sparkContext.addPyFile("dbfs:/FileStore/python/common.py")

# COMMAND ----------

df = spark.range(10)

display(df)

# COMMAND ----------

from common import inc # import the py file we created earlier
from pyspark.sql.types import *

inc_udf = udf(inc, LongType()) # refer the udf in the py file

# COMMAND ----------

df_inc = df.select("id", inc_udf("id").alias("incremented"))
display(df_inc)

# COMMAND ----------

df.createOrReplaceTempView("addpyfile")

spark.udf.register("incrementedWithPython", inc)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT id,
# MAGIC        incrementedWithPython(id) AS sql_incremented
# MAGIC FROM addpyfile

# COMMAND ----------

# MAGIC %md 
# MAGIC # install python package

# COMMAND ----------

# MAGIC %fs ls /FileStore/python/central_model_registry

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook scope vs init script on cluster

# COMMAND ----------

# MAGIC %pip install /dbfs/FileStore/python/central_model_registry/central_model_registry-0.0.13-py3-none-any.whl

# COMMAND ----------

# MAGIC %fs head /databricks/scripts/test_init.sh

# COMMAND ----------

from central_model_registry.wallet import Wallet

# COMMAND ----------

my_wallet = Wallet()
print(my_wallet.balance)
my_wallet.add_cash(80)
print(my_wallet.balance)

# COMMAND ----------


