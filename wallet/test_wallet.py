# Databricks notebook source
from wallet import Wallet, InsufficientAmount

# COMMAND ----------

def wallet():
  return Wallet(20)


# COMMAND ----------

w = wallet()

# COMMAND ----------

w.balance

# COMMAND ----------

w.
