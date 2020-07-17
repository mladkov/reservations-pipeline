# Databricks notebook source
# MAGIC %md # Investigation
# MAGIC 
# MAGIC As we investigate our content, the job can run at the same time on its _own_ cluster and _own_ resources, never contending with what we're doing here as an analyst

# COMMAND ----------

# MAGIC %sql select * from delta.`/mnt/mladen-reservation-sample/delta/reservations` order by eventid

# COMMAND ----------

# MAGIC %sql select userid, count(*) as num_adjustments from delta.`/mnt/mladen-reservation-sample/delta/reservations` group by userid

# COMMAND ----------

# MAGIC %sql create database if not exists airline

# COMMAND ----------

# MAGIC %sql create table if not exists airline.reservations using delta location '/mnt/mladen-reservation-sample/delta/reservations'

# COMMAND ----------

# MAGIC %sql select * from airline.reservations