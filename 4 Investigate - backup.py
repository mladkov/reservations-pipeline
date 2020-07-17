# Databricks notebook source
# MAGIC %sql select * from delta.`/mnt/mladen-reservation-sample/delta/reservations` order by eventid

# COMMAND ----------

# MAGIC %sql describe history delta.`/mnt/mladen-reservation-sample/delta/reservations`

# COMMAND ----------

# MAGIC %sql describe formatted delta.`/mnt/mladen-reservation-sample/delta/reservations`

# COMMAND ----------

history = spark.sql("describe history delta.`/mnt/mladen-reservation-sample/delta/reservations`")

# COMMAND ----------

history.printSchema()

# COMMAND ----------

history.createOrReplaceTempView("history")

# COMMAND ----------

# MAGIC %sql select * from history

# COMMAND ----------

# MAGIC %sql select version, operation, job.jobId, job.jobName, job.runId from history

# COMMAND ----------

latest_version_row = spark.sql("select max(version) as version from history").collect()

# COMMAND ----------

latest_version = latest_version_row[0]["version"]

# COMMAND ----------

latest_version

# COMMAND ----------

# MAGIC %sql create table if not exists mladen using delta location '/mnt/mladen-reservation-sample/delta/reservations'

# COMMAND ----------

# MAGIC %sql select * from mladen

# COMMAND ----------

# MAGIC %sql describe formatted mladen

# COMMAND ----------

