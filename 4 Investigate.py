# Databricks notebook source
# MAGIC %md # Investigation
# MAGIC 
# MAGIC As we investigate our content, the job can run at the same time on its _own_ cluster and _own_ resources, never contending with what we're doing here as an analyst

# COMMAND ----------

# MAGIC %sql select * from delta.`/mnt/mladen-reservation-sample/delta/reservations` order by eventid

# COMMAND ----------

# MAGIC %sql select userid, count(*) as num_adjustments from delta.`/mnt/mladen-reservation-sample/delta/reservations` group by userid

# COMMAND ----------

dfjson = spark.read.json("/mnt/mladen-reservation-sample/landing-reservations")

# COMMAND ----------

d = dfjson.coalesce(1)

# COMMAND ----------

d.write.format('parquet').mode('overwrite').save("/mnt/mladen-reservation-sample/landing-reservations-parquet")

# COMMAND ----------

# MAGIC %sql select count(*) from parquet.`/mnt/mladen-reservation-sample/landing-reservations-parquet`

# COMMAND ----------

# MAGIC %sql create table cldr.json_ingest (c1 int, c2 string) partitioned by (c3 int) using parquet location '/mnt/mladen-reservation-sample/landing-reservations-parquet'

# COMMAND ----------

/landing-reservation-parquet
  -> c3='abc' /
  -> c3='def' /
  -> c3='ghi'

# COMMAND ----------

# MAGIC %sql msck repair table cldr.json_ingest

# COMMAND ----------

dfjson.createOrReplaceTempView("landing_json")

# COMMAND ----------

# MAGIC %sql select *, input_file_name() as input_file from landing_json

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql select *, input_file_name() from delta.`/mnt/mladen-reservation-sample/delta/reservations`

# COMMAND ----------

input_file_name()

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

