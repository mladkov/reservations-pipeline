-- Databricks notebook source
-- MAGIC %md # Batch Streaming: Raw to Staging
-- MAGIC 
-- MAGIC We demonstrate the use of the new Auto Loader by _listening_ on the `/mnt/mladen-reservation-sample/landing-reservations` path and using the `cloudFiles` structured streaming source.
-- MAGIC 
-- MAGIC See [Load Files from Azure Blob storage or Azure Data Lake Storage Gen2 using Auto Loader](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader) for details.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC 
-- MAGIC val schema = new StructType()
-- MAGIC   .add("eventid",IntegerType,true)
-- MAGIC   .add("event_dtm",StringType,true)
-- MAGIC   .add("userid",StringType,true)
-- MAGIC   .add("reservation",StringType,true)
-- MAGIC   .add("flight",StringType,true)
-- MAGIC   .add("origin",StringType,true)
-- MAGIC   .add("destination",StringType,true)
-- MAGIC   .add("seat",StringType,true)
-- MAGIC   .add("food",StringType,true)
-- MAGIC 
-- MAGIC val df = spark.readStream.format("cloudFiles")
-- MAGIC   .option("cloudFiles.format", "json")
-- MAGIC   .option("cloudFiles.region", "us-east-1")
-- MAGIC   .schema(schema)
-- MAGIC   .load("/mnt/mladen-reservation-sample/landing-reservations")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df2 = df.withColumn("input_file", input_file_name())

-- COMMAND ----------

-- MAGIC 
-- MAGIC %scala 
-- MAGIC import org.apache.spark.sql.streaming.Trigger
-- MAGIC df.writeStream
-- MAGIC   .format("delta")
-- MAGIC   .trigger(Trigger.Once)
-- MAGIC   .option("checkpointLocation", "/mnt/mladen-reservation-sample/delta-checkpoints/reservations")
-- MAGIC   .start("/mnt/mladen-reservation-sample/delta/reservations")