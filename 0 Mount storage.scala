// Databricks notebook source
// MAGIC %md ## Object storage
// MAGIC 
// MAGIC Object storage account is `mladenstrg1`.
// MAGIC 
// MAGIC We're storing the actual key using the `secrets` API through databricks cli.
// MAGIC 
// MAGIC ```
// MAGIC $ databricks secrets create-scope --scope reservations
// MAGIC $ databricks secrets put --scope reservations --key mladenstrg1 --string-value <key-value>
// MAGIC ```
// MAGIC 
// MAGIC We can see that this is stored here:
// MAGIC 
// MAGIC ```
// MAGIC $ databricks secrets list --scope reservations
// MAGIC Key name       Last updated
// MAGIC -----------  --------------
// MAGIC mladenstrg1   1537198007039
// MAGIC ```

// COMMAND ----------

display(dbutils.fs.mounts())

// COMMAND ----------

dbutils.fs.unmount("/mnt/mnt/mladen-reservation-sample")

// COMMAND ----------

// MAGIC %python
// MAGIC AWS_BUCKET_NAME = "mladenstrg1-useast1"
// MAGIC MOUNT_NAME = "mladen-reservation-sample"
// MAGIC dbutils.fs.mount("s3a://%s" % AWS_BUCKET_NAME, "/mnt/%s" % MOUNT_NAME)
// MAGIC display(dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://reservation-sample@mladenstrg1.blob.core.windows.net/",
  mountPoint = "/mnt/mladen-reservation-sample",
  extraConfigs = Map("fs.azure.account.key.mladenstrg1.blob.core.windows.net" -> dbutils.secrets.get(scope = "reservations", key = "mladenstrg1")))

// COMMAND ----------

// MAGIC %fs ls /mnt/mladen-reservation-sample/archive

// COMMAND ----------

// MAGIC %fs ls /mnt/mladen-reservation-sample

// COMMAND ----------

// MAGIC %fs mkdirs /mnt/mladen-reservation-sample/landing-reservations