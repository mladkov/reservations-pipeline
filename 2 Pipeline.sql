-- Databricks notebook source
-- MAGIC %md ## Pipeline
-- MAGIC 
-- MAGIC Import and process reservation data into slowly-changing-dimension (SCD) Type 2 tables.

-- COMMAND ----------

-- DBTITLE 1,Widgets
-- MAGIC %scala
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text("projectPath", "/mnt/mladen-reservation-sample", "Project path")

-- COMMAND ----------

-- DBTITLE 1,Set data path variables
-- MAGIC %scala
-- MAGIC val projPath    = dbutils.widgets.get("projectPath")
-- MAGIC val landingPath = projPath + "/landing"
-- MAGIC val stagingPath = projPath + "/staging"
-- MAGIC val archivePath = projPath + "/archive"
-- MAGIC val errorPath   = projPath + "/error"

-- COMMAND ----------

-- DBTITLE 1,Move "landing" into "staging"
-- MAGIC %scala dbutils.fs.ls(landingPath).foreach(x => dbutils.fs.mv(x.path, stagingPath))

-- COMMAND ----------

-- MAGIC %scala display(dbutils.fs.ls(landingPath))

-- COMMAND ----------

-- MAGIC %scala spark.read.json(stagingPath).createOrReplaceTempView("reservation_events")

-- COMMAND ----------

-- New/modified records that become the current record no matter what
with r_events (
  select 
  d.eventid, cast(d.event_dtm as timestamp) event_dtm, d.userid, d.reservation, d.flight, d.origin, d.destination, d.seat, d.food,
  d.event_dtm as eff_dtm, 
  "2999-12-31 23:59:59" as exp_dtm
  from reservation_events d )
-- Historic records that may or may not be modified
, r_historic (select * from mladen.reservations_type2 where exp_dtm = "2999-12-31 23:59:59")
-- Following includes existing rows that certainly are untouched
, r_untouchable (select * from mladen.reservations_type2 where exp_dtm != "2999-12-31 23:59:59")

insert overwrite table mladen.reservations_type2_daily

-- Existing (historic) unchanging records
select 
  d.eventid, d.event_dtm, d.userid, d.reservation, d.flight, d.origin, d.destination, d.seat, d.food,
  d.eff_dtm, d.exp_dtm
from r_historic d left outer join r_events r
on r.reservation = d.reservation
where r.reservation is null
union all
-- New records
select 
  d.eventid, d.event_dtm, d.userid, d.reservation, d.flight, d.origin, d.destination, d.seat, d.food,
  d.eff_dtm, d.exp_dtm
from r_events d left outer join r_historic r
on d.reservation = r.reservation
where r.reservation is null
-- Modified events - NEW ROW (taking over the old)
union all
select d.eventid, d.event_dtm, d.userid, d.reservation, d.flight, d.origin, d.destination, d.seat, d.food, d.eff_dtm, d.exp_dtm 
  from r_events d join r_historic r
  on d.reservation = r.reservation
union all
-- Modified events - OLD/HISTORIC ROW (being replaced by the above)
select r.eventid, r.event_dtm, r.userid, r.reservation, r.flight, r.origin, r.destination, r.seat, r.food, r.eff_dtm, d.eff_dtm 
  from r_events d join r_historic r
  on d.reservation = r.reservation
union all
-- Untouched events
select * from r_untouchable

-- COMMAND ----------

insert overwrite table mladen.reservations_type2 select * from mladen.reservations_type2_daily

-- COMMAND ----------

select * from mladen.reservations_type2

-- COMMAND ----------

-- DBTITLE 1,Move "staging" into "archive"
-- MAGIC %scala dbutils.fs.ls(stagingPath).foreach(x => dbutils.fs.mv(x.path, archivePath))

-- COMMAND ----------

