-- Databricks notebook source
-- MAGIC %md ## Transform data from Bronze to Gold
-- MAGIC 
-- MAGIC We will start with our prepared Type2 historical table of reservations that were ingested by our primary pipeline.  We will consider that data to be our "bronze" data for this particular pipeline, where we will aggregate (silver), then we will enrich it to prepare for dashboarding (gold)

-- COMMAND ----------

refresh table mladen.reservations_type2

-- COMMAND ----------

-- DBTITLE 1,Bronze data - quick peak
select event_dtm from mladen.reservations_type2

-- COMMAND ----------

-- DBTITLE 1,Bronze to Silver aggregation
drop table if exists mladen.reservations_silver;
create table if not exists mladen.reservations_silver 
  using parquet as
    select 
      max(event_dtm) event_dtm, 
      count(*) as num_events, 
      userid, 
      reservation, 
      first(flight) as flight, 
      first(origin) as origin, 
      first(destination) as destination
    from mladen.reservations_type2
    group by userid, reservation

-- COMMAND ----------

select * from mladen.reservations_silver

-- COMMAND ----------

-- DBTITLE 1,Silver to Gold enrichment
drop table if exists mladen.reservations_gold;
create table if not exists mladen.reservations_gold
  using parquet as
    select 
      event_dtm, 
      num_events, 
      userid, 
      reservation, 
      flight, 
      origin,
      a.airport_name as origin_name,
      destination,
      b.airport_name as destination_name
    from mladen.reservations_silver r 
    left outer join mladen.airport_lookup a
      on origin = a.airport_code
    left outer join mladen.airport_lookup b
      on destination = b.airport_code;

-- COMMAND ----------

select * from mladen.reservations_gold

-- COMMAND ----------

-- MAGIC %run ./GatherMetadata $1="reservations"