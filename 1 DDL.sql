-- Databricks notebook source
-- MAGIC %md ## DDL for Pipeline
-- MAGIC 
-- MAGIC Set of setup, DDL and other snippets required to run the job

-- COMMAND ----------

describe database mladen

-- COMMAND ----------

drop table if exists mladen.reservations_type2;
create table if not exists mladen.reservations_type2 
(
  eventid bigint,
  event_dtm timestamp,
  userid string,
  reservation string,
  flight string,
  origin string,
  destination string,
  seat string,
  food string,
  eff_dtm string,
  exp_dtm string
)
using delta;

drop table if exists mladen.reservations_type2_daily;
create table if not exists mladen.reservations_type2_daily
(
  eventid bigint,
  event_dtm timestamp,
  userid string,
  reservation string,
  flight string,
  origin string,
  destination string,
  seat string,
  food string,
  eff_dtm string,
  exp_dtm string
)
using delta;

-- COMMAND ----------

drop table if exists mladen.airport_lookup;
create table if not exists mladen.airport_lookup
(
  airport_code string,
  airport_name string
)
using delta;

-- COMMAND ----------

insert into mladen.airport_lookup values ("YYZ", "Toronto"), ("ORD", "Chicago O'Hare")