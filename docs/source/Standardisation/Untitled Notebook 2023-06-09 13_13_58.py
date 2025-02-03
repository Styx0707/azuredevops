# Databricks notebook source
# MAGIC %sql
# MAGIC select * from bronze_dev.standarized_raw.wyscout_teams_squads
# MAGIC --where META_EXTRACTION_DATE_DAY = 09

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from governance_dev.config_data_platform.watermarktable
# MAGIC --where data_source_name = 'teams_squad'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze_dev.standarized_raw.wyscout_players_careers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_dev.standarized_raw.wyscout_players_career

# COMMAND ----------

# MAGIC %sql
# MAGIC update governance_dev.config_data_platform.watermarktable
# MAGIC set raw_watermark_value = '2023-06-12'
# MAGIC where data_source_name = 'teams_matches_advanced_stats'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from governance_dev.config_data_platform.parameters
# MAGIC where parameter_key = 'list_new_matches'
