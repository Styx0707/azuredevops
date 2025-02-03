# Databricks notebook source
# MAGIC %run "../Framework/Configuration Management"

# COMMAND ----------

BRONZE_CATALOG_NAME=get_parameter("GLOBAL","BRONZE_CATALOG_NAME")
HISTORIZED_RAW_CONTAINER=get_parameter("GLOBAL","HISTORIZED_RAW_CONTAINER")

print(BRONZE_CATALOG_NAME)
print(HISTORIZED_RAW_CONTAINER)

# COMMAND ----------

file_path = "/mnt/standarized-raw-data/wyscout/areas/2023/04/07/areas.parquet"

# COMMAND ----------

new_areas=spark.read.parquet(file_path)

new_areas.show()

# COMMAND ----------

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.wyscout_areas
                 (area_id int)
                 USING DELTA
                 LOCATION '{1}/areas/'   
                """.format(BRONZE_CATALOG_NAME,HISTORIZED_RAW_CONTAINER))

# COMMAND ----------

spark.sql("set spark.databricks.delta.schema.autoMerge.enabled=true")

new_areas.createOrReplaceTempView("v_new_areas")

sqlContext.sql("""MERGE INTO {0}.staging_raw.wyscout_areas as target USING v_new_areas as source
                    ON target.area_id = source.area_id
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """.format(BRONZE_CATALOG_NAME))

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from bronze_prd.staging_raw.wyscout_areas

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bronze_dev.staging_raw.areas
