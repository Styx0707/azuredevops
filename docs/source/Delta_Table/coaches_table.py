# Databricks notebook source
# MAGIC %run "../Framework/Configuration Management"

# COMMAND ----------

BRONZE_CATALOG_NAME=get_parameter("GLOBAL","BRONZE_CATALOG_NAME")
HISTORIZED_RAW_CONTAINER=get_parameter("GLOBAL","HISTORIZED_RAW_CONTAINER")

print(BRONZE_CATALOG_NAME)
print(HISTORIZED_RAW_CONTAINER)

# COMMAND ----------

file_path = "/mnt/standarized-raw-data/wyscout/coaches/coaches/2023/04/06/coaches.parquet"

# COMMAND ----------

new_coaches=spark.read.parquet(file_path)

new_coaches.show()

# COMMAND ----------

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.historized_raw.coaches
                 (wyId int)
                  USING DELTA
                 LOCATION '{1}/coaches/'   
                """.format(BRONZE_CATALOG_NAME,HISTORIZED_RAW_CONTAINER))

# COMMAND ----------

spark.sql("set spark.databricks.delta.schema.autoMerge.enabled=true")

new_coaches.createOrReplaceTempView("v_new_coaches")

sqlContext.sql("""MERGE INTO {0}.historized_raw.coaches as target USING v_new_coaches as source
                    ON target.wyId = source.wyId
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """.format(BRONZE_CATALOG_NAME))

# COMMAND ----------

"""%sql
select * from bronze_prd.historized_raw.coaches"""
