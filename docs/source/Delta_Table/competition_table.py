# Databricks notebook source
# MAGIC %run "../Framework/Configuration Management"

# COMMAND ----------

BRONZE_CATALOG_NAME=get_parameter("GLOBAL","BRONZE_CATALOG_NAME")
HISTORIZED_RAW_CONTAINER=get_parameter("GLOBAL","HISTORIZED_RAW_CONTAINER")

print(BRONZE_CATALOG_NAME)
print(HISTORIZED_RAW_CONTAINER)

# COMMAND ----------

files = "/mnt/standarized-raw-data/wyscout/competitions/players/2023/04/25/"
display(dbutils.fs.ls(files))


# COMMAND ----------

new_competitions=spark.read.option("inferSchema","false").parquet(file_path)

new_competitions.show()

# COMMAND ----------

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.historized_raw.competitions
                 (wyId int)
                  USING DELTA
                 LOCATION '{1}/competitions/'   
                """.format(BRONZE_CATALOG_NAME,HISTORIZED_RAW_CONTAINER))

# COMMAND ----------

spark.sql("set spark.databricks.delta.schema.autoMerge.enabled=true")

new_competitions.createOrReplaceTempView("v_new_competitions")

sqlContext.sql("""MERGE INTO {0}.historized_raw.competitions as target USING v_new_competitions as source
                    ON target.wyId = source.wyId
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """.format(BRONZE_CATALOG_NAME))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_prd.historized_raw.competitions
