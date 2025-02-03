# Databricks notebook source
# MAGIC %run "../Framework/Configuration Management"

# COMMAND ----------

# MAGIC %run "../Framework/ETL Framework"

# COMMAND ----------

BRONZE_CATALOG_NAME=get_parameter("GLOBAL","BRONZE_CATALOG_NAME")
STAGING_RAW_CONTAINER=get_parameter("GLOBAL","STAGING_RAW_CONTAINER")

print(BRONZE_CATALOG_NAME)
print(STAGING_RAW_CONTAINER)

# COMMAND ----------

data_provider = 'wyscout'
data_source_name = 'videos_qualities'
table_name = data_provider+'_'+data_source_name
display(table_name)

# COMMAND ----------

set_standarize_watermark_value(data_provider=data_provider,data_source_name=data_source_name,standarize_watermark_value='2023-04-21')
#set_staging_watermark_value('wyscout','areas','2023-04-12')

# COMMAND ----------

std_date = get_standarize_watermark_value(data_provider,data_source_name)
standarize_watermark_value = f"{std_date:%Y-%m-%d}"
display(standarize_watermark_value)

stg_date = get_staging_watermark_value(data_provider,data_source_name)
staging_watermark_value = f"{stg_date:%Y-%m-%d}"
display(staging_watermark_value)

# COMMAND ----------

if(is_staging_raw_updated(data_provider,data_source_name)==False):
    print ('Process the staging Layer ')

    file_path = get_standarize_raw_path(data_provider=data_provider,data_source_name=data_source_name)
    display(file_path)

    df_parc = spark.read.format('parquet').load(file_path+'*')
    #display(df_parc)

    df_delta = df_parc.write.mode("overWrite").format("delta").save(STAGING_RAW_CONTAINER+"/"+table_name)
    #display(df_delta)

    sqlContext.sql("""DROP TABLE IF EXISTS {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    
    sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.staging_raw.{1} 
    USING DELTA 
    LOCATION '{2}/{1}/' """.format(BRONZE_CATALOG_NAME, table_name,STAGING_RAW_CONTAINER))

    spark.sql("""SELECT String(areas_alpha2code), String(areas_alpha3code), String(areas_id),String(areas_name) from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))

    df1 = spark.sql("""select * from {0}.staging_raw.{1}""".format(BRONZE_CATALOG_NAME,table_name))
    display(df1)

    set_staging_watermark_value(data_provider=data_provider,data_source_name=data_source_name,staging_watermark_value=standarize_watermark_value)
else:
    print('No need to process')

    #set_staging_watermark_value(data_provider=data_provider,data_source_name=data_source_name,staging_watermark_value=staging_watermark_value)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from governance_dev.config_data_platform.watermarktable 

# COMMAND ----------

"""df = spark.read.format('parquet').load('/mnt/standarized-raw-data/wyscout/matches/players_advancedstats/2023/04/25/*')
display(df)"""

