# Databricks notebook source
########### Init the Environnement Variables ##############################################

GOVERNANCE_CATALOG_NAME=dbutils.secrets.get(scope = "key-vault-scope", key = "GOVERNANCE-CATALOG-NAME")
CONFIG_DATA_PLATEFORM_CONTAINER=dbutils.secrets.get(scope = "key-vault-scope", key = "CONFIG-DATA-PLATEFORM-CONTAINER")
spark.sql("set spark.databricks.delta.schema.autoMerge.enabled=true")


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Creating the Parameter tables    ##############################################
###########################################################################################
###########################################################################################

sqlContext.sql("""CREATE TABLE IF NOT EXISTS {0}.config_data_platform.parameters(
                  parameter_scope STRING,
                  parameter_key STRING,
                  parameter_value STRING
                  )
                  USING DELTA
                  LOCATION '{1}/parameters/'   
                """.format(GOVERNANCE_CATALOG_NAME,CONFIG_DATA_PLATEFORM_CONTAINER))


# COMMAND ----------

###########################################################################################
###########################################################################################
########### Setting a Value for a parameter  ##############################################
####        GLOBAL as default value for the scope                                     ####
####        SCOPE : Module , Notebook etc...   
###########################################################################################
###########################################################################################


def set_parameter(parameter_scope,parameter_key,parameter_value):
  new_parameters=[(parameter_scope,parameter_key,parameter_value)]
  column=["parameter_scope","parameter_key","parameter_value"]
  dataframe=spark.createDataFrame(data=new_parameters,schema=column)
  dataframe.createOrReplaceTempView("v_new_parameters")
  sqlContext.sql("""MERGE INTO {0}.config_data_platform.parameters as target USING v_new_parameters as source
                    ON target.parameter_key = source.parameter_key and target.parameter_scope = source.parameter_scope
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """.format(GOVERNANCE_CATALOG_NAME))

# COMMAND ----------

###########################################################################################
###########################################################################################
########### Getting a Value for a parameter for a given key################################
###########################################################################################
###########################################################################################


def get_parameter(parameter_scope,parameter_key):
  dataframe=sqlContext.sql("""SELECT parameter_value FROM {0}.config_data_platform.parameters
                           where parameter_scope='{1}' and parameter_key='{2}' 
                           """.format(GOVERNANCE_CATALOG_NAME,parameter_scope,parameter_key))
  if dataframe.count()>0:
    return dataframe.collect()[0][0]
  else :
    return "PARAMETER_NOT_FOUND"
    

# COMMAND ----------

########### Parameters Initiation ##############################################
def init_parameters():
    set_parameter('GLOBAL','DATA_CATALOG_CONTAINER',"abfss://admin@abfss://admin@datalakefootballdatadev.dfs.core.windows.net/config_data_platform.dfs.core.windows.net/data_catalog")
    set_parameter('GLOBAL','RAW_CONTAINER',"abfss://raw@datalakefootballdatadev.dfs.core.windows.net")
    set_parameter('GLOBAL','STANDARIZED_RAW_CONTAINER',"abfss://standarized-raw-data@datalakefootballdatadev.dfs.core.windows.net")
    set_parameter('GLOBAL','BRONZE_CATALOG_NAME',"bronze_dev")
    set_parameter('GLOBAL','SILVER_CATALOG_NAME',"silver_dev")
    set_parameter('GLOBAL','GOLD_CATALOG_NAME',"gold_dev")

    set_parameter('GLOBAL','HISTORIZED_RAW_CONTAINER',"abfss://historized-raw-data@datalakefootballdatadev.dfs.core.windows.net/wyscout")
    set_parameter('GLOBAL','BUSINESS_VAULT_CONTAINER',"abfss://enriched@datalakefootballdatadev.dfs.core.windows.net/business_vault")
    set_parameter('GLOBAL','MASTER_DATA_CONTAINER',"abfss://curated@datalakefootballdatadev.dfs.core.windows.net/wyscout/master_data")
    set_parameter('GLOBAL','PERFORMANCE_MART_CONTAINER',"abfss://curated@datalakefootballdatadev.dfs.core.windows.net/wyscout/players_performance_mart")

