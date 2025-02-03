# Databricks notebook source
# Coming from Master Branch
########################################################################
#############Function : get_data_processing_connection_string ##########
# get_data_processing_connection_string return Connection string of   ##
# database Data Processing (Test change)                              ##
########################################################################
########################################################################

def get_dwh_connection_string():
  
   #Get data-processing credentiels  from Key Vault
  dwh_port= dbutils.secrets.get(scope = "azure-key-vault-scope", key = "dwh-port")
  dwh_database_name=dbutils.secrets.get(scope = "azure-key-vault-scope", key = "dwh-database-name")
  dwh_host=dbutils.secrets.get(scope = "azure-key-vault-scope", key = "dwh-host")
  dwh_user=dbutils.secrets.get(scope = "azure-key-vault-scope", key = "dwh-user")
  dwh_password=dbutils.secrets.get(scope = "azure-key-vault-scope", key = "dwh-password")
  
  #Built the connection string
  dwh_cari_url="jdbc:sqlserver://"+dwh_host+":"+dwh_port+";database="+dwh_database_name+";"
  dwh_cari_url+="user="+dwh_user+";password="+dwh_password+";encrypt=true;trustServerCertificate=true;loginTimeout=30"
  return dwh_cari_url

# COMMAND ----------

##########################################################
#############Function : get_notebook_path#################
#get_notebook_path returns the notebook path to be      ##
#runned based on the Environment and the Notebook name  ##
##########################################################
##########################################################

def get_notebook_path(User,DataFactory,Notebook):
  
  #Query Databricks configuration tables
  dwh_cari_url=get_dwh_connection_string()
  Databricks_User=spark.read.jdbc(dwh_cari_url, "Databricks.Databricks_User")
  Databricks_Release=spark.read.jdbc(dwh_cari_url, "Databricks.Databricks_Release")
  Databricks_Notebook=spark.read.jdbc(dwh_cari_url, "Databricks.Databricks_Notebook")
  
  #identifying the Debug Environment
  if(DataFactory=='adf-cari-dev' and User!='no user'):
    print("Debug Environment by the user:"+User)
    Environment='Debug'
    Release=''
    
  #identifying the Development Environment
  if(DataFactory=='adf-cari-dev' and User=='no user'):
    print("Development Environment")
    Environment='Development'
    Release=''
    User='cari.dev'
  
  #identifying the Acceptance Environment
  if(DataFactory=='adf-cari-uat'):
    print("Acceptance Environment")
    Environment='Acceptance'
    #Define the right release 
    Release=Databricks_Release.filter("Environment='{0}' and  Enabled=1".format(Environment)).collect()[0]["Release"]+"/"
    User='cari.uat'
    
  
   #identifying the Production Environment
  if(DataFactory=='adf-cari-prd'):
    print("Production Environment")
    Environment='Production'
    #Define the right release 
    Release=Databricks_Release.filter("Environment='{0}' and  Enabled=1".format(Environment)).collect()[0]["Release"]+"/"
    User='cari.prd'
    
  
  
  Workspace_path=Databricks_User.filter("Enabled==1 and Databricks_User=='"+User+"'").collect()[0]["Workspace_Path"]
  Notebook_path=Databricks_Notebook.filter("Notebook_Name='{0}'".format(Notebook)).collect()[0]["Notebook_Path"]
  print("Notebook path="+Workspace_path+Release+Notebook_path)
  
  return Workspace_path+Release+Notebook_path

# COMMAND ----------

def run_with_retry(notebook, timeout, args = {}, max_retries = 3):
  num_retries = 0
  while True:
    try:
      return dbutils.notebook.run(notebook, timeout, args)
    except Exception as e:
      if num_retries > max_retries:
        raise e
      else:
        print ("Retrying error", e)
        num_retries += 1

# COMMAND ----------

import ast

#Define widgets
dbutils.widgets.text("User","")
dbutils.widgets.text("DataFactory","")
dbutils.widgets.text("Notebook","")
dbutils.widgets.text("Parameters","")

#Get widgets value
User=dbutils.widgets.get("User")
DataFactory=dbutils.widgets.get("DataFactory")
Notebook=dbutils.widgets.get("Notebook")
Parameters=dbutils.widgets.get("Parameters")

#Convert Json parameters to dictionnary 
if(Parameters!=''):
  notebook_parameters=ast.literal_eval(Parameters.replace("\"","'"))
else:
  notebook_parameters={"":""}

#Get the Nothbook Path 
notebook_path=get_notebook_path(User,DataFactory,Notebook)

#Run the Notebook  
RunOutput = run_with_retry(notebook_path, 0,notebook_parameters,3)
dbutils.notebook.exit(RunOutput)

# COMMAND ----------


