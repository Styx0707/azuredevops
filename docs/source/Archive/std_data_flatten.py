# Databricks notebook source
###########################################################
#------------------------- IMPORTS -----------------------#
###########################################################
import sys
import json

from pyspark.sql.functions import *
from pyspark.sql.types import * 
from pyspark.sql import SparkSession

from pyspark.sql import DataFrame

from flatten_json import flatten

# COMMAND ----------

directory = "/mnt/raw/corporate/wyscout/"
path = directory + "/seasons/career/2023/04/21/*.json"

jsonfile= "/mnt/raw/corporate/wyscout/seasons/career/2023/04/21/seasons_career_15051_2023_04_21.json"

'''dbutils.fs.ls("/mnt/raw")

dbutils.fs.mount(
  source = "s3a://my-bucket/my-folder",
  mount_point = "/mnt/my-mount-point",
  extra_configs = {"key": "value"}
)

file_contents = dbutils.fs.head("/mnt/raw/corporate/wyscout/seasons/career/2023/04/21/seasons_career_15051_2023_04_21.json")

file = open(file_contents,'r')

parsed_json = json.load(file)'''

df1 = spark.read.option("multiline","true").json(jsonfile)
#df1.printSchema()
df1.show()
file = df1.toPandas().to_json(orient='records')
file2 = '{"liste" :' + file + "}"
#type(file2)
flat_json = flatten(json.loads(file2))
#print(flat_json.keys())

unflat_json = {'user':
               {'Rachel':
                [{'UserID': 1717171717,
                 'Email': 'rachel1999@gmail.com',
                 'friends': ['John', 'Jeremy', 'Emily']
                 }]
                }
               }
 
flat_json = flatten(unflat_json)

def flatten_json(y):
    out = {}
 
    def flatten(x, name=''):
 
        # If the Nested key-value
        # pair is of dict type
        if type(x) is dict:
            print (name)
            for a in x:
                flatten(x[a], name + a + '_')
 
        # If the Nested key-value
        # pair is of list type
        elif type(x) is list:
 
            i = 0
 
            for a in x:
                print (name)
                flatten(a, name + str(i) + '_')
              
                i += 1
        else:
            out[name[:-1]] = x
 
    flatten(y)
    return out
 
 
# Driver code
print(flatten_json(json.loads(file2)))
 
print(flat_json)



# COMMAND ----------

# Write the parquet file into the designed folder

# creer la date du jour + parametrer pour le dossier...

# rajouter supprimer les fichiers TODO

#flattened_df.write.parquet("/mnt/standarized-raw-data/wyscout/areas/2023/04/07/areas.parquet")

#parDF=spark.read.parquet("/mnt/standarized-raw-data/wyscout/areas/2023/04/07/areas.parquet")

#parDF.show()


