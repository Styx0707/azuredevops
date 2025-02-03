# Databricks notebook source
import json

# Remplacez la variable suivante par le nom de votre catalogue Silver
SILVER_CATALOG_NAME = "silver_dev"

##############################

# JSON d'exemple
json_str = '''[
	{
		"id": "SatAirport",
		"type": "Sattelite",
		"comment":"Airport sattelite",
		"parent":"SAirport",
		"fields":[
			{"name":"Name","type":"STRING","entity_source":"raw_airport_vw","column_source":"Name"},
			{"name":"RunwayLenght","type":"DECIMAL(18,2)","entity_source":"raw_airport_vw","column_source":"RunwayLenght"},
			{"name":"RunwayElevation","type":"DECIMAL(18,2)","entity_source":"raw_airport_vw","column_source":"RunwayLenght"},
			{"name":"Longtitude","type":"DECIMAL(18,2)","entity_source":"raw_airport_vw","column_source":"Longtitude"},
			{"name":"Latitude","type":"DECIMAL(18,2)","entity_source":"raw_airport_vw","column_source":"Latitude"},
			{"name":"Telephone","type":"STRING","entity_source":"raw_airport_vw","column_source":"Telephone"}
		],
		"record_source":"BookingSystem"
	},
	{
		"id": "SatConnection",
		"type": "Sattelite",
		"comment":"Connection sattelite",
		"parent":"LinkConnection",
		"fields":[
			{"name":"Deptime","type":"timestamp","entity_source":"raw_connection_vw","column_source":"Deptime"},
			{"name":"Depdelay","type":"timestamp","entity_source":"raw_connection_vw","column_source":"Depdelay"},
			{"name":"Arrtime","type":"timestamp","entity_source":"raw_connection_vw","column_source":"Arrtime"},
			{"name":"ArrDelay","type":"timestamp","entity_source":"raw_connection_vw","column_source":"ArrDelay"}
		],
		"record_source":"BookingSystem",
		"driving_key":["SourceAirportHashKey","DestinationAirportHashKey","CarrierHashKey"]
	}
]
'''

##############################

# Charger la chaîne JSON en un dictionnaire Python
data = json.loads(json_str)

# Générer la requête CREATE TABLE pour chaque entité
for entity in data:
	table_name = entity["id"]
	comment = entity["comment"]
	fields = entity["fields"]

	# Générer la partie colonnes de la requête SQL
	columns = []
	for field in fields:
		column_name = field["name"]
		column_type = field["type"]
		columns.append("{0} {1}".format(column_name, column_type))

	# Ajouter les colonnes supplémentaires pour l'entité "SatAirport"
	if table_name == "SatAirport":
		columns.extend([
			"AirportHashKey STRING NOT NULL",
			"LoadDate TIMESTAMP NOT NULL",
			"LoadEndDate TIMESTAMP NOT NULL",
			"RecordSource STRING",
			"HashDiff STRING"
		])

	# Générer la requête SQL
	sql = """
CREATE TABLE IF NOT EXISTS {0}.raw_vault.{1}(
  {2}
)
COMMENT "{3}"
""".format(SILVER_CATALOG_NAME, table_name, ",\n  ".join(columns), comment)

	# Afficher la requête SQL générée
	print(sql)


# COMMAND ----------


