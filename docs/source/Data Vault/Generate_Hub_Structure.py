# Databricks notebook source
import json

# Remplacez la variable suivante par le nom de votre catalogue Silver
SILVER_CATALOG_NAME = "silver_dev"

##############################

# JSON d'exemple
json_str = '''{
	"id": "HubFlightCode",
	"type": "Hub",
	"comment":"Hub HubFlightCode Table",
	"primary_key":{"name":"FlightCodeHashKey","type":"STRING","entity_source":"raw_flightcode_vw","column_source":"sha1_hub_flightCode"},
	"record_source":"FlightSystem",
	"business_keys":[
					{"name":"Carrier","type":"STRING","entity_source":"raw_flightcode_vw","column_source":"Carrier"},
					{"name":"FlightNum","type":"STRING","entity_source":"raw_flightcode_vw","column_source":"FlightNum"},
					{"name":"FlightCode","type":"STRING","entity_source":"raw_flightcode_vw","column_source":"FlightCode"}
					]
}
'''

##############################

##############################
"""
json_str = '''{
	"id": "HubFlight",
	"type": "Hub",
	"comment":"Hub HubFlight Table",
	"primary_key":{"name":"FlightHashKey","type":"STRING","entity_source":"raw_flight_vw","column_source":"sha1_hub_flightID"},
	"record_source":"BookingSystem",
	"business_keys":[
					{"name":"Carrier","type":"STRING","entity_source":"raw_flight_vw","column_source":"Carrier"},
					{"name":"FlightNum","type":"STRING","entity_source":"raw_flight_vw","column_source":"FlightNum"}
					]
}
'''
"""
##############################

##############################
"""
json_str = '''{
	"id": "HubAirline",
	"type": "Hub",
	"comment":"Hub Airline Table",
	"primary_key":{"name":"AirlineHashKey","type":"STRING","entity_source":"raw_airline_vw","column_source":"sha1_hub_airlineID"},
	"record_source":"BookingSystem",
	"business_keys":[
					{"name":"AirlineID","type":"BIGINT","entity_source":"raw_airline_vw","column_source":"airlineID"}
					]
}
'''
"""
##############################

# Charger la chaîne JSON en un dictionnaire Python
data = json.loads(json_str)

# Extraire les informations nécessaires pour la requête SQL
table_name = data["id"]
comment = data["comment"]
columns = [{"name": data["primary_key"]["name"], "type": data["primary_key"]["type"], "nullable": False, "primary_key": True}]
business_keys = data.get("business_keys", [])
for key in business_keys:
	columns.append({"name": key["name"], "type": key["type"], "nullable": True, "primary_key": False})

# Générer la partie colonnes de la requête SQL en utilisant les informations sur les colonnes
sql_columns = []
pk_columns = []
for col in columns:
	column_name = col["name"]
	column_type = col["type"]
	nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
	pk = col.get("primary_key", False)
	sql_columns.append("{0} {1} {2}".format(column_name, column_type, nullable))
	if pk:
		pk_columns.append(column_name)

# Générer la requête SQL
sql = """
CREATE TABLE IF NOT EXISTS {0}.{1}(
  {2},
  LoadDate TIMESTAMP,
  RecordSource STRING,
  PRIMARY KEY ({3})
)
COMMENT "{4}"
""".format(SILVER_CATALOG_NAME, table_name, ",\n  ".join(sql_columns), ", ".join(pk_columns), comment)

# Afficher la requête SQL générée
print(sql)

