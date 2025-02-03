# Databricks notebook source
import json

# Remplacez la variable suivante par le nom de votre catalogue Silver
SILVER_CATALOG_NAME = "silver_dev"


##############################
# JSON d'exemple
json_str = '''{
	"id": "LinkConnection",
	"type": "Link",
	"comment":"Link for a flight Table",
	"primary_key":{"name":"ConnectionHashKey","type":"STRING"},
	"Foreign_keys":[
			{"hub":"HubFlight","name":"FlightHashKey","entity_source":"raw_airline_vw","column_source":"airlineID"},
			{"hub":"HubCarrier","name":"CarrierHashKey","entity_source":"raw_carrier_vw","column_source":"carrierID"},
			{"hub":"HubAirport","name":"SourceAirportHashKey","entity_source":"raw_connection_vw","column_source":"SourceAirportID"},
   		    {"hub":"HubAirport","name":"DestinationAirportHashKey","entity_source":"raw_connection_vw","column_source":"DestinationAirportID"}
	],
 	"record_source":"BookingSystem"
}
'''
##############################
# Charger la chaîne JSON en un dictionnaire Python
data = json.loads(json_str)

# Extraire les informations nécessaires pour la requête SQL
table_name = data["id"]
comment = data["comment"]
primary_key = data["primary_key"]
foreign_keys = data["Foreign_keys"]

# Générer la partie colonnes de la requête SQL en utilisant les informations sur la clé primaire et les clés étrangères
columns = []
pk_column = primary_key["name"]
pk_column_type = primary_key["type"]
columns.append("{0} {1} NOT NULL".format(pk_column, pk_column_type))
for fk in foreign_keys:
	hub_name = fk["hub"]
	column_name = fk["name"]
	column_type = "STRING"  # Par défaut, type de colonne STRING
	nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
	columns.append("{0} {1} {2}".format(column_name, column_type, nullable))

# Générer la requête SQL
sql = """
CREATE TABLE IF NOT EXISTS {0}.raw_vault.{1}(
  {2},
  LoadDate TIMESTAMP,
  RecordSource STRING
)
COMMENT "{3}"
""".format(SILVER_CATALOG_NAME, table_name, ",\n  ".join(columns), comment)

# Afficher la requête SQL générée
print(sql)
