# Databricks notebook source
## Il y'a eu 92 ou bien 1084 matches pour cette saison? c'est pas possible

len(dbutils.fs.ls('/mnt/raw/wyscout/matches/formations/2023/05/08/'))

# COMMAND ----------

#il y'a eu 1486 joueurs contract info -- c'est à dire il manque deux joueurs qu'on a pas de contact info?
#pourquoi conversion en parquet prends >3h

len(dbutils.fs.ls('/mnt/raw/wyscout/players/contractinfo/2023/05/08/'))


# COMMAND ----------

#il y'a eu 1488 joueurs pour cette saison en Jupiler Pro league?

len(dbutils.fs.ls('/mnt/raw/wyscout/players/players/2023/05/08/'))

# COMMAND ----------

len(dbutils.fs.ls('/mnt/raw/wyscout/coaches/coaches/2023/05/08/'))

# COMMAND ----------

## Il y'a eu 92 ou bien 1084 matches pour cette saison? c'est pas possible

print(len(dbutils.fs.ls('/mnt/raw/wyscout/matches/advancedstats/2023/05/08/'))) 
print(len(dbutils.fs.ls('/mnt/raw/wyscout/matches/events/2023/05/08/')))
print(len(dbutils.fs.ls('/mnt/raw/wyscout/matches/formations/2023/05/08/')))
print(len(dbutils.fs.ls('/mnt/raw/wyscout/matches/matches/2023/05/08/')))
print(len(dbutils.fs.ls('/mnt/raw/wyscout/matches/players_advancedstats/2023/05/08/')))

# COMMAND ----------


