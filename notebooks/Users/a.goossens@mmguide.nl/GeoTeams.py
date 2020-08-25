# Databricks notebook source
import re
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
from pyspark.sql.functions import to_json, struct


#filename ="DINOBRO_Entities_20200623.json"
#filename ="DINOBRO_EntityDescriptions_20200623.json"
#filename ="DINOBRO_TimeEntities_20200623.json"
#filename ="SUNFLOWER_Entities_20200616.json"
#filename ="SUNFLOWER_EntityDescriptions_20200616.json"
#filename ="SUNFLOWER_TimeEntities_20200616.json"


#dbutils.notebook.run("/Users/a.goossens@mmguide.nl/GeoTeam", 60, {"file": "DINOBRO_EntityDescriptions_20200623.json"})


# COMMAND ----------

# MAGIC %run /Users/a.goossens@mmguide.nl/GeoTeam $file =DINOBRO_Entities_20200623.json

# COMMAND ----------

# MAGIC %run /Users/a.goossens@mmguide.nl/GeoTeam $file =DINOBRO_EntityDescriptions_20200623.json

# COMMAND ----------

# MAGIC %run /Users/a.goossens@mmguide.nl/GeoTeam $file =DINOBRO_TimeEntities_20200623.json

# COMMAND ----------

# MAGIC %run /Users/a.goossens@mmguide.nl/GeoTeam $file =SUNFLOWER_Entities_20200616.json

# COMMAND ----------

# MAGIC %run /Users/a.goossens@mmguide.nl/GeoTeam $file =SUNFLOWER_EntityDescriptions_20200616.json

# COMMAND ----------

# MAGIC %run /Users/a.goossens@mmguide.nl/GeoTeam $file =SUNFLOWER_TimeEntities_20200616.json