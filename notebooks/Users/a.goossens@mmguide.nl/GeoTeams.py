# Databricks notebook source
#https://datathirst.net/blog/2018/10/12/executing-sql-server-stored-procedures-on-databricks-pyspark
%sh
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
apt-get update
ACCEPT_EULA=Y apt-get install msodbcsql17
apt-get -y install unixodbc-dev
sudo apt-get install python3-pip -y
pip3 install --upgrade pyodbc

# COMMAND ----------

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

# COMMAND ----------

import pyodbc

conn = pyodbc.connect( 'DRIVER={ODBC Driver 17 for SQL Server};'
                       'SERVER=server00000nd4wods4xqefm.database.windows.net;'
                       'DATABASE=database000nd4wods4xqefm;UID=Ard;'
                       'PWD=Goossens.')
conn.autocommit = True

#jdbc:sqlserver://server00000nd4wods4xqefm.database.windows.net:1433;database=database000nd4wods4xqefm;user=Ard@server00000nd4wods4xqefm;password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;

# Example doing a simple execute
conn.execute('exec team.stp_import')

conn.close()