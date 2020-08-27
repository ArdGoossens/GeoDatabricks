# Databricks notebook source
dbutils.widgets.text("file","")
filename= dbutils.widgets.get("file")
Filetype=""
Customer=""

import datetime
nu= datetime.datetime.now()

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
# MAGIC apt-get update
# MAGIC ACCEPT_EULA=Y apt-get install msodbcsql17
# MAGIC apt-get -y install unixodbc-dev
# MAGIC sudo apt-get install python3-pip -y
# MAGIC pip3 install --upgrade pyodbc

# COMMAND ----------

#key vault secrets
Servername = dbutils.secrets.get(scope = "keyvault", key = "ServerName")
DatabaseName = dbutils.secrets.get(scope = "keyvault", key = "DatabaseName")
StorageName = dbutils.secrets.get(scope = "keyvault", key = "StorageName")
StorageKey = dbutils.secrets.get(scope = "keyvault", key = "StorageKey")
administratorLogin = dbutils.secrets.get(scope = "keyvault", key = "administratorLogin")
administratorLoginPassword = dbutils.secrets.get(scope = "keyvault", key = "administratorLoginPassword")

#derived strings
jdbc_ConnectionString = "jdbc:sqlserver://"+Servername+".database.windows.net:1433;database="+DatabaseName+";user=Ard@"+Servername+";password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
StorageSource = "wasbs://uploads@"+StorageName+".blob.core.windows.net"
StorageConfig= "fs.azure.account.key."+StorageName+".blob.core.windows.net"

odbc_ConnectionString ="DRIVER={ODBC Driver 17 for SQL Server};SERVER="+Servername+".database.windows.net;DATABASE="+DatabaseName+";UID="+administratorLogin+";PWD="+administratorLoginPassword

print (Servername)


# COMMAND ----------

JsonFilename ="dbfs:/mnt/GeoUpload/" +filename
import re

datafeed =re.compile("DINOBRO")
if datafeed.search(filename):
  Customer="DINOBRO"
datafeed =re.compile("SUNFLOWER")
if datafeed.search(filename):
  Customer="SUNFLOWER"

datafeed =re.compile("_Entities_")
if datafeed.search(filename):
  Filetype="Entities"

datafeed =re.compile("_EntityDescriptions_")
if datafeed.search(filename):
  Filetype="EntityDescriptions"
  
datafeed =re.compile("_TimeEntities_")
if datafeed.search(filename):
  Filetype="TimeEntities"

  
target ="team.Staging_" + Filetype
#print(Customer)
#print (Type)  
  

# COMMAND ----------

#print(jdbc_ConnectionString)

# COMMAND ----------

#mount the container if not yet mounted

if not any(mount.mountPoint == '/mnt/GeoUpload' for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
   source = StorageSource,
   mount_point = "/mnt/GeoUpload",
   extra_configs = {StorageConfig: StorageKey }
  )


# COMMAND ----------

from pyspark.sql.functions import lit
# read the JSON file in a dataframe
JsonDF = (spark.read 
    .option("inferSchema", "true")
    .json(JsonFilename, multiLine=True)
     .withColumn('Customer', lit(Customer))
     .withColumn('Type', lit(Filetype))
    .withColumn('ImportDateTime',lit(nu))
 )
#JsonDF.printSchema()
#JsonDF.createOrReplaceTempView("JsonFile")

# COMMAND ----------

#JsonDF.printSchema()

# COMMAND ----------

print(Filetype)

# COMMAND ----------

from pyspark.sql.functions import explode
from pyspark.sql.functions import to_json, struct
from pyspark.sql import functions as F
          

if Filetype=="Entities":
  stagingDF = JsonDF.select ("ExternalId","Created","Customer","ImportDateTime", to_json("Location").alias("Location"))
  
if Filetype=="EntityDescriptions":
  stagingDF = JsonDF.select("EntityExternalId","Created","Valid",to_json("Data").alias("Data"),"Customer","ImportDateTime")

if Filetype=="TimeEntities":
  DFTemp = JsonDF.select(explode(F.col("TimeSerieDtos")).alias("TimeSerie"),"EntityExternalId","TimeResolution","TimeSerie.Time","TimeSerie.Tags","TimeSerie.Value","Customer","ImportDateTime").drop("TimeSerie")
  if dict(DFTemp.dtypes)['Tags'] == "string":
    stagingDF = DFTemp.select("EntityExternalId","TimeResolution","Time","Tags","Value","Customer","ImportDateTime")
  else:
    stagingDF = DFTemp.select("EntityExternalId","TimeResolution","Time",to_json("Tags").alias("Tags"),"Value","Customer","ImportDateTime")


#stagingDF.printSchema()

# COMMAND ----------

stagingDF.write.jdbc(url=jdbc_ConnectionString, table=target, mode="append")

# COMMAND ----------

import pyodbc
conn = pyodbc.connect( odbc_ConnectionString)
conn.autocommit = True
command =  "exec team.stp_import '" + Filetype +"','"+str(nu)+"'" 
conn.execute(command)
conn.close()