# Databricks notebook source
#DINOBRO_Entities_20200623.json
#DINOBRO_EntityDescriptions_20200623.json
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
odbc_ConnectionString ="DRIVER={ODBC Driver 17 for SQL Server};SERVER="+Servername+".database.windows.net;DATABASE="+DatabaseName+";UID="+administratorLogin+";PWD="+administratorLoginPassword
StorageSource = "wasbs://uploads@"+StorageName+".blob.core.windows.net"
StorageConfig= "fs.azure.account.key."+StorageName+".blob.core.windows.net"
JsonFilename ="dbfs:/mnt/GeoUpload/" +filename

# COMMAND ----------

#mount the container if not yet mounted

if not any(mount.mountPoint == '/mnt/GeoUpload' for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
   source = StorageSource,
   mount_point = "/mnt/GeoUpload",
   extra_configs = {StorageConfig: StorageKey }
  )


# COMMAND ----------

# read the JSON file in a dataframe
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
from pyspark.sql.functions import to_json, struct
from pyspark.sql import functions as F

JsonDF = (spark.read 
    .option("inferSchema", "true")
    .json(JsonFilename, multiLine=True)
    .withColumn('ImportDateTime',lit(nu))
 )

# COMMAND ----------

# determing import actions
import re

#declarations
DINO1=0
DINO2=0
DINO3=0
DINO4=0
DINO5=0
DINO6=0
SUN1=0
SUN2=0
SUN3=0
SUN4=0
SUN5=0
SUN6=0
DataImportCode=""

datafeed =re.compile("DINOBRO_Entities_")
if datafeed.search(filename):
  DINO1=1
  
datafeed =re.compile("DINOBRO_EntityDescriptions_")
if datafeed.search(filename):
  DINO2=1
  DINO3=1
  
datafeed =re.compile("DINOBRO_TimeEntities_")
if datafeed.search(filename):
  DINO4=1
  DINO5=1
  DINO6=1
  
datafeed =re.compile("SUNFLOWER_Entities_")
if datafeed.search(filename):
  SUN1=1
  
datafeed =re.compile("SUNFLOWER_EntityDescriptions_")
if datafeed.search(filename):
  SUN2=1
  SUN3=1
  
datafeed =re.compile("SUNFLOWER_TimeEntities_")
if datafeed.search(filename):
  SUN4=1
  SUN5=1
  SUN6=1


  

# COMMAND ----------

# exploding , renaming amd EntryId
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.window import Window
#from pyspark.sql import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import desc

window = Window.partitionBy("EntityName","StartDate").orderBy("EntityName")  

if DINO1+SUN1>0:
  RenamedDF = JsonDF.select (col("ExternalId").alias("EntityName"),col("Created").alias("StartDate"), \
                             "Location") \
  .withColumn('ImportDateTime',lit(nu)) \
  .withColumn("EntryId", row_number().over(window))
if DINO2+DINO3+SUN2+SUN3>0:
  RenamedDF = JsonDF.select(col("EntityExternalId").alias("EntityName"),col("Created").alias("StartDate"), \
                            "Valid","Data")\
  .withColumn('ImportDateTime',lit(nu)) \
  .withColumn("EntryId", row_number().over(window))
if DINO4+DINO5+DINO6+SUN4+SUN5+SUN6>0:
  
  RenamedDF= JsonDF.select(explode(F.col("TimeSerieDtos")).alias("TimeSerie"),\
                           col("EntityExternalId").alias("EntityName"),col("TimeSerie.Time").alias("StartDate")\
                           ,"TimeResolution","TimeSerie.Tags","TimeSerie.Value").drop("TimeSerie") \
  .withColumn('ImportDateTime',lit(nu)) \
  .withColumn("EntryId", row_number().over(window))
  
  #display(RenamedDF)
  

# COMMAND ----------

# making export dataframe
if DINO1==1:
  DINO1DF = RenamedDF.withColumn('ImportDateTime',lit(nu)).withColumn('DataImportCode',lit("DINO1"))\
  .select ("DataImportCode", "EntityName","EntryId","StartDate","ImportDateTime"\
           ,to_json("Location").alias("JsonValue"))
if SUN1==1:
  SUN1DF = RenamedDF.withColumn('ImportDateTime',lit(nu)).withColumn('DataImportCode',lit("SUN1"))\
  .select ("DataImportCode", "EntityName","EntryId","StartDate","ImportDateTime",\
           to_json("Location").alias("JsonValue"))
if DINO2==1:
  DINO2DF = RenamedDF.withColumn('ImportDateTime',lit(nu)).withColumn('DataImportCode',lit("DINO2")) \
  .select ("DataImportCode", "EntityName","EntryId","StartDate","ImportDateTime"\
           ,col("Valid").alias("JsonValue"))
if SUN2==1:
  SUN2DF = RenamedDF.withColumn('ImportDateTime',lit(nu)).withColumn('DataImportCode',lit("SUN2")) \
  .select ("DataImportCode", "EntityName","EntryId","StartDate","ImportDateTime"\
           ,col("Valid").alias("JsonValue"))
if DINO3==1:
  DINO3DF = RenamedDF.withColumn('ImportDateTime',lit(nu)).withColumn('DataImportCode',lit("DINO3")) \
  .select ("DataImportCode", "EntityName","EntryId","StartDate","ImportDateTime"\
             ,to_json("Data").alias("JsonValue"))
if SUN3==1:
  SUN3DF = RenamedDF.withColumn('ImportDateTime',lit(nu)).withColumn('DataImportCode',lit("SUN3")) \
  .select ("DataImportCode", "EntityName","EntryId","StartDate","ImportDateTime"\
             ,to_json("Data").alias("JsonValue"))  
if DINO4==1:
  DINO4DF = RenamedDF.withColumn('ImportDateTime',lit(nu)).withColumn('DataImportCode',lit("DINO4")) \
  .select ("DataImportCode", "EntityName","EntryId","StartDate","ImportDateTime"\
           ,to_json("Tags").alias("JsonValue"))
if SUN4==1:
  SUN4DF = RenamedDF.withColumn('ImportDateTime',lit(nu)).withColumn('DataImportCode',lit("SUN4")) \
  .select ("DataImportCode", "EntityName","EntryId","StartDate","ImportDateTime"\
           ,col("Tags").alias("JsonValue"))
if DINO5==1:
    DINO5DF = RenamedDF.withColumn('ImportDateTime',lit(nu)).withColumn('DataImportCode',lit("DINO5")) \
  .select ("DataImportCode", "EntityName","EntryId","StartDate","ImportDateTime"\
           ,col("Value").alias("JsonValue"))
if SUN5==1:
  SUN5DF = RenamedDF.withColumn('ImportDateTime',lit(nu)).withColumn('DataImportCode',lit("SUN5")) \
  .select ("DataImportCode", "EntityName","EntryId","StartDate","ImportDateTime"\
           ,col("Value").alias("JsonValue"))
if DINO6==1:
    DINO6DF = RenamedDF.withColumn('ImportDateTime',lit(nu)).withColumn('DataImportCode',lit("DINO6")) \
  .select ("DataImportCode", "EntityName","EntryId","StartDate","ImportDateTime"\
           ,col("TimeResolution").alias("JsonValue"))  
if SUN6==1:
  SUN6DF = RenamedDF.withColumn('ImportDateTime',lit(nu)).withColumn('DataImportCode',lit("SUN6")) \
  .select ("DataImportCode", "EntityName","EntryId","StartDate","ImportDateTime"\
           ,col("TimeResolution").alias("JsonValue"))
 
    

    
#  if dict(DFTemp.dtypes)['Tags'] == "string":
#    stagingDF = DFTemp.select("EntityExternalId","TimeResolution","Time","Tags","Value","Customer","ImportDateTime")
#  else:
#    stagingDF = DFTemp.select("EntityExternalId","TimeResolution","Time",to_json("Tags").alias("Tags"),"Value","Customer","ImportDateTime")
    
  

# COMMAND ----------


          

#if Filetype=="Entities":
#  stagingDF = JsonDF.select ("ExternalId","Created","Customer","ImportDateTime", to_json("Location").alias("Location"))
#  
#if Filetype=="EntityDescriptions":
#  stagingDF = JsonDF.select("EntityExternalId","Created","Valid",to_json("Data").alias("Data"),"Customer","ImportDateTime")
#
#if Filetype=="TimeEntities":
#  DFTemp = JsonDF.select(explode(F.col("TimeSerieDtos")).alias("TimeSerie"),"EntityExternalId","TimeResolution","TimeSerie.Time","TimeSerie.Tags","TimeSerie.Value","Customer","ImportDateTime").drop("TimeSerie")
#  if dict(DFTemp.dtypes)['Tags'] == "string":
#    stagingDF = DFTemp.select("EntityExternalId","TimeResolution","Time","Tags","Value","Customer","ImportDateTime")
#  else:
#    stagingDF = DFTemp.select("EntityExternalId","TimeResolution","Time",to_json("Tags").alias("Tags"),"Value","Customer","ImportDateTime")

#stagingDF.printSchema()

# COMMAND ----------

#DINO4DF.printSchema()


# COMMAND ----------

if DINO1==1:
  DINO1DF.write.jdbc(url=jdbc_ConnectionString, table="gen.Staging_Json", mode="append")
if DINO2==1:
  DINO2DF.write.jdbc(url=jdbc_ConnectionString, table="gen.Staging_Json", mode="append")
if DINO3==1:
  DINO3DF.write.jdbc(url=jdbc_ConnectionString, table="gen.Staging_Json", mode="append")
if DINO4==1:
  DINO4DF.write.jdbc(url=jdbc_ConnectionString, table="gen.Staging_Json", mode="append")
if DINO5==1:
  DINO5DF.write.jdbc(url=jdbc_ConnectionString, table="gen.Staging_Json", mode="append")
if DINO6==1:
  DINO6DF.write.jdbc(url=jdbc_ConnectionString, table="gen.Staging_Json", mode="append")
if SUN1==1:
  SUN1DF.write.jdbc(url=jdbc_ConnectionString, table="gen.Staging_Json", mode="append")
if SUN2==1:
  SUN2DF.write.jdbc(url=jdbc_ConnectionString, table="gen.Staging_Json", mode="append")
if SUN3==1:
  SUN3DF.write.jdbc(url=jdbc_ConnectionString, table="gen.Staging_Json", mode="append")
if SUN4==1:
  SUN4DF.write.jdbc(url=jdbc_ConnectionString, table="gen.Staging_Json", mode="append")
if SUN5==1:
  SUN5DF.write.jdbc(url=jdbc_ConnectionString, table="gen.Staging_Json", mode="append")
if SUN6==1:
  SUN6DF.write.jdbc(url=jdbc_ConnectionString, table="gen.Staging_Json", mode="append")


# stagingDF.write.jdbc(url=jdbc_ConnectionString, table=target, mode="append")

# COMMAND ----------

import pyodbc
conn = pyodbc.connect( odbc_ConnectionString)
conn.autocommit = True
command =  "exec gen.stp_import '"+str(nu)+"'" 
conn.execute(command)
conn.close()