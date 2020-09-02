# Databricks notebook source
#DINOBRO_Entities_20200623.json
#DINOBRO_EntityDescriptions_20200623.json
dbutils.widgets.text("file","")
filename= dbutils.widgets.get("file")

#spark.createDataFrame([("filename", filename)], ("Name", "Value")).createOrReplaceTempView('Variable')
  
  
Filetype=""
Customer=""

import datetime
nu= datetime.datetime.now()

# COMMAND ----------

# MAGIC %sql select * from Variable

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
CustomerName =""

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
  
if DINO1+DINO2+DINO3+DINO4+DINO5+DINO6>0:
  CustomerName ="DINOBRO"
if SUN1+SUN2+SUN3+SUN4+SUN5+SUN6>0:
  CustomerName ="SUNFLOWER"



# COMMAND ----------

display(JsonDF)

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
  
  
  
  
#display(JsonDF)

  

# COMMAND ----------








spark.createDataFrame([("CustomerName",CustomerName)], ("Name", "Value")).createOrReplaceTempView('CustomerName')  



# COMMAND ----------

(spark.read.format("jdbc")\
  .option("url", jdbc_ConnectionString)\
  .option("dbtable", "DataBricksIn.Customer")\
  .load()).createOrReplaceTempView("CustomerW")

(spark.read.format("jdbc")\
  .option("url", jdbc_ConnectionString)\
  .option("dbtable", "DataBricksIn.Entities")\
  .load()).createOrReplaceTempView("EntitiesW")

(spark.read.format("jdbc")\
  .option("url", jdbc_ConnectionString)\
  .option("dbtable", "DataBricksIn.EntityDescriptions")\
  .load()).createOrReplaceTempView("EntityDescriptionsW")

(spark.read.format("jdbc")\
  .option("url", jdbc_ConnectionString)\
  .option("dbtable", "DataBricksIn.TimeEntities")\
  .load()).createOrReplaceTempView("TimeEntitiesW")

(spark.read.format("jdbc")\
  .option("url", jdbc_ConnectionString)\
  .option("dbtable", "DataBricksIn.TimeSeries")\
  .load()).createOrReplaceTempView("TimeSeriesW")



(spark.read.format("jdbc")\
  .option("url", jdbc_ConnectionString)\
  .option("dbtable", "DataBricksOut.Customer")\
  .load()).createOrReplaceTempView("CustomerR")

(spark.read.format("jdbc")\
  .option("url", jdbc_ConnectionString)\
  .option("dbtable", "DataBricksOut.Entities")\
  .load()).createOrReplaceTempView("EntitiesR")

(spark.read.format("jdbc")\
  .option("url", jdbc_ConnectionString)\
  .option("dbtable", "DataBricksOut.EntityDescriptions")\
  .load()).createOrReplaceTempView("EntityDescriptionsR")

(spark.read.format("jdbc")\
  .option("url", jdbc_ConnectionString)\
  .option("dbtable", "DataBricksOut.TimeEntities")\
  .load()).createOrReplaceTempView("TimeEntitiesR")

(spark.read.format("jdbc")\
  .option("url", jdbc_ConnectionString)\
  .option("dbtable", "DataBricksOut.TimeSeries")\
  .load()).createOrReplaceTempView("TimeSeriesR")







# COMMAND ----------

# MAGIC %sql
# MAGIC --add Customer
# MAGIC insert into CustomerW
# MAGIC select Value
# MAGIC from CustomerName CN
# MAGIC   left join CustomerR C
# MAGIC     on C.CustomerName=CN.Value
# MAGIC where C.CustomerName is null

# COMMAND ----------

RenamedDF.select("EntityName","StartDate").withColumn('CustomerName',lit(CustomerName)).distinct().createOrReplaceTempView('EntityName') 


# COMMAND ----------

# MAGIC %sql 
# MAGIC -- add entities
# MAGIC insert into EntitiesW
# MAGIC select NULL, EN.StartDate, C.Id, EN.EntityName
# MAGIC from EntityName EN
# MAGIC   inner join CustomerR C
# MAGIC     on C.CustomerName=EN.CustomerName
# MAGIC   left join EntitiesR E
# MAGIC     on E.ExternalId=EN.EntityName
# MAGIC     and E.CustomerId =C.Id
# MAGIC where E.ExternalId is null

# COMMAND ----------

# MAGIC %sql
# MAGIC --update entities

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from EntityName