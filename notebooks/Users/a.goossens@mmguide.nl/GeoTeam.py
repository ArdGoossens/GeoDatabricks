# Databricks notebook source
dbutils.widgets.text("file","")
filename= dbutils.widgets.get("file")
Filetype=""
Customer=""

# COMMAND ----------

GenID="nd4wods4xqefm"

DB_ConnectionString = "jdbc:sqlserver://server00000"+GenID+".database.windows.net:1433;database=database000"+GenID+";user=Ard@server00000"+GenID+";password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

#jdbc:sqlserver://server00000nd4wods4xqefm.database.windows.net:1433;database=database000nd4wods4xqefm;user=Ard@server00000nd4wods4xqefm;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;
      
StorageSource = "wasbs://uploads@storage0000"+GenID+".blob.core.windows.net"
Storagekey="LOiBWxB03MobgbYT74xZo7e6ec89m9iV+pgctpIhMWKtXcSyqxGrzhRHp/RLI5uj9BCEYPSPsXMsS7MPCOzPQw=="
StorageConfig= "fs.azure.account.key.storage0000"+GenID+".blob.core.windows.net"

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

#mount the container if not yet mounted

if not any(mount.mountPoint == '/mnt/GeoUpload' for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
   source = StorageSource,
   mount_point = "/mnt/GeoUpload",
   extra_configs = {StorageConfig: Storagekey }
  )


# COMMAND ----------

from pyspark.sql.functions import lit
# read the JSON file in a dataframe
JsonDF = (spark.read 
    .option("inferSchema", "true")
    .json(JsonFilename, multiLine=True)
     .withColumn('Customer', lit(Customer))
     .withColumn('Type', lit(Filetype))
 )
#JsonDF.printSchema()
#JsonDF.createOrReplaceTempView("JsonFile")

# COMMAND ----------

#stagingDF = JsonDF.select("ExternalId").head()

# COMMAND ----------

from pyspark.sql.functions import explode
from pyspark.sql.functions import to_json, struct
from pyspark.sql import functions as F
          

if Filetype=="Entities":
  stagingDF = JsonDF.select ("ExternalId","Created","Customer","Type", to_json("Location").alias("Location"))
  
if Filetype=="EntityDescriptions":
  stagingDF = JsonDF.select("EntityExternalId","Created","Valid",to_json("Data").alias("Data"),"Customer","Type")

if Filetype=="TimeEntities":
  stagingDF = JsonDF.select(explode(F.col("TimeSerieDtos")).alias("TimeSerie"),"EntityExternalId","TimeResolution","TimeSerie.Time",to_json("TimeSerie.Tags").alias("Tags"),"TimeSerie.Value","Customer","Type").drop("TimeSerie")

#stagingDF.printSchema()

# COMMAND ----------

stagingDF.write.jdbc(url=DB_ConnectionString, table=target, mode="append")

# COMMAND ----------

#dbutils.notebook.exit("returnValue")