# Databricks notebook source
#mount the container
#dbutils.fs.mount(
#  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
#  mount_point = "/mnt/<mount-name>",
#  extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

if not any(mount.mountPoint == '/mnt/GeoUpload' for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
   source = "wasbs://uploads@storage0000s7qefz5aot56o.blob.core.windows.net",
   mount_point = "/mnt/GeoUpload",
   extra_configs = {"fs.azure.account.key.storage0000s7qefz5aot56o.blob.core.windows.net": "KSVFTSoMw5iUPjheqOR9+KWX2RN6bEUsk73shpl/Y+NGuM3WlVZZYRazyIn3y9EzbUJjyXZAuiRNATRzVB5cFg=="}
  )

  # OR
try:
   dbutils.fs.mount(
   source = "wasbs://uploads@storage0000s7qefz5aot56o.blob.core.windows.net",
   mount_point = "/mnt/GeoUpload",
   extra_configs = {"fs.azure.account.key.storage0000s7qefz5aot56o.blob.core.windows.net": "KSVFTSoMw5iUPjheqOR9+KWX2RN6bEUsk73shpl/Y+NGuM3WlVZZYRazyIn3y9EzbUJjyXZAuiRNATRzVB5cFg=="}
  )
except Exception as e:
  print("already mounted. Try to unmount first")

# COMMAND ----------

# list mounts
dbutils.fs.mounts()

# COMMAND ----------

#list files in a mount: 
dbutils.fs.ls("/mnt/GeoUpload/")

# COMMAND ----------

#see the start of a file:  
# %fs head dbfs:/mnt/GeoUpload/DINOBRO_EntityDescriptions_20200623.json
dbutils.fs.head("/mnt/GeoUpload/DINOBRO_EntityDescriptions_20200623.json", 10000)

# COMMAND ----------

# read JSON file 
jsonFile = "dbfs:/mnt/GeoUpload/DINOBRO_EntityDescriptions_20200623.json"

RawDF = (spark.read           # The DataFrameReader
    .option("inferSchema", "true")  # Automatically infer data types & column names
    .json(jsonFile, multiLine=True)                 # Creates a DataFrame from JSON after reading in the file
 )
RawDF.printSchema()

# COMMAND ----------

# create a view called wiki_edits
RawDF.createOrReplaceTempView("RawView")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM RawView 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT Created, EntityExternalId, Valid, Data.deliveryContext.`@xmlns` `Data.deliveryContext.@xmlns`   FROM RawView 

# COMMAND ----------

   paths="Created, EntityExternalId, Valid, Data.deliveryContext.`@xmlns`"
columns ='"Created","EntityExternalId","Valid","Data.deliveryContext.@xmlns"'

exec('neatDF = spark.sql(" SELECT '+paths+' FROM RawView").toDF('+columns+')')
neatDF.show()


# COMMAND ----------

columns ='Created, EntityExternalId, Valid, Data.deliveryContext.`@xmlns` `Data.deliveryContext.@xmlns`'
RawDF.select("Created","EntityExternalId","Valid","Data.deliveryContext.`@xmlns`").show()

# COMMAND ----------

columns ='"Created","EntityExternalId","Valid","Data.deliveryContext.`@xmlns`"'
print (columns)
Neat2 = RawDF.select("Created","EntityExternalId","Valid","Data.deliveryContext.`@xmlns`").toDF("Created","EntityExternalId","Valid","Data.deliveryContext.@xmlns")
display(Neat2)

# COMMAND ----------

# dynamic dataframa manupulation

paths ='"Created","EntityExternalId","Valid","Data.deliveryContext.`@xmlns`"'
columns ='"Created","EntityExternalId","Valid","Data.deliveryContext.@xmlns"'
command='Neat3 = RawDF.select('+paths+').toDF('+columns+')'
print (command)
exec(command)
display(Neat3)

# COMMAND ----------

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#mount the container
#mount only of not mounted
get the storage key and name from keyvault
get a value from SQL server
use a function in SQL server
# dynamic python dataframe code
connect to SQL server




# COMMAND ----------

# MAGIC %scala
# MAGIC // import into cluster  com.microsoft.azure:azure-sqldb-spark:1.0.2.
# MAGIC 
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC 
# MAGIC val config = Config(Map(
# MAGIC   "url"          -> "server00000s7qefz5aot56o.database.windows.net",
# MAGIC   "databaseName" -> "database000s7qefz5aot56o",
# MAGIC   "queryCustom"  -> "SELECT * FROM [dbo].[Datamart]",  //Sql query
# MAGIC   "user"         -> "Ard",
# MAGIC   "password"     -> "Goossens."
# MAGIC ))
# MAGIC 
# MAGIC 
# MAGIC val collection = sqlContext.read.sqlDB(config)
# MAGIC val ardDF = collection.toDF()
# MAGIC ardDF.createTempView("ardTV")
# MAGIC display(ardDF)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC ardP = sqlContext.table("ardTV")
# MAGIC ardP.createTempView("ardVT")
# MAGIC 
# MAGIC display(ardP)

# COMMAND ----------

# MAGIC %scala
# MAGIC val ardS = spark.table("ardVT")
# MAGIC ardS.show