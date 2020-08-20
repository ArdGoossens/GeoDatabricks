# Databricks notebook source
#mount the container
#dbutils.fs.mount(
#  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
#  mount_point = "/mnt/<mount-name>",
#  extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

dbutils.fs.mount(
 source = "wasbs://uploads@storage0000s7qefz5aot56o.blob.core.windows.net",
 mount_point = "/mnt/GeoUpload",
 extra_configs = {"fs.azure.account.key.storage0000s7qefz5aot56o.blob.core.windows.net": "KSVFTSoMw5iUPjheqOR9+KWX2RN6bEUsk73shpl/Y+NGuM3WlVZZYRazyIn3y9EzbUJjyXZAuiRNATRzVB5cFg=="}
)

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

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

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