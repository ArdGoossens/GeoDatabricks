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

# read JSON file in python
jsonFile = "dbfs:/mnt/GeoUpload/DINOBRO_EntityDescriptions_20200623.json"

RawDF = (spark.read           # The DataFrameReader
    .option("inferSchema", "true")  # Automatically infer data types & column names
    .json(jsonFile, multiLine=True) # Creates a DataFrame from JSON after reading in the file
 )
RawDF.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, ArrayType  
from pandas import DataFrame
def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + "`"+ field.name+"`" if prefix else "`"+field.name+"`"
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType

        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)

    return fields

x=flatten(RawDF.schema)


df = DataFrame (x,columns=['First_Name'])
print (df)


#RawDF.select(flatten(RawDF.schema)).show()

# COMMAND ----------

# create a view from a dataframe
RawDF.createOrReplaceTempView("RawView")

# COMMAND ----------

# dynamic sql selection in python
paths="Created, EntityExternalId, Valid, Data.deliveryContext.`@xmlns`"
columns ='"Created","EntityExternalId","Valid","Data.deliveryContext.@xmlns"'

exec('neatDF = spark.sql(" SELECT '+paths+' FROM RawView").toDF('+columns+')')
neatDF.show()

# COMMAND ----------

# dynamic dataframa selection in python
paths   ='"Created","EntityExternalId","Valid","Data.deliveryContext.`@xmlns`"'
columns ='"Created","EntityExternalId","Valid","Data.deliveryContext.@xmlns"'
command='Neat2 = RawDF.select('+paths+').toDF('+columns+')'
print (command)
exec(command)
display(Neat2)

# COMMAND ----------

#loop though rows
for row in Neat2.collect():
      print (row.EntityExternalId)
    

# COMMAND ----------

# from dataframe to variable in python
for row in Neat2.filter(Neat2.EntityExternalId == 'B31G0262').collect():
      X = str(row.EntityExternalId)
print (X)

# COMMAND ----------

# MAGIC %scala
# MAGIC // from table to variable in scala
# MAGIC //val X =   DataMart.select("col1").collectAsList().get(0).getString(0)
# MAGIC //print (X)

# COMMAND ----------

# create a view from a database table
url = "jdbc:sqlserver://server00000s7qefz5aot56o.database.windows.net:1433;database=database000s7qefz5aot56o;user=Ard@server00000s7qefz5aot56o;password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
table = "DB.DataMart"
DataMartPython = spark.read.format("jdbc")\
  .option("url", url)\
  .option("dbtable", table)\
  .load()

DataMartPython.createOrReplaceTempView("DataMartPython")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DataMartPython

# COMMAND ----------

# MAGIC %scala
# MAGIC //fetch metadata data from the catalog
# MAGIC spark.catalog.listDatabases.show(false)
# MAGIC spark.catalog.listTables.show(false)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select a tempview with substructure columns in SQL
# MAGIC SELECT Created, EntityExternalId, Valid, Data.deliveryContext.`@xmlns` `Data.deliveryContext.@xmlns`   FROM RawView 

# COMMAND ----------

#fetch metadata data from the catalog
spark.catalog.listDatabases()



# COMMAND ----------

#fetch metadata data from the catalog
spark.catalog.listTables()

# COMMAND ----------



# COMMAND ----------

# drop a view 
spark.catalog.dropTempView("DataMartPython")
spark.catalog.dropTempView("rawview")

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



# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcUsername = "Ard" //dbutils.secrets.get(scope = "key-vault-secrets", key = "sql-username")
# MAGIC val jdbcPassword = "Goossens."//dbutils.secrets.get(scope = "key-vault-secrets", key = "sql-password")
# MAGIC 
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
# MAGIC 
# MAGIC val jdbcHostname = "server00000s7qefz5aot56o"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "database000s7qefz5aot56o"
# MAGIC 
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC val jdbcUrl = "jdbc:sqlserver://server00000s7qefz5aot56o.database.windows.net:1433;database=database000s7qefz5aot56o;user=Ard@server00000s7qefz5aot56o;password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
# MAGIC 
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC connectionProperties.put("user", s"${jdbcUsername}")
# MAGIC connectionProperties.put("password", s"${jdbcPassword}")
# MAGIC 
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)

# COMMAND ----------

# MAGIC %scala
# MAGIC val DataMart = spark.read.jdbc(jdbcUrl, "dbo.DataMart", connectionProperties)
# MAGIC DataMart.printSchema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE DataMart
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://server00000s7qefz5aot56o.database.windows.net:1433;database=database000s7qefz5aot56o;user=Ard@server00000s7qefz5aot56o;password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;",
# MAGIC   dbtable "DB.DataMart"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC --using view to insert with IDENTITY ON
# MAGIC insert into DataMart
# MAGIC select 'databricks'

# COMMAND ----------

# MAGIC %scala
# MAGIC // get result table function
# MAGIC val DataMart = spark.read.jdbc(jdbcUrl, "tablefunction ('a')", connectionProperties)
# MAGIC DataMart.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC // from dataset to variable
# MAGIC val naam2 =   DataMart.select("col1").collectAsList().get(0).getString(0)
# MAGIC print (naam2)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create database table reference
# MAGIC CREATE TABLE DataMart
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://server00000s7qefz5aot56o.database.windows.net:1433;database=database000s7qefz5aot56o;user=Ard@server00000s7qefz5aot56o;password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;",
# MAGIC   dbtable "DB.DataMart"
# MAGIC )

# COMMAND ----------

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

# COMMAND ----------

from pyspark.sql.types import LongType
def squared(s):
  return s * s
spark.udf.register("squaredWithPython", squared ,)

spark.range(1, 20).createOrReplaceTempView("test")

# COMMAND ----------

# MAGIC %sql select  squaredWithPython(2) 

# COMMAND ----------

df1 = sc.parallelize([[1,2], [3,4]]).toDF(("Name", "Value"))
df1.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE TF_table
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://server00000s7qefz5aot56o.database.windows.net:1433;database=database000s7qefz5aot56o;user=Ard@server00000s7qefz5aot56o;password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;",
# MAGIC   dbtable "tablefunction ('a')"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TF_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(col1) from TF_table

# COMMAND ----------

dbutils.widgets.help()

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
# MAGIC 
# MAGIC ardP = sqlContext.table("ardTV")
# MAGIC ardP.createTempView("ardVT")
# MAGIC 
# MAGIC display(ardP)

# COMMAND ----------

# MAGIC %scala
# MAGIC val ardS = spark.table("ardVT")
# MAGIC ardS.show