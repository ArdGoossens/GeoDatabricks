# Databricks notebook source
# variables
#filename ="DINOBRO_Entities_20200623.json"
#filename ="DINOBRO_EntityDescriptions_20200623.json"
#filename ="DINOBRO_TimeEntities_20200623.json"
#filename ="SUNFLOWER_Entities_20200616.json"
#filename ="SUNFLOWER_EntityDescriptions_20200616.json"
#filename ="SUNFLOWER_TimeEntities_20200616.json"

DB_ConnectionString = "jdbc:sqlserver://server00000s7qefz5aot56o.database.windows.net:1433;database=database000s7qefz5aot56o;user=Ard@server00000s7qefz5aot56o;password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"



JsonFilename ="dbfs:/mnt/GeoUpload/" +filename




# COMMAND ----------

#view to communicate acroos languages

sc.parallelize([['filename',filename], ['DB_ConnectionString',DB_ConnectionString]]).toDF(("Name", "Value")).createOrReplaceTempView("Variables")


# COMMAND ----------

#mount the container if not yet mounted

if not any(mount.mountPoint == '/mnt/GeoUpload' for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
   source = "wasbs://uploads@storage0000s7qefz5aot56o.blob.core.windows.net",
   mount_point = "/mnt/GeoUpload",
   extra_configs = {"fs.azure.account.key.storage0000s7qefz5aot56o.blob.core.windows.net": "KSVFTSoMw5iUPjheqOR9+KWX2RN6bEUsk73shpl/Y+NGuM3WlVZZYRazyIn3y9EzbUJjyXZAuiRNATRzVB5cFg=="}
  )


# COMMAND ----------

# read the JSON file in a dataframe
JsonDF = (spark.read 
    .option("inferSchema", "true")
    .json(JsonFilename, multiLine=True)
 )

#JsonDF.printSchema()

# COMMAND ----------

# get a flat(!) list of all the qualified(!) columns in the JSON file
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

# generate a list of all the columns
ColumnList = flatten(JsonDF.schema)

#print(ColumnList)

# COMMAND ----------

# from list to view

#convert list into panda dataframe
ColumnDF = DataFrame (ColumnList,columns=['path'])

# convert panda into spark
ColumnDF2 = spark.createDataFrame(ColumnDF)

#convert spark dataframe into table
ColumnDF2.createOrReplaceTempView("CurrentAttributes")


#display(ColumnDF2)

# COMMAND ----------

# create connection to geo.StagingAttributes in the database

url = DB_ConnectionString
table = "geo.StagingAttributes"
StagingAttributes = spark.read.format("jdbc")\
  .option("url", url)\
  .option("dbtable", table)\
  .load()
StagingAttributes.createOrReplaceTempView("StagingAttributes")


# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from Variables where Name ='filename'

# COMMAND ----------

# MAGIC %sql
# MAGIC --insert attribute data from view to database
# MAGIC insert into StagingAttributes
# MAGIC select V.Value,CA.path 
# MAGIC from CurrentAttributes CA
# MAGIC cross join (select Value from Variables where Name ='filename') V

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from StagingAttributes

# COMMAND ----------



# COMMAND ----------

