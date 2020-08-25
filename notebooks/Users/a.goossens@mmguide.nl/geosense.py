# Databricks notebook source
# variables
#filename ="DINOBRO_Entities_20200623.json"
filename ="DINOBRO_EntityDescriptions_20200623.json"
#filename ="DINOBRO_TimeEntities_20200623.json"
#filename ="SUNFLOWER_Entities_20200616.json"
#filename ="SUNFLOWER_EntityDescriptions_20200616.json"
#filename ="SUNFLOWER_TimeEntities_20200616.json"
GenID="nd4wods4xqefm"

DB_ConnectionString = "jdbc:sqlserver://server00000"+GenID+".database.windows.net:1433;database=database000"+GenID+";user=Ard@server00000"+GenID+";password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

#jdbc:sqlserver://server00000nd4wods4xqefm.database.windows.net:1433;database=database000nd4wods4xqefm;user=Ard@server00000nd4wods4xqefm;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;
      
StorageSource = "wasbs://uploads@storage0000"+GenID+".blob.core.windows.net"
Storagekey="LOiBWxB03MobgbYT74xZo7e6ec89m9iV+pgctpIhMWKtXcSyqxGrzhRHp/RLI5uj9BCEYPSPsXMsS7MPCOzPQw=="
StorageConfig= "fs.azure.account.key.storage0000"+GenID+".blob.core.windows.net"

JsonFilename ="dbfs:/mnt/GeoUpload/" +filename




# COMMAND ----------

print (DB_ConnectionString)

# COMMAND ----------

#view to communicate across languages

sc.parallelize([['filename',filename], ['DB_ConnectionString',DB_ConnectionString]]).toDF(("Name", "Value")).createOrReplaceTempView("Variables")


# COMMAND ----------

#mount the container if not yet mounted

if not any(mount.mountPoint == '/mnt/GeoUpload' for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
   source = StorageSource,
   mount_point = "/mnt/GeoUpload",
   extra_configs = {StorageConfig: Storagekey }
  )


# COMMAND ----------

# read the JSON file in a dataframe
JsonDF = (spark.read 
    .option("inferSchema", "true")
    .json(JsonFilename, multiLine=True)
 )

JsonDF.printSchema()
#JsonDF.createOrReplaceTempView("JsonFile")

# COMMAND ----------

# get a flat(!) list of all the qualified(!) columns in the JSON file
from pyspark.sql.types import StructType, ArrayType  
from pandas import DataFrame

def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + "`"+ field.name+"`" if prefix else "`"+field.name+"`"
        dtype = field.dataType
        Dstr =str(dtype)
        if isinstance(dtype, ArrayType):
            fields.append(name+ +"§"+"ArrayType")
            dtype = dtype.elementType
            
        if isinstance(dtype, StructType):
            fields.append(name+ "§"+"StructType")
            fields += flatten(dtype, prefix=name)
            
        else:
            fields.append(name+"§" +Dstr)

    return fields

# generate a list of all the columns
ColumnList = flatten(JsonDF.schema)

print(ColumnList)

# COMMAND ----------

# from list to view

#convert list into panda dataframe
ColumnDF = DataFrame (ColumnList,columns=['output'])

# convert panda into spark
ColumnDF2 = spark.createDataFrame(ColumnDF)

# split string into 2
import  pyspark.sql.functions as f
split_col =f.split(ColumnDF2['output'], "§")
ColumnDF2 = ColumnDF2.withColumn('path', split_col.getItem(0))
ColumnDF2 = ColumnDF2.withColumn('type', split_col.getItem(1))
display(ColumnDF2.select('path','type'))

#convert spark dataframe into table
ColumnDF2.createOrReplaceTempView("CurrentAttributes")


display(ColumnDF2)

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
# MAGIC --insert attribute data from view to database
# MAGIC insert into StagingAttributes
# MAGIC select V.Value,CA.path, CA.type
# MAGIC from CurrentAttributes CA
# MAGIC cross join (select Value from Variables where Name ='filename') V

# COMMAND ----------

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

# COMMAND ----------

display(JsonDF.select ("`Created`","`EntityExternalId`","`Data`.`result`"))
# [DatafeedID], [FileName], [EntityValue],      [StartDate], [AttributePath], [AttributeValue]
# 1             filename    EntityExternalId   Created       "`Data`.`result`"   value



# COMMAND ----------

# MAGIC %sql
# MAGIC select '1' DatafeedID, File.Value, J.EntityExternalId,J.Created, P.path, 
# MAGIC case when P.path ='Data.result' then J.`Data`.`result`
# MAGIC when P.path ='Data.brocom:deliveryAccountableParty' then J.`Data`.`brocom:deliveryAccountableParty`
# MAGIC end 
# MAGIC from JsonFile J
# MAGIC cross join (select 'Data.result' path
# MAGIC             union select 'Data.brocom:deliveryAccountableParty'
# MAGIC ) P
# MAGIC 
# MAGIC cross join (select Value from Variables where Name ='filename') File

# COMMAND ----------


