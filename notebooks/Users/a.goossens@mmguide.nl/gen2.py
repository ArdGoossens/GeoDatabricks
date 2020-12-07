# Databricks notebook source
import datetime
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode

from pyspark.sql.functions import to_json, struct, split
from pyspark.sql import functions as F
# exploding , renaming amd EntryId
from pyspark.sql.functions import col
from pyspark.sql.window import Window
#from pyspark.sql import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import desc

# COMMAND ----------

from pyspark.sql.types import StructType, ArrayType  
from pandas import DataFrame
def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix +  "." + '"'+ field.name+'"' if prefix else '"'+field.name+'"'
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType

        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)

    return fields


# COMMAND ----------

filename="DINOBRO_TimeEntities_20200623.json"
filename="DINOBRO_EntityDescriptions_20200623.json"
nu= datetime.datetime.now()


# COMMAND ----------

StorageSource = "wasbs://"+"archive"+"@"+"storagexxxxmuupl4c6zvywi"+".blob.core.windows.net"
StorageConfig= "fs.azure.account.key."+"storagexxxxmuupl4c6zvywi"+".blob.core.windows.net"
StorageKey = "DlD0gMuSD5Scix9v1SeoDkYdWYTray+gGqbsaZ/lWSTDZahq4VTwCUR2W8rALLWVv6vao4Z7/cT4xkQKOJdcsg=="
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

# get a flat(!) list of all the qualified(!) columns in the JSON file
from pyspark.sql.types import StructType, ArrayType  ,StructField,StringType
from pandas import DataFrame

def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + ""+ field.name+"" if prefix else ""+field.name+""
        dtype = field.dataType
        Dstr =str(dtype)
        if isinstance(dtype, ArrayType):
            fields.append([name+"§"+"ArrayType"])
            dtype = dtype.elementType
            
        if isinstance(dtype, StructType):
            fields.append([name+ "§"+"StructType"])
            fields += flatten(dtype, prefix=name)
            
        else:
            fields.append([name+"§" +Dstr])

    return fields

def flatter(schema, prefix=None): 
    ColumnList = flatten(schema, prefix)
    cschema = StructType([StructField("org", StringType())])
    rdd = sc.parallelize(ColumnList)
    df = sqlContext.createDataFrame(rdd,cschema)
    split_col = split(df['org'], '§')
    df = df.withColumn('Name', split_col.getItem(0))
    df = df.withColumn('Type', split_col.getItem(1))
    df = df.drop('org')
    return df

# COMMAND ----------

# read the JSON file in a dataframe
filename="DINOBRO_TimeEntities_20200623.json"
JsonFilename ="dbfs:/mnt/GeoUpload/" +filename
JsonDF = (spark.read 
    .option("inferSchema", "true")
    .json(JsonFilename, multiLine=True)
    .withColumn('ImportDateTime',lit(nu))
 )
dfTE= flatter(JsonDF.schema)
dfTE=dfTE.withColumn('import',lit('TE'))
display(dfTE)


# COMMAND ----------

filename="DINOBRO_EntityDescriptions_20200623.json"
JsonFilename ="dbfs:/mnt/GeoUpload/" +filename
JsonDF = (spark.read 
    .option("inferSchema", "true")
    .json(JsonFilename, multiLine=True)
    .withColumn('ImportDateTime',lit(nu))
 )
dfED= flatter(JsonDF.schema)
dfED=dfED.withColumn('import',lit('ED'))
display(dfED)



# COMMAND ----------

filename="DINOBRO_Entities_20200623.json"
JsonFilename ="dbfs:/mnt/GeoUpload/" +filename
JsonDF = (spark.read 
    .option("inferSchema", "true")
    .json(JsonFilename, multiLine=True)
    .withColumn('ImportDateTime',lit(nu))
 )
dfE= flatter(JsonDF.schema)
dfE=dfE.withColumn('import',lit('E'))
display(dfE)


# COMMAND ----------

dfT = dfE.union(dfED).union(dfTE)
display(dfT)

# COMMAND ----------


# generate a list of all the columns
ColumnList = flatten(JsonDF.schema)

print(ColumnList)

cschema = StructType([StructField("org", StringType())])
rdd = sc.parallelize(ColumnList)
df = sqlContext.createDataFrame(rdd,cschema)


display(df)


# COMMAND ----------

print(ColumnList)

# COMMAND ----------

rdd.foreach(print)