# Databricks notebook source
import datetime
from pyspark.sql.functions import lit, expr
from pyspark.sql.functions import explode

from pyspark.sql.functions import to_json, struct, split
from pyspark.sql import functions as F
# exploding , renaming amd EntryId
from pyspark.sql.functions import col
from pyspark.sql.window import Window
#from pyspark.sql import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import desc

from pyspark.sql.types import StructType, ArrayType  ,StructField,StringType
from pandas import DataFrame



# COMMAND ----------

# get a flat(!) list of all the qualified(!) columns in the JSON file
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
# get a dataframe of all the qualified(!) columns in the JSON file
def flatter(schema, prefix=None): 
    ColumnList = flatten(schema, prefix)
    cschema = StructType([StructField("org", StringType())])
    rdd = sc.parallelize(ColumnList)
    df = sqlContext.createDataFrame(rdd,cschema)
    split_col = split(df['org'], '§')
    df = df.withColumn('Name', split_col.getItem(0))
    df = df.withColumn('Type', split_col.getItem(1))
    df = df.drop('org')
    df = df.dropDuplicates(['name'])
    return df

# COMMAND ----------

filename="DINOBRO_TimeEntities_20200623.json"
#filename="DINOBRO_EntityDescriptions_20200623.json"
#filename="DINOBRO_Entities_20200623.json"
Customer ="DINOBRO"
nu= datetime.datetime.now()


# COMMAND ----------

StorageSource = "wasbs://"+"archive"+"@"+"storagexxxxmuupl4c6zvywi"+".blob.core.windows.net"
StorageConfig= "fs.azure.account.key."+"storagexxxxmuupl4c6zvywi"+".blob.core.windows.net"
StorageKey = "DlD0gMuSD5Scix9v1SeoDkYdWYTray+gGqbsaZ/lWSTDZahq4VTwCUR2W8rALLWVv6vao4Z7/cT4xkQKOJdcsg=="
JsonFilename ="dbfs:/mnt/GeoUpload/" +filename


# COMMAND ----------

#mount the container if not yet mounted

if not any(mount.mountPoint == '/mnt/archive' for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
   source = StorageSource,
   mount_point = "/mnt/archive",
   extra_configs = {StorageConfig: StorageKey }
  )


# COMMAND ----------

# read the JSON file in a dataframe
JsonDF = (spark.read 
    .option("inferSchema", "true")
    .json(JsonFilename, multiLine=True)
    .withColumn('ImportDateTime',lit(nu))
 )
StrucDF= flatter(JsonDF.schema)
display(StrucDF)


# COMMAND ----------

jdbcUrl ="jdbc:sqlserver://serverxxxxxmuupl4c6zvywi.database.windows.net:1433;database=databasexxxmuupl4c6zvywi;user=Ard@serverxxxxxmuupl4c6zvywi;password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

pushdown_query = "(select [importid] importid2, [path] path2 , colid from FileColumns where Customer ='{}') FC".format(Customer)

FileCol = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, lowerBound=1, upperBound=100000, numPartitions=100)
display(FileCol)




# COMMAND ----------

# distinct ImportId
FileDist =FileCol.select(col("importid2").alias('importid')).distinct()
#FileDist.show()

#combine filecolumns with possible imports
FileImp=StrucDF.crossJoin(FileDist).join(FileCol, (StrucDF.Name == FileCol.path2) & (FileDist.importid == FileCol.importid2), how='full')
#FileImp.show()

#determine all imports that don't fit
FileDel = FileImp.select('importid2').where(col("importid").isNull()).union(FileImp.select('importid').where(col("importid2").isNull())).distinct()
#display(FileDel)

#remove ill fitting imports
ImpFit = FileImp.where(~(FileImp["importid"].isNull()) & ~(FileImp["importid2"].isNull()))\
.join(FileDel, FileDel.importid2 == FileImp.importid2, how="leftanti")\
.select ('importid2').distinct()
ImpFit.show()



# COMMAND ----------

if ImpFit.count()==1: 
  print("matching import definition found!")
else:
  dbutils.notebook.exit("No matching import definition")


# COMMAND ----------

ImportCode = ImpFit.collect()[0][0]
#print (ImportCode)

FileCol= FileCol.where(col('importid2')== ImportCode)
#display(FileCol)

pushdown_query = "(SELECT [recordtype],[colId],[fieldtype]  FROM [dbo].[ImportColumns] where importid ='{}') FC".format(ImportCode)

ImportCol = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, lowerBound=1, upperBound=100000, numPartitions=100)
display(ImportCol)

# COMMAND ----------

ImportCol= ImportCol.join(FileCol,ImportCol.colId==FileCol.colid, how='inner').select(ImportCol.recordtype,ImportCol.colId,ImportCol.fieldtype,FileCol.path2)

# COMMAND ----------

display(ImportCol)

# COMMAND ----------

StrucDF2=StrucDF.withColumn('point',concat('Name' ,lit('.')))

display(StrucDF2)






# COMMAND ----------

#display(ImportCol.join(StrucDF, StrucDF.Name.contains(ImportCol.path2), how='left'))
#display(StrucDF.join(ImportCol, ImportCol.path2.contains(StrucDF.Name), how='inner'))


#display(StrucDF.join(ImportCol, StrucDF.Name.substring(1,F.length('path2')) == ImportCol.path2 , how='inner'))

#display(StrucDF.join(ImportCol, StrucDF.Name.substr(1,F.length(StrucDF.Name).toint()) == ImportCol.path2.substr(1,F.length(ImportCol.path2)) , how='inner'))

arrayDF = StrucDF.join(ImportCol, (ImportCol.path2.startswith(concat(StrucDF.Name,lit('.'))))  & (StrucDF.Type.like('%ArrayType%')), how='inner').select('Name').distinct()
ArrayCode=""
ArrayCode = arrayDF.collect()[0][0]
print (ArrayCode)


# COMMAND ----------

PycomTotal =str("")
PycomExplode1 =str("")
PycomExplode2 =str("")
PycomSelect = str("")


if ArrayCode!="":
  PycomExplode1= "explode(F.col('{}')).alias('{}_exploded'),".format(ArrayCode,ArrayCode)
  
  PycomExplode2= ".drop('{}_exploded')".format(ArrayCode)

for row in ImportCol.collect():
  recordtype= str(row.recordtype)
  colId= str(row.colId)
  fieldtype= str(row.fieldtype)
  path2= str(row.path2)
  line = "col('{}').alias('Col{}'),".format(path2,colId)
  if path2.startswith(ArrayCode):
    line=line.replace("'"+ArrayCode+'.',"'"+ArrayCode+"_exploded.")
  PycomSelect=PycomSelect+line

PycomSelect=PycomSelect + "lit(nu).alias('ImportDateTime'))  "

PycomTotal = "DFq= JsonDF.select({}{}{}".format(PycomExplode1,PycomSelect,PycomExplode2)
# print (PycomExplode1)
# print (PycomSelect)
# print (PycomExplode2)

print (PycomTotal)
        # cmd ='DFq= JsonDF.select(\
        #  explode(F.col("TimeSerieDtos")).alias("TimeSerieDtos_exploded"),\
        # col("EntityExternalId").alias("EntityName"),col("TimeSerieDtos_exploded.Time").alias("StartDate"),col("TimeResolution").alias("TimeResolution"),
        # col("TimeSerieDtos_exploded.Tags").alias("Tags"),col("TimeSerieDtos_exploded.Value").alias("Value"))\
        #  .withColumn("ImportDateTime",lit(nu))\
        #  .drop("TimeSerieDtos_exploded")\


# COMMAND ----------

DFq= JsonDF.select(explode(col('TimeSerieDtos')).alias('TimeSerieDtos_exploded'),col('EntityExternalId').alias('Col53'),col('TimeResolution').alias('Col56'),col('TimeSerieDtos_exploded.Tags').alias('Col59'),col('TimeSerieDtos_exploded.Time').alias('Col63'),col('TimeSerieDtos_exploded.Value').alias('Col65'),lit(nu).alias('ImportDateTime')).drop('TimeSerieDtos_exploded')

display(DFq)

# COMMAND ----------

cols = list(set(JsonDF.columns) - {'TimeSerieDtos'})

display(JsonDF.select(\
explode(F.col("TimeSerieDtos")).alias("TimeSerieDtos_exploded"))
  .drop("TimeSerieDtos")\
  )


# COMMAND ----------

df=spark.createDataFrame([("abcdff",4),("dlaldajfa",3)],["valuetext","Glength"])
from pyspark.sql.functions import *
df.withColumn("vx",expr("substring(valuetext,0,Glength)")).show()

df.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

