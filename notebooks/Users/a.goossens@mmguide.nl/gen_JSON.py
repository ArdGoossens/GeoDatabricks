# Databricks notebook source
# cluster (6.4 (includes Apache Spark 2.4.5, Scala 2.11)) should have maven:
# com.microsoft.azure:spark-mssql-connector:1.0.0
import datetime

from pyspark.sql.functions import lit, expr,concat
from pyspark.sql.functions import explode

from pyspark.sql.functions import to_json, struct, split
from pyspark.sql import functions as F
# exploding , renaming amd EntryId
from pyspark.sql.functions import col
from pyspark.sql.window import Window
#from pyspark.sql import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import desc

from pyspark.sql.types import StructType, ArrayType  ,StructField,StringType, TimestampType
from pandas import DataFrame

Runschema = StructType([
  StructField('StagingTable', StringType(), True),
  StructField('FileName', StringType(), True),
  StructField('Customer', StringType(), True)
  #,StructField('TimeStamp', TimestampType(), True)
  ])


#CHECK IMPORTS FROM PACKAGES NEEDED


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

# SELECTING THE FILE FOR TESTING PURPOSES

filename="DINOBRO_TimeEntities_20200623.json"
#filename="DINOBRO_EntityDescriptions_20200623.json"
#filename="DINOBRO_Entities_20200623.json"
#filename ="SUNFLOWER_Entities_20200616.json"
#filename ="SUNFLOWER_EntityDescriptions_20200616.json"
#filename ="SUNFLOWER_TimeEntities_20200616.json"

Customer ="DINOBRO"
#Customer ="SUNFLOWER"


# COMMAND ----------

# SETTING THE CONNECTION PROPERTIES, SHOULD COME FROM KEYVAULT
StorageSource = "wasbs://"+"archive"+"@"+"storagexxxxmuupl4c6zvywi"+".blob.core.windows.net"
StorageConfig= "fs.azure.account.key."+"storagexxxxmuupl4c6zvywi"+".blob.core.windows.net"
StorageKey = "DlD0gMuSD5Scix9v1SeoDkYdWYTray+gGqbsaZ/lWSTDZahq4VTwCUR2W8rALLWVv6vao4Z7/cT4xkQKOJdcsg=="
JsonFilename ="dbfs:/mnt/GeoUpload/" +filename
jdbcUrl="jdbc:sqlserver://serverxxxxxmuupl4c6zvywi.database.windows.net:1433;database=databasexxxmuupl4c6zvywi;user=Ard@serverxxxxxmuupl4c6zvywi;password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

pushdown_query = "(select  NEWID() as ID) FC"
RunID = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, lowerBound=1, upperBound=100000, numPartitions=100).collect()[0][0]
RunID='Stg_'+RunID.replace('-','')


# COMMAND ----------

#mount the container if not yet mounted
if not any(mount.mountPoint == '/mnt/archive' for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
   source = StorageSource,
   mount_point = "/mnt/archive",
   extra_configs = {StorageConfig: StorageKey }
  )


# COMMAND ----------

# read the JSON file into a dataframe
OriginalDataDF = (spark.read 
    .option("inferSchema", "true")
    .json(JsonFilename, multiLine=True)
 )

# COMMAND ----------

#insering the schema information into a dataframe
SchemaDF= flatter(OriginalDataDF.schema)

# COMMAND ----------

#getting the configured file defintions on a columns level 
pushdown_query = "(select [importid] importid2, [path] path2 , colid from FileColumns where Customer ='{}') FC".format(Customer)
FileDefinitionsDF = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, lowerBound=1, upperBound=100000, numPartitions=100)

# COMMAND ----------

# make a dataframe of the different file defintions
FileDist =FileDefinitionsDF.select(col("importid2").alias('importid')).distinct()

#combine the columns found in the submitted file with all possible file definitions
FileImp=SchemaDF.crossJoin(FileDist).join(FileDefinitionsDF, (SchemaDF.Name == FileDefinitionsDF.path2) & (FileDist.importid == FileDefinitionsDF.importid2), how='full')

#determine all file defnititions that don't fit ( missed item in submitted file or misses item in definition)
FileDel = FileImp.select('importid2').where(col("importid").isNull()).union(FileImp.select('importid').where(col("importid2").isNull())).distinct()
#display(FileDel)

#remove all rows for ill fitting imports
ImpFit = FileImp.where(~(FileImp["importid"].isNull()) & ~(FileImp["importid2"].isNull()))\
.join(FileDel, FileDel.importid2 == FileImp.importid2, how="leftanti")\
.select ('importid2').distinct()

# COMMAND ----------

# if we found exactly ONE file defintion, than we can continue, else abort here
if ImpFit.count()==1: 
  print("matching import definition found!")
else:
  dbutils.notebook.exit("No matching import definition")


# COMMAND ----------

#display(FileImp)
#display(ImpFit)

# COMMAND ----------

# put the filedefinition code into a variable
ImportCode = ImpFit.collect()[0][0]

# remove all filedefinitions except the one we matched on
FileDefinitionsDF= FileDefinitionsDF.where(col('importid2')== ImportCode)

#get the columns that are going to be imported
pushdown_query = "(SELECT [DatasetType],DatasetID,[colId],[fieldtype]  FROM [dbo].[ImportColumns] where importid ='{}') FC".format(ImportCode)
ImportDefinitionDF = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, lowerBound=1, upperBound=100000, numPartitions=100)

# COMMAND ----------

# enrich the imported columns with the path found in the file
ImportDefinitionDF= ImportDefinitionDF.join(FileDefinitionsDF,ImportDefinitionDF.colId==FileDefinitionsDF.colid, how='inner').select(ImportDefinitionDF.DatasetType,ImportDefinitionDF.DatasetID,ImportDefinitionDF.colId,ImportDefinitionDF.fieldtype,FileDefinitionsDF.path2)

# COMMAND ----------

# check if there is an array present that needs to be expoded and put in in a variable
#STILL NEED TO DO ARRAY-IN-ARRAY AND  ARRAY-ALONGSIDE-ARRAY
arrayDF = SchemaDF.join(ImportDefinitionDF, (ImportDefinitionDF.path2.startswith( concat(SchemaDF.Name,lit('.'))))  & (SchemaDF.Type.like('%ArrayType%')), how='inner').select('Name').distinct()
if arrayDF.count()>0:
  ArrayCode = arrayDF.collect()[0][0]
else:
  ArrayCode=""

# COMMAND ----------

# this code extracts all the needed columns out of the original file. The columns are named with the column ID's used in the database.

#prep with empty valiables
PycomTotal =str("")
PycomExplode1 =str("")
PycomExplode2 =str("")
PycomSelect = str("")

# if there is an array, filling in the needed code
if ArrayCode!="":
  PycomExplode1= "explode(F.col('{}')).alias('{}_exploded'),".format(ArrayCode,ArrayCode)  
  PycomExplode2= ".drop('{}_exploded')".format(ArrayCode)

# get the unique columns
ImportColumnsDF = ImportDefinitionDF.select('colId','path2').distinct()
  
# loop though all needed columns and build the select part of the code
for row in ImportColumnsDF.collect():
  colId= str(row.colId)
  path2= str(row.path2)
  line = "col('{}').cast('string').alias('Col{}'),".format(path2,colId)
  # if there in as array, use the exploded object for those columns
  if path2.startswith(ArrayCode):
    line=line.replace("'"+ArrayCode+'.',"'"+ArrayCode+"_exploded.")
  PycomSelect=PycomSelect+line
PycomSelect=   PycomSelect[0:len(PycomSelect)-1] +") "

#building the entire python command
PycomTotal = "ImportDataDF= OriginalDataDF.select({}{}{}".format(PycomExplode1,PycomSelect,PycomExplode2)

exec(PycomTotal)

# COMMAND ----------

#display(ImportDataDF)
#print (PycomTotal)
#display(arrayDF)
#print(ArrayCode)
#arrayDF.count()
arrayDF.collect()[0][0]

# COMMAND ----------

# jdbcUrl="jdbc:sqlserver://serverxxxxxmuupl4c6zvywi.database.windows.net:1433;database=databasexxxmuupl4c6zvywi;user=Ard@serverxxxxxmuupl4c6zvywi;password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


server_name = "jdbc:sqlserver://serverxxxxxmuupl4c6zvywi.database.windows.net:1433"
database_name = "databasexxxmuupl4c6zvywi"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = RunID
table_name2 ='NewRun2'
username = "Ard"
password = "Goossens." # Please specify password here

#RunDF =spark.createDataFrame([(RunID,filename,Customer, datetime.datetime.now())],Runschema)
RunDF =spark.createDataFrame([(RunID,filename,Customer)],Runschema)


# COMMAND ----------

ImportDataDF.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password) \
    .save()


RunDF.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("append") \
    .option("url", url) \
    .option("dbtable", 'NewRun') \
    .option("user", username) \
    .option("password", password) \
    .save()


# COMMAND ----------

