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
#       if isinstance(dtype, ArrayType):
#            fields.append([name+"§"+"ArrayType"])
#            dtype = dtype.elementType
            
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
    return df

# COMMAND ----------

filename="DINOBRO_TimeEntities_20200623.json"
filename="DINOBRO_EntityDescriptions_20200623.json"
filename="DINOBRO_Entities_20200623.json"
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

pushdown_query = "(select [importid] importid2, [path] path2, [type] as type2 from FileColumns where Customer ='{}') FC".format(Customer)

ImpCol = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, lowerBound=1, upperBound=100000, numPartitions=100)
display(ImpCol)


# COMMAND ----------

# distinct ImportId
ImpDist =ImpCol.select(col("importid2").alias('importid')).distinct()
#ImpDist.show()

#combine filecolumns with possible imports
FileImp=StrucDF.crossJoin(ImpDist).join(ImpCol, (StrucDF.Name == ImpCol.path2) & (StrucDF.Type == ImpCol.type2) & (ImpDist.importid == ImpCol.importid2), how='full')
#FileImp.show()

#determine all imports that don't fit
ImpDel = FileImp.select('importid2').where(col("importid").isNull()).union(FileImp.select('importid').where(col("importid2").isNull())).distinct()
#display(ImpDel)

#remove ill fitting imports
ImpFit = FileImp.where(~(FileImp["importid"].isNull()) & ~(FileImp["importid2"].isNull()))\
.join(ImpDel, ImpDel.importid2 == FileImp.importid2, how="leftanti")\
.select ('importid2').distinct()
ImpFit.show()



# COMMAND ----------

if ImpFit.count()==1: 
  print("ok")
else:
  print("not ok")


# COMMAND ----------

#res2=StrucDF.crossJoin(ImpDist).join(ImpCol, (StrucDF.Name == ImpCol.path2) & (StrucDF.Type == ImpCol.type2) & (ImpDist.importid == ImpCol.importid2), how='full')
#res2.show()

# COMMAND ----------

#res = StrucDF.crossJoin(ImpDist)


#res.show()

# COMMAND ----------


#res2=res.join(ImpCol, (res.Name == ImpCol.path2) & (res.Type == ImpCol.type2) & (res.importid == ImpCol.importid2), how='full')
#res2.show()

# COMMAND ----------

#ImpDel = res2.select('importid2').where(col("importid").isNull()).union(res2.select('importid').where(col("importid2").isNull())).distinct()
#display(ImpDel)

# COMMAND ----------

#ImpDel1 = res2.select('importid').where(col("importid2").isNull()).distinct()
#ImpDel2 = res2.select('importid2').where(col("importid").isNull()).distinct()

#ImpDel = ImpDel1.union(ImpDel2).distinct()
#display(ImpDel)

# COMMAND ----------

#res3 = res2.where(~(res2["importid"].isNull()) & ~(res2["importid2"].isNull()))
#res4=  res3.join(ImpDel, (res2.importid==ImpDel.importid), "leftanti")
#res5=res4.select ('importid').distinct()
#res5.show()

# COMMAND ----------

#res5 = res2.where(~(res2["importid"].isNull()) & ~(res2["importid2"].isNull()))\
#.join(ImpDel, (res2.importid==ImpDel.importid), "leftanti")\
#.select ('importid').distinct()
#res5.show()