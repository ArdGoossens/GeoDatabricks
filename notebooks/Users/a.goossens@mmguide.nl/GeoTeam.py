# Databricks notebook source
# variables
filename ="DINOBRO_Entities_20200623.json"
#filename ="DINOBRO_EntityDescriptions_20200623.json"
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



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

