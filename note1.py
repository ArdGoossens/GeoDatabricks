# Databricks notebook source
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