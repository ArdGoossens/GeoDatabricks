# Databricks notebook source
# MAGIC %scala
# MAGIC val jdbcHostname = "server00000s7qefz5aot56o"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "database000s7qefz5aot56o"
# MAGIC  
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC  
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC  
# MAGIC connectionProperties.put("user", s"Ard")
# MAGIC connectionProperties.put("password", s"Goossens.")
# MAGIC 
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)
# MAGIC 
# MAGIC 
# MAGIC val application_cities = spark.read.jdbc(jdbcUrl, "Application.Cities", connectionProperties)
# MAGIC val application_countries = spark.read.jdbc(jdbcUrl, "Application.Countries", connectionProperties)
# MAGIC val application_stateprovinces = spark.read.jdbc(jdbcUrl, "Application.StateProvinces", connectionProperties)
# MAGIC application_cities.createOrReplaceTempView("Cities")
# MAGIC application_countries.createOrReplaceTempView("Countries")
# MAGIC application_stateprovinces.createOrReplaceTempView("StateProvinces")

# COMMAND ----------

