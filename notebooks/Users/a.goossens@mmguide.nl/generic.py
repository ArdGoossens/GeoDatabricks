# Databricks notebook source
import datetime



# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
# MAGIC apt-get update
# MAGIC ACCEPT_EULA=Y apt-get install msodbcsql17
# MAGIC apt-get -y install unixodbc-dev
# MAGIC sudo apt-get install python3-pip -y
# MAGIC pip3 install --upgrade pyodbc

# COMMAND ----------

filename=""
Filetype=""
Customer=""




nu= datetime.datetime.now()

# COMMAND ----------

#key vault secrets
Servername = dbutils.secrets.get(scope = "keyvault", key = "ServerName")
DatabaseName = dbutils.secrets.get(scope = "keyvault", key = "DatabaseName")
StorageName = dbutils.secrets.get(scope = "keyvault", key = "StorageName")
StorageKey = dbutils.secrets.get(scope = "keyvault", key = "StorageKey")
administratorLogin = dbutils.secrets.get(scope = "keyvault", key = "administratorLogin")
administratorLoginPassword = dbutils.secrets.get(scope = "keyvault", key = "administratorLoginPassword")

#derived strings
jdbc_ConnectionString = "jdbc:sqlserver://"+Servername+".database.windows.net:1433;database="+DatabaseName+";user=Ard@"+Servername+";password=Goossens.;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
odbc_ConnectionString ="DRIVER={ODBC Driver 17 for SQL Server};SERVER="+Servername+".database.windows.net;DATABASE="+DatabaseName+";UID="+administratorLogin+";PWD="+administratorLoginPassword
StorageSource = "wasbs://uploads@"+StorageName+".blob.core.windows.net"
StorageConfig= "fs.azure.account.key."+StorageName+".blob.core.windows.net"
JsonFilename ="dbfs:/mnt/GeoUpload/" +filename