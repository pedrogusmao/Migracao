# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ##Migração Azure SQL -> Databricks -> Delta Lake
# MAGIC                                                    
# MAGIC 
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 10px;">
# MAGIC   <img src="https://www.tapclicks.com/wp-content/uploads/AzureSQL.svg" alt="Databricks Learning" style="width:250px">
# MAGIC 
# MAGIC   <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/6/63/Databricks_Logo.png/220px-Databricks_Logo.png" alt="Databricks Learning" style="width: 250px">
# MAGIC 
# MAGIC  > <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" alt="Databricks Learning" style="width: 250px">
# MAGIC 
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Bibliotecas
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import col
import pytz
from pyspark.sql.functions import lit
from pyspark.sql import Window
from datetime import date
from datetime import datetime
#from datetime import timedelta

# COMMAND ----------

# DBTITLE 1,Varíáveis
# Variáveis de location e de database
delta_path_svr = "/mnt/silver/db_migration/"
delta_path_brz = "/mnt/bronze/db_migration/"
db_svr = 'silver'
db_brz = 'bronze'
# Variáveis para try/except
NOTEBOOK_RESULT = "{'result':{'status': {'statusCode': 'status_code', 'message': 'status_message' }}}"
message = NOTEBOOK_RESULT.replace("status_code","1").replace("status_message","SUCCESFUL")

# COMMAND ----------

# DBTITLE 1,Variáveis p/ Conexão JDBC
# Variáveis p/ conexão JDBC no Azure SQL. Em algumas delas foram usados secrets de Key Vault p/ boas práticas de segurança
jdbcHostname = "server-migration.database.windows.net"
jdbcPort     =  1433
jdbcDatabase = "AdventureWorks"
jdbcUsername = dbutils.secrets.get(scope="azurescope", key="mig-usr-database") 
jdbcPassword = dbutils.secrets.get(scope="azurescope", key="mig-pwd-database")
jdbcUrl      = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)

# COMMAND ----------

# DBTITLE 1,Conexão JDBC
try:
        
  # Conexão JDBC
    jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"
    connectionProperties = {
                            "user"     : jdbcUsername,
                            "password" : jdbcPassword,
                            "driver"   : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                           }
except Exception as ex:
    print(f"ERRO => {ex}")    
    message = NOTEBOOK_RESULT.replace("status_code","0").replace("status_message",str(ex).replace("'","`"))  

# COMMAND ----------

# DBTITLE 1,Query System Tables e System Views
try:
    
  # Query que verifica todas as tables e views do db AdventureWorks
  pushdown_query = """ 
                       (select 
                            s.name + '.' + t.name Tables
                        from sys.tables t 
                         left join sys.schemas s 
                          on t.schema_id = s.schema_id 
                  ------------------------------------------------      
                                      union all 
                  ------------------------------------------------
                        select 
                            s.name + '.' + v.name 
                        from sys.views v 
                         left join sys.schemas s 
                          on v.schema_id = s.schema_id) tbls """

  # Dataframe que armazena a query
  df_stg = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
  
except Exception as ex:
    print(f"ERRO => {ex}")    
    message = NOTEBOOK_RESULT.replace("status_code","0").replace("status_message",str(ex).replace("'","`"))

# COMMAND ----------

# DBTITLE 1,Criação de Tabelas Delta e DDL de Tabelas de Metadados
try:
    # Variável que coleta todos os dados de todas as tables e views do db
    lst_tbls = df_stg.collect()
    
    # Variável que não trará 4 tabelas na migração    
    lst_exc = ["dbo.ErrorLog","dbo.BuildVersion","sys.database_firewall_rules","sys.ipv6_database_firewall_rules"]
        #------------------------------------------------------------------------------------------------------------------#
    
    # Loop p/ se capturar todas as tabelas e views e não trazer as tabelas que estão atribuídas à variável lst_exc    
    for i in lst_tbls:

        if i["Tables"] in lst_exc:
            continue
        # Nome das entidades
        entity = i["Tables"].replace('.','_')
        
        #------------------------------------------------------------------------------------------------------------------#
        
        # Aqui é criada a variável e dataframe que trazem todos os dados de todas as tabelas e views        
        pushdown_query = "(select * from {0}) query".format(i["Tables"])        
        df_fnl = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
        
        #------------------------------------------------------------------------------------------------------------------#
                
        # Novo campo para controle da data de migração das tabelas
        df_fnl = df_fnl.withColumn("MigrationCreationDate",lit(str(datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S.%f'))).cast("timestamp"))
                
        # Aqui é feita a persistência da tabela delta dando overwrite nos dados
        df_fnl.write.format("delta").mode("overwrite").option("mergeSchema", "false").save(delta_path_svr + entity)
        
        # Drop de tabelas caso as mesmas ja existam no db silver
        sql_drop = (f"drop table if exists {db_svr}.{entity}")
        print('=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n',sql_drop)
        spark.sql(sql_drop)
        
        # Criação das tabelas de metadados no db silver do Databricks
        sql_create= (f" create table {db_svr}.{entity} using delta location '{delta_path_svr}{entity}'")
        print(sql_create,'\n Tabela Criada com Sucesso!')
        spark.sql(sql_create)
        
        #------------------------------------------------------------------------------------------------------------------#
        
        # Aqui é feito o cast do tipo de dado de todas as colunas de cada tabela para string
        # Devido a ingestão ser na camada bronze neste momento
        # Geralmente na camada bronze trazemos os dados brutos como texto        
        for i in df_fnl.columns:
            df_fnl = df_fnl.withColumn(i,col(i).cast(StringType()))
        
        #------------------------------------------------------------------------------------------------------------------#
        
        # Novo campo para controle da data de migração das tabelas     
        df_fnl = df_fnl.withColumn("MigrationCreationDate",lit(str(datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S.%f'))).cast("string"))
                        
        # Aqui é feita a persistência da tabela delta dando overwrite nos dados
        df_fnl.write.format("delta").mode("overwrite").option("mergeSchema", "false").save(delta_path_brz + entity)
                         
        print(' ')
        # Drop de tabelas caso as mesmas ja existam no db silver
        sql_drop = (f" drop table if exists {db_brz}.{entity}")
        print(sql_drop)
        spark.sql(sql_drop)
        
        # Criação das tabelas de metadados no db silver do Databricks
        sql_create = (f" create table {db_brz}.{entity} using delta location '{delta_path_brz}{entity}'")
        print(sql_create,'\n Tabela Criada com Sucesso!')
        spark.sql(sql_create)
        
       #------------------------------------------------------------------------------------------------------------------#
    print('                           =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=*MIGRAÇÃO CONCLUÍDA COM SUCESSO!*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=')
    
except Exception as ex:
    print(f"ERRO => {ex}")    
    message = NOTEBOOK_RESULT.replace("status_code","0").replace("status_message",str(ex).replace("'","`"))    

# COMMAND ----------

# DBTITLE 1,Remoção de todos os blocos de dataframes da memória e do disco
try:
  
  df_stg.unpersist()
  del df_stg
    
  df_fnl.unpersist()
  del df_fnl
  
except Exception as ex:
    print(f"Waring => {ex}")  
    pass

# COMMAND ----------

# DBTITLE 1,Saída do Notebook
dbutils.notebook.exit(message)
