# Databricks notebook source
import re
from pyspark.sql.types import ArrayType, StringType
import pyspark.sql.functions as f
from pyspark.sql.functions import explode, desc

# COMMAND ----------

#Lectura de archivo JSON
dfTweets = spark.read.format("json").option("multiLine", False).load("dbfs:///FileStore/_latam/farmers_protest_tweets_2021_2_4.json")

# COMMAND ----------

# Explodimos el campo de contenido para obtener los emojis
dfTweets_MentionUser = dfTweets.select(
                dfTweets["content"].alias("Tweet"),
               )

# COMMAND ----------

# Función para extraer todas las menciones de usuario de un texto
def extract_user_mentions(text):
    # Patrón de expresión regular para encontrar todas las menciones de usuario
    mention_pattern = r'@(\w+)'
    return re.findall(mention_pattern, text)

# Registrar la función UDF
extract_user_mentions_udf = udf(extract_user_mentions, ArrayType(StringType()))

# Aplicar la función UDF a la columna "content" para extraer todas las menciones de usuario
df_with_all_mentions = dfTweets_MentionUser.withColumn("all_mentions", extract_user_mentions_udf(f.col("Tweet")))


# COMMAND ----------

df_with_all_mentions.show(20,False)

# COMMAND ----------

# Explode para convertir la lista de emojis en filas individuales
dfExploded = df_with_all_mentions.select(f.explode(df_with_all_mentions["all_mentions"]).alias("username"))

# COMMAND ----------

# Contar las menciones de username, se muestra el top 10 de los usuarios
dfMentionCount = dfExploded.groupBy("username").agg(f.count("*").alias("MentionQuantity")).orderBy(f.col("MentionQuantity").desc()).limit(10)

# COMMAND ----------

dfMentionCount.show()

# COMMAND ----------

########################################################################################################
########################################################################################################
########################################################################################################
########################################################################################################
########################################################################################################

# COMMAND ----------

from typing import List, Tuple
from pyspark.sql.types import ArrayType, StringType
import pyspark.sql.functions as f
import re

# COMMAND ----------

# Función para extraer todas las menciones de usuario de un texto
def extract_user_mentions(text):
    # Patrón de expresión regular para encontrar todas las menciones de usuario
    mention_pattern = r'@(\w+)'
    return re.findall(mention_pattern, text)

# Registrar la función UDF
extract_user_mentions_udf = udf(extract_user_mentions, ArrayType(StringType()))

# COMMAND ----------

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    
    # Lectura de archivo JSON
    dfTweets = spark.read.format("json").option("multiLine", False).load(file_path)

    # Explodimos el campo de contenido para obtener los emojis
    dfTweets_MentionUser = dfTweets.select(
                dfTweets["content"].alias("Tweet"),
               )
    
    # Aplicar la función UDF a la columna "content" para extraer todas las menciones de usuario
    df_with_all_mentions = dfTweets_MentionUser.withColumn("all_mentions", extract_user_mentions_udf(f.col("Tweet")))

    # Explode para convertir la lista de emojis en filas individuales
    dfExploded = df_with_all_mentions.select(f.explode(df_with_all_mentions["all_mentions"]).alias("username"))

    # Contar las menciones de username, se muestra el top 10 de los usuarios
    dfMentionCount = dfExploded.groupBy("username").agg(f.count("*").alias("MentionQuantity")).orderBy(f.col("MentionQuantity").desc()).limit(10)

    # Recolectamos los resultados y los devolvemos como una lista de tuplas
    result = [(row["username"], row["MentionQuantity"]) for row in dfMentionCount.collect()]
   
    return result

# COMMAND ----------

file_path = "dbfs:///FileStore/_latam/farmers_protest_tweets_2021_2_4.json"

# COMMAND ----------

result = q3_time(file_path)
print(result)

# COMMAND ----------

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    
    # Lectura de archivo JSON
    dfTweets = spark.read.format("json").option("multiLine", False).load(file_path)

    # Explodimos el campo de contenido para obtener los emojis
    dfTweets_MentionUser = dfTweets.select(
                dfTweets["content"].alias("Tweet"),
               )
    
    # Aplicar la función UDF a la columna "content" para extraer todas las menciones de usuario
    df_with_all_mentions = dfTweets_MentionUser.withColumn("all_mentions", extract_user_mentions_udf(f.col("Tweet")))

    # Explode para convertir la lista de emojis en filas individuales
    dfExploded = df_with_all_mentions.select(f.explode(df_with_all_mentions["all_mentions"]).alias("username"))

    # Contar las menciones de username, se muestra el top 10 de los usuarios
    dfMentionCount = dfExploded.groupBy("username").agg(f.count("*").alias("MentionQuantity")).orderBy(f.col("MentionQuantity").desc()).limit(10)

    # Recolectamos los resultados y los devolvemos como una lista de tuplas
    result = [(row["username"], row["MentionQuantity"]) for row in dfMentionCount.toLocalIterator()]
   
    return result

# COMMAND ----------

result = q3_memory(file_path)
print(result)
