# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

## CHALLENGE 1

# COMMAND ----------

#Lectura de archivo JSON
dfTweets = spark.read.format("json").option("multiLine", False).load("dbfs:///FileStore/_latam/farmers_protest_tweets_2021_2_4.json")

# COMMAND ----------

dfTweets.show(20, False)

# COMMAND ----------

#Mostramos el esquema de metadatos
dfTweets.printSchema()

# COMMAND ----------

#Selecciono los campos necesarios
dfTweetsColumns = dfTweets.select(
  f.date_format(dfTweets["date"], "yyyy-MM-dd").alias("date"),
  dfTweets["user.username"]
)

#Mostrar data
dfTweetsColumns.show(20, False)

# COMMAND ----------

dfTweetsColumns.printSchema()

# COMMAND ----------

# Agrupar por fecha y contar el número de tweets por fecha
dfTweetsQuantityDateUser = dfTweetsColumns.groupBy("date","username").agg(f.count("*").alias("TweetsQuantity"))

# COMMAND ----------

dfTweetsQuantityDateUser.show()

# COMMAND ----------

# Encontrar las 10 primeras fechas donde se dieron más ventas
dfTop10Dates = dfTweetsQuantityDateUser.groupBy("date").agg(f.sum("TweetsQuantity").alias("TweetsQuantity")).orderBy(f.col("TweetsQuantity").desc()).limit(10)

# COMMAND ----------

dfTop10Dates.show()

# COMMAND ----------

dfTweetsTop10DateUser = dfTop10Dates.join(
    dfTweetsQuantityDateUser,
    dfTop10Dates["date"] == dfTweetsQuantityDateUser["date"],
    "inner"
).groupBy(dfTop10Dates["date"]).agg(
    f.first("username").alias("username"),  
    f.max(dfTweetsQuantityDateUser["TweetsQuantity"]).alias("TweetsQuantity")
)

# COMMAND ----------

dfTweetsTop10DateUser.show()

# COMMAND ----------

###########################################################################################################
###########################################################################################################
###########################################################################################################
###########################################################################################################
###########################################################################################################
###########################################################################################################
###########################################################################################################
###########################################################################################################
###########################################################################################################
###########################################################################################################
###########################################################################################################
###########################################################################################################

# COMMAND ----------

from typing import List, Tuple
from datetime import datetime
import pyspark.sql.functions as f

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    
    # Lectura de archivo JSON
    dfTweets = spark.read.format("json").option("multiLine", False).load(file_path)

    # Selecciono los campos necesarios, para el ejercicio 1, solo necesitamos date y username
    dfTweetsColumns = dfTweets.select(
        f.to_date(dfTweets["date"]).alias("date"),
        dfTweets["user.username"].alias("username")
    )

    # Contar el Numero de Tweets, agrupando fecha y usuario
    dfTweetsQuantityDateUser = dfTweetsColumns.groupBy("date", "username").agg(f.count("*").alias("TweetsQuantity"))

    # Top 10 de las fechas donde se tiene mas tweets
    dfTop10Dates = dfTweetsQuantityDateUser.groupBy("date").agg(f.sum("TweetsQuantity").alias("TweetsQuantity")).orderBy(f.col("TweetsQuantity").desc()).limit(10)

    # Unimos el top 10 de las fechas con el usuario que realizó mas tweets dentro de esas fechas.
    dfTweetsTop10DateUser = dfTop10Dates.join(
        dfTweetsQuantityDateUser,
        dfTop10Dates["date"] == dfTweetsQuantityDateUser["date"],
        "inner"
    ).groupBy(dfTop10Dates["date"]).agg(
        f.first("username").alias("username"),
        f.max(dfTweetsQuantityDateUser["TweetsQuantity"]).alias("TweetsQuantity")
    )

    # Recolectamos los resultados y los devolvemos como una lista de tuplas
    result = [(row["date"], row["username"]) for row in dfTweetsTop10DateUser.collect()]
   

    return result

# COMMAND ----------

file_path = "dbfs:///FileStore/_latam/farmers_protest_tweets_2021_2_4.json"

# COMMAND ----------

result = q1_time(file_path)
print(result)

# COMMAND ----------

######################

# COMMAND ----------

from typing import List, Tuple
from datetime import datetime
import pyspark.sql.functions as f

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    
    # Lectura de archivo JSON
    dfTweets = spark.read.format("json").option("multiLine", False).load(file_path)

    # Selecciono los campos necesarios, para el ejercicio 1, solo necesitamos date y username
    dfTweetsColumns = dfTweets.select(
        f.to_date(dfTweets["date"]).alias("date"),
        dfTweets["user.username"].alias("username")
    )

    # Contar el Numero de Tweets, agrupando fecha y usuario
    dfTweetsQuantityDateUser = dfTweetsColumns.groupBy("date", "username").agg(f.count("*").alias("TweetsQuantity"))

    # Top 10 de las fechas donde se tiene mas tweets
    dfTop10Dates = dfTweetsQuantityDateUser.groupBy("date").agg(f.sum("TweetsQuantity").alias("TweetsQuantity")).orderBy(f.col("TweetsQuantity").desc()).limit(10)

    # Unimos el top 10 de las fechas con el usuario que realizó mas tweets dentro de esas fechas.
    dfTweetsTop10DateUser = dfTop10Dates.join(
        dfTweetsQuantityDateUser,
        dfTop10Dates["date"] == dfTweetsQuantityDateUser["date"],
        "inner"
    ).groupBy(dfTop10Dates["date"]).agg(
        f.first("username").alias("username"),
        f.max(dfTweetsQuantityDateUser["TweetsQuantity"]).alias("TweetsQuantity")
    )

    # Recolectamos los resultados y los devolvemos como una lista de tuplas
    result = [(row["date"], row["username"]) for row in dfTweetsTop10DateUser.toLocalIterator()]
   

    return result

# COMMAND ----------

result = q1_memory(file_path)
print(result)


