# Databricks notebook source
pip install emoji

# COMMAND ----------

from pyspark.sql.types import ArrayType, StringType
import pyspark.sql.functions as f
import emoji
import re

# COMMAND ----------

#Lectura de archivo JSON
dfTweets = spark.read.format("json").option("multiLine", False).load("dbfs:///FileStore/_latam/farmers_protest_tweets_2021_2_4.json")

# COMMAND ----------

# Campos content tiene el contenido del tweet y los emojis
dfTweetEmoji = dfTweets.select(
                dfTweets["content"].alias("TweetEmoji"),
               )

# COMMAND ----------

# Definir una función UDF para extraer los emojis de un texto
def extract_emojis(text):
    return re.findall(r'(:[^:\s]+:)', emoji.demojize(text))

# Convertir la función en un UDF de PySpark
extract_emojis_udf = udf(extract_emojis, ArrayType(StringType()))

# COMMAND ----------


# Aplicar el UDF a la columna "Emojitext"
dfTweetEmojiText = dfTweetEmoji.withColumn("EmojiText", extract_emojis_udf(dfTweetEmoji["TweetEmoji"]))


# COMMAND ----------

# Explode para convertir la lista de emojis en filas individuales
dfTweetEmojiExplode = dfTweetEmojiText.select(f.explode(dfTweetEmojiText["EmojiText"]).alias("Emoji"))

# COMMAND ----------

# Contar la frecuencia de cada emoji, y obtenemos los 10 primeros
dfTweetEmojiQuantity = dfTweetEmojiExplode.groupBy("Emoji").agg(f.count("*").alias("EmojiQuantity")).orderBy(f.col("EmojiQuantity").desc()).limit(10)

# COMMAND ----------

# Definir una función UDF para convertir los códigos de emoji a emojis visuales
def visualize_emoji(emoji_code):
    return emoji.emojize(emoji_code)

# Registrar la función UDF
visualize_emoji_udf = udf(visualize_emoji, StringType())

# COMMAND ----------

# Aplicar la función UDF a la columna "Emoji" para obtener los emojis visuales
dfTweetEmojiResult = dfTweetEmojiQuantity.withColumn("VisualEmoji", visualize_emoji_udf(f.col("Emoji")))

# COMMAND ----------

dfTweetEmojiResult.show()

# COMMAND ----------

####################################################################################################
####################################################################################################
####################################################################################################
####################################################################################################
####################################################################################################
####################################################################################################
####################################################################################################

# COMMAND ----------

# Definir una función UDF para extraer los emojis de un texto
def extract_emojis(text):
    return re.findall(r'(:[^:\s]+:)', emoji.demojize(text))

# Convertir la función en un UDF de PySpark
extract_emojis_udf = udf(extract_emojis, ArrayType(StringType()))

##########################################################################################

# Definir una función UDF para convertir los códigos de emoji a emojis visuales
def visualize_emoji(emoji_code):
    return emoji.emojize(emoji_code)

# Registrar la función UDF
visualize_emoji_udf = udf(visualize_emoji, StringType())


# COMMAND ----------

from typing import List, Tuple
from datetime import datetime
from pyspark.sql.types import ArrayType, StringType
import pyspark.sql.functions as f
import emoji
import re

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    
    # Lectura de archivo JSON
    dfTweets = spark.read.format("json").option("multiLine", False).load(file_path)

    # Campos content tiene el contenido del tweet y los emojis
    dfTweetEmoji = dfTweets.select(
                dfTweets["content"].alias("TweetEmoji"),
               )
    
    # Aplicar el UDF a la columna "Emojitext", para identificar los emojis dentro del tweet
    dfTweetEmojiText = dfTweetEmoji.withColumn("EmojiText", extract_emojis_udf(dfTweetEmoji["TweetEmoji"]))

    # Explode para convertir la lista de emojis en filas individuales
    dfTweetEmojiExplode = dfTweetEmojiText.select(f.explode(dfTweetEmojiText["EmojiText"]).alias("Emoji"))

    # Contar la frecuencia de cada emoji, y obtenemos los 10 primeros
    dfTweetEmojiQuantity = dfTweetEmojiExplode.groupBy("Emoji").agg(f.count("*").alias("EmojiQuantity")).orderBy(f.col("EmojiQuantity").desc()).limit(10)

    # Aplicar la función UDF a la columna "Emoji" para obtener los emojis visuales
    dfTweetEmojiResult = dfTweetEmojiQuantity.withColumn("VisualEmoji", visualize_emoji_udf(f.col("Emoji")))

    # Recolectamos los resultados y los devolvemos como una lista de tuplas
    result = [(row["VisualEmoji"], row["EmojiQuantity"]) for row in dfTweetEmojiResult.collect()]
   

    return result

# COMMAND ----------

file_path = "dbfs:///FileStore/_latam/farmers_protest_tweets_2021_2_4.json"

# COMMAND ----------

result = q2_time(file_path)
print(result)

# COMMAND ----------

from typing import List, Tuple
from datetime import datetime
from pyspark.sql.types import ArrayType, StringType
import pyspark.sql.functions as f
import emoji
import re

def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    
    # Lectura de archivo JSON
    dfTweets = spark.read.format("json").option("multiLine", False).load(file_path)

    # Campos content tiene el contenido del tweet y los emojis
    dfTweetEmoji = dfTweets.select(
                dfTweets["content"].alias("TweetEmoji"),
               )
    
    # Aplicar el UDF a la columna "Emojitext", para identificar los emojis dentro del tweet
    dfTweetEmojiText = dfTweetEmoji.withColumn("EmojiText", extract_emojis_udf(dfTweetEmoji["TweetEmoji"]))

    # Explode para convertir la lista de emojis en filas individuales
    dfTweetEmojiExplode = dfTweetEmojiText.select(f.explode(dfTweetEmojiText["EmojiText"]).alias("Emoji"))

    # Contar la frecuencia de cada emoji, y obtenemos los 10 primeros
    dfTweetEmojiQuantity = dfTweetEmojiExplode.groupBy("Emoji").agg(f.count("*").alias("EmojiQuantity")).orderBy(f.col("EmojiQuantity").desc()).limit(10)

    # Aplicar la función UDF a la columna "Emoji" para obtener los emojis visuales
    dfTweetEmojiResult = dfTweetEmojiQuantity.withColumn("VisualEmoji", visualize_emoji_udf(f.col("Emoji")))

    # Recolectamos los resultados y los devolvemos como una lista de tuplas
    result = [(row["VisualEmoji"], row["EmojiQuantity"]) for row in dfTweetEmojiResult.toLocalIterator()]
   

    return result

# COMMAND ----------

result = q2_memory(file_path)
print(result)
