from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Abrindo uma sessão para uso do spark
spark = (
    SparkSession.builder
    .master('local')
    .appName('example01')
    .getOrCreate()
)

#Chamando o Data Frame baixado do Kaggle
df = spark.read.csv('/resources/credit_card_transactions.csv', header=True, inferSchema=True)

# Exibindo a tabela
df.show(5)
