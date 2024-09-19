from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
#from pyspark.sql.types import *

# Abrindo uma sessão para uso do spark
spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('Auto Escola Direção Certa')
    .getOrCreate()
)

df = spark.read.csv('app/resources/data/qtdCondutoresHabilitadosAgosto2024.csv', header=True, inferSchema=True)
df.show()
