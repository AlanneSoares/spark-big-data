from flask import Flask, render_template
from pyspark.sql import SparkSession

# Inicializa o Flask
app = Flask(__name__)

# Abrindo uma sessão para uso do Spark
spark = (
    SparkSession.builder
    .master('local[*]')  # Usando todos os núcleos disponíveis
    .appName('credit_card_transactions')
    .getOrCreate()
)

# Habilitar suporte para Arrow (melhora a eficiência)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

@app.route('/')
def index():
    try:
        # Carregando o DataFrame a partir do CSV (ajuste o caminho para o seu arquivo CSV)
        df = spark.read.csv('C:/Users/Darly/Desktop/big-data/qtdCondutoresHabilitadosAgosto2024.csv', header=True, inferSchema=True)

         # Limite opcional de linhas para evitar sobrecarga de memória
        df = df.limit(100)

        # Coleta os dados do Spark DataFrame como uma lista de dicionários
        data = [row.asDict() for row in df.collect()]

        # Obtém os nomes das colunas
        columns = df.columns

        # Renderiza o template HTML com os dados e colunas
        return render_template('index.html', data=data, columns=columns)
    except Exception as e:
        return f'Error: {str(e)}', 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
