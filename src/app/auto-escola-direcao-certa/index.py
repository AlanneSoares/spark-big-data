from flask import Flask, request, render_template
from pyspark.sql import SparkSession

# Inicializa o Flask
app = Flask(__name__)

# Abrindo uma sessão para uso do Spark
spark = (
    SparkSession.builder
    .master('local[*]')  # Usando todos os núcleos disponíveis
    .appName('Auto Escola Direção Certa')
    .getOrCreate()
    .getActiveSession()
)

# Habilitar suporte para Arrow (melhora a eficiência)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

@app.route('/')
def index():
    try:
        # Carregando o DataFrame a partir do CSV (ajuste o caminho para o seu arquivo CSV)
        df = spark.read.csv('qtd_condutores_habilitados_agosto_2024.csv', header=True, inferSchema=True)

        # Lendo arquivo a partir do HDFS
        #file = "hdfs://localhost:9000/data/qtd_condutores_habilitados_agosto_2024.csv"
        #df = spark.read.format("csv").option("header", "true").load(file)

         # Limite opcional de linhas para evitar sobrecarga de memória
        df = df.limit(50)

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
