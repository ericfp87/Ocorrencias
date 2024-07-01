from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, count, when, col, trim
import pandas as pd
import os
import glob


Arquivo_csv_unificado = r"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNLE\\Ocorrencias_UNLE.csv"
arquivo_saida_parquet = r"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNLE\\Ocorrencias_UNLE.parquet"

# Inicializando a sessão Spark
spark = SparkSession.builder.config('spark.driver.memory', '8g').getOrCreate()

# Carregando o arquivo parquet
df = spark.read.parquet(r"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\historico_UNLE.parquet")

folder_path = r"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNLE\\TEMP"

# Selecionando as colunas necessárias
df = df.select("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "MatriculaClienteImovel", "DataHoraServico", "Ocorrencia01", "Ocorrencia02")

# Removendo espaços em branco de cada coluna
for column in df.columns:
    df = df.withColumn(column, trim(df[column]))

# Convertendo a coluna "DataHoraServico" para data
df = df.withColumn("DataHoraServico", to_date(df["DataHoraServico"]))

# Ordenando o DataFrame pela coluna "DataHoraServico"
df = df.orderBy("DataHoraServico")


# Criando a nova coluna "Ocorrencia" de acordo com as condições especificadas
df = df.withColumn("Ocorrencia", 
                   F.when(F.col("Ocorrencia01") > 0, F.col("Ocorrencia01"))
                    .otherwise(F.when((F.col("Ocorrencia02") > 0) & (F.col("Ocorrencia01") == 0), F.col("Ocorrencia02"))
                                .otherwise(0)))


df = df.select("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "MatriculaClienteImovel", "DataHoraServico", "Ocorrencia")

df = df.filter(df["Ocorrencia"] != 0)

# Dividindo o DataFrame em partes e salvando cada parte como um arquivo .csv
num_parts = 10
df_parts = df.randomSplit([1.0] * num_parts)
for i, df_part in enumerate(df_parts):
    df_part.toPandas().to_csv(f"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNLE\\TEMP\\Ocorrencias_UNLE_part{i}.csv", sep=';', index=False, mode='w')

# Lendo todos os arquivos .csv e concatenando-os em um único DataFrame do Pandas
df_pandas = pd.concat([pd.read_csv(f"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNLE\\TEMP\\Ocorrencias_UNLE_part{i}.csv", sep=';') for i in range(num_parts)])

# Salvando o DataFrame do Pandas como um arquivo .csv com overwrite
df_pandas.to_csv(Arquivo_csv_unificado, sep=';', index=False, mode='w')

df = pd.read_csv(Arquivo_csv_unificado, delimiter=";")
df.to_parquet(arquivo_saida_parquet, engine='pyarrow')
os.remove(Arquivo_csv_unificado)

files = glob.glob(folder_path + '\\*')

# Itere sobre a lista de caminhos de arquivo e exclua cada arquivo
for file in files:
    os.remove(file)

print("UNLE Concluído")



Arquivo_csv_unificado = r"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNMT\\Ocorrencias_UNMT.csv"
arquivo_saida_parquet = r"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNMT\\Ocorrencias_UNMT.parquet"

# Carregando o arquivo parquet
df = spark.read.parquet(r"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\historico_UNMT.parquet")

folder_path = r"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNMT\\TEMP"

# Selecionando as colunas necessárias
df = df.select("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "MatriculaClienteImovel", "DataHoraServico", "Ocorrencia01", "Ocorrencia02")

# Removendo espaços em branco de cada coluna
for column in df.columns:
    df = df.withColumn(column, trim(df[column]))

# Convertendo a coluna "DataHoraServico" para data
df = df.withColumn("DataHoraServico", to_date(df["DataHoraServico"]))

# Ordenando o DataFrame pela coluna "DataHoraServico"
df = df.orderBy("DataHoraServico")


# Criando a nova coluna "Ocorrencia" de acordo com as condições especificadas
df = df.withColumn("Ocorrencia", 
                   F.when(F.col("Ocorrencia01") > 0, F.col("Ocorrencia01"))
                    .otherwise(F.when((F.col("Ocorrencia02") > 0) & (F.col("Ocorrencia01") == 0), F.col("Ocorrencia02"))
                                .otherwise(0)))

df = df.select("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "MatriculaClienteImovel", "DataHoraServico", "Ocorrencia")

df = df.filter(df["Ocorrencia"] != 0)

# Dividindo o DataFrame em partes e salvando cada parte como um arquivo .csv
num_parts = 10
df_parts = df.randomSplit([1.0] * num_parts)
for i, df_part in enumerate(df_parts):
    df_part.toPandas().to_csv(f"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNMT\\TEMP\\Ocorrencias_UNMT_part{i}.csv", sep=';', index=False, mode='w')

# Lendo todos os arquivos .csv e concatenando-os em um único DataFrame do Pandas
df_pandas = pd.concat([pd.read_csv(f"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNMT\\TEMP\\Ocorrencias_UNMT_part{i}.csv", sep=';') for i in range(num_parts)])

# Salvando o DataFrame do Pandas como um arquivo .csv com overwrite
df_pandas.to_csv(Arquivo_csv_unificado, sep=';', index=False, mode='w')

df = pd.read_csv(Arquivo_csv_unificado, delimiter=";")
df.to_parquet(arquivo_saida_parquet, engine='pyarrow')
os.remove(Arquivo_csv_unificado)

files = glob.glob(folder_path + '\\*')

# Itere sobre a lista de caminhos de arquivo e exclua cada arquivo
for file in files:
    os.remove(file)

print("UNMT Concluído")
