## Descrição do Projeto

Este projeto processa arquivos `.parquet` para gerar arquivos CSV unificados contendo dados de ocorrências. O código utiliza PySpark para manipulação eficiente de dados e Pandas para operações finais de leitura e escrita. Os arquivos são divididos em partes menores, processados e combinados para formar arquivos finais que são salvos tanto em formato CSV quanto `.parquet`.

## Funcionamento do Código

### Importação das Bibliotecas

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, count, when, col, trim
import pandas as pd
import os
import glob
```

As bibliotecas `pyspark.sql` são importadas para manipulação de dados com PySpark, `pandas` para operações de dados, `os` para manipulações no sistema de arquivos, e `glob` para encontrar arquivos correspondentes a um padrão específico.

### Definição de Caminhos

```python
Arquivo_csv_unificado = r"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNLE\\Ocorrencias_UNLE.csv"
arquivo_saida_parquet = r"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNLE\\Ocorrencias_UNLE.parquet"
```

Os caminhos para o arquivo CSV unificado e o arquivo `.parquet` de saída são definidos.

### Inicialização da Sessão Spark

```python
spark = SparkSession.builder.config('spark.driver.memory', '8g').getOrCreate()
```

A sessão Spark é inicializada com 8 GB de memória alocada.

### Carregamento do Arquivo Parquet

```python
df = spark.read.parquet(r"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\historico_UNLE.parquet")
```

O arquivo `.parquet` existente é carregado em um DataFrame do Spark.

### Seleção e Limpeza de Colunas

```python
df = df.select("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "MatriculaClienteImovel", "DataHoraServico", "Ocorrencia01", "Ocorrencia02")

for column in df.columns:
    df = df.withColumn(column, trim(df[column]))

df = df.withColumn("DataHoraServico", to_date(df["DataHoraServico"]))
df = df.orderBy("DataHoraServico")
```

As colunas necessárias são selecionadas e os espaços em branco são removidos. A coluna `DataHoraServico` é convertida para o formato de data e o DataFrame é ordenado por esta coluna.

### Criação da Coluna de Ocorrências

```python
df = df.withColumn("Ocorrencia", 
                   F.when(F.col("Ocorrencia01") > 0, F.col("Ocorrencia01"))
                    .otherwise(F.when((F.col("Ocorrencia02") > 0) & (F.col("Ocorrencia01") == 0), F.col("Ocorrencia02"))
                                .otherwise(0)))

df = df.select("Unidade", "Gerencia", "Grupo", "Localidade", "NomeFuncionario", "MatriculaClienteImovel", "DataHoraServico", "Ocorrencia")
df = df.filter(df["Ocorrencia"] != 0)
```

A coluna `Ocorrencia` é criada com base nas condições especificadas. Apenas as linhas com valores diferentes de zero na coluna `Ocorrencia` são mantidas.

### Divisão e Salvamento de Partes do DataFrame

```python
num_parts = 10
df_parts = df.randomSplit([1.0] * num_parts)
for i, df_part in enumerate(df_parts):
    df_part.toPandas().to_csv(f"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNLE\\TEMP\\Ocorrencias_UNLE_part{i}.csv", sep=';', index=False, mode='w')
```

O DataFrame é dividido em 10 partes e cada parte é salva como um arquivo CSV separado.

### Leitura e Unificação dos Arquivos CSV

```python
df_pandas = pd.concat([pd.read_csv(f"C:\\Files\\DADOS LEITURAS TEMPO REAL\\BASES\\Dados_historico\\UNLE\\TEMP\\Ocorrencias_UNLE_part{i}.csv", sep=';') for i in range(num_parts)])
df_pandas.to_csv(Arquivo_csv_unificado, sep=';', index=False, mode='w')
```

Os arquivos CSV gerados são lidos e concatenados em um único DataFrame do Pandas, que é então salvo como um arquivo CSV unificado.

### Conversão para Formato Parquet

```python
df = pd.read_csv(Arquivo_csv_unificado, delimiter=";")
df.to_parquet(arquivo_saida_parquet, engine='pyarrow')
os.remove(Arquivo_csv_unificado)
```

O arquivo CSV unificado é lido e convertido para o formato `.parquet`. O arquivo CSV unificado é então removido.

### Limpeza de Arquivos Temporários

```python
files = glob.glob(folder_path + '\\*')
for file in files:
    os.remove(file)
```

Todos os arquivos temporários na pasta especificada são removidos.

### Repetição para UNMT

O mesmo processo é repetido para os arquivos na pasta UNMT, com os caminhos e variáveis ajustados adequadamente.

## Como Executar

1. Certifique-se de que os arquivos `.parquet` e as pastas especificadas existem.
2. Atualize os caminhos das pastas e dos arquivos `.parquet`, se necessário.
3. Execute o script em um ambiente Python com PySpark, Pandas e PyArrow instalados.
4. Verifique os arquivos `.parquet` e CSV gerados nos diretórios de destino.

## Requisitos

- Python 3.x
- PySpark
- Pandas
- PyArrow (para manipulação de arquivos `.parquet`)

```sh
pip install pyspark pandas pyarrow
```

## Contribuições

Sinta-se à vontade para contribuir com melhorias para este projeto. Abra um pull request ou reporte um problema no repositório.

---

Espero que isso ajude a entender o funcionamento do código e como ele pode ser usado para processar e atualizar dados de ocorrências em tempo real!
