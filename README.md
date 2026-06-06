<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Spark-FE7A16?style=for-the-badge&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Big%20Data-000000?style=for-the-badge&logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/Kaggle-20BEFF?style=for-the-badge&logo=kaggle&logoColor=white"/>
</p>


<p align="center">
  <img src="https://spark.apache.org/images/spark-logo-trademark.png" alt="Apache Spark Logo" width="200"/>
</p>

# Análise de Dados de E-Commerce com Apache Spark & Python

Este projeto simula um ambiente corporativo real de Big Data. A solução engloba a ingestão automatizada de arquivos CSV massivos de e-commerce, com dataset baixado automaticamente via Kaggle, além de uma arquitetura de pastas previamente estruturada para receber e organizar esses dados. Também contempla a estruturação e otimização das camadas de um Data Lake utilizando Apache Spark e o formato Parquet, e a extração de insights estratégicos de negócio através de queries analíticas escaláveis.

---

## 🎯 Contexto e Desafio de Negócio

Nas empresas modernas, sistemas transacionais e plataformas web geram gigabytes de logs de eventos diariamente. Arquivos brutos no formato CSV são frequentemente utilizados para a transferência inicial desses dados. No entanto, processar estes dados com ferramentas tradicionais que operam estritamente em memória RAM pura (como o Pandas) torna-se inviável devido ao volume massivo de linhas, causando travamentos por estouro de memória (*Out Of Memory*).

Para este projeto, foi utilizada uma base de dados real de comportamento de e-commerce contendo milhões de registros históricos de interações de utilizadores (visualizações, adições ao carrinho e compras).

### Objetivos do Projeto:

- **Ingestão Automatizada:** Download programático direto da API do Kaggle e movimentação automatizada de arquivos no sistema operacional Linux.  
- **Performance e Escalabilidade:** Configuração e inicialização de um ambiente `SparkSession` distribuído localmente maximizando todos os núcleos do processador (`local[*]`).  
- **Otimização com Schema Estático:** Aplicação de tipos estáticos estritos para acelerar a leitura e evitar o overhead de processamento que o Spark gasta para inferir tipos em arquivos gigantes.  
- **Migração de Camadas (Data Lake):** Conversão de dados brutos da camada **Bronze** (CSV) para arquivos de alta performance, compactados e colunares na camada **Silver** (Parquet).  
- **Analytics & Business Intelligence:** Execução de agregações complexas e regras de negócio na camada **Gold**, comparando performance e sintaxe entre **Spark SQL** e **DataFrame API**.  

---

## 🏗️ Arquitetura do Data Lake & Fluxo de Dados

<p align="center"><img src="https://upload.wikimedia.org/wikipedia/commons/7/7c/Kaggle_logo.png" width="120"/></p>

O projeto segue os princípios de engenharia e governança da arquitetura medalhão para o refinamento progressivo dos dados:

```
[ Ingestão via API KaggleHub ]
│
▼
┌──────────────────────────────────────┐
│         CAMADA BRONZE (CSV)         │ -> Dados brutos e massivos armazenados localmente
└──────────────────┬───────────────────┘
│
▼ [ Otimização via Schema Estático & Escrita Parquet ]
┌──────────────────────────────────────┐
│        CAMADA SILVER (Parquet)       │ -> Compactação por coluna e alta performance de leitura
└──────────────────┬───────────────────┘
│
▼ [ Agregações de Negócio / Criação de Views Temporárias ]
┌──────────────────────────────────────┐
│         CAMADA GOLD (Métricas)       │ -> Consultas analíticas prontas para Consumo / BI
└──────────────────┬───────────────────┘
```

---

## 🛠️ Tecnologias Utilizadas

Para garantir a execução do pipeline de ponta a ponta, o ecossistema tecnológico apoia-se em ferramentas líderes do mercado de Big Data:

- **Python (v3.8.10):** Linguagem base para desenvolvimento dos scripts de automação.  
- **Apache Spark (v3.5.1):** Mecanismo de processamento distribuído utilizado para a transformação e agregação de dados em larga escala.  
- **PySpark:** Interface Python para integração rápida com o Spark Core.  
- **KaggleHub (v0.2.9):** API oficial usada para automação do download do dataset de forma programática.  
- **Parquet:** Formato de armazenamento colunar otimizado para consultas analíticas rápidas (OLAP).  

---

## ⚙️ Configuração e Estrutura do Código

### 1. Ingestão e Filtragem de Dados do Sistema Operacional

O pipeline inicializa baixando o dataset completo e isolando apenas a base específica para análise, poupando armazenamento local e otimizando o processamento:

```python
import os
import shutil
import kagglehub

destino_final = "/opt/spark/resources/E-Commerce_Behavior/base"
os.makedirs(destino_final, exist_ok=True)

print("Iniciando o download do dataset do Kaggle...")
path_temporario = kagglehub.dataset_download("egirizkiyansyah44/ecommerce-behavior-data-from-multi-category-store")

arquivo_desejado = "2019-Nov.csv"
caminho_origem = os.path.join(path_temporario, arquivo_desejado)
caminho_destino = os.path.join(destino_final, arquivo_desejado)

if os.path.exists(caminho_origem):
    if os.path.exists(caminho_destino):
        os.remove(caminho_destino)
    shutil.move(caminho_origem, caminho_destino)
    print(f"Sucesso! O arquivo {arquivo_desejado} foi movido para: {destino_final}")
else:
    print(f"Erro: O arquivo {arquivo_desejado} não foi encontrado no cache.")
```

---

### 2. Inicialização da SparkSession Otimizada

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = (
    SparkSession.builder
        .appName("E-Commerce")
        .master("local[*]")   
        .getOrCreate()
)
```

---

### 3. Definição do Schema Estático (Camada Bronze)

```python
schema = T.StructType([
    T.StructField("event_time", T.TimestampType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("product_id", T.LongType(), True),
    T.StructField("category_id", T.LongType(), True),
    T.StructField("category_code", T.StringType(), True),
    T.StructField("brand", T.StringType(), True),
    T.StructField("price", T.FloatType(), True),
    T.StructField("user_id", T.LongType(), True),
    T.StructField("user_session", T.StringType(), True)
])

base_df = (spark.read
           .format("csv")
           .option("header", "true")
           .option("sep", ",")
           .schema(schema)
           .load("/opt/spark/resources/E-Commerce_Behavior/base/2019-Nov.csv"))

base_df.printSchema()
```

---

### 4. Persistência na Camada Silver (Parquet)

```python
(base_df.write.format("parquet")
    .mode("overwrite")
    .save("/opt/spark/resources/E-Commerce_Behavior/base/E-Commerce_parquet")
)
```

---

## 📊 Camada Gold: Análises de Negócio & Insights Extraídos

Na camada Gold, o Spark foi mapeado para criar uma view temporária na memória (`ecommerce_events`), permitindo a execução cruzada de lógicas tanto em Spark SQL quanto em DataFrame API.

---

### 📉 Análise do Funil de Conversão

```sql
SELECT 
    event_type,
    COUNT(*) AS total_eventos,
    COUNT(DISTINCT user_id) AS usuarios_unicos
FROM ecommerce_events
GROUP BY event_type
ORDER BY total_eventos DESC
```

Insight Gerencial: Identificação precisa do volume de perda de utilizadores entre o tráfego do site (view) e a conversão em receita transacional (purchase).

---

### 💰 Desempenho Financeiro por Marca (Top 10)

```sql
SELECT 
    brand,
    COUNT(*) AS quantidade_vendida,
    ROUND(SUM(price), 2) AS faturamento_total,
    ROUND(AVG(price), 2) AS ticket_medio
FROM ecommerce_events
WHERE event_type = 'purchase' AND brand IS NOT NULL
GROUP BY brand
ORDER BY faturamento_total DESC
LIMIT 10
```

Insight Gerencial: Descoberta de marcas líderes em faturação impulsionadas pelo elevado valor de ticket médio por item vendido.

---

### ⏰ Comportamento do Utilizador por Hora do Dia

```sql
SELECT 
    HOUR(event_time) AS hora_do_dia,
    COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS total_visualizacoes,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_compras
FROM ecommerce_events
GROUP BY hora_do_dia
ORDER BY hora_do_dia ASC
```

Insight Gerencial: Cruzamento temporal que revela janelas ideais para campanhas de marketing.

---

### 🏷️ Categorias Mais Desejadas vs. Compradas

```sql
SELECT 
    category_code,
    COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS visualizacoes,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS compras,
    ROUND((COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) * 100.0 / COUNT(CASE WHEN event_type = 'view' THEN 1 END)), 2) AS taxa_conversao_pct
FROM ecommerce_events
WHERE category_code IS NOT NULL
GROUP BY category_code
HAVING visualizacoes > 1000
ORDER BY compras DESC
LIMIT 10
```

---

### 👑 Clientes VIP (Maiores Compradores)

```sql
SELECT 
    user_id,
    COUNT(*) AS total_itens_comprados,
    ROUND(SUM(price), 2) AS total_gasto
FROM ecommerce_events
WHERE event_type = 'purchase'
GROUP BY user_id
ORDER BY total_gasto DESC
LIMIT 10
```

Insight Gerencial: Segmentação de clientes de alto valor para estratégias de fidelização.

---

## 🔒 Boas Práticas e Encerramento

```python
spark.stop()
```

---

## 🚀 Como Executar este Projeto

**Pré-requisitos:**  
Ambiente Linux ou Docker com Java 8+ e Apache Spark configurado.

**Instalação:**

```bash
pip install pyspark kagglehub watermark
```

**Execução:**

Abra o notebook `E-Commerce.ipynb` e execute as células sequencialmente.

---

Desenvolvido com foco no estudo e simulação de arquiteturas escaláveis de Big Data por **André Luiz Colombo** 🧑‍💻
