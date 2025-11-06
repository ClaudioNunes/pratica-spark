# Atividade Prática: PySpark com Python e Docker

## Informações Gerais

**Público-alvo:** Alunos de graduação em Ciência de Dados  
**Temática:** Infraestrutura para projetos de Big Data com Apache Spark  
**Nível:** Intermediário

---

## Objetivos de Aprendizagem

Ao final desta atividade, você será capaz de:

1. Compreender a arquitetura e conceitos fundamentais do Apache Spark
2. Implementar transformações e ações em PySpark
3. Processar dados estruturados e não estruturados usando DataFrames e RDDs
4. Containerizar aplicações Spark usando Docker
5. Aplicar operações de análise de dados em larga escala
6. Comparar o paradigma Spark com MapReduce tradicional

---

## Pré-requisitos

- Conhecimento básico de Python
- Familiaridade com linha de comando (terminal/bash)
- Conceitos básicos de SQL (desejável)
- Conta no GitHub (gratuita)
- Conta no Docker Hub (gratuita)
- Navegador web moderno

---

## Recursos Necessários

Todos os recursos podem ser acessados online gratuitamente:

- **GitHub Codespaces** (ambiente de desenvolvimento na nuvem)
- **Play with Docker** (https://labs.play-with-docker.com/) - Ambiente Docker online
- **Dataset**: Dados de vendas de e-commerce para análise

---

## Parte 1: Fundamentos do Apache Spark

### 1.1 O que é Apache Spark?

Apache Spark é um framework de processamento de dados distribuído de código aberto, projetado para ser **rápido**, **escalável** e **fácil de usar**. Foi desenvolvido na UC Berkeley em 2009 e se tornou um projeto Apache em 2013.

#### Principais Características:

- **Velocidade**: Até 100x mais rápido que MapReduce (processamento em memória)
- **Facilidade de uso**: APIs em Python, Scala, Java, R e SQL
- **Generalidade**: Suporta batch, streaming, ML, grafos
- **Execução**: Local, cluster (Standalone, YARN, Mesos, Kubernetes)

### 1.2 Arquitetura do Spark

```
┌─────────────────────────────────────────────────────────────┐
│                      SPARK APPLICATION                       │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              DRIVER PROGRAM                           │   │
│  │  • SparkContext/SparkSession                         │   │
│  │  • Converte programa em tarefas                      │   │
│  │  • Agenda tarefas nos executors                      │   │
│  │  • Monitora execução                                 │   │
│  └──────────────────────────────────────────────────────┘   │
│                           │                                  │
│                           ▼                                  │
│  ┌────────────────────────────────────────────────────────┐ │
│  │              CLUSTER MANAGER                           │ │
│  │  • Standalone / YARN / Mesos / Kubernetes            │ │
│  │  • Gerencia recursos do cluster                      │ │
│  └────────────────────────────────────────────────────────┘ │
│                           │                                  │
│         ┌─────────────────┼─────────────────┐               │
│         ▼                 ▼                 ▼               │
│  ┌───────────┐     ┌───────────┐     ┌───────────┐         │
│  │ EXECUTOR  │     │ EXECUTOR  │     │ EXECUTOR  │         │
│  │ ┌───────┐ │     │ ┌───────┐ │     │ ┌───────┐ │         │
│  │ │ Task  │ │     │ │ Task  │ │     │ │ Task  │ │         │
│  │ ├───────┤ │     │ ├───────┤ │     │ ├───────┤ │         │
│  │ │ Task  │ │     │ │ Task  │ │     │ │ Task  │ │         │
│  │ └───────┘ │     │ └───────┘ │     │ └───────┘ │         │
│  │  Cache    │     │  Cache    │     │  Cache    │         │
│  └───────────┘     └───────────┘     └───────────┘         │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

#### Componentes da Arquitetura:

1. **Driver Program**:
   - Executa a função `main()` da aplicação
   - Cria o SparkContext/SparkSession
   - Converte o código em um DAG (Directed Acyclic Graph)
   - Divide o DAG em stages e tasks
   - Agenda tasks para execução

2. **Cluster Manager**:
   - Gerencia recursos do cluster
   - Aloca recursos para a aplicação
   - Tipos: Standalone, YARN, Mesos, Kubernetes

3. **Executors**:
   - Processos que executam as tasks
   - Armazenam dados em cache/memória
   - Enviam resultados ao driver

4. **Tasks**:
   - Menor unidade de trabalho
   - Enviadas aos executors
   - Processam partições de dados

### 1.3 Conceitos Fundamentais

#### RDD (Resilient Distributed Dataset)

RDD é a abstração fundamental do Spark - uma coleção distribuída e imutável de objetos.

**Características:**
- **Resiliente**: Recupera-se de falhas automaticamente
- **Distribuído**: Dados particionados através do cluster
- **Dataset**: Coleção de dados

**Criação de RDD:**
```python
# A partir de uma coleção
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# A partir de arquivo
rdd = spark.sparkContext.textFile("data/input.txt")
```

#### DataFrames

DataFrames são coleções distribuídas de dados organizados em colunas nomeadas (similar a tabelas SQL ou DataFrames do pandas).

**Vantagens:**
- Otimização automática (Catalyst Optimizer)
- Suporte a SQL
- Schema definido
- Melhor performance que RDDs

**Criação de DataFrame:**
```python
# A partir de arquivo CSV
df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

# A partir de dados em memória
data = [(1, "João", 1000), (2, "Maria", 1500)]
df = spark.createDataFrame(data, ["id", "nome", "salario"])
```

#### Transformações vs Ações

**Transformações** (Lazy - não executam imediatamente):
- `map()`, `filter()`, `groupBy()`, `join()`, `select()`, `where()`
- Criam um novo RDD/DataFrame
- Exemplo: `df.filter(df.age > 18)`

**Ações** (Eager - disparam execução):
- `count()`, `collect()`, `first()`, `show()`, `save()`
- Retornam valores ao driver
- Exemplo: `df.count()`

### 1.4 Execução Lazy (Lazy Evaluation)

Spark utiliza **avaliação preguiçosa**: transformações não são executadas imediatamente.

**Fluxo de Execução:**
```
Código → DAG → Logical Plan → Physical Plan → Execução
```

1. **DAG Creation**: Spark constrói um grafo de dependências
2. **Logical Plan**: Otimizações lógicas (Catalyst)
3. **Physical Plan**: Escolha do melhor plano físico
4. **Execution**: Tasks são executadas nos executors

**Exemplo:**
```python
# Nada é executado aqui
df_filtered = df.filter(df.age > 18)
df_selected = df_filtered.select("name", "age")

# Somente aqui a execução acontece
result = df_selected.count()  # Ação dispara o processamento
```

### 1.5 Spark vs MapReduce

| Característica | MapReduce | Spark |
|---------------|-----------|-------|
| **Velocidade** | Mais lento (disco) | Até 100x mais rápido (memória) |
| **Facilidade** | Complexo (Java) | Simples (Python, Scala, SQL) |
| **Processamento** | Batch apenas | Batch, Streaming, ML, Grafos |
| **Iterações** | Lento (I/O disco) | Rápido (cache em memória) |
| **APIs** | Baixo nível | Alto nível (DataFrames, SQL) |
| **Uso** | Hadoop específico | Multi-plataforma |

### ✅ Checkpoint 1.1

Antes de prosseguir, responda:

- [ ] Você compreende a diferença entre Driver e Executor?
- [ ] Você entende o que são RDDs e DataFrames?
- [ ] Você sabe a diferença entre Transformações e Ações?
- [ ] Você compreende o conceito de Lazy Evaluation?
- [ ] Você consegue comparar Spark com MapReduce?

---

## Parte 2: Caso de Uso - Análise de Vendas de E-commerce

### 2.1 Contexto do Problema

**Cenário**: Uma empresa de e-commerce precisa analisar suas vendas para tomar decisões estratégicas.

**Objetivos da Análise**:
1. Calcular receita total por categoria de produto
2. Identificar os produtos mais vendidos
3. Analisar padrões de vendas por região
4. Calcular ticket médio por cliente
5. Identificar tendências temporais de vendas

**Dataset**: `sales_data.csv`

**Estrutura dos dados:**
```csv
transaction_id,date,customer_id,product_id,product_name,category,quantity,price,region
TX001,2024-01-15,C101,P501,Notebook,Electronics,1,2500.00,Southeast
TX002,2024-01-15,C102,P502,Mouse,Electronics,2,45.00,South
TX003,2024-01-16,C103,P503,Book,Books,3,35.00,Northeast
...
```

**Campos:**
- `transaction_id`: ID único da transação
- `date`: Data da venda
- `customer_id`: ID do cliente
- `product_id`: ID do produto
- `product_name`: Nome do produto
- `category`: Categoria do produto
- `quantity`: Quantidade vendida
- `price`: Preço unitário
- `region`: Região da venda

### 2.2 Análises a Realizar

#### Análise 1: Receita Total por Categoria
Calcular a receita total (quantidade × preço) agrupada por categoria.

#### Análise 2: Top 10 Produtos Mais Vendidos
Identificar os 10 produtos com maior volume de vendas.

#### Análise 3: Vendas por Região
Analisar a distribuição de vendas entre as regiões do Brasil.

#### Análise 4: Ticket Médio por Cliente
Calcular o valor médio gasto por cada cliente.

#### Análise 5: Análise Temporal
Identificar tendências de vendas ao longo do tempo (diária/mensal).

### ✅ Checkpoint 2.1

Verifique:

- [ ] Você compreende o contexto do problema de negócio?
- [ ] Você entende a estrutura dos dados?
- [ ] Você sabe quais análises precisam ser realizadas?
- [ ] Você consegue pensar em como o Spark pode ajudar?

---

## Parte 3: Configuração do Ambiente

### 3.1 Fazendo Fork e Clonando o Repositório no GitHub Codespaces

**Passo 1:** Acesse https://github.com e faça login

**Passo 2:** Faça um fork do repositório do laboratório

1. Acesse o repositório original fornecido pelo professor
2. Clique no botão "Fork" no canto superior direito
3. Selecione sua conta como destino do fork
4. Aguarde a criação do fork (alguns segundos)

**Passo 3:** Clone seu fork usando GitHub Codespaces

1. No seu fork, clique no botão verde "Code"
2. Selecione a aba "Codespaces"
3. Clique em "Create codespace on main"
4. Aguarde o ambiente carregar (pode levar 2-3 minutos - Spark requer mais recursos)

**Passo 4:** Verifique o ambiente

```bash
python3 --version
docker --version
```

**Passo 5:** Explore a estrutura do projeto

```bash
ls -la pyspark_app/
```

Você verá:
- `spark_sales_analysis.py` - Script principal de análise
- `spark_word_count.py` - Exemplo simples (Word Count)
- `data_generator.py` - Gerador de dados de vendas
- `Dockerfile` - Configuração do container
- `requirements.txt` - Dependências Python
- `data/` - Diretório com datasets de exemplo

### ✅ Checkpoint 3.1

Verifique:

- [ ] Fork do repositório foi criado com sucesso
- [ ] Repositório foi clonado no Codespaces
- [ ] Python 3.x está instalado
- [ ] Docker está disponível
- [ ] Todos os arquivos da aplicação estão presentes
- [ ] Você consegue visualizar os scripts Python

---

## Parte 4: Implementação com PySpark

### 4.1 Explorando a Estrutura do Projeto

O repositório já contém todos os scripts necessários. Vamos entender cada componente:

```bash
cd pyspark_app
ls -la
```

Estrutura esperada:

```
pyspark_app/
├── spark_sales_analysis.py    # Análise completa de vendas
├── spark_word_count.py         # Exemplo básico
├── data_generator.py           # Gera dados de teste
├── spark_stream_example.py     # Exemplo de streaming
├── Dockerfile                  # Imagem Docker com Spark
├── requirements.txt            # Dependências
├── docker-compose.yml          # Orquestração
└── data/                       # Datasets
    ├── sales_data.csv          # Dados de vendas
    ├── products.csv            # Catálogo de produtos
    └── input.txt               # Texto para word count
```

### 4.2 Entendendo o Dataset

Primeiro, vamos gerar dados de exemplo:

```bash
python3 data_generator.py
```

Visualize o conteúdo:

```bash
head -20 data/sales_data.csv
```

### 4.3 Exemplo Simples: Word Count com PySpark

Antes da análise completa, vamos executar um exemplo simples para entender os conceitos:

```bash
cat spark_word_count.py
```

Execute o exemplo:

```bash
python3 spark_word_count.py
```

**O que este script faz:**
1. Cria uma SparkSession
2. Lê um arquivo de texto
3. Aplica transformações (split, flatMap, map, reduceByKey)
4. Executa uma ação (collect/show)

### 4.4 Análise de Vendas - Parte 1: Carregamento e Exploração

Abra o arquivo `spark_sales_analysis.py` e analise o código:

```bash
cat spark_sales_analysis.py
```

**Seção 1: Inicialização**
- Cria SparkSession
- Configura memória e cores

**Seção 2: Carregamento de Dados**
- Lê CSV com schema inference
- Valida dados carregados

**Seção 3: Exploração Inicial**
- Exibe schema
- Mostra primeiras linhas
- Calcula estatísticas

### 4.5 Análise de Vendas - Parte 2: Transformações e Agregações

**Análise Implementada:**

1. **Receita por Categoria**
```python
df.withColumn("revenue", col("quantity") * col("price"))
  .groupBy("category")
  .agg(sum("revenue").alias("total_revenue"))
  .orderBy(desc("total_revenue"))
```

2. **Top Produtos**
```python
df.groupBy("product_name")
  .agg(sum("quantity").alias("total_sold"))
  .orderBy(desc("total_sold"))
  .limit(10)
```

3. **Vendas por Região**
```python
df.groupBy("region")
  .agg(
    count("*").alias("num_transactions"),
    sum(col("quantity") * col("price")).alias("total_revenue")
  )
```

### 4.6 Executando a Análise Completa

Execute o script de análise:

```bash
python3 spark_sales_analysis.py
```

Observe a saída:
- Schema do DataFrame
- Estatísticas descritivas
- Resultados de cada análise
- Métricas de performance

### ✅ Checkpoint 4.1

Verifique:

- [ ] Você conseguiu gerar os dados de vendas?
- [ ] O exemplo de Word Count executou com sucesso?
- [ ] A análise de vendas foi executada completamente?
- [ ] Você compreende as transformações utilizadas?
- [ ] Os resultados fazem sentido do ponto de vista de negócio?

---

## Parte 5: Containerização com Docker

### 5.1 Entendendo o Dockerfile

Examine o Dockerfile:

```bash
cat Dockerfile
```

**Componentes:**
- Imagem base com Spark e Python
- Instalação de dependências
- Configuração do ambiente Spark
- Cópia dos scripts

### 5.2 Construindo a Imagem Docker

```bash
# Navegar para o diretório correto
cd pyspark_app

# Construir a imagem
docker build -t pyspark-app:v1.0 .
```

Aguarde o build (pode levar 3-5 minutos na primeira vez).

### 5.3 Executando o Container

**Opção 1: Executar análise de vendas**
```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  pyspark-app:v1.0 \
  python spark_sales_analysis.py
```

**Opção 2: Executar word count**
```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  pyspark-app:v1.0 \
  python spark_word_count.py
```

**Opção 3: Shell interativo**
```bash
docker run -it --rm \
  -v "$(pwd)/data:/app/data" \
  pyspark-app:v1.0 \
  bash
```

### 5.4 Docker Compose

Para orquestração mais simples, use Docker Compose:

```bash
# Análise de vendas
docker-compose up sales-analysis

# Word count
docker-compose up word-count

# PySpark Shell interativo
docker-compose up pyspark-shell
```

### ✅ Checkpoint 5.1

Verifique:

- [ ] A imagem Docker foi construída com sucesso?
- [ ] Os containers executam sem erros?
- [ ] Os volumes estão montados corretamente?
- [ ] Você consegue acessar os resultados?
- [ ] O Docker Compose está funcionando?

---

## Parte 6: Exercícios Práticos

### Exercício 1: Análise Adicional - Sazonalidade

**Objetivo**: Identificar padrões de vendas por dia da semana

**Tarefas**:
1. Extrair o dia da semana da coluna `date`
2. Agrupar vendas por dia da semana
3. Calcular receita média por dia
4. Identificar o dia mais lucrativo

**Dica**: Use `dayofweek()` do PySpark

### Exercício 2: Análise de Clientes VIP

**Objetivo**: Identificar os top 20 clientes que mais gastaram

**Tarefas**:
1. Calcular total gasto por cliente
2. Ordenar por valor total
3. Selecionar top 20
4. Calcular ticket médio destes clientes

### Exercício 3: Análise de Categorias em Declínio

**Objetivo**: Identificar categorias com queda nas vendas

**Tarefas**:
1. Agrupar vendas por categoria e mês
2. Calcular variação percentual mês a mês
3. Identificar categorias com variação negativa
4. Gerar relatório de alerta

### Exercício 4: Recomendação Simples

**Objetivo**: Produtos frequentemente comprados juntos

**Tarefas**:
1. Agrupar compras por `customer_id` e `date`
2. Identificar produtos comprados na mesma transação
3. Contar co-ocorrências
4. Gerar top 10 pares de produtos

### Exercício 5: Análise de Performance

**Objetivo**: Comparar performance com diferentes configurações

**Tarefas**:
1. Execute a análise com 1, 2 e 4 cores
2. Meça o tempo de execução
3. Compare uso de memória
4. Documente os resultados

**Comandos**:
```bash
# 1 core
spark-submit --master local[1] spark_sales_analysis.py

# 2 cores
spark-submit --master local[2] spark_sales_analysis.py

# 4 cores
spark-submit --master local[4] spark_sales_analysis.py
```

### ✅ Checkpoint 6.1

Após completar os exercícios:

- [ ] Você implementou pelo menos 3 exercícios?
- [ ] Os resultados são consistentes?
- [ ] Você documentou suas descobertas?
- [ ] Você compreende o impacto das configurações?

---

## Parte 7: Comparação MapReduce vs Spark

### 7.1 Exercício Comparativo

**Objetivo**: Comparar a mesma análise (Word Count) em ambos frameworks

**Passos**:

1. Execute o Word Count com MapReduce (roteiro anterior)
2. Execute o Word Count com Spark (roteiro atual)
3. Compare:
   - Linhas de código
   - Tempo de execução
   - Facilidade de implementação
   - Legibilidade do código

### 7.2 Tabela Comparativa

Preencha a tabela:

| Métrica | MapReduce | Spark | Observações |
|---------|-----------|-------|-------------|
| Linhas de código | | | |
| Tempo execução (pequeno dataset) | | | |
| Tempo execução (grande dataset) | | | |
| Facilidade (1-5) | | | |
| Legibilidade (1-5) | | | |
| Memória utilizada | | | |

### ✅ Checkpoint 7.1

- [ ] Você executou ambas as implementações?
- [ ] Você preencheu a tabela comparativa?
- [ ] Você compreende quando usar cada framework?

---

## Parte 8: Publicação e Documentação

### 8.1 Versionamento no GitHub

```bash
# Configure seu git (se necessário)
git config --global user.name "Seu Nome"
git config --global user.email "seu@email.com"

# Adicione todos os arquivos
git add .

# Commit das alterações
git commit -m "feat: Implementa análise de vendas com PySpark"

# Push para o GitHub
git push origin main
```

### 8.2 Publicando Imagem no Docker Hub

```bash
# Login no Docker Hub
docker login

# Tag da imagem
docker tag pyspark-app:v1.0 seu-usuario/pyspark-app:v1.0

# Push da imagem
docker push seu-usuario/pyspark-app:v1.0
```

### 8.3 Criando Documentação

Crie um arquivo `RESULTADOS.md` documentando:

1. **Introdução**: Breve descrição do projeto
2. **Configuração**: Passos para reproduzir
3. **Resultados**: Principais descobertas das análises
4. **Gráficos**: Visualizações (se aplicável)
5. **Conclusões**: Insights de negócio
6. **Aprendizados**: Lições sobre Spark

### ✅ Checkpoint 8.1

- [ ] Código foi commitado no GitHub?
- [ ] Imagem foi publicada no Docker Hub?
- [ ] Documentação foi criada?
- [ ] README está completo e claro?

---

## Parte 9: Recursos Adicionais e Próximos Passos

### 9.1 Conceitos Avançados para Estudo

1. **Spark SQL**: Queries SQL em DataFrames
2. **Spark Streaming**: Processamento de dados em tempo real
3. **Spark MLlib**: Machine Learning distribuído
4. **GraphX**: Processamento de grafos
5. **Delta Lake**: ACID transactions em Data Lakes

### 9.2 Recursos de Aprendizagem

**Documentação Oficial**:
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

**Cursos Online**:
- [Databricks Academy](https://www.databricks.com/learn/training)
- [Coursera - Big Data Specialization](https://www.coursera.org/specializations/big-data)

**Livros**:
- "Learning Spark" (O'Reilly)
- "Spark: The Definitive Guide" (O'Reilly)

**Comunidade**:
- [Stack Overflow - Apache Spark](https://stackoverflow.com/questions/tagged/apache-spark)
- [Spark User Mailing List](https://spark.apache.org/community.html)

### 9.3 Próximos Passos

1. **Deploy em Cluster**: Configure Spark em modo cluster (Standalone/YARN)
2. **Integração com Cloud**: Use AWS EMR, Azure Databricks ou GCP Dataproc
3. **Streaming**: Implemente processamento em tempo real com Kafka
4. **Machine Learning**: Crie modelos preditivos com MLlib
5. **Otimização**: Aprenda técnicas de tuning e particionamento

### 9.4 Certificações

- **Databricks Certified Associate Developer for Apache Spark**
- **Cloudera Certified Spark and Hadoop Developer**

---

## Parte 10: Avaliação e Entrega

### 10.1 Checklist Final

Antes de entregar, verifique:

**Código**:
- [ ] Todos os scripts executam sem erros
- [ ] Código está comentado e legível
- [ ] Boas práticas de PySpark foram aplicadas
- [ ] Tratamento de erros foi implementado

**Docker**:
- [ ] Imagem Docker constrói com sucesso
- [ ] Container executa corretamente
- [ ] Volumes estão configurados
- [ ] Docker Compose funciona

**Documentação**:
- [ ] README está completo
- [ ] Comentários no código estão claros
- [ ] RESULTADOS.md foi criado
- [ ] Tabela comparativa foi preenchida

**GitHub**:
- [ ] Repositório está público/compartilhado
- [ ] Commits têm mensagens descritivas
- [ ] .gitignore está configurado
- [ ] README renderiza corretamente

**Exercícios**:
- [ ] Pelo menos 3 exercícios foram completados
- [ ] Resultados foram documentados
- [ ] Análise comparativa foi realizada

### 10.2 Critérios de Avaliação

| Critério | Peso | Descrição |
|----------|------|-----------|
| Implementação PySpark | 30% | Correção e qualidade do código |
| Containerização | 20% | Docker configurado corretamente |
| Análises | 25% | Qualidade e profundidade das análises |
| Documentação | 15% | Clareza e completude |
| Exercícios | 10% | Exercícios adicionais completados |

### 10.3 Formato de Entrega

1. **Link do repositório GitHub**: URL do seu fork
2. **Link da imagem Docker Hub**: URL da imagem publicada
3. **Documento RESULTADOS.md**: Com análises e conclusões
4. **Screenshots**: Evidências de execução (opcional)

---

## Apêndice A: Troubleshooting

### Problema: "Java not found"

**Solução**:
```bash
# No Codespaces
sudo apt-get update
sudo apt-get install -y default-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### Problema: "Out of Memory"

**Solução**:
```python
spark = SparkSession.builder \
    .appName("SalesAnalysis") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
```

### Problema: "Permission Denied"

**Solução**:
```bash
chmod +x *.py
```

### Problema: Docker Build Falha

**Solução**:
```bash
# Limpe cache do Docker
docker system prune -a

# Rebuild sem cache
docker build --no-cache -t pyspark-app:v1.0 .
```

---

## Apêndice B: Comandos Úteis

### PySpark Shell Interativo

```bash
# Inicie o PySpark shell
pyspark

# Com configurações personalizadas
pyspark --master local[4] --driver-memory 2g
```

### Spark Submit

```bash
# Submeta uma aplicação
spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --executor-memory 2g \
  spark_sales_analysis.py
```

### Monitoramento

```bash
# Spark UI (quando executando localmente)
# Acesse: http://localhost:4040
```

---

## Conclusão

Parabéns! Você completou o laboratório de PySpark. 

**O que você aprendeu**:
- ✅ Arquitetura e conceitos do Apache Spark
- ✅ Diferença entre RDDs e DataFrames
- ✅ Transformações e Ações
- ✅ Lazy Evaluation
- ✅ Análise de dados com PySpark
- ✅ Containerização de aplicações Spark
- ✅ Comparação com MapReduce

**Próximos passos**:
- Continue praticando com datasets reais
- Explore Spark Streaming e MLlib
- Considere certificações
- Contribua com projetos open source

---

**Desenvolvido para o curso de Ciência de Dados**  
**Versão 1.0 - Novembro 2025**

**Autor**: Professor/Instrutor  
**Contato**: professor@cienciadados.edu  
**Licença**: MIT - Livre para uso educacional
