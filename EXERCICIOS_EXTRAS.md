# Exercícios Extras - PySpark Lab

Exercícios adicionais para aprofundar seus conhecimentos em PySpark e processamento de Big Data.

---

## Exercício Extra 1: Análise de Cohort

**Nível:** Intermediário  
**Tempo estimado:** 45 minutos

### Objetivo
Implementar uma análise de cohort para identificar padrões de retenção de clientes ao longo do tempo.

### Descrição
Uma análise de cohort agrupa clientes pela data da primeira compra e acompanha seu comportamento ao longo dos meses subsequentes.

### Tarefas

1. Identifique a primeira compra de cada cliente
2. Agrupe clientes por mês da primeira compra (cohort)
3. Calcule quantos clientes de cada cohort fizeram compras nos meses seguintes
4. Calcule a taxa de retenção mensal

### Dicas

```python
from pyspark.sql.functions import min, max, months_between, floor

# Primeira compra
first_purchase = df.groupBy("customer_id").agg(
    min("date").alias("first_purchase_date")
)

# Join com dados originais
df_with_cohort = df.join(first_purchase, "customer_id")

# Calcule meses desde primeira compra
df_with_cohort = df_with_cohort.withColumn(
    "months_since_first",
    floor(months_between(col("date"), col("first_purchase_date")))
)
```

### Resultado Esperado
Tabela mostrando retenção por cohort:

```
Cohort     | Month 0 | Month 1 | Month 2 | Month 3 |
2024-01    |   100%  |   45%   |   32%   |   28%   |
2024-02    |   100%  |   52%   |   38%   |    -    |
```

---

## Exercício Extra 2: Detecção de Anomalias

**Nível:** Avançado  
**Tempo estimado:** 60 minutos

### Objetivo
Implementar um sistema simples de detecção de anomalias em vendas usando estatísticas descritivas.

### Descrição
Identifique transações anômalas (outliers) baseadas em:
- Valor da transação muito alto ou baixo
- Quantidade incomum de produtos
- Padrões incomuns por região

### Tarefas

1. Calcule média e desvio padrão da receita
2. Identifique transações fora de 3 desvios padrão
3. Analise padrões anômalos por categoria
4. Gere relatório de alertas

### Dicas

```python
from pyspark.sql.functions import mean, stddev

# Estatísticas
stats = df.select(
    mean("revenue").alias("avg_revenue"),
    stddev("revenue").alias("std_revenue")
).first()

# Identifica outliers
outliers = df.filter(
    (col("revenue") > stats.avg_revenue + 3 * stats.std_revenue) |
    (col("revenue") < stats.avg_revenue - 3 * stats.std_revenue)
)
```

### Resultado Esperado
- Lista de transações anômalas
- Estatísticas de anomalias por categoria
- Visualização de distribuição

---

## Exercício Extra 3: Análise de RFM

**Nível:** Intermediário  
**Tempo estimado:** 45 minutos

### Objetivo
Implementar análise RFM (Recency, Frequency, Monetary) para segmentação de clientes.

### Descrição
RFM é uma técnica de marketing que segmenta clientes baseado em:
- **Recency**: Quão recentemente o cliente comprou
- **Frequency**: Com que frequência o cliente compra
- **Monetary**: Quanto o cliente gastou

### Tarefas

1. Calcule R, F, M para cada cliente
2. Crie scores de 1-5 para cada métrica
3. Combine scores para criar segmentos
4. Gere insights sobre cada segmento

### Dicas

```python
from pyspark.sql.functions import datediff, current_date, ntile
from pyspark.sql.window import Window

# Recency (dias desde última compra)
rfm = df.groupBy("customer_id").agg(
    datediff(current_date(), max("date")).alias("recency"),
    count("*").alias("frequency"),
    sum("revenue").alias("monetary")
)

# Cria scores usando ntile (quintis)
window = Window.orderBy("recency")
rfm = rfm.withColumn("R_score", ntile(5).over(window))
```

### Resultado Esperado
Segmentos como:
- **Champions** (R=5, F=5, M=5): Melhores clientes
- **At Risk** (R=1, F=5, M=5): Clientes valiosos que não compram há tempo
- **Lost** (R=1, F=1, M=1): Clientes perdidos

---

## Exercício Extra 4: Market Basket Analysis

**Nível:** Avançado  
**Tempo estimado:** 90 minutos

### Objetivo
Implementar análise de cesta de compras para identificar produtos frequentemente comprados juntos.

### Descrição
Análise de associação entre produtos usando métricas:
- **Support**: Frequência da combinação
- **Confidence**: P(B|A) - probabilidade de comprar B dado que comprou A
- **Lift**: Quanto a compra de A aumenta a probabilidade de comprar B

### Tarefas

1. Agrupe produtos por transação (mesmo cliente, mesma data)
2. Encontre pares de produtos frequentemente comprados juntos
3. Calcule support, confidence e lift
4. Identifique as associações mais fortes

### Dicas

```python
# Agrupa por transação
baskets = df.groupBy("customer_id", "date").agg(
    collect_list("product_id").alias("products")
)

# Self-join para encontrar pares
# Pode usar MLlib FPGrowth para análise mais sofisticada
from pyspark.ml.fpm import FPGrowth

fp = FPGrowth(itemsCol="products", minSupport=0.05, minConfidence=0.3)
model = fp.fit(baskets)

# Frequent itemsets
model.freqItemsets.show()

# Association rules
model.associationRules.show()
```

### Resultado Esperado
```
Antecedent    | Consequent   | Confidence | Lift  |
[Notebook]    | [Mouse]      |    0.75    |  2.3  |
[Mouse]       | [Mouse Pad]  |    0.60    |  1.8  |
```

---

## Exercício Extra 5: Time Series Analysis

**Nível:** Avançado  
**Tempo estimado:** 60 minutos

### Objetivo
Análise de séries temporais com tendências e sazonalidade.

### Tarefas

1. Calcule vendas diárias agregadas
2. Calcule média móvel (7 dias, 30 dias)
3. Identifique tendências de crescimento
4. Detecte sazonalidade (dia da semana)

### Dicas

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, lag, lead

# Vendas diárias
daily_sales = df.groupBy("date").agg(
    sum("revenue").alias("daily_revenue")
).orderBy("date")

# Média móvel 7 dias
window_7d = Window.orderBy("date").rowsBetween(-6, 0)
daily_sales = daily_sales.withColumn(
    "ma_7d",
    avg("daily_revenue").over(window_7d)
)

# Crescimento dia a dia
window_lag = Window.orderBy("date")
daily_sales = daily_sales.withColumn(
    "prev_day",
    lag("daily_revenue").over(window_lag)
).withColumn(
    "growth_rate",
    (col("daily_revenue") - col("prev_day")) / col("prev_day") * 100
)
```

---

## Exercício Extra 6: Análise Geoespacial

**Nível:** Intermediário  
**Tempo estimado:** 40 minutos

### Objetivo
Análise avançada por região com métricas comparativas.

### Tarefas

1. Calcule market share de cada região
2. Compare performance entre regiões
3. Identifique regiões com maior potencial de crescimento
4. Analise mix de produtos por região

### Dicas

```python
# Market share
total_revenue = df.select(sum("revenue")).first()[0]

region_analysis = df.groupBy("region").agg(
    sum("revenue").alias("region_revenue"),
    count("*").alias("num_transactions")
).withColumn(
    "market_share",
    (col("region_revenue") / total_revenue) * 100
)

# Performance vs média
avg_revenue = df.select(avg("revenue")).first()[0]

region_analysis = region_analysis.withColumn(
    "performance_index",
    col("region_revenue") / col("num_transactions") / avg_revenue
)
```

---

## Exercício Extra 7: Customer Lifetime Value (CLV)

**Nível:** Avançado  
**Tempo estimado:** 75 minutos

### Objetivo
Calcular o Customer Lifetime Value de cada cliente.

### Descrição
CLV = (Valor médio de compra) × (Frequência de compra) × (Tempo como cliente)

### Tarefas

1. Calcule métricas por cliente:
   - Valor médio de compra
   - Frequência de compra (compras/mês)
   - Tempo como cliente (dias)
2. Estime CLV para cada cliente
3. Segmente clientes por CLV
4. Identifique características de alto CLV

### Dicas

```python
from pyspark.sql.functions import datediff

customer_metrics = df.groupBy("customer_id").agg(
    avg("revenue").alias("avg_purchase_value"),
    count("*").alias("num_purchases"),
    datediff(max("date"), min("date")).alias("customer_lifetime_days")
)

# CLV simplificado
customer_metrics = customer_metrics.withColumn(
    "purchase_frequency",
    col("num_purchases") / (col("customer_lifetime_days") / 30)
).withColumn(
    "clv",
    col("avg_purchase_value") * col("purchase_frequency") * 
    (col("customer_lifetime_days") / 365)
)
```

---

## Exercício Extra 8: Otimização de Performance

**Nível:** Avançado  
**Tempo estimado:** 60 minutos

### Objetivo
Otimizar queries Spark para melhor performance.

### Tarefas

1. Execute a análise de vendas com diferentes configurações
2. Compare tempos de execução
3. Analise planos de execução (explain)
4. Aplique otimizações

### Configurações para testar:

```python
# Teste 1: Padrão
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Teste 2: Otimizado
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Teste 3: Com cache
df.cache()
df.count()  # Materializa cache

# Teste 4: Com reparticionamento
df_repartitioned = df.repartition(8, "category")
```

### Métricas para coletar:
- Tempo de execução total
- Tempo por stage
- Shuffle read/write
- Memória utilizada

---

## Exercício Extra 9: Join Optimization

**Nível:** Avançado  
**Tempo estimado:** 45 minutos

### Objetivo
Entender e otimizar joins no Spark.

### Tarefas

1. Crie um dataset de produtos (se ainda não existir)
2. Faça join entre vendas e produtos
3. Compare diferentes tipos de join:
   - Broadcast join (pequeno + grande)
   - Sort-merge join (grande + grande)
4. Analise planos de execução

### Código:

```python
# Dataset de produtos (pequeno)
products_df = spark.read.csv("data/products.csv", header=True)

# Join regular
result = df.join(products_df, "product_id")

# Broadcast join (força)
from pyspark.sql.functions import broadcast
result_broadcast = df.join(broadcast(products_df), "product_id")

# Compare planos
result.explain()
result_broadcast.explain()
```

---

## Exercício Extra 10: Data Quality Assessment

**Nível:** Intermediário  
**Tempo estimado:** 45 minutos

### Objetivo
Implementar checks de qualidade de dados.

### Tarefas

1. Verificar valores nulos/missing
2. Identificar duplicatas
3. Validar ranges de valores
4. Verificar consistência de dados
5. Gerar relatório de qualidade

### Código:

```python
# Valores nulos por coluna
df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
]).show()

# Duplicatas
duplicates = df.groupBy("transaction_id").count().filter(col("count") > 1)

# Validações de negócio
quality_checks = df.agg(
    count(when(col("price") < 0, True)).alias("negative_prices"),
    count(when(col("quantity") < 1, True)).alias("invalid_quantity"),
    count(when(col("date") > current_date(), True)).alias("future_dates")
)

quality_checks.show()
```

---

## Dicas Gerais para Todos os Exercícios

### Boas Práticas:

1. **Sempre use `.explain()`** para entender o plano de execução
2. **Cache DataFrames** que serão reutilizados
3. **Particione adequadamente** para joins e agregações
4. **Use funções built-in** ao invés de UDFs quando possível
5. **Monitore memória** e ajuste configurações conforme necessário

### Debugging:

```python
# Conta linhas em cada stage
df.cache()
print(f"Total records: {df.count()}")

# Mostra plano físico
df.explain(mode="formatted")

# Verifica partições
print(f"Number of partitions: {df.rdd.getNumPartitions()}")

# Mostra schema
df.printSchema()
```

### Performance Tips:

1. Use `filter()` antes de `groupBy()`
2. Use `select()` para escolher apenas colunas necessárias
3. Evite `collect()` em datasets grandes
4. Use `limit()` durante desenvolvimento/testes
5. Configure `spark.sql.shuffle.partitions` adequadamente

---

## Recursos Adicionais

### Datasets Públicos para Prática:

1. **Kaggle**: Diversos datasets de vendas e e-commerce
2. **UCI Machine Learning Repository**: Datasets educacionais
3. **AWS Open Data**: Datasets públicos na nuvem
4. **Google Public Datasets**: BigQuery public datasets

### Ferramentas de Visualização:

- **Matplotlib/Seaborn**: Gráficos em Python
- **Plotly**: Visualizações interativas
- **Tableau/Power BI**: BI profissional
- **Spark UI**: Monitoramento de jobs (localhost:4040)

---

## Critérios de Avaliação

Para cada exercício, considere:

| Critério | Peso | Descrição |
|----------|------|-----------|
| **Correção** | 40% | Código funciona e produz resultado correto |
| **Performance** | 20% | Uso eficiente de recursos Spark |
| **Código Limpo** | 20% | Legibilidade e organização |
| **Documentação** | 10% | Comentários e explicações |
| **Insights** | 10% | Qualidade das conclusões |

---

## Entrega dos Exercícios

1. Crie uma branch para seus exercícios: `git checkout -b exercicios-extras`
2. Implemente cada exercício em arquivo separado: `exercicio_01.py`, etc.
3. Documente resultados em `RESULTADOS_EXERCICIOS.md`
4. Commit e push: `git push origin exercicios-extras`
5. (Opcional) Crie Pull Request para revisão

---

**Boa sorte com os exercícios!** 🚀

Se tiver dúvidas, consulte a documentação oficial do PySpark ou entre em contato com o instrutor.
