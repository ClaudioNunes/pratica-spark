#!/usr/bin/env python3
"""
Gerador de Dados de Vendas para Análise com PySpark
Gera um dataset sintético de transações de e-commerce
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Configurações
NUM_TRANSACTIONS = 1000
OUTPUT_FILE = "data/sales_data.csv"

# Dados fictícios em Português Brasileiro
PRODUCTS = [
    ("P001", "Notebook Dell", "Eletrônicos", 2800.00),
    ("P002", "Mouse Logitech", "Eletrônicos", 45.00),
    ("P003", "Teclado Mecânico", "Eletrônicos", 250.00),
    ("P004", "Monitor LG 24\"", "Eletrônicos", 850.00),
    ("P005", "Webcam HD", "Eletrônicos", 180.00),
    ("P006", "Fone Bluetooth", "Eletrônicos", 120.00),
    ("P007", "SSD 500GB", "Eletrônicos", 350.00),
    ("P008", "Livro Python", "Livros", 45.00),
    ("P009", "Livro Ciência de Dados", "Livros", 65.00),
    ("P010", "Livro Machine Learning", "Livros", 80.00),
    ("P011", "Caderno", "Papelaria", 15.00),
    ("P012", "Caneta Pack 10", "Papelaria", 12.00),
    ("P013", "Mochila Executiva", "Acessórios", 180.00),
    ("P014", "Garrafa Térmica", "Acessórios", 45.00),
    ("P015", "Tablet Samsung", "Eletrônicos", 1200.00),
    ("P016", "SmartWatch", "Eletrônicos", 800.00),
    ("P017", "Carregador USB-C", "Eletrônicos", 35.00),
    ("P018", "Hub USB", "Eletrônicos", 60.00),
    ("P019", "Cabo HDMI 2m", "Eletrônicos", 25.00),
    ("P020", "Mouse Pad", "Acessórios", 20.00),
]

REGIONS = ["Sudeste", "Sul", "Nordeste", "Norte", "Centro-Oeste"]

# Probabilidades para tornar dados mais realistas
CATEGORY_WEIGHTS = {
    "Eletrônicos": 0.5,
    "Livros": 0.2,
    "Papelaria": 0.15,
    "Acessórios": 0.15,
}

def generate_transaction_id(index):
    """Gera ID único para transação"""
    return f"TX{index:06d}"

def generate_customer_id():
    """Gera ID de cliente"""
    return f"C{random.randint(100, 999)}"

def generate_date(start_date, end_date):
    """Gera data aleatória no intervalo"""
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

def select_product():
    """Seleciona produto com distribuição ponderada"""
    # Filtra produtos por categoria e aplica pesos
    category = random.choices(
        list(CATEGORY_WEIGHTS.keys()),
        weights=list(CATEGORY_WEIGHTS.values())
    )[0]
    
    category_products = [p for p in PRODUCTS if p[2] == category]
    return random.choice(category_products)

def generate_quantity(product_price):
    """Gera quantidade baseada no preço (produtos mais caros: menor quantidade)"""
    if product_price > 1000:
        return random.randint(1, 2)
    elif product_price > 200:
        return random.randint(1, 3)
    else:
        return random.randint(1, 10)

def generate_sales_data():
    """Gera dataset de vendas"""
    print("🔄 Gerando dados de vendas...")
    
    # Cria diretório se não existir
    Path("data").mkdir(exist_ok=True)
    
    # Define intervalo de datas (últimos 6 meses)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)
    
    # Gera transações
    transactions = []
    
    for i in range(1, NUM_TRANSACTIONS + 1):
        transaction_id = generate_transaction_id(i)
        date = generate_date(start_date, end_date)
        customer_id = generate_customer_id()
        
        # Seleciona produto
        product_id, product_name, category, price = select_product()
        
        # Gera quantidade
        quantity = generate_quantity(price)
        
        # Seleciona região
        region = random.choice(REGIONS)
        
        transactions.append([
            transaction_id,
            date,
            customer_id,
            product_id,
            product_name,
            category,
            quantity,
            f"{price:.2f}",
            region
        ])
    
    # Salva em CSV
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            "transaction_id",
            "date",
            "customer_id",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "price",
            "region"
        ])
        
        # Dados
        writer.writerows(transactions)
    
    print(f"✅ {NUM_TRANSACTIONS} transações geradas!")
    print(f"📁 Arquivo salvo em: {OUTPUT_FILE}")
    
    # Estatísticas
    print("\n📊 Estatísticas dos dados gerados:")
    print(f"   - Período: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
    print(f"   - Produtos únicos: {len(PRODUCTS)}")
    print(f"   - Categorias: {', '.join(CATEGORY_WEIGHTS.keys())}")
    print(f"   - Regiões: {', '.join(REGIONS)}")
    
    # Preview
    print("\n📄 Preview das primeiras linhas:")
    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i < 5:
                print(f"   {line.strip()}")
            else:
                break

def generate_products_catalog():
    """Gera catálogo de produtos separado"""
    catalog_file = "data/products.csv"
    
    with open(catalog_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "product_name", "category", "base_price"])
        writer.writerows(PRODUCTS)
    
    print(f"✅ Catálogo de produtos gerado: {catalog_file}")

def generate_text_data():
    """Gera arquivo de texto para exemplo de word count"""
    text_file = "data/input.txt"
    
    text_content = """Apache Spark is a unified analytics engine for large-scale data processing.
Spark provides high-level APIs in Java, Scala, Python and R.
PySpark is the Python API for Apache Spark.
Big Data processing has become essential in modern data science.
Data engineers use Spark to build robust data pipelines.
Machine Learning at scale is possible with Spark MLlib.
Real-time stream processing can be done with Spark Streaming.
Spark SQL provides a DataFrame API for structured data processing.
The Catalyst optimizer makes Spark SQL queries fast and efficient.
Spark runs on Hadoop, Apache Mesos, Kubernetes, standalone, or in the cloud.
Data scientists love PySpark for its simplicity and power.
Distributed computing enables processing of massive datasets.
Apache Spark revolutionized big data analytics.
In-memory computation makes Spark incredibly fast.
RDDs are the fundamental data structure in Spark.
DataFrames provide a higher-level abstraction than RDDs.
Lazy evaluation allows Spark to optimize the execution plan.
Transformations and actions are key concepts in Spark programming.
Spark applications can be written in multiple programming languages.
The Spark ecosystem includes MLlib, GraphX, and Spark Streaming."""
    
    with open(text_file, 'w', encoding='utf-8') as f:
        f.write(text_content)
    
    print(f"✅ Arquivo de texto gerado: {text_file}")

if __name__ == "__main__":
    print("=" * 60)
    print("  GERADOR DE DADOS - PYSPARK LAB")
    print("=" * 60)
    print()
    
    # Gera todos os datasets
    generate_sales_data()
    print()
    generate_products_catalog()
    print()
    generate_text_data()
    
    print()
    print("=" * 60)
    print("✅ Todos os arquivos foram gerados com sucesso!")
    print("=" * 60)
