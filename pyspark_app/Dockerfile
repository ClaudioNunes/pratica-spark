# Imagem base com Spark e Python
FROM apache/spark-py:v3.5.0

# Metadata
LABEL maintainer="estudante@cienciadados.edu"
LABEL description="PySpark Application for Big Data Analytics"

# Define usuário root temporariamente para instalações
USER root

# Define diretório de trabalho
WORKDIR /app

# Copia requirements
COPY requirements.txt .

# Instala dependências Python adicionais
RUN pip install --no-cache-dir -r requirements.txt

# Cria diretórios necessários
RUN mkdir -p /app/data /app/data/output && \
    chmod -R 777 /app/data

# Copia scripts Python
COPY *.py ./

# Torna scripts executáveis
RUN chmod +x *.py

# Volta para usuário spark (não-root)
USER spark

# Configura variáveis de ambiente
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

# Define comando padrão
CMD ["python3", "spark_sales_analysis.py"]
