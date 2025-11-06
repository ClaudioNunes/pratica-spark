#!/bin/bash
# Bash script para inicializar o repositório PySpark no GitHub Codespaces
# Autor: Professor/Instrutor
# Versão: 1.0

echo "========================================"
echo "  SETUP - PySpark Lab Environment"
echo "========================================"
echo ""

# Cores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Verifica se está no Codespaces
if [ "$CODESPACES" = "true" ]; then
    echo -e "${GREEN}✅ Ambiente GitHub Codespaces detectado${NC}"
else
    echo -e "${YELLOW}⚠️  Executando fora do Codespaces${NC}"
fi

echo ""
echo -e "${CYAN}📦 Verificando dependências...${NC}"

# Verifica Python
echo -n "   Python: "
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo -e "${GREEN}✅ $PYTHON_VERSION${NC}"
else
    echo -e "${RED}❌ Não encontrado${NC}"
    exit 1
fi

# Verifica Docker
echo -n "   Docker: "
if command -v docker &> /dev/null; then
    DOCKER_VERSION=$(docker --version)
    echo -e "${GREEN}✅ $DOCKER_VERSION${NC}"
else
    echo -e "${RED}❌ Não encontrado${NC}"
fi

# Verifica Java
echo -n "   Java: "
if command -v java &> /dev/null; then
    echo -e "${GREEN}✅ Instalado${NC}"
else
    echo -e "${YELLOW}⚠️  Não encontrado - Instalando...${NC}"
    sudo apt-get update -qq
    sudo apt-get install -y openjdk-11-jdk -qq
    echo -e "   ${GREEN}✅ Java instalado${NC}"
fi

echo ""
echo -e "${CYAN}📚 Instalando dependências Python...${NC}"
cd pyspark_app
python3 -m pip install -q --upgrade pip
python3 -m pip install -q -r requirements.txt

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Dependências instaladas com sucesso${NC}"
else
    echo -e "${RED}❌ Erro ao instalar dependências${NC}"
    exit 1
fi

echo ""
echo -e "${CYAN}🔧 Configurando ambiente...${NC}"

# Torna scripts executáveis
chmod +x *.py
echo -e "${GREEN}✅ Permissões configuradas${NC}"

# Cria diretórios necessários
mkdir -p data/output
echo -e "${GREEN}✅ Diretórios criados${NC}"

echo ""
echo -e "${CYAN}📊 Gerando dados de exemplo...${NC}"
python3 data_generator.py

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Dados gerados com sucesso${NC}"
else
    echo -e "${RED}❌ Erro ao gerar dados${NC}"
    exit 1
fi

echo ""
echo "========================================"
echo -e "${GREEN}✅ Setup concluído com sucesso!${NC}"
echo "========================================"
echo ""
echo -e "${CYAN}📚 Próximos passos:${NC}"
echo "   1. Explore o README.md do projeto"
echo "   2. Execute: python3 spark_word_count.py"
echo "   3. Execute: python3 spark_sales_analysis.py"
echo ""
echo -e "${CYAN}🐳 Para usar Docker:${NC}"
echo "   docker build -t pyspark-app:v1.0 ."
echo "   docker-compose up sales-analysis"
echo ""
