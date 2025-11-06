# PowerShell script para inicializar o repositório PySpark no GitHub Codespaces
# Autor: Professor/Instrutor
# Versão: 1.0

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  SETUP - PySpark Lab Environment" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verifica se está no Codespaces
if ($env:CODESPACES -eq "true") {
    Write-Host "✅ Ambiente GitHub Codespaces detectado" -ForegroundColor Green
} else {
    Write-Host "⚠️  Executando fora do Codespaces" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "📦 Verificando dependências..." -ForegroundColor Cyan

# Verifica Python
Write-Host -NoNewline "   Python: "
$pythonVersion = python3 --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ $pythonVersion" -ForegroundColor Green
} else {
    Write-Host "❌ Não encontrado" -ForegroundColor Red
    exit 1
}

# Verifica Docker
Write-Host -NoNewline "   Docker: "
$dockerVersion = docker --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ $dockerVersion" -ForegroundColor Green
} else {
    Write-Host "❌ Não encontrado" -ForegroundColor Red
}

# Verifica Java
Write-Host -NoNewline "   Java: "
$javaVersion = java -version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Instalado" -ForegroundColor Green
} else {
    Write-Host "⚠️  Não encontrado - Instalando..." -ForegroundColor Yellow
    sudo apt-get update -qq
    sudo apt-get install -y openjdk-11-jdk -qq
    Write-Host "   ✅ Java instalado" -ForegroundColor Green
}

Write-Host ""
Write-Host "📚 Instalando dependências Python..." -ForegroundColor Cyan
Set-Location pyspark_app
python3 -m pip install -q --upgrade pip
python3 -m pip install -q -r requirements.txt

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Dependências instaladas com sucesso" -ForegroundColor Green
} else {
    Write-Host "❌ Erro ao instalar dependências" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "🔧 Configurando ambiente..." -ForegroundColor Cyan

# Torna scripts executáveis
if ($IsLinux -or $IsMacOS) {
    chmod +x *.py
    Write-Host "✅ Permissões configuradas" -ForegroundColor Green
}

# Cria diretórios necessários
if (!(Test-Path "data/output")) {
    New-Item -ItemType Directory -Path "data/output" -Force | Out-Null
    Write-Host "✅ Diretórios criados" -ForegroundColor Green
}

Write-Host ""
Write-Host "📊 Gerando dados de exemplo..." -ForegroundColor Cyan
python3 data_generator.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Dados gerados com sucesso" -ForegroundColor Green
} else {
    Write-Host "❌ Erro ao gerar dados" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "✅ Setup concluído com sucesso!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "📚 Próximos passos:" -ForegroundColor Cyan
Write-Host "   1. Explore o README.md do projeto" -ForegroundColor White
Write-Host "   2. Execute: python3 spark_word_count.py" -ForegroundColor White
Write-Host "   3. Execute: python3 spark_sales_analysis.py" -ForegroundColor White
Write-Host ""
Write-Host "🐳 Para usar Docker:" -ForegroundColor Cyan
Write-Host "   docker build -t pyspark-app:v1.0 ." -ForegroundColor White
Write-Host "   docker-compose up sales-analysis" -ForegroundColor White
Write-Host ""
