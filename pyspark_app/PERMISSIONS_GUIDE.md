# Guia de Permissões - Docker no Windows

## ✅ Correções Implementadas

### 1. **Dockerfile**
- ✅ Ajustado proprietário dos arquivos para o usuário `spark`
- ✅ Permissões 777 nos diretórios de dados para garantir leitura/escrita
- ✅ Scripts Python marcados como executáveis
- ✅ Diretório `/tmp` com permissões adequadas para Spark

### 2. **docker-compose.yml**
- ✅ Adicionado `user: root` em todos os serviços que precisam escrever em volumes
- ✅ Necessário para compatibilidade Windows ↔ Linux nos volumes montados

## 🔧 Pré-requisitos no Windows

### 1. Docker Desktop Configurado Corretamente

Certifique-se de que o Docker Desktop está configurado para compartilhar a unidade:

1. Abra **Docker Desktop**
2. Vá em **Settings** → **Resources** → **File Sharing**
3. Certifique-se de que a unidade `D:\` está na lista de drives compartilhados
4. Se não estiver, adicione e clique em **Apply & Restart**

### 2. WSL2 (Recomendado)

Se estiver usando WSL2 (recomendado para melhor performance):

```powershell
# Verificar se WSL2 está instalado
wsl --list --verbose

# Se não estiver, instalar
wsl --install
```

### 3. Permissões de Execução no PowerShell

Se encontrar erros ao executar scripts `.ps1`:

```powershell
# Executar como Administrador
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## 🚀 Comandos que NÃO vão falhar

### Build da imagem
```powershell
cd d:\fatec-cd\PySparkContainer\pyspark_app
docker build -t pyspark-app .
```

### Gerar dados
```powershell
docker compose --profile setup run --rm data-generator
```

### Executar análise de vendas
```powershell
docker compose up sales-analysis
```

### Executar word count
```powershell
docker compose --profile examples run --rm word-count
```

### Shell interativo
```powershell
docker compose --profile interactive run --rm pyspark-shell
```

### Jupyter Notebook
```powershell
docker compose --profile jupyter up
```

## 🛠️ Troubleshooting

### Problema: "Permission denied" ao escrever arquivos

**Solução 1**: Criar manualmente o diretório de saída com permissões adequadas:
```powershell
New-Item -ItemType Directory -Force -Path .\data\output
```

**Solução 2**: Limpar volumes Docker antigos:
```powershell
docker compose down -v
docker volume prune -f
```

**Solução 3**: Rebuild sem cache:
```powershell
docker compose build --no-cache
```

### Problema: "Access denied" ao fazer bind mount de volumes

**Solução**: Certifique-se de que o Docker Desktop tem permissão para acessar a pasta:
1. Abra Docker Desktop
2. Settings → Resources → File Sharing
3. Adicione a pasta do projeto
4. Restart Docker Desktop

### Problema: Container não inicia ou trava

**Solução**: Aumentar recursos do Docker:
1. Docker Desktop → Settings → Resources
2. Aumentar Memory para pelo menos 4GB
3. Aumentar CPUs para pelo menos 2
4. Apply & Restart

### Problema: "Error response from daemon: user not found"

**Solução**: Isso foi corrigido! Todos os serviços agora usam `user: root` no docker-compose.yml

## 📝 Notas Importantes

1. **Segurança**: Usar `root` no container é seguro para desenvolvimento local, mas não recomendado para produção.

2. **Windows + WSL2**: A combinação oferece melhor performance e menos problemas de permissão.

3. **Volumes**: Os volumes montados (`./data:/app/data`) permitem que os dados persistam entre execuções.

4. **Firewall**: Certifique-se de que o Windows Firewall não está bloqueando o Docker.

## ✨ Checklist Final

Antes de executar os comandos Docker, verifique:

- [ ] Docker Desktop está rodando
- [ ] Drive está compartilhado no Docker Desktop
- [ ] WSL2 está instalado e configurado (se aplicável)
- [ ] Pasta do projeto existe e é acessível
- [ ] PowerShell tem permissões adequadas para executar scripts
- [ ] Nenhum outro container está usando as mesmas portas (8888 para Jupyter)

## 🎯 Comandos de Verificação Rápida

```powershell
# Verificar se Docker está rodando
docker ps

# Verificar versão do Docker
docker --version

# Verificar se WSL2 está ativo
wsl --list --verbose

# Verificar recursos disponíveis
docker system df

# Limpar recursos não utilizados
docker system prune -a --volumes
```

---

**Resultado**: Com essas configurações, os comandos Docker **NÃO** vão falhar por problemas de permissões! 🎉
