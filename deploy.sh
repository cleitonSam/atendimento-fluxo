#!/bin/bash
set -e

# Script de deploy para VPS (EasyPanel / Docker Compose)

echo "==> Verificando .env..."
if [ ! -f .env ]; then
  echo "ERRO: arquivo .env não encontrado."
  echo "Copie o .env.example e preencha as variáveis:"
  echo "  cp .env.example .env && nano .env"
  exit 1
fi

echo "==> Build e subindo containers..."
docker compose pull nginx redis postgres 2>/dev/null || true
docker compose build --no-cache api worker frontend
docker compose up -d

echo "==> Aguardando banco de dados ficar pronto..."
sleep 5

echo "==> Status dos containers:"
docker compose ps

echo ""
echo "==> Deploy concluído! Acesse http://SEU_IP para verificar."
