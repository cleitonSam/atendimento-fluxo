#!/bin/bash
# =============================================
# Setup inicial da VPS para atendimento-fluxo
# Execute como root: bash setup-vps.sh
# =============================================
set -e

REPO_URL="https://github.com/cleitonSam/atendimento-fluxo.git"
APP_DIR="/opt/atendimento-fluxo"

echo "==> Instalando Docker..."
if ! command -v docker &>/dev/null; then
  apt-get update -y
  apt-get install -y ca-certificates curl gnupg
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
    https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    | tee /etc/apt/sources.list.d/docker.list > /dev/null
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
  systemctl enable docker
  systemctl start docker
  echo "Docker instalado com sucesso."
else
  echo "Docker já instalado, pulando."
fi

echo "==> Clonando repositório em $APP_DIR..."
if [ ! -d "$APP_DIR" ]; then
  git clone "$REPO_URL" "$APP_DIR"
else
  echo "Diretório já existe, pulando clone."
fi

cd "$APP_DIR"

echo "==> Criando arquivo .env..."
if [ ! -f .env ]; then
  cp .env.example .env

  # Gera JWT_SECRET_KEY automaticamente
  JWT_SECRET=$(openssl rand -hex 32)
  sed -i "s/troque_esta_chave_secreta/$JWT_SECRET/" .env

  echo ""
  echo "======================================================"
  echo "  ATENÇÃO: Edite o arquivo .env antes de continuar!"
  echo "  Preencha: POSTGRES_PASSWORD, OPENROUTER_API_KEY,"
  echo "            CHATWOOT_URL, CHATWOOT_TOKEN, etc."
  echo "  Comando: nano $APP_DIR/.env"
  echo "======================================================"
  echo ""
  read -p "Pressione ENTER depois de editar o .env para continuar..."
fi

echo "==> Configurando SSH para deploy automático pelo GitHub Actions..."
echo ""
echo "  1. Gere um par de chaves SSH dedicado para o deploy:"
echo "     ssh-keygen -t ed25519 -C 'github-deploy' -f ~/.ssh/github_deploy -N ''"
echo ""
echo "  2. Adicione a chave pública ao authorized_keys:"
echo "     cat ~/.ssh/github_deploy.pub >> ~/.ssh/authorized_keys"
echo ""
echo "  3. Adicione a chave privada como secret no GitHub:"
echo "     Repositório → Settings → Secrets → Actions → New secret"
echo "     - VPS_HOST  = $(curl -s ifconfig.me 2>/dev/null || echo 'SEU_IP')"
echo "     - VPS_USER  = $(whoami)"
echo "     - VPS_SSH_KEY = (conteúdo de ~/.ssh/github_deploy)"
echo "     - VPS_PORT  = 22"
echo ""
read -p "Pressione ENTER para subir os containers agora..."

echo "==> Fazendo build e subindo os containers..."
docker compose build --no-cache
docker compose up -d

echo ""
echo "==> Setup concluído!"
echo "  Acesse: http://$(curl -s ifconfig.me 2>/dev/null || echo 'SEU_IP')"
echo ""
echo "  A partir de agora, cada push na branch main fará deploy automático."
