# Deploy do Chatwoot no EasyPanel (VPS)

Este guia configura um deploy funcional do Chatwoot no EasyPanel usando o `docker/Dockerfile` deste repositório.

## Arquitetura recomendada

Crie **2 apps** no mesmo projeto do EasyPanel:

1. `chatwoot-web` (Rails)
2. `chatwoot-worker` (Sidekiq)

Ambos devem usar:

- **Source**: Git repository
- **Dockerfile path**: `docker/Dockerfile`
- **Build context**: `.`
- **Porta do web**: `3000`

> O `web` e o `worker` usam o mesmo build e só mudam o comando de start.

## Variáveis obrigatórias

Use as variáveis do arquivo `deployment/easypanel/.env.easypanel.example`.

Mínimo obrigatório para subir:

- `RAILS_ENV=production`
- `NODE_ENV=production`
- `SECRET_KEY_BASE`
- `FRONTEND_URL`
- `FORCE_SSL=true`
- `ENABLE_ACCOUNT_SIGNUP=false`
- `REDIS_URL`
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DATABASE`
- `POSTGRES_USERNAME`
- `POSTGRES_PASSWORD`

## Configuração pronta (Fluxo Digital Tech)

O arquivo `deployment/easypanel/.env.easypanel.example` já foi preenchido com os dados enviados para:

- PostgreSQL remoto (`server.fluxodigitaltech.com.br`)
- Redis remoto (`server.fluxodigitaltech.com.br`)
- SMTP Hostinger (`smtp.hostinger.com`)

> Importante: a `DATABASE_URL` no arquivo já está com senha URL-encoded para suportar `!`, `@` e `#`.

## Comandos por app

### App `chatwoot-web`

**Start command**

```bash
bundle exec rails ip_lookup:setup && bundle exec rails server -p 3000 -e production -b 0.0.0.0
```

### App `chatwoot-worker`

**Start command**

```bash
bundle exec rails ip_lookup:setup && bundle exec sidekiq -C config/sidekiq.yml
```

## Migrações (release)

Após o primeiro deploy (e em toda atualização), rode no console do app web:

```bash
POSTGRES_STATEMENT_TIMEOUT=600s bundle exec rails db:chatwoot_prepare
```

## Domínio e SSL

1. Aponte o DNS para a VPS.
2. No EasyPanel, vincule o domínio ao `chatwoot-web`.
3. Ative SSL (Let's Encrypt).
4. Garanta que `FRONTEND_URL` use `https://`.

## Checklist de validação

1. `chatwoot-web` em status **running**.
2. `chatwoot-worker` em status **running**.
3. Comando de migração executado sem erro.
4. Acesso ao domínio retornando tela de login.
5. Criação de conversa/tarefa assíncrona funcionando (confirma Sidekiq/Redis).
