# 🚀 Nexus DM Platform

<div align="center">

![Version](https://img.shields.io/badge/version-5.0.1-blue?style=for-the-badge)
![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Discord](https://img.shields.io/badge/Discord.py-2.x-5865F2?style=for-the-badge&logo=discord&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-aio-003B57?style=for-the-badge&logo=sqlite&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green?style=for-the-badge)
![Status](https://img.shields.io/badge/status-production%20ready-success?style=for-the-badge)

<img src="https://i.imgur.com/placeholder.png" alt="Nexus DM Platform Banner" width="800"/>

**Enterprise Discord Campaign Engine — Automatize seu onboarding com precisão cirúrgica.**

[Features](#-features) • [Instalação](#-instalação) • [Uso](#-uso) • [Arquitetura](#-arquitetura) • [Contribuindo](#-contribuindo) • [Licença](#-licença)

</div>

---

## 📖 Sobre

A **Nexus DM Platform** é uma engine de campanhas Discord de nível empresarial, projetada para automatizar o envio de mensagens diretas (DM) em massa com controle granular, rastreamento completo e resiliência de produção.

Construída sobre uma arquitetura assíncrona com workers concorrentes, rate limiting inteligente, idempotência por hash e banco de dados SQLite, esta plataforma transforma o que seria um simples script em um **sistema SaaS-ready**.

### ✨ Destaques

```python
# De um script simples para uma engine profissional
┌─────────────────────────────────────────┐
│  🏗️  Workers Concorrentes (1-5)        │
│  ⏱️  Rate Limiting Inteligente          │
│  🔐  Idempotência SHA256                │
│  💾  Persistência SQLite + JSON         │
│  📊  Dashboard em Tempo Real            │
│  📝  Templates de Mensagem              │
│  📋  Histórico de Campanhas             │
│  📁  Exportação CSV                     │
│  🔍  Filtro por Data de Entrada         │
│  🎯  Variáveis Dinâmicas                │
│  ⚡  Estimativa de Tempo (ETA)          │
│  🔄  Retry Automático                   │
│  🛡️  Anti-Duplicação Multi-nível       │
└─────────────────────────────────────────┘
```

---

## 🎯 Features

### 📬 Motor de Envio

| Feature | Descrição |
|---------|-----------|
| **Workers Paralelos** | De 1 a 5 workers concorrentes processando a fila |
| **Rate Limiting** | Janela deslizante de 5 DMs/5s com burst detection |
| **Idempotência** | Hash SHA256 (`user_id + campaign_id + message`) + UNIQUE INDEX |
| **Retry Automático** | Até 3 tentativas com backoff em rate limits |
| **Variáveis Dinâmicas** | `{user}`, `{mention}`, `{server}`, `{register_channel}` |

### 📊 Monitoramento

| Feature | Descrição |
|---------|-----------|
| **Dashboard ao Vivo** | Atualização a cada 3s com taxa de sucesso, velocidade e ETA |
| **Histórico Detalhado** | Todas as campanhas com estatísticas e timestamps |
| **Estimativa de Tempo** | Cálculo automático: `(membros / workers) × delay` |
| **Exportação CSV** | Compatível com Excel/LibreOffice |

### 🎨 Gerenciamento

| Feature | Descrição |
|---------|-----------|
| **Templates de Mensagem** | Salve e carregue templates reutilizáveis |
| **Filtro por Data** | Últimas 24h, 7 dias, 30 dias ou todos |
| **Preview Renderizado** | Visualize a mensagem exata com dados fictícios |
| **Confirmação de Campanha** | Review completo antes de iniciar |
| **Configuração Persistente** | Token, IDs e preferências salvos em JSON |

---

## 🏗️ Arquitetura

```
┌──────────────────────────────────────────────────────────────┐
│                    NEXUS DM PLATFORM V5                      │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────────┐   │
│  │   CLI    │───▶│ Campaign │───▶│    Worker Pool       │   │
│  │  Menu    │    │ Manager  │    │  ┌────┐ ┌────┐ ┌────┐│   │
│  └──────────┘    └──────────┘    │  │ W1 │ │ W2 │ │ W3 ││   │
│                                  │  └────┘ └────┘ └────┘│   │
│                                  └──────────┬───────────┘   │
│                                             │               │
│  ┌──────────┐    ┌──────────┐    ┌──────────▼───────────┐   │
│  │ Config   │    │ Templates│    │    Rate Limiter       │   │
│  │ Manager  │    │ Manager  │    │  (Janela Deslizante)  │   │
│  └──────────┘    └──────────┘    └──────────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                    DATABASE LAYER                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │
│  │  │  Campaigns  │  │ Deliveries  │  │ Idempotency │   │   │
│  │  │   Table     │  │   Table     │  │   Hashes    │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘   │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                 DISCORD API LAYER                     │   │
│  │         discord.py + asyncio + aiosqlite             │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

---

## 📦 Instalação

### Pré-requisitos

- **Python 3.11+**
- **pip** atualizado
- **Token** de bot Discord com intents privilegiados

### 1. Clone o repositório

```bash
git clone https://github.com/LucasDesignerF/mass-dm.git
cd mass-dm
```

### 2. Instale as dependências

```bash
pip install -r requirements.txt
```

**requirements.txt:**
```text
discord.py>=2.3.0
aiosqlite>=0.19.0
colorama>=0.4.6
```

### 3. Configure o bot no Discord Developer Portal

1. Acesse [discord.com/developers/applications](https://discord.com/developers/applications)
2. Crie uma aplicação e um bot
3. Habilite **Server Members Intent**
4. Copie o token

### 4. Execute

```bash
python main.py
```

---

## 🎮 Uso

### Menu Principal

```
⚙️  Token:✅ Guild:✅ Role:✅ Channel:✅ Delay:15s Workers:3

📋 Campanhas recentes:
  ✅ Registro Junho - 128/128 enviados
  🔄 Onboarding Q2 - 45/200 enviados

📌 MENU PRINCIPAL V5

1  - Definir Token
2  - Definir Guild ID
3  - Definir Role ID
4  - Definir Canal de Registro
5  - Gerenciar Mensagem
6  - Configurar Workers (1-5)
7  - Configurar Filtro de Data
8  - Criar campanha
9  - Iniciar campanha
10 - Dashboard ao vivo
11 - Parar campanha
12 - Resumir campanha
13 - Estatísticas
14 - Exportar campanha (CSV)
15 - Histórico detalhado
16 - Deletar campanha
17 - Salvar configuração
0  - Sair
```

### Fluxo de Campanha

```bash
# 1. Configure o token, guild, role e canal
1 > [cole seu token]
2 > [ID do servidor]
3 > [ID do cargo]
4 > [ID do canal de registro]

# 2. Configure a mensagem
5 > 3 > [digite sua mensagem com variáveis]

# 3. Crie a campanha
8 > [nome da campanha]

# 4. Inicie com confirmação
9 > [número da campanha]

# Resultado:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✅ DM enviada para Usuario#1234
  ❌ DM fechada para Usuario#5678
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### Variáveis de Mensagem

| Variável | Descrição | Exemplo |
|----------|-----------|---------|
| `{user}` | Nome do usuário | `João Silva` |
| `{mention}` | Menção do usuário | `@João Silva` |
| `{server}` | Nome do servidor | `Nexus Community` |
| `{register_channel}` | Canal de registro | `#registro` |

### Dashboard ao Vivo

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Campanha: Registro Junho
Status: ATIVA

Enviados: 84
Falhas: 3
Pendentes: 41

Taxa de sucesso: 96.5%
Velocidade: 3.8 DM/min
Tempo decorrido: 22min
Tempo restante: 11min
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## 📊 Schema do Banco

### Tabela `campaigns`

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | TEXT PK | UUID da campanha |
| `name` | TEXT | Nome da campanha |
| `guild_id` | INTEGER | ID do servidor |
| `role_id` | INTEGER | ID do cargo |
| `register_channel_id` | INTEGER | ID do canal de registro |
| `message` | TEXT | Mensagem template |
| `status` | TEXT | draft/active/paused/completed/failed |
| `delay` | INTEGER | Delay entre envios (s) |
| `workers` | INTEGER | Número de workers |
| `join_filter` | TEXT | Filtro de data (24h/7d/30d/all) |
| `total_members` | INTEGER | Total de membros processados |
| `estimated_duration` | REAL | Duração estimada (s) |
| `started_at` | TIMESTAMP | Início da campanha |
| `finished_at` | TIMESTAMP | Término da campanha |

### Tabela `deliveries`

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id` | TEXT PK | UUID da entrega |
| `campaign_id` | TEXT FK | Campanha relacionada |
| `user_id` | INTEGER | ID do usuário |
| `username` | TEXT | Nome do usuário |
| `message_hash` | TEXT UNIQUE | Hash de idempotência |
| `status` | TEXT | pending/sent/failed/rate_limited/dm_closed |
| `retry_count` | INTEGER | Tentativas de reenvio |
| `error` | TEXT | Mensagem de erro |

---

## 🔧 Configuração Avançada

### Variáveis de Ambiente

```bash
# Sharding (para bots grandes)
SHARD_COUNT=2
SHARD_IDS=0,1
```

### Arquivos de Configuração

```
mass-dm/
├── main.py              # Engine principal
├── message.txt          # Template de mensagem
├── config.json          # Configuração persistente
├── mass_dm.db           # Banco de dados SQLite
├── mass_dm.log          # Logs do sistema
├── templates/           # Templates salvos
│   ├── boas_vindas.txt
│   └── evento.txt
└── exports/             # CSVs exportados
    └── campaign_export_20260611_221030.csv
```

---

## 🤝 Contribuindo

Contribuições são bem-vindas! Siga o fluxo:

```bash
# 1. Fork o repositório
# 2. Crie uma branch
git checkout -b feature/nova-feature

# 3. Commit suas mudanças
git commit -m "feat: nova feature incrível"

# 4. Push para a branch
git push origin feature/nova-feature

# 5. Abra um Pull Request
```

### Convenções de Commit

- `feat:` Nova funcionalidade
- `fix:` Correção de bug
- `docs:` Documentação
- `refactor:` Refatoração de código
- `perf:` Melhoria de performance

---

## 📈 Roadmap

- [ ] **v5.1** — FastAPI Dashboard Web
- [ ] **v5.2** — Agendamento de Campanhas (Cron)
- [ ] **v5.3** — Suporte a Múltiplos Servidores
- [ ] **v5.4** — WebSocket para Progresso em Tempo Real
- [ ] **v6.0** — Painel Administrativo Completo

---

## 👤 Autor

<div align="center">

<img src="https://github.com/LucasDesignerF.png" width="100" style="border-radius: 50%;">

**Lucas Designer**

[![GitHub](https://img.shields.io/badge/GitHub-LucasDesignerF-181717?style=flat-square&logo=github)](https://github.com/LucasDesignerF)

**Nexus Platforms** — Transformando automação Discord em produto.

</div>

---

## 📄 Licença

MIT © 2026 [Lucas Designer](https://github.com/LucasDesignerF)

---

<div align="center">

**⭐ Se este projeto foi útil, deixe uma estrela!**

Feito com ❤️ pela Nexus Platforms

</div>