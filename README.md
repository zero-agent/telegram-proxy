# Telegram Proxy

A dedicated proxy server that handles all Telegram communication for the 0agent cluster.

## Why?

Direct Telegram bot connections are brittle for multi-node setups:
- Only one node can poll for updates at a time
- Handover requires stopping one node before starting another
- Race conditions during failover

The proxy solves this by:
- Being the **only** thing connected to Telegram
- Routing messages to the appropriate node based on DNS leader election
- Allowing explicit routing to specific nodes
- Enabling inter-node communication

## Architecture

```
┌─────────────┐
│  Telegram   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────┐
│    Telegram Proxy       │  ← Dedicated hardened server
│  - Polls Telegram       │
│  - Checks DNS for leader│
│  - Routes messages      │
└──────┬──────────────────┘
       │
       ├─────────────────────┐
       ▼                     ▼
┌─────────────┐       ┌─────────────┐
│   Node-1    │       │   Node-2    │
│  Clawdbot   │       │  Clawdbot   │
│  Receiver   │       │  Receiver   │
└─────────────┘       └─────────────┘
```

## Message Routing

### From 0age to nodes:

| Prefix | Routes to | Example |
|--------|-----------|---------|
| (none) | Current leader | `hello` → leader |
| `1 ` | Node-1 | `1 check status` |
| `2 ` | Node-2 | `2 check status` |
| `B ` | Backup (non-leader) | `B are you okay?` |

### From nodes to 0age:

Messages are labeled with source and status:
- `[node-1 🟢]` = from node-1, currently leader
- `[node-2 ⚪]` = from node-2, currently backup

### Inter-node messaging:

Nodes can message each other (visible to 0age):
- Prefix with `to: node-X` in the message
- Shows as `[node-1 → node-2]` in Telegram

## Deployment

### Create new proxy server:

```bash
# Set up secrets first
echo "your-bot-token" > secrets/telegram-bot-token
openssl rand -hex 32 > secrets/proxy-secret

# Deploy to BitLaunch
cd telegram-proxy
./deploy.sh create
```

### Attach to existing server:

```bash
./deploy.sh attach 1.2.3.4
```

### Check status:

```bash
./deploy.sh status
```

## Files

| File | Purpose |
|------|---------|
| `proxy.py` | Main proxy server |
| `node-receiver.py` | Node-side receiver script |
| `telegram-proxy.service` | Systemd service for proxy |
| `deploy.sh` | Deployment script |

## Configuration

### Proxy server (`/opt/telegram-proxy/.env`):

```bash
TELEGRAM_BOT_TOKEN=xxx
ADMIN_CHAT_ID=683399551
PROXY_SECRET=xxx
LEADER_DOMAIN=0agent.ai
HTTP_PORT=8080
NODE1_IP=64.176.199.40
NODE2_IP=45.63.18.27
```

### Node receiver (environment):

```bash
PROXY_URL=http://proxy-ip:8080
PROXY_SECRET=xxx
NODE_NAME=node-1
CLAWDBOT_URL=http://localhost:18789
RECEIVER_PORT=18790
```

## Security

The proxy server is hardened:
- Minimal attack surface (Python stdlib only)
- UFW firewall (only SSH + proxy port)
- fail2ban for SSH protection
- Systemd security options (NoNewPrivileges, ProtectSystem, etc.)
- HMAC signatures for node authentication

## API Endpoints

### Proxy server:

- `GET /health` - Health check, returns leader info
- `POST /send` - Receive message from node to forward to Telegram

### Node receiver:

- `GET /health` - Health check
- `POST /receive` - Receive message from proxy
- `POST /send` - Send message through proxy (called by Clawdbot)

## TODO

- [ ] Integrate node-receiver with Clawdbot's message injection API
- [ ] Add message queue for reliability
- [ ] Add metrics/monitoring
- [ ] Support for media messages
- [ ] Support for reactions
