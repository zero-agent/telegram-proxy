#!/usr/bin/env python3
"""
0agent Telegram Proxy (WebSocket Version)

Maintains WebSocket connections to Clawdbot nodes and bridges Telegram messages.
Uses Clawdbot's native WebSocket RPC protocol.
"""

import os
import sys
import json
import time
import socket
import logging
import asyncio
import uuid
from typing import Optional, Dict, Any
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# Try to import websockets
try:
    import websockets
except ImportError:
    print("ERROR: websockets library required. Install with: pip install websockets")
    sys.exit(1)

# Configuration
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "683399551")
DOMAIN = os.environ.get("LEADER_DOMAIN", "0agent.ai")
CF_TOKEN = os.environ.get("CF_TOKEN", "")
CF_ZONE_ID = os.environ.get("CF_ZONE_ID", "")

# Node configuration
NODES = {
    "node-1": {
        "ip": os.environ.get("NODE1_IP", "64.176.199.40"),
        "port": int(os.environ.get("NODE1_PORT", "18789")),
        "token": os.environ.get("NODE1_TOKEN", ""),
    },
    "node-2": {
        "ip": os.environ.get("NODE2_IP", "45.63.18.27"),
        "port": int(os.environ.get("NODE2_PORT", "18789")),
        "token": os.environ.get("NODE2_TOKEN", ""),
    }
}

# Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("proxy-ws")

# State
last_update_id = 0
leader_cache = {"ip": None, "ts": 0}
LEADER_CACHE_TTL = 30

# Connection state per node
class NodeConnection:
    def __init__(self, name: str):
        self.name = name
        self.ws: Optional[Any] = None
        self.connected = False
        self.pending: Dict[str, asyncio.Future] = {}
        self.request_counter = 0
        self.lock = asyncio.Lock()

node_connections: Dict[str, NodeConnection] = {}


def telegram_api(method: str, data: Optional[Dict] = None) -> Dict:
    """Call Telegram Bot API (sync)."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    
    if data:
        req = Request(url, data=json.dumps(data).encode(),
                     headers={"Content-Type": "application/json"})
    else:
        req = Request(url)
    
    try:
        with urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except (URLError, HTTPError) as e:
        log.error(f"Telegram API error: {e}")
        return {"ok": False, "error": str(e)}


async def telegram_api_async(method: str, data: Optional[Dict] = None) -> Dict:
    """Call Telegram Bot API (async - runs in thread pool to not block event loop)."""
    return await asyncio.to_thread(telegram_api, method, data)


def send_telegram(chat_id: str, text: str, parse_mode: str = "Markdown") -> Dict:
    """Send a message via Telegram."""
    return telegram_api("sendMessage", {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode
    })


def send_typing(chat_id: str) -> Dict:
    """Send typing indicator to Telegram."""
    return telegram_api("sendChatAction", {
        "chat_id": chat_id,
        "action": "typing"
    })


def get_leader_ip() -> Optional[str]:
    """Get current leader IP from Cloudflare API or DNS."""
    now = time.time()
    
    if leader_cache["ip"] and (now - leader_cache["ts"]) < LEADER_CACHE_TTL:
        return leader_cache["ip"]
    
    # Try Cloudflare API first
    if CF_TOKEN and CF_ZONE_ID:
        try:
            url = f"https://api.cloudflare.com/client/v4/zones/{CF_ZONE_ID}/dns_records?name={DOMAIN}&type=A"
            req = Request(url, headers={"Authorization": f"Bearer {CF_TOKEN}"})
            with urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
                if data.get("success") and data.get("result"):
                    ip = data["result"][0].get("content")
                    if ip:
                        leader_cache["ip"] = ip
                        leader_cache["ts"] = now
                        return ip
        except Exception as e:
            log.warning(f"Cloudflare API failed: {e}")
    
    # Fallback to DNS
    try:
        ip = socket.gethostbyname(DOMAIN)
        leader_cache["ip"] = ip
        leader_cache["ts"] = now
        return ip
    except socket.gaierror as e:
        log.error(f"DNS lookup failed: {e}")
        return leader_cache.get("ip")


def get_leader_node() -> Optional[str]:
    """Get the node name that is currently the leader."""
    leader_ip = get_leader_ip()
    if not leader_ip:
        return None
    for name, node in NODES.items():
        if node["ip"] == leader_ip:
            return name
    return None


def get_backup_node() -> Optional[str]:
    """Get the node that is NOT the leader."""
    leader = get_leader_node()
    for name in NODES:
        if name != leader:
            return name
    return None


async def do_clawdbot_handshake(ws: Any, token: str) -> bool:
    """Perform Clawdbot WebSocket handshake."""
    try:
        # Wait for challenge
        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        challenge = json.loads(raw)
        
        if challenge.get("type") != "event" or challenge.get("event") != "connect.challenge":
            log.error(f"Expected connect.challenge, got: {challenge}")
            return False
        
        nonce = challenge.get("payload", {}).get("nonce", "")
        log.debug(f"Received challenge nonce: {nonce[:20]}...")
        
        # Send connect request (use "cli" as client.id and mode)
        connect_req = {
            "type": "req",
            "id": str(uuid.uuid4()),
            "method": "connect",
            "params": {
                "minProtocol": 3,
                "maxProtocol": 3,
                "client": {
                    "id": "cli",
                    "version": "1.0.0",
                    "platform": "linux",
                    "mode": "cli"
                },
                "role": "operator",
                "scopes": ["operator.read", "operator.write"],
                "caps": [],
                "commands": [],
                "permissions": {},
                "auth": {"token": token},
                "locale": "en-US",
                "userAgent": "telegram-proxy/1.0.0",
            }
        }
        
        await ws.send(json.dumps(connect_req))
        
        # Wait for hello-ok
        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        response = json.loads(raw)
        
        if response.get("type") == "res" and response.get("ok"):
            log.info("Handshake successful")
            return True
        else:
            log.error(f"Handshake failed: {response}")
            return False
            
    except asyncio.TimeoutError:
        log.error("Handshake timeout")
        return False
    except Exception as e:
        log.error(f"Handshake error: {e}")
        return False


async def connect_to_node(node_name: str) -> bool:
    """Establish WebSocket connection to a node with handshake."""
    if node_name not in NODES:
        return False
    
    node = NODES[node_name]
    conn = node_connections.get(node_name)
    if not conn:
        conn = NodeConnection(node_name)
        node_connections[node_name] = conn
    
    async with conn.lock:
        if conn.ws and conn.connected:
            return True
        
        url = f"ws://{node['ip']}:{node['port']}"
        token = node.get("token", "")
        
        try:
            log.info(f"Connecting to {node_name} at {url}...")
            ws = await websockets.connect(
                url,
                ping_interval=30,
                ping_timeout=10,
            )
            
            # Perform handshake
            if await do_clawdbot_handshake(ws, token):
                conn.ws = ws
                conn.connected = True
                log.info(f"Connected to {node_name}")
                
                # Start listener
                asyncio.create_task(listen_to_node(conn))
                return True
            else:
                await ws.close()
                return False
                
        except Exception as e:
            log.error(f"Failed to connect to {node_name}: {e}")
            return False


async def listen_to_node(conn: NodeConnection):
    """Listen for messages from a node."""
    try:
        async for raw in conn.ws:
            try:
                msg = json.loads(raw)
                await handle_node_message(conn, msg)
            except json.JSONDecodeError:
                log.warning(f"Invalid JSON from {conn.name}")
    except websockets.ConnectionClosed as e:
        log.warning(f"Connection to {conn.name} closed: {e}")
    except Exception as e:
        log.error(f"Error listening to {conn.name}: {e}")
    finally:
        conn.connected = False
        conn.ws = None


async def handle_node_message(conn: NodeConnection, msg: Dict):
    """Handle a message from a node."""
    msg_type = msg.get("type")
    msg_id = msg.get("id")
    
    log.debug(f"Node message: type={msg_type} id={msg_id} keys={list(msg.keys())}")
    
    # Response to a pending request
    if msg_type == "res" and msg_id and msg_id in conn.pending:
        conn.pending[msg_id].set_result(msg)
        return
    
    # Event from the node (e.g., assistant message)
    if msg_type == "event":
        event = msg.get("event", "")
        payload = msg.get("payload", {})
        
        # Log agent events with stream info
        if event == "agent":
            stream = payload.get("stream", "") if isinstance(payload, dict) else ""
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            log.info(f"Agent event: stream={stream}, data_keys={list(data.keys()) if isinstance(data, dict) else 'n/a'}")
        else:
            log.info(f"Event received: {event}, payload keys: {list(payload.keys()) if isinstance(payload, dict) else 'n/a'}")
        
        # Handle chat messages from assistant
        if event == "chat":
            message = payload.get("message", {})
            state = payload.get("state", {})
            session_key = payload.get("sessionKey", "")
            
            # Handle case where message or state might be strings
            if not isinstance(message, dict):
                log.debug(f"message is not dict: {type(message)}")
                return
            if not isinstance(state, dict):
                state = {}
            
            # Check if this is a complete message (state.status == "complete" or has assistant content)
            status_val = state.get("status", "")
            role = message.get("role", "")
            content = message.get("content", [])
            
            log.info(f"Chat event: status={status_val}, role={role}, content_items={len(content)}, state={state}, msg_keys={list(message.keys())}")
            
            # Extract text from assistant messages
            if role == "assistant" and content:
                text_parts = []
                for item in content:
                    if isinstance(item, str):
                        text_parts.append(item)
                    elif isinstance(item, dict) and item.get("type") == "text":
                        text_parts.append(item.get("text", ""))
                
                text = "".join(text_parts)
                stop_reason = message.get("stopReason", "")
                # Send when status is complete OR when we have a stopReason
                if text and (status_val == "complete" or stop_reason):
                    log.info(f"Message complete: stopReason={stop_reason}, status={status_val}")
                    # Full message - send to Telegram
                    leader = get_leader_node()
                    is_leader = conn.name == leader
                    status_icon = "🟢" if is_leader else "⚪"
                    label = f"[{conn.name} {status_icon}]"
                    log.info(f"Sending to Telegram: {text[:50]}...")
                    send_telegram(ADMIN_CHAT_ID, f"{label}\n{text}", parse_mode="")
        
        # Handle agent events for stream completion
        if event == "agent":
            stream = payload.get("stream", "") if isinstance(payload, dict) else ""
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            
            # Check for lifecycle end (completion)
            if stream == "lifecycle" and isinstance(data, dict):
                phase = data.get("phase", "")
                log.info(f"Lifecycle event: phase={phase}")
            
            # Capture assistant text when expecting a response
            if stream == "assistant" and isinstance(data, dict):
                text = data.get("text", "")
                expecting = getattr(conn, 'expecting_response', False)
                expect_time = getattr(conn, 'expect_time', 0)
                now = time.time()
                
                # Capture if we're expecting and within 60s window
                if expecting and (now - expect_time < 60) and text:
                    conn.last_assistant_text = text
                    
                    # Send typing indicator
                    last_typing = getattr(conn, 'last_typing_time', 0)
                    if now - last_typing > 4:
                        send_typing(ADMIN_CHAT_ID)
                        conn.last_typing_time = now
        
        # Send on lifecycle end when expecting a response
        if event == "agent":
            stream = payload.get("stream", "") if isinstance(payload, dict) else ""
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            
            if stream == "lifecycle" and isinstance(data, dict):
                phase = data.get("phase", "")
                expecting = getattr(conn, 'expecting_response', False)
                expect_time = getattr(conn, 'expect_time', 0)
                now = time.time()
                has_text = hasattr(conn, 'last_assistant_text') and conn.last_assistant_text
                
                # Debug: log state on lifecycle end
                if phase == "end":
                    log.info(f"Lifecycle end check: expecting={expecting}, time_ok={now - expect_time < 60}, has_text={has_text}, conn={conn.name}")
                
                # Send if expecting and within window
                if phase == "end" and expecting and (now - expect_time < 60):
                    if hasattr(conn, 'last_assistant_text') and conn.last_assistant_text:
                        text = conn.last_assistant_text
                        leader = get_leader_node()
                        is_leader = conn.name == leader
                        status_icon = "🟢" if is_leader else "⚪"
                        label = f"[{conn.name} {status_icon}]"
                        log.info(f"Sending response: {text[:50]}...")
                        result = send_telegram(ADMIN_CHAT_ID, f"{label}\n{text}", parse_mode="")
                        log.info(f"send_telegram result: ok={result.get('ok')}, error={result.get('error', 'none')}")
                        conn.last_assistant_text = None
                        conn.expecting_response = False


async def send_rpc(conn: NodeConnection, method: str, params: Dict, timeout: float = 60) -> Optional[Dict]:
    """Send an RPC request to a node."""
    if not conn.connected or not conn.ws:
        log.error(f"send_rpc: not connected to {conn.name}")
        return None
    
    conn.request_counter += 1
    req_id = f"proxy-{conn.request_counter}"
    
    request = {
        "type": "req",
        "id": req_id,
        "method": method,
        "params": params,
    }
    
    future = asyncio.get_event_loop().create_future()
    conn.pending[req_id] = future
    
    try:
        log.info(f"Sending RPC {method} to {conn.name} (id={req_id})")
        await conn.ws.send(json.dumps(request))
        log.info(f"RPC sent, waiting for response...")
        response = await asyncio.wait_for(future, timeout=timeout)
        log.info(f"RPC response: ok={response.get('ok')}")
        return response
    except asyncio.TimeoutError:
        log.error(f"RPC timeout: {method} to {conn.name}")
        return None
    except websockets.ConnectionClosed as e:
        log.warning(f"Connection closed during RPC: {e}")
        conn.connected = False
        conn.ws = None
        return None
    except Exception as e:
        log.error(f"RPC error: {e}")
        # Mark connection as dead so next attempt reconnects
        conn.connected = False
        conn.ws = None
        return None
    finally:
        conn.pending.pop(req_id, None)


async def send_message_to_node(node_name: str, text: str, sender: str, retries: int = 2) -> bool:
    """Send a chat message to a node with retry logic."""
    log.info(f"send_message_to_node: {node_name}, text={text[:30]}...")
    
    for attempt in range(retries + 1):
        # Ensure connection
        conn = node_connections.get(node_name)
        if not conn or not conn.connected:
            log.info(f"Not connected to {node_name}, attempting connect (attempt {attempt + 1}/{retries + 1})...")
            if not await connect_to_node(node_name):
                log.error(f"Failed to connect to {node_name}")
                if attempt < retries:
                    await asyncio.sleep(1)
                    continue
                return False
            conn = node_connections.get(node_name)
        
        if not conn or not conn.connected:
            log.error(f"Still not connected to {node_name}")
            if attempt < retries:
                await asyncio.sleep(1)
                continue
            return False
        
        # Use WebSocket chat.send - simple and it works
        session_key = f"telegram:dm:{ADMIN_CHAT_ID}"
        idempotency_key = f"tg-{int(time.time() * 1000)}-{conn.request_counter}"
        
        log.info(f"Sending via chat.send to {session_key}")
        
        # Mark that we're expecting a response
        conn.expecting_response = True
        conn.expect_time = time.time()
        log.info(f"Set expecting_response=True on conn={conn.name}")
        
        result = await send_rpc(conn, "chat.send", {
            "sessionKey": session_key,
            "message": text,
            "idempotencyKey": idempotency_key,
        })
        
        if result and result.get("ok"):
            log.info(f"chat.send succeeded!")
            return True
        
        # If send failed due to connection issue, retry
        if attempt < retries:
            log.info(f"Retrying in 1s...")
            await asyncio.sleep(1)
            continue
    
    log.error(f"All {retries + 1} attempts failed for {node_name}")
    return False


def parse_routing_prefix(text: str) -> tuple[Optional[str], str]:
    """Parse routing prefix from message text."""
    text = text.strip()
    
    if text.startswith(("1 ", "1:")):
        return "node-1", text[2:].strip()
    if text.startswith(("2 ", "2:")):
        return "node-2", text[2:].strip()
    if text.upper().startswith(("B ", "B:")):
        return get_backup_node(), text[2:].strip()
    
    return None, text


async def handle_telegram_message(message: Dict):
    """Process an incoming Telegram message."""
    chat_id = str(message.get("chat", {}).get("id", ""))
    text = message.get("text", "")
    from_user = message.get("from", {}).get("username") or message.get("from", {}).get("first_name", "unknown")
    
    if not text or chat_id != ADMIN_CHAT_ID:
        return
    
    log.info(f"Received from {from_user}: {text[:50]}...")
    
    # Send typing indicator immediately so user knows we got the message
    send_typing(chat_id)
    
    # Parse routing
    target_node, clean_text = parse_routing_prefix(text)
    if not target_node:
        target_node = get_leader_node()
    
    if not target_node:
        send_telegram(chat_id, "⚠️ Could not determine target node")
        return
    
    # Send to node
    success = await send_message_to_node(target_node, clean_text, from_user)
    
    if not success:
        leader = get_leader_node()
        is_leader = target_node == leader
        status = "🟢 leader" if is_leader else "⚪ backup"
        send_telegram(chat_id, f"⚠️ Failed to reach {target_node} ({status})")


async def poll_telegram():
    """Poll Telegram for updates."""
    global last_update_id
    
    log.info("Starting Telegram polling...")
    
    while True:
        try:
            params = {"offset": last_update_id + 1, "timeout": 30}
            result = await telegram_api_async("getUpdates", params)
            
            if result.get("ok"):
                for update in result.get("result", []):
                    update_id = update.get("update_id", 0)
                    if update_id > last_update_id:
                        last_update_id = update_id
                        
                        msg = update.get("message", {})
                        if msg:
                            await handle_telegram_message(msg)
            else:
                log.error(f"getUpdates failed: {result}")
                await asyncio.sleep(5)
                
        except Exception as e:
            log.error(f"Polling error: {e}")
            await asyncio.sleep(5)


async def maintain_connections():
    """Periodically ensure connections to all nodes."""
    while True:
        await asyncio.sleep(30)
        for node_name in NODES:
            conn = node_connections.get(node_name)
            if not conn or not conn.connected:
                log.info(f"Reconnecting to {node_name}...")
                await connect_to_node(node_name)


async def handle_telegram_webhook(writer: asyncio.StreamWriter, body: bytes, headers: dict):
    """Handle incoming Telegram webhook - inject message via WebSocket RPC."""
    log.info(f"Received Telegram webhook, body size: {len(body)}")
    
    try:
        update = json.loads(body.decode()) if body else {}
        log.info(f"Webhook update: {json.dumps(update)[:200]}...")
    except json.JSONDecodeError:
        log.error("Invalid JSON in webhook")
        writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
        await writer.drain()
        return
    
    # Extract message info from Telegram update
    message = update.get("message") or update.get("edited_message") or {}
    chat_id = message.get("chat", {}).get("id")
    text = message.get("text", "")
    from_user = message.get("from", {})
    
    if not chat_id or not text:
        log.info("Webhook has no actionable message, acknowledging")
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
        await writer.drain()
        return
    
    # Determine leader node
    leader = get_leader_node()
    if not leader:
        log.error("No leader node available!")
        writer.write(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
        await writer.drain()
        return
    
    log.info(f"Injecting message from {from_user.get('username', chat_id)} to {leader} via WebSocket")
    
    # Use WebSocket RPC to send message to leader node
    sender = from_user.get("username") or from_user.get("first_name") or str(chat_id)
    success = await send_message_to_node(leader, text, sender)
    
    if success:
        log.info("Message injected successfully")
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
    else:
        log.error("Failed to inject message")
        writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
    
    await writer.drain()


async def handle_telegram_api_mock(writer: asyncio.StreamWriter, method: str, body: bytes, headers: dict):
    """Mock Telegram Bot API - forward calls to real Telegram."""
    log.info(f"Telegram API mock: {method}")
    
    try:
        data = json.loads(body.decode()) if body else {}
    except json.JSONDecodeError:
        data = {}
    
    # Methods we forward to real Telegram
    forward_methods = ['sendMessage', 'sendChatAction', 'editMessageText', 
                       'deleteMessage', 'sendPhoto', 'sendDocument', 'sendSticker',
                       'answerCallbackQuery', 'getChat', 'getChatMember']
    
    if method in forward_methods:
        # Forward to real Telegram API
        result = telegram_api(method, data)
        response = json.dumps(result)
        log.info(f"Forwarded {method}, result ok={result.get('ok')}")
        writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(response)}\r\n\r\n{response}".encode())
    
    elif method == 'getMe':
        # Return bot info
        result = telegram_api('getMe')
        response = json.dumps(result)
        writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(response)}\r\n\r\n{response}".encode())
    
    elif method == 'setWebhook':
        # Acknowledge but don't actually set - proxy handles webhooks
        webhook_url = data.get('url', '')
        log.info(f"Node requested webhook: {webhook_url} (acknowledged, proxy handles webhooks)")
        response = json.dumps({"ok": True, "result": True, "description": "Webhook handled by proxy"})
        writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(response)}\r\n\r\n{response}".encode())
    
    elif method == 'deleteWebhook':
        # Acknowledge
        log.info("Node requested deleteWebhook (acknowledged)")
        response = json.dumps({"ok": True, "result": True, "description": "Webhook was deleted"})
        writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(response)}\r\n\r\n{response}".encode())
    
    elif method == 'getUpdates':
        # Return empty - we use webhooks to push updates
        log.info("Node called getUpdates (returning empty, using webhook mode)")
        response = json.dumps({"ok": True, "result": []})
        writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(response)}\r\n\r\n{response}".encode())
    
    elif method == 'getWebhookInfo':
        # Return info showing webhook is set
        response = json.dumps({
            "ok": True,
            "result": {
                "url": "https://proxy-managed",
                "has_custom_certificate": False,
                "pending_update_count": 0
            }
        })
        writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(response)}\r\n\r\n{response}".encode())
    
    else:
        # Unknown method - try forwarding anyway
        log.warning(f"Unknown Telegram method: {method}, attempting forward")
        result = telegram_api(method, data if data else None)
        response = json.dumps(result)
        writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(response)}\r\n\r\n{response}".encode())
    
    await writer.drain()


async def handle_http_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Handle incoming HTTP request from Clawdbot."""
    try:
        # Read request line
        request_line = await asyncio.wait_for(reader.readline(), timeout=10)
        if not request_line:
            return
        
        request_line = request_line.decode().strip()
        parts = request_line.split()
        if len(parts) < 2:
            return
        
        method, path = parts[0], parts[1]
        
        # Read headers
        headers = {}
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=10)
            if not line or line == b'\r\n':
                break
            line = line.decode().strip()
            if ':' in line:
                key, value = line.split(':', 1)
                headers[key.strip().lower()] = value.strip()
        
        # Read body if present
        body = b''
        content_length = int(headers.get('content-length', 0))
        if content_length > 0:
            body = await asyncio.wait_for(reader.read(content_length), timeout=10)
        
        # Handle incoming Telegram webhook (real Telegram sends updates here)
        if path in ['/webhook', '/telegram-webhook', f'/bot{BOT_TOKEN}']:
            await handle_telegram_webhook(writer, body, headers)
            return
        
        # Handle Telegram Bot API mock (nodes call this thinking it's api.telegram.org)
        if path.startswith('/bot') and '/' in path[4:]:
            # Extract token and method: /bot<token>/<method>
            path_parts = path[4:].split('/', 1)  # Remove '/bot' prefix
            if len(path_parts) == 2:
                token_from_path, tg_method = path_parts
                await handle_telegram_api_mock(writer, tg_method, body, headers)
                return
        
        # Handle /send endpoint (legacy)
            try:
                data = json.loads(body.decode())
                text = data.get('text', '')
                node = data.get('node', 'unknown')
                to_node = data.get('to_node')
                
                log.info(f"HTTP /send from {node}: {text[:50]}...")
                
                # Forward to Telegram
                leader = get_leader_node()
                is_leader = node == leader.replace('node-', '') if leader else False
                status = "🟢" if node == leader else "⚪"
                label = f"[{node} {status}]"
                
                result = send_telegram(ADMIN_CHAT_ID, f"{label}\n{text}", parse_mode="")
                
                response = json.dumps({"ok": result.get("ok", False)})
                writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(response)}\r\n\r\n{response}".encode())
            except Exception as e:
                log.error(f"Error handling /send: {e}")
                writer.write(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
        
        # Handle /health endpoint
        elif method == 'GET' and path == '/health':
            leader = get_leader_node()
            health = {
                "ok": True,
                "leader": leader,
                "leader_ip": get_leader_ip(),
                "nodes": list(NODES.keys()),
            }
            response = json.dumps(health)
            writer.write(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(response)}\r\n\r\n{response}".encode())
        
        else:
            writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
        
        await writer.drain()
    except asyncio.TimeoutError:
        pass
    except Exception as e:
        log.error(f"HTTP handler error: {e}")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except:
            pass


async def start_http_server(port: int = 8080):
    """Start HTTP server for receiving messages from Clawdbot."""
    server = await asyncio.start_server(handle_http_client, '0.0.0.0', port)
    log.info(f"HTTP server listening on port {port}")
    async with server:
        await server.serve_forever()


async def main():
    """Main entry point."""
    if not BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN not set")
        sys.exit(1)
    
    log.info("=" * 50)
    log.info("0agent Telegram Proxy (WebSocket) starting...")
    log.info(f"Admin chat ID: {ADMIN_CHAT_ID}")
    log.info(f"Leader domain: {DOMAIN}")
    log.info(f"Nodes: {list(NODES.keys())}")
    log.info("=" * 50)
    
    # Initial connections
    for node_name in NODES:
        await connect_to_node(node_name)
    
    # Start background tasks
    asyncio.create_task(maintain_connections())
    asyncio.create_task(start_http_server(8080))
    
    # Start Telegram polling
    await poll_telegram()


if __name__ == "__main__":
    asyncio.run(main())
