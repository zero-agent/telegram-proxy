#!/usr/bin/env python3
"""
0agent Telegram Proxy

A dedicated proxy that handles all Telegram communication for the 0agent cluster.
Routes messages between 0age and the appropriate node based on DNS leader election.

Features:
- Routes messages to current leader (determined by DNS)
- Explicit routing with prefixes: 1, 2, B (backup)
- Labels outgoing messages with node source
- Inter-node messaging with "to: node-X" prefix
"""

import os
import sys
import json
import time
import socket
import logging
import hashlib
import hmac
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.parse import parse_qs
from typing import Optional, Dict, Any

# Configuration (from environment)
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "683399551")  # 0age
PROXY_SECRET = os.environ.get("PROXY_SECRET", "")  # For node auth
DOMAIN = os.environ.get("LEADER_DOMAIN", "0agent.ai")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))
CF_TOKEN = os.environ.get("CF_TOKEN", "")
CF_ZONE_ID = os.environ.get("CF_ZONE_ID", "")

# Node configuration
NODES = {
    "node-1": {
        "ip": os.environ.get("NODE1_IP", "64.176.199.40"),
        "port": int(os.environ.get("NODE1_PORT", "18789")),
        "name": "node-1"
    },
    "node-2": {
        "ip": os.environ.get("NODE2_IP", "45.63.18.27"),
        "port": int(os.environ.get("NODE2_PORT", "18789")),
        "name": "node-2"
    }
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("proxy")

# State
last_update_id = 0
leader_cache = {"ip": None, "ts": 0}
LEADER_CACHE_TTL = 30  # seconds


def telegram_api(method: str, data: Optional[Dict] = None) -> Dict:
    """Call Telegram Bot API."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    
    if data:
        req = Request(url, data=json.dumps(data).encode(), 
                     headers={"Content-Type": "application/json"})
    else:
        req = Request(url)
    
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except (URLError, HTTPError) as e:
        log.error(f"Telegram API error: {e}")
        return {"ok": False, "error": str(e)}


def send_telegram(chat_id: str, text: str, reply_to: Optional[int] = None) -> Dict:
    """Send a message via Telegram."""
    data = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_to:
        data["reply_to_message_id"] = reply_to
    return telegram_api("sendMessage", data)


def get_leader_ip() -> Optional[str]:
    """Get current leader IP from Cloudflare API or DNS."""
    now = time.time()
    
    # Use cache if fresh
    if leader_cache["ip"] and (now - leader_cache["ts"]) < LEADER_CACHE_TTL:
        return leader_cache["ip"]
    
    # Try Cloudflare API first (gets origin IP, not proxy IP)
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
                        log.debug(f"Leader IP (Cloudflare): {ip}")
                        return ip
        except Exception as e:
            log.warning(f"Cloudflare API failed: {e}")
    
    # Fallback to DNS (may return Cloudflare proxy IP)
    try:
        ip = socket.gethostbyname(DOMAIN)
        leader_cache["ip"] = ip
        leader_cache["ts"] = now
        log.debug(f"Leader IP (DNS): {ip}")
        return ip
    except socket.gaierror as e:
        log.error(f"DNS lookup failed: {e}")
        return leader_cache.get("ip")  # Return stale cache


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


def forward_to_node(node_name: str, message: Dict, from_node: Optional[str] = None) -> bool:
    """Forward a message to a specific node."""
    if node_name not in NODES:
        log.error(f"Unknown node: {node_name}")
        return False
    
    node = NODES[node_name]
    url = f"http://{node['ip']}:{node['port']}/receive"
    
    payload = {
        "message": message,
        "from_node": from_node,
        "timestamp": time.time()
    }
    
    # Sign the payload
    if PROXY_SECRET:
        sig = hmac.new(PROXY_SECRET.encode(), json.dumps(payload).encode(), hashlib.sha256).hexdigest()
        payload["signature"] = sig
    
    try:
        req = Request(url, data=json.dumps(payload).encode(),
                     headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            log.info(f"Forwarded to {node_name}: {result}")
            return result.get("ok", False)
    except (URLError, HTTPError) as e:
        log.error(f"Failed to forward to {node_name}: {e}")
        return False


def parse_routing_prefix(text: str) -> tuple[Optional[str], str]:
    """
    Parse routing prefix from message text.
    Returns (target_node, remaining_text).
    
    Prefixes:
    - "1 " or "1:" -> node-1
    - "2 " or "2:" -> node-2  
    - "B " or "B:" -> backup node
    - "to: node-X" -> explicit node (for inter-node)
    """
    text = text.strip()
    
    # Check for "1 " or "1:" prefix
    if text.startswith(("1 ", "1:")):
        return "node-1", text[2:].strip()
    
    # Check for "2 " or "2:" prefix
    if text.startswith(("2 ", "2:")):
        return "node-2", text[2:].strip()
    
    # Check for "B " or "B:" prefix (backup)
    if text.upper().startswith(("B ", "B:")):
        return get_backup_node(), text[2:].strip()
    
    # Check for "to: node-X" prefix (inter-node messaging)
    if text.lower().startswith("to: node-"):
        parts = text.split(" ", 2)
        if len(parts) >= 2:
            target = parts[1].rstrip(":")
            remaining = parts[2] if len(parts) > 2 else ""
            if target in NODES:
                return target, remaining.strip()
    
    return None, text


def handle_telegram_update(update: Dict):
    """Process an incoming Telegram update."""
    global last_update_id
    
    update_id = update.get("update_id", 0)
    if update_id <= last_update_id:
        return
    last_update_id = update_id
    
    message = update.get("message", {})
    if not message:
        return
    
    chat_id = str(message.get("chat", {}).get("id", ""))
    text = message.get("text", "")
    from_user = message.get("from", {}).get("username", "unknown")
    message_id = message.get("message_id")
    
    if not text:
        return
    
    log.info(f"Received from {from_user} ({chat_id}): {text[:50]}...")
    
    # Only process messages from admin
    if chat_id != ADMIN_CHAT_ID:
        log.warning(f"Ignoring message from non-admin: {chat_id}")
        return
    
    # Parse routing prefix
    target_node, clean_text = parse_routing_prefix(text)
    
    if not target_node:
        # Default to leader
        target_node = get_leader_node()
        if not target_node:
            send_telegram(chat_id, "⚠️ Could not determine leader node")
            return
    
    # Build message to forward
    forward_msg = {
        "chat_id": chat_id,
        "text": clean_text,
        "message_id": message_id,
        "from_user": from_user,
        "original_text": text
    }
    
    # Forward to target node
    success = forward_to_node(target_node, forward_msg)
    
    if not success:
        leader = get_leader_node()
        is_leader = target_node == leader
        status = "🟢 leader" if is_leader else "⚪ backup"
        send_telegram(chat_id, f"⚠️ Failed to reach {target_node} ({status})")


def poll_telegram():
    """Poll Telegram for updates."""
    global last_update_id
    
    log.info("Starting Telegram polling...")
    
    while True:
        try:
            params = {"offset": last_update_id + 1, "timeout": 30}
            result = telegram_api("getUpdates", params)
            
            if result.get("ok"):
                for update in result.get("result", []):
                    handle_telegram_update(update)
            else:
                log.error(f"getUpdates failed: {result}")
                time.sleep(5)
                
        except Exception as e:
            log.error(f"Polling error: {e}")
            time.sleep(5)


class ProxyHTTPHandler(BaseHTTPRequestHandler):
    """HTTP handler for receiving messages from nodes."""
    
    def log_message(self, format, *args):
        log.debug(f"HTTP: {args[0]}")
    
    def do_POST(self):
        """Handle POST requests from nodes."""
        if self.path != "/send":
            self.send_error(404)
            return
        
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            data = json.loads(body)
            
            # Verify signature if secret is set
            if PROXY_SECRET:
                sig = data.pop("signature", "")
                expected = hmac.new(PROXY_SECRET.encode(), json.dumps(data).encode(), hashlib.sha256).hexdigest()
                if not hmac.compare_digest(sig, expected):
                    log.warning("Invalid signature from node")
                    self.send_error(403, "Invalid signature")
                    return
            
            # Extract message details
            node_name = data.get("node", "unknown")
            text = data.get("text", "")
            target_node = data.get("to_node")  # For inter-node messaging
            
            # Determine if this node is leader
            leader = get_leader_node()
            is_leader = node_name == leader
            status_emoji = "🟢" if is_leader else "⚪"
            role = "leader" if is_leader else "backup"
            
            # Check for inter-node messaging
            if target_node and target_node in NODES:
                # Forward to target node AND show to admin
                label = f"[{node_name} → {target_node}]"
                send_telegram(ADMIN_CHAT_ID, f"{label}\n{text}")
                forward_to_node(target_node, {
                    "chat_id": ADMIN_CHAT_ID,
                    "text": text,
                    "from_node": node_name
                }, from_node=node_name)
            else:
                # Send to admin with label
                label = f"[{node_name} {status_emoji}]"
                send_telegram(ADMIN_CHAT_ID, f"{label}\n{text}")
            
            # Success response
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True}).encode())
            
        except Exception as e:
            log.error(f"HTTP handler error: {e}")
            self.send_error(500, str(e))
    
    def do_GET(self):
        """Health check endpoint."""
        if self.path == "/health":
            leader = get_leader_node()
            health = {
                "ok": True,
                "leader": leader,
                "leader_ip": get_leader_ip(),
                "nodes": list(NODES.keys())
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(health).encode())
        else:
            self.send_error(404)


def run_http_server():
    """Run the HTTP server for node communication."""
    server = HTTPServer(("0.0.0.0", HTTP_PORT), ProxyHTTPHandler)
    log.info(f"HTTP server listening on port {HTTP_PORT}")
    server.serve_forever()


def main():
    """Main entry point."""
    if not BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN not set")
        sys.exit(1)
    
    log.info("=" * 50)
    log.info("0agent Telegram Proxy starting...")
    log.info(f"Admin chat ID: {ADMIN_CHAT_ID}")
    log.info(f"Leader domain: {DOMAIN}")
    log.info(f"Nodes: {list(NODES.keys())}")
    log.info("=" * 50)
    
    # Start HTTP server in background thread
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    
    # Start Telegram polling (blocking)
    poll_telegram()


if __name__ == "__main__":
    main()
