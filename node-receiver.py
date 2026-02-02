#!/usr/bin/env python3
"""
Node-side receiver for Telegram proxy messages.

Runs alongside Clawdbot and receives messages from the Telegram proxy,
forwarding them to Clawdbot's local API.

Also provides an endpoint for Clawdbot to send messages back through the proxy.
"""

import os
import sys
import json
import time
import hmac
import hashlib
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from typing import Optional

# Configuration
PROXY_URL = os.environ.get("PROXY_URL", "")  # e.g., http://proxy-ip:8080
PROXY_SECRET = os.environ.get("PROXY_SECRET", "")
NODE_NAME = os.environ.get("NODE_NAME", "node-1")
CLAWDBOT_URL = os.environ.get("CLAWDBOT_URL", "http://localhost:18789")
LISTEN_PORT = int(os.environ.get("RECEIVER_PORT", "18790"))

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger("receiver")


def verify_signature(data: dict, signature: str) -> bool:
    """Verify HMAC signature from proxy."""
    if not PROXY_SECRET:
        return True  # No secret configured, skip verification
    
    data_copy = dict(data)
    data_copy.pop("signature", None)
    expected = hmac.new(
        PROXY_SECRET.encode(),
        json.dumps(data_copy).encode(),
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(signature, expected)


def sign_payload(data: dict) -> dict:
    """Add HMAC signature to payload."""
    if not PROXY_SECRET:
        return data
    
    sig = hmac.new(
        PROXY_SECRET.encode(),
        json.dumps(data).encode(),
        hashlib.sha256
    ).hexdigest()
    data["signature"] = sig
    return data


def forward_to_clawdbot(message: dict, from_node: Optional[str] = None) -> bool:
    """Forward message to Clawdbot via HTTP handler."""
    # Call the telegram-proxy HTTP endpoint
    url = f"{CLAWDBOT_URL}/__clawdbot__/telegram-proxy/receive"
    
    # Build the payload
    payload = {
        "message": message,
        "from_node": from_node,
        "timestamp": time.time(),
    }
    
    # Add signature if we have a secret
    if PROXY_SECRET:
        sig = hmac.new(
            PROXY_SECRET.encode(),
            json.dumps(payload).encode(),
            hashlib.sha256
        ).hexdigest()
        payload["signature"] = sig
    
    try:
        req = Request(url, data=json.dumps(payload).encode(),
                     headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            log.info(f"HTTP response: {result}")
            return result.get("ok", False)
    except Exception as e:
        log.error(f"Failed to call Clawdbot HTTP endpoint: {e}")
        
        # Fallback: write to a file
        fallback_file = "/tmp/telegram-proxy-messages.jsonl"
        with open(fallback_file, "a") as f:
            f.write(json.dumps({"message": message, "from_node": from_node}) + "\n")
        log.info(f"Wrote to fallback file: {fallback_file}")
        return False


def send_to_proxy(text: str, to_node: Optional[str] = None) -> bool:
    """Send a message through the proxy."""
    if not PROXY_URL:
        log.error("PROXY_URL not configured")
        return False
    
    url = f"{PROXY_URL}/send"
    
    payload = {
        "node": NODE_NAME,
        "text": text,
        "timestamp": time.time()
    }
    
    if to_node:
        payload["to_node"] = to_node
    
    payload = sign_payload(payload)
    
    try:
        req = Request(url, data=json.dumps(payload).encode(),
                     headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            return result.get("ok", False)
    except Exception as e:
        log.error(f"Failed to send to proxy: {e}")
        return False


class ReceiverHandler(BaseHTTPRequestHandler):
    """HTTP handler for receiving messages from proxy."""
    
    def log_message(self, format, *args):
        log.debug(f"HTTP: {args[0]}")
    
    def do_POST(self):
        """Handle incoming messages from proxy."""
        if self.path == "/receive":
            self._handle_receive()
        elif self.path == "/send":
            self._handle_send()
        else:
            self.send_error(404)
    
    def _handle_receive(self):
        """Receive message from proxy, forward to Clawdbot."""
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            data = json.loads(body)
            
            # Verify signature
            sig = data.get("signature", "")
            if not verify_signature(data, sig):
                log.warning("Invalid signature")
                self.send_error(403, "Invalid signature")
                return
            
            message = data.get("message", {})
            from_node = data.get("from_node")
            
            log.info(f"Received message from proxy (from_node={from_node}): {message.get('text', '')[:50]}...")
            
            # Forward to Clawdbot
            forward_to_clawdbot(message, from_node)
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True}).encode())
            
        except Exception as e:
            log.error(f"Receive error: {e}")
            self.send_error(500, str(e))
    
    def _handle_send(self):
        """Handle send request from Clawdbot."""
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            data = json.loads(body)
            
            text = data.get("text", "")
            to_node = data.get("to_node")
            
            log.info(f"Sending to proxy: {text[:50]}...")
            
            success = send_to_proxy(text, to_node)
            
            self.send_response(200 if success else 500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": success}).encode())
            
        except Exception as e:
            log.error(f"Send error: {e}")
            self.send_error(500, str(e))
    
    def do_GET(self):
        """Health check."""
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            health = {
                "ok": True,
                "node": NODE_NAME,
                "proxy_url": PROXY_URL,
                "clawdbot_url": CLAWDBOT_URL
            }
            self.wfile.write(json.dumps(health).encode())
        else:
            self.send_error(404)


def main():
    if not PROXY_URL:
        log.warning("PROXY_URL not set - will only receive, not send")
    
    log.info("=" * 50)
    log.info(f"Node receiver starting...")
    log.info(f"Node: {NODE_NAME}")
    log.info(f"Proxy: {PROXY_URL}")
    log.info(f"Clawdbot: {CLAWDBOT_URL}")
    log.info(f"Listen port: {LISTEN_PORT}")
    log.info("=" * 50)
    
    server = HTTPServer(("0.0.0.0", LISTEN_PORT), ReceiverHandler)
    log.info(f"Listening on port {LISTEN_PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
