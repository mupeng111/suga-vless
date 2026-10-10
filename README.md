# suga-vless

Minimal VLESS over WebSocket server (Python stdlib only) for Suga.

Env vars:
- `UUID` (required)
- `WS_PATH` (secret path, e.g. `/p-xxxxxxxx`)
- `PORT` (default 8080)

The server sends a WebSocket ping every 50s to keep idle connections alive.
