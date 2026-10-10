# Minimal VLESS over WebSocket server (Python stdlib only).
# Env: UUID, WS_PATH (default /ws), PORT (default 3000),
#      TUNNEL_TOKEN (optional: also expose this node via a fixed tunnel domain
#      whose origin is a second instance on port 8001).
import asyncio
import base64
import hashlib
import json
import os
import struct
import subprocess
import sys
import urllib.request

UUID_STR = os.environ.get("UUID", "")
WS_PATH = os.environ.get("WS_PATH", "/ws")
PORT = int(os.environ.get("PORT") or "3000")
UUID_BYTES = bytes.fromhex(UUID_STR.replace("-", "")) if UUID_STR else None
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def log(*a):
    print(*a, flush=True)


async def pipe_tcp_to_ws(reader, ws_send):
    try:
        while True:
            data = await reader.read(32768)
            if not data:
                break
            await ws_send(data)
    except Exception:
        pass


class WSConn:
    def __init__(self, reader, writer):
        self.r = reader
        self.w = writer
        self.buf = bytearray()

    async def read_exact(self, n):
        while len(self.buf) < n:
            chunk = await self.r.read(n - len(self.buf))
            if not chunk:
                raise ConnectionError("eof")
            self.buf += chunk
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out

    async def send_frame(self, payload, opcode=0x2):
        n = len(payload)
        if n < 126:
            head = struct.pack("!BB", 0x80 | opcode, n)
        elif n < 65536:
            head = struct.pack("!BBH", 0x80 | opcode, 126, n)
        else:
            head = struct.pack("!BBQ", 0x80 | opcode, 127, n)
        self.w.write(head + payload)
        await self.w.drain()

    async def read_message(self):
        """Return (opcode, payload) with fragmentation reassembled."""
        payload = bytearray()
        opcode = None
        while True:
            h = await self.read_exact(2)
            fin = h[0] & 0x80
            op = h[0] & 0x0F
            masked = h[1] & 0x80
            ln = h[1] & 0x7F
            if ln == 126:
                ln = struct.unpack("!H", await self.read_exact(2))[0]
            elif ln == 127:
                ln = struct.unpack("!Q", await self.read_exact(8))[0]
            mask = await self.read_exact(4) if masked else None
            data = await self.read_exact(ln) if ln else b""
            if mask:
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
            if op == 0x9:  # ping -> pong
                await self.send_frame(data, opcode=0xA)
                continue
            if op == 0x8:  # close
                raise ConnectionError("ws close")
            if opcode is None:
                opcode = op
            payload += data
            if fin:
                return opcode, bytes(payload)


async def handle(reader, writer):
    try:
        raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=15)
    except Exception:
        writer.close()
        return
    head = raw.decode("latin1")
    lines = head.split("\r\n")
    try:
        method, path = lines[0].split(" ")[0], lines[0].split(" ")[1]
    except Exception:
        writer.close()
        return
    headers = {}
    for ln in lines[1:]:
        if ":" in ln:
            k, v = ln.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    is_ws = headers.get("upgrade", "").lower() == "websocket"
    clean_path = path.split("?")[0]
    if not (is_ws and clean_path == WS_PATH):
        body = b"ok"
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: "
            + str(len(body)).encode()
            + b"\r\nConnection: close\r\n\r\n"
            + body
        )
        await writer.drain()
        writer.close()
        return
    accept = base64.b64encode(
        hashlib.sha1((headers.get("sec-websocket-key", "") + WS_GUID).encode()).digest()
    ).decode()
    writer.write(
        (
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
            "Connection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n"
        ).encode()
    )
    await writer.drain()

    ws = WSConn(reader, writer)

    async def _ping_loop():
        try:
            while True:
                await asyncio.sleep(50)
                await ws.send_frame(b"", opcode=0x9)
        except Exception:
            pass

    ping_task = asyncio.create_task(_ping_loop())
    # leftover bytes after headers belong to the ws stream
    ws.buf += reader._buffer if hasattr(reader, "_buffer") else b""
    try:
        _, msg = await ws.read_message()
        if len(msg) < 24:
            return
        ver = msg[0]
        if UUID_BYTES and msg[1:17] != UUID_BYTES:
            return
        i = 17
        i += msg[i] + 1  # option length
        if msg[i] != 1:  # only TCP command
            return
        i += 1
        port = struct.unpack("!H", msg[i : i + 2])[0]
        i += 2
        atyp = msg[i]
        i += 1
        if atyp == 1:
            host = ".".join(str(b) for b in msg[i : i + 4])
            i += 4
        elif atyp == 2:
            ln = msg[i]
            i += 1
            host = msg[i : i + ln].decode()
            i += ln
        elif atyp == 3:
            raw6 = msg[i : i + 16]
            i += 16
            host = ":".join(
                "%x" % struct.unpack("!H", raw6[j : j + 2])[0] for j in range(0, 16, 2)
            )
        else:
            return
        payload = msg[i:]
        t_reader, t_writer = await asyncio.open_connection(host, port)
        await ws.send_frame(bytes([ver, 0]) + payload)
        up = asyncio.create_task(pipe_tcp_to_ws(t_reader, ws.send_frame))
        try:
            while True:
                _, data = await ws.read_message()
                t_writer.write(data)
                await t_writer.drain()
        except Exception:
            pass
        finally:
            up.cancel()
            t_writer.close()
    except Exception:
        pass
    finally:
        ping_task.cancel()
        try:
            writer.close()
        except Exception:
            pass


def boot_report():
    try:
        with urllib.request.urlopen("https://ipinfo.io/json", timeout=8) as r:
            info = json.loads(r.read().decode())
        log("egress:", json.dumps(info))
    except Exception as e:
        log("egress check failed:", e)
    log(f"vless-ws listening on :{PORT} path={WS_PATH}")


def maybe_tunnel():
    token = os.environ.get("TUNNEL_TOKEN", "")
    if not token or os.environ.get("_TUNNEL_CHILD"):
        return
    cf = "/tmp/cloudflared"
    try:
        if not os.path.exists(cf):
            urllib.request.urlretrieve(
                "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64",
                cf,
            )
            os.chmod(cf, 0o755)
        subprocess.Popen([cf, "tunnel", "--no-autoupdate", "--protocol", "http2", "run", "--token", token])
        log("tunnel connector started")
    except Exception as e:
        log("tunnel start failed:", e)
    env = dict(os.environ, PORT="8001", _TUNNEL_CHILD="1")
    subprocess.Popen([sys.executable, os.path.abspath(__file__)], env=env)


async def main():
    boot_report()
    maybe_tunnel()
    server = await asyncio.start_server(handle, "0.0.0.0", PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
