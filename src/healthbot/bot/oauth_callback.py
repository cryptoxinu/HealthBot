"""Temporary local HTTP server to catch OAuth redirect callbacks.

Spins up on 127.0.0.1, handles a single GET, then shuts down.
Used by WHOOP and Oura OAuth flows.
"""
from __future__ import annotations

import asyncio
from http import HTTPStatus
from urllib.parse import parse_qs, urlparse


async def wait_for_oauth_callback(
    port: int = 8765, timeout: int = 120
) -> dict[str, str | None]:
    """Start a temporary server and wait for an OAuth redirect callback.

    Returns:
        {"code": str|None, "state": str|None, "error": str|None}

    Raises:
        asyncio.TimeoutError: If no callback arrives within *timeout* seconds.
        OSError: If the port is already in use.
    """
    result: dict[str, str | None] = {"code": None, "state": None, "error": None}
    got_request = asyncio.Event()

    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        is_callback = False
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=10)
            text = line.decode("utf-8", errors="replace").strip()

            # Parse: GET /callback?code=XXX&state=YYY HTTP/1.1
            parts = text.split(" ")
            if len(parts) < 2:
                return

            parsed = urlparse(parts[1])

            # Ignore non-callback requests (favicon, preflight, etc.)
            if not parsed.path.rstrip("/").endswith("/callback"):
                body_bytes = b"<html><body>Not found</body></html>"
                header = (
                    f"HTTP/1.1 {HTTPStatus.NOT_FOUND.value} {HTTPStatus.NOT_FOUND.phrase}\r\n"
                    f"Content-Type: text/html; charset=utf-8\r\n"
                    f"Content-Length: {len(body_bytes)}\r\n"
                    f"Connection: close\r\n\r\n"
                )
                writer.write(header.encode() + body_bytes)
                await writer.drain()
                return

            is_callback = True
            qs = parse_qs(parsed.query)
            result["code"] = qs.get("code", [None])[0]
            result["state"] = qs.get("state", [None])[0]
            result["error"] = qs.get("error", [None])[0]

            if result["error"]:
                body = (
                    "<html><body><h1>Authorization Denied</h1>"
                    "<p>You can close this tab.</p></body></html>"
                )
            else:
                body = (
                    "<html><body><h1>Connected!</h1>"
                    "<p>Authorization received. You can close this tab "
                    "and return to Telegram.</p></body></html>"
                )

            body_bytes = body.encode("utf-8")
            header = (
                f"HTTP/1.1 {HTTPStatus.OK.value} {HTTPStatus.OK.phrase}\r\n"
                f"Content-Type: text/html; charset=utf-8\r\n"
                f"Content-Length: {len(body_bytes)}\r\n"
                f"Connection: close\r\n\r\n"
            )
            writer.write(header.encode() + body_bytes)
            await writer.drain()
        except Exception:
            is_callback = True  # Stop server on unexpected errors too
        finally:
            writer.close()
            if is_callback:
                got_request.set()

    server = await asyncio.start_server(_handle, "127.0.0.1", port)
    try:
        await asyncio.wait_for(got_request.wait(), timeout=timeout)
    finally:
        server.close()
        await server.wait_closed()

    return result
