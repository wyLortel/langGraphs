"""MCP Tool 서버 — AI가 '실행 중 필요할 때' 호출하는 도구 모음 (독립 컨테이너, http://mcp:8100/mcp).

전송 방식: Streamable HTTP. 요청 1개 = HTTP POST 1개, 본문은 JSON-RPC 2.0.
  stateless_http=True : 서버가 세션을 메모리에 들고 있지 않다 -> MCP 서버를 재시작해도 클라이언트는 그냥 다시 호출하면 된다
  json_response=True  : 응답을 SSE 스트림 대신 일반 JSON으로 -> curl로 보기 쉽다

이 서버는 원문을 MinIO에서 직접 읽지 않는다 (아키텍처상 MCP -> S3 연결이 없음). 필요한 텍스트는 인자로 받는다.
"""
import json
import os

import httpx
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from common import config
from common.obs import log

mcp = FastMCP(
    "scholarship-tools",
    stateless_http=True,
    json_response=True,
    # 실습용: Docker 내부망에서 Host 헤더가 'mcp:8100'으로 오므로 DNS rebinding 보호를 끈다
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)


@mcp.tool()
async def fetch_page(url: str) -> dict:
    """공식 공고 페이지를 가져온다. (실습: mock-sites 컨테이너만 허용)"""
    if not url.startswith(config.MOCK_SITE_URL):
        raise ValueError(f"허용되지 않은 URL: {url}")
    async with httpx.AsyncClient(timeout=5) as c:
        resp = await c.get(url)
    log("TOOL_FETCH_PAGE", url=url, status=resp.status_code, bytes=len(resp.content),
        note="MCP 서버 → 기관 사이트(MOCK) 실제 HTTP GET")
    return {"url": url, "status": resp.status_code, "text": resp.text}


@mcp.tool()
async def parse_document(text: str) -> dict:
    """공고 원문을 섹션으로 나눈다. '# 제목', '■ 섹션' 형식만 이해하는 단순 파서 (실제라면 PDF/HWP 파서)."""
    sections, cur = [], None
    for line in text.splitlines():
        if line.startswith("# "):
            sections.append({"heading": "title", "text": line[2:].strip()})
        elif line.startswith("■"):
            cur = {"heading": line.strip("■ ").strip(), "text": ""}
            sections.append(cur)
        elif cur is not None and line.strip():
            cur["text"] += line.strip() + "\n"
    return {"sections": sections, "count": len(sections)}


@mcp.tool()
async def web_search(query: str) -> dict:
    """MOCK: 외부 검색 API 대체. 고정 결과를 돌려준다."""
    return {"query": query, "results": [{"title": "(MOCK) 장학금 기관 목록", "url": f"{config.MOCK_SITE_URL}/"}]}


class WireLogger:
    """ASGI 미들웨어: 실제로 네트워크로 오간 JSON-RPC 본문을 그대로 로그로 남긴다 (프로토콜 관찰용)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        body = b""
        more = True
        while more:
            m = await receive()
            body += m.get("body", b"")
            more = m.get("more_body", False)
        try:
            req = json.loads(body) if body else None
        except ValueError:
            req = body[:200]
        log("MCP_WIRE_IN", method=scope["method"], path=scope["path"], jsonrpc=req)

        sent = False

        async def replay():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        out = b""

        async def capture(m):
            nonlocal out
            if m["type"] == "http.response.start":
                log("MCP_WIRE_STATUS", status=m["status"])
            if m["type"] == "http.response.body":
                out += m.get("body", b"")
                if not m.get("more_body", False) and out:
                    try:
                        log("MCP_WIRE_OUT", jsonrpc=json.loads(out))
                    except ValueError:
                        log("MCP_WIRE_OUT", raw=out[:300])
            await send(m)

        await self.app(scope, replay, capture)


if __name__ == "__main__":
    log("PROCESS_START", pid=os.getpid(), tools=["fetch_page", "parse_document", "web_search"],
        endpoint="http://mcp:8100/mcp")
    uvicorn.run(WireLogger(mcp.streamable_http_app()), host="0.0.0.0", port=8100, log_level="warning")
