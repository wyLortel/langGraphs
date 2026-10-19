"""MCP 클라이언트 — 독립된 MCP 서버(mcp 컨테이너)에 실제 프로토콜(JSON-RPC over Streamable HTTP)로 도구를 호출한다.

함수 import가 아니다: 요청은 네트워크를 타고 http://mcp:8100/mcp 로 간다.
  1) initialize      (클라이언트/서버가 서로 버전·기능을 알려줌)
  2) notifications/initialized
  3) tools/call      {"name": "fetch_page", "arguments": {...}}
WHY 호출마다 세션을 새로 여나: 코드가 가장 단순하고, MCP 서버가 재시작돼도 다음 호출은 자동으로 성공한다.
  트레이드오프: 호출마다 initialize 왕복이 1번 더 든다 (실습 규모에선 무시 가능).
"""
import json

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from common import config
from common.obs import emit


async def call_tool(redis, name: str, arguments: dict, thread_id: str | None = None) -> dict:
    await emit(redis, "MCP_REQUEST", route=f"{config.WORKER_ID} → MCP", thread_id=thread_id,
               url=config.MCP_SERVER_URL,
               jsonrpc={"jsonrpc": "2.0", "method": "tools/call", "params": {"name": name, "arguments": arguments}})
    async with streamablehttp_client(config.MCP_SERVER_URL) as (read, write, _get_session_id):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            result = await session.call_tool(name, arguments)
    if result.isError:
        raise RuntimeError(f"MCP tool {name} error: {result.content}")
    data = result.structuredContent
    if data is None:
        data = json.loads(result.content[0].text)
    # 일부 SDK 버전은 dict가 아닌 반환값을 {"result": ...}로 감싼다
    if isinstance(data, dict) and set(data.keys()) == {"result"}:
        data = data["result"]
    await emit(redis, "MCP_RESPONSE", route=f"MCP → {config.WORKER_ID}", thread_id=thread_id, tool=name,
               server=f"{init.serverInfo.name} {init.serverInfo.version}", protocol=init.protocolVersion,
               result=data)
    return data
