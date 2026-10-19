"""관찰(observability) 도구: 모든 서버가 같은 형식으로 로그를 찍고, 같은 내용을 Pub/Sub로 흘린다.

로그 1줄 형식:
  [10:00:01.123] [ai-worker-1 container=3f2a9c pid=7] EVENT key=value ...

- WORKER_ID : docker-compose에서 준 논리적 이름 (재시작해도 같음)
- container : 컨테이너 hostname = 컨테이너 ID 앞 12자리 (컨테이너를 새로 만들면 바뀜)
- pid       : 컨테이너 안 Python 프로세스 번호 (프로세스가 재시작되면 바뀔 수 있음)
"""
import json
import os
import socket
import time
from datetime import datetime, timedelta, timezone

from . import config

CONTAINER = socket.gethostname()
# 로그 시각을 한국 시간으로 (컨테이너 기본 시간대는 UTC)
TZ = timezone(timedelta(hours=int(os.environ.get("LOG_TZ_OFFSET_HOURS", "9"))))
STARTED_AT = time.time()


def _fmt(v):
    if isinstance(v, (dict, list)):
        s = json.dumps(v, ensure_ascii=False, default=str)
    else:
        s = str(v)
    return s if len(s) <= 400 else s[:400] + "…"


def log(event: str, **fields):
    ts = datetime.now(TZ).strftime("%H:%M:%S.%f")[:-3]
    kv = " ".join(f"{k}={_fmt(v)}" for k, v in fields.items() if v is not None)
    print(f"[{ts}] [{config.WORKER_ID} container={CONTAINER} pid={os.getpid()}] {event} {kv}", flush=True)


async def emit(redis, event: str, route: str | None = None, **fields):
    """로그 + Pub/Sub 발행.

    WHY Pub/Sub: 진행 신호는 '지금 보고 있는 사람'에게만 의미가 있다. 구독자가 없으면 그냥 버려진다.
    그래서 이 함수가 실패해도 업무 처리는 계속한다 (영구 상태는 PostgreSQL에 따로 있음).
    """
    log(event, route=route, **fields)
    msg = {
        "ts": time.time(),
        "source": config.WORKER_ID,
        "container": CONTAINER,
        "pid": os.getpid(),
        "event": event,
        "route": route,
        "data": fields,
    }
    try:
        receivers = await redis.publish(config.EVENTS_CHANNEL, json.dumps(msg, ensure_ascii=False, default=str))
        return receivers   # 이 메시지를 받은 구독자 수 (0이면 아무도 못 봄 = 유실)
    except Exception as e:  # noqa: BLE001
        log("PUBLISH_FAILED", error=repr(e))
        return 0
