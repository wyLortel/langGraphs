"""장애 주입 도구.

WHY Redis에 "이미 터뜨렸음" 표시를 남기나:
  프로세스를 os._exit로 죽이면 메모리 변수는 전부 사라진다. 메모리에 '한 번만' 플래그를 두면
  재시작 후 또 터져서 무한 크래시가 된다. 그래서 프로세스 밖(Redis)에 기록한다.
"""
import os

from .obs import log


async def once(redis, key: str) -> bool:
    """이 key로 처음 호출되면 True (24시간 유지)."""
    return bool(await redis.set(f"chaos_fired:{key}", "1", nx=True, ex=86400))


async def crash_if(redis, wanted: str | None, point: str, key: str):
    """wanted == point 이고 처음이면 프로세스를 즉시 종료한다 (finally/except도 실행 안 됨 = 진짜 크래시)."""
    if wanted == point and await once(redis, f"{key}:{point}"):
        log("CHAOS_CRASH", point=point, key=key, note="os._exit(1) — 정리 코드 없이 프로세스 즉사")
        os._exit(1)
