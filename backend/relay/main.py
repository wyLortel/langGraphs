"""Outbox Relay — outbox 테이블의 '아직 안 보낸 메시지'를 Redis Stream으로 옮기는 작은 프로세스.

보장 수준: at-least-once (최소 1번).
  XADD 성공 후 published_at 표시 전에 죽으면, 재시작 후 같은 메시지를 또 XADD 한다 (중복).
  -> 그래서 Worker 쪽 멱등성(jobs 상태 확인 + UNIQUE 제약)이 반드시 필요하다.
  exactly-once는 Redis와 PostgreSQL 사이에서 공짜로 얻을 수 없다.
"""
import asyncio
import json
import os
import signal

from common import chaos, config
from common.infra import make_db_pool, make_redis
from common.obs import emit, log


async def main():
    pool = await make_db_pool()
    r = make_redis()
    await emit(r, "RELAY_STARTED", note="1초마다 outbox에서 published_at IS NULL 인 행을 찾아 XADD")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)

    while not stop.is_set():
        try:
            # WHY 트랜잭션 + FOR UPDATE SKIP LOCKED:
            #   relay를 2개 띄워도 같은 행을 동시에 보내지 않는다 (한 쪽이 잠근 행은 다른 쪽이 건너뜀)
            async with pool.connection() as conn, conn.transaction():
                rows = await (await conn.execute(
                    "SELECT id, dedup_key, job_id, stream, fields FROM outbox WHERE published_at IS NULL "
                    "ORDER BY id LIMIT 20 FOR UPDATE SKIP LOCKED")).fetchall()
                for row in rows:
                    msg_id = await r.xadd(row["stream"], row["fields"])
                    await emit(r, "XADD", route="outbox-relay → Redis", stream=row["stream"], msg_id=msg_id,
                               outbox_id=row["id"], job_id=row["job_id"])
                    payload = json.loads(row["fields"]["job"]).get("payload") or {}
                    # 시나리오 G: XADD는 됐는데 '보냈음' 표시 전에 죽는다 -> 트랜잭션 rollback -> 재시작 후 또 XADD
                    await chaos.crash_if(r, payload.get("chaos"), "crash_after_xadd", f"outbox:{row['id']}")
                    await conn.execute("UPDATE outbox SET published_at=now(), stream_msg_id=%s, "
                                       "publish_count=publish_count+1 WHERE id=%s", (msg_id, row["id"]))
                    if row["job_id"]:
                        await conn.execute("UPDATE jobs SET status='queued', stream_msg_id=%s, updated_at=now() "
                                           "WHERE job_id=%s AND status='created'", (msg_id, row["job_id"]))
        except Exception as e:  # noqa: BLE001
            log("RELAY_ERROR", error=repr(e), note="다음 주기에 다시 시도 (outbox 행은 그대로 남아 있음)")
        try:
            await asyncio.wait_for(stop.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
    log("PROCESS_EXIT", pid=os.getpid())


if __name__ == "__main__":
    asyncio.run(main())
