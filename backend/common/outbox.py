"""Transactional Outbox 쓰기 쪽 (API가 사용).

WHY: "DB에 job 저장"과 "Redis에 XADD"는 서로 다른 시스템이라 하나의 트랜잭션으로 묶을 수 없다.
  commit 후 XADD 전에 죽으면 -> DB에는 job이 있는데 큐에는 없다 -> 아무도 처리하지 않는다 (영원히 created).
해결: XADD할 내용을 같은 DB의 outbox 테이블에 job과 '같은 트랜잭션'으로 넣는다.
  둘 다 저장되거나 둘 다 안 된다. 실제 XADD는 outbox-relay 프로세스가 나중에 반복해서 시도한다.
"""
from psycopg.types.json import Jsonb

from .jobs import job_id_for, stream_fields


async def enqueue_job(conn, *, type_: str, payload: dict, idem_key: str, stream: str):
    """반드시 호출자의 트랜잭션 안에서 호출. 반환: (job_id, 새로 만들었는가)"""
    job_id = job_id_for(idem_key)
    row = await (await conn.execute(
        "INSERT INTO jobs(job_id, idempotency_key, type, payload, status) VALUES (%s,%s,%s,%s,'created') "
        "ON CONFLICT (idempotency_key) DO NOTHING RETURNING job_id",
        (job_id, idem_key, type_, Jsonb(payload)))).fetchone()
    if row is None:
        # 멱등성 1차 방어: 같은 키의 요청이 이미 있었다 -> 새 job도, 새 메시지도 만들지 않는다
        old = await (await conn.execute("SELECT job_id FROM jobs WHERE idempotency_key=%s",
                                        (idem_key,))).fetchone()
        return str(old["job_id"]), False
    await conn.execute(
        "INSERT INTO outbox(dedup_key, job_id, stream, fields) VALUES (%s,%s,%s,%s)",
        (idem_key, job_id, stream, Jsonb(stream_fields(job_id, type_, payload, idem_key))))
    return job_id, True


async def enqueue_message(conn, *, dedup_key: str, stream: str, fields: dict) -> bool:
    """jobs 행 없이 메시지만 (Crawler 트리거용: Crawler는 DB를 쓰지 않는 서버이므로)"""
    row = await (await conn.execute(
        "INSERT INTO outbox(dedup_key, stream, fields) VALUES (%s,%s,%s) ON CONFLICT (dedup_key) DO NOTHING RETURNING id",
        (dedup_key, stream, Jsonb(fields)))).fetchone()
    return row is not None
