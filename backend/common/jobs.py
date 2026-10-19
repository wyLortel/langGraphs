"""jobs 테이블의 생명주기 + 펜싱 토큰.

Redis는 "이 메시지를 다시 줄까?"만 안다. "그 작업이 이미 끝났나?"는 PostgreSQL의 jobs 행이 안다.
그래서 Worker는 메시지를 받으면 먼저 jobs 행을 보고, 이미 done이면 아무것도 하지 않고 ACK만 한다.
"""
import json
import os
import uuid
from dataclasses import dataclass

from psycopg.types.json import Jsonb

from . import config
from .obs import CONTAINER, emit
from .streams import Msg, StaleOwner


@dataclass
class Lease:
    job_id: str
    token: int        # 펜싱 토큰
    run_id: int
    attempts: int


def job_id_for(key: str) -> str:
    """멱등성 키로부터 항상 같은 job_id를 만든다 (DB를 모르는 Crawler도 같은 id를 만들 수 있게)."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


async def begin(pool, redis, msg: Msg) -> Lease | None:
    """작업 시작 등록. 이미 끝난 작업이면 None (= 처리 없이 ACK만 하라)."""
    j = msg.job
    async with pool.connection() as conn, conn.transaction():
        # Crawler가 만든 작업처럼 DB에 행이 없을 수도 있다 -> 없으면 만든다 (있으면 아무것도 안 함)
        await conn.execute(
            "INSERT INTO jobs(job_id, idempotency_key, type, payload, status) VALUES (%s,%s,%s,%s,'queued') "
            "ON CONFLICT DO NOTHING",
            (j["job_id"], j.get("idempotency_key") or j["job_id"], j["type"], Jsonb(j.get("payload") or {})))
        # FOR UPDATE: 두 Worker가 동시에 같은 job을 시작하려 해도 이 행에서 한 줄로 세운다
        row = await (await conn.execute(
            "SELECT status, worker_id, lease_token FROM jobs WHERE job_id=%s FOR UPDATE", (j["job_id"],))).fetchone()
        if row["status"] in ("done", "dead"):
            await conn.execute(
                "INSERT INTO job_runs(job_id, worker_id, container, pid, lease_token, delivery, outcome, note, finished_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,'already_done',%s,now())",
                (j["job_id"], config.WORKER_ID, CONTAINER, os.getpid(), row["lease_token"], msg.delivery,
                 f"status={row['status']} → 실행 생략, ACK만"))
            prev = None
        else:
            prev = row
            # 이전 실행 기록이 아직 running이면 '죽은 것으로 추정'. (살아 있었다면 나중에 stale_rejected로 덮어씀)
            await conn.execute("UPDATE job_runs SET outcome='presumed_dead', finished_at=now() "
                               "WHERE job_id=%s AND outcome='running'", (j["job_id"],))
            new = await (await conn.execute(
                "UPDATE jobs SET status='running', attempts=attempts+1, worker_id=%s, lease_token=lease_token+1, "
                "stream_msg_id=%s, updated_at=now() WHERE job_id=%s RETURNING lease_token, attempts",
                (config.WORKER_ID, msg.msg_id, j["job_id"]))).fetchone()
            run = await (await conn.execute(
                "INSERT INTO job_runs(job_id, worker_id, container, pid, lease_token, delivery) "
                "VALUES (%s,%s,%s,%s,%s,%s) RETURNING run_id",
                (j["job_id"], config.WORKER_ID, CONTAINER, os.getpid(), new["lease_token"], msg.delivery))).fetchone()
    if prev is None:
        await emit(redis, "ALREADY_DONE_SKIP", route=f"{config.WORKER_ID} → PostgreSQL", job_id=j["job_id"],
                   status=row["status"], note="중복 전달 감지: 실행하지 않고 ACK만 한다 (멱등성 2차 방어)")
        return None
    if prev["status"] == "running":
        alive = bool(prev["worker_id"] and await redis.exists(f"heartbeat:{prev['worker_id']}"))
        await emit(redis, "TAKEOVER", job_id=j["job_id"], prev_worker=prev["worker_id"],
                   prev_token=prev["lease_token"], new_token=new["lease_token"], prev_worker_alive=alive,
                   note=("이전 Worker가 아직 살아 있음 → 같은 작업이 동시에 2번 실행될 수 있다 (DUPLICATE_EXECUTION 위험)"
                         if alive and prev["worker_id"] != config.WORKER_ID else "이전 실행이 중단된 작업을 이어받음"))
    await emit(redis, "JOB_STARTED", route=f"{config.WORKER_ID} → PostgreSQL", job_id=j["job_id"],
               lease_token=new["lease_token"], attempts=new["attempts"], run_id=run["run_id"])
    return Lease(j["job_id"], new["lease_token"], run["run_id"], new["attempts"])


async def complete(conn, lease: Lease, result: dict):
    """반드시 호출자의 트랜잭션 안에서 부른다. 업무 데이터 저장과 '작업 완료' 표시가 함께 commit/rollback 된다.

    펜싱: WHERE lease_token = 내 토큰. 그 사이 다른 Worker가 작업을 가져갔다면 토큰이 올라가 있어서 0행 -> StaleOwner
    -> 예외로 트랜잭션 전체(업무 데이터 포함)가 rollback 된다.
    """
    cur = await conn.execute(
        "UPDATE jobs SET status='done', result=%s, error=NULL, updated_at=now() "
        "WHERE job_id=%s AND lease_token=%s AND status='running'",
        (Jsonb(result), lease.job_id, lease.token))
    if cur.rowcount == 0:
        raise StaleOwner(f"job {lease.job_id}: 내 토큰 {lease.token}이 더 이상 최신이 아님")
    await conn.execute("UPDATE job_runs SET outcome='committed', finished_at=now() WHERE run_id=%s", (lease.run_id,))


async def mark_run(pool, lease: Lease, outcome: str, note: str):
    async with pool.connection() as conn:
        await conn.execute("UPDATE job_runs SET outcome=%s, note=%s, finished_at=now() WHERE run_id=%s",
                           (outcome, note[:500], lease.run_id))


async def fail(pool, lease: Lease, error: str):
    async with pool.connection() as conn, conn.transaction():
        await conn.execute("UPDATE jobs SET status='failed', error=%s, updated_at=now() "
                           "WHERE job_id=%s AND lease_token=%s", (error[:1000], lease.job_id, lease.token))
        await conn.execute("UPDATE job_runs SET outcome='error', note=%s, finished_at=now() WHERE run_id=%s",
                           (error[:500], lease.run_id))


async def mark_dead(pool, msg: Msg, reason: str):
    async with pool.connection() as conn:
        await conn.execute("UPDATE jobs SET status='dead', error=%s, updated_at=now() WHERE job_id=%s",
                           (reason, msg.job.get("job_id")))


def stream_fields(job_id: str, type_: str, payload: dict, idem_key: str) -> dict:
    """Redis Stream 메시지 본문. 큰 데이터는 넣지 않는다 (파일은 MinIO 위치만)."""
    return {"job": json.dumps({"job_id": job_id, "type": type_, "payload": payload,
                               "idempotency_key": idem_key}, ensure_ascii=False)}
