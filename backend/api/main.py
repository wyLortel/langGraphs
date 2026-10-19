"""API 서버 (FastAPI) — 모든 요청의 입구.

원칙: 오래 걸리는 일(AI 그래프 실행, 크롤링)은 직접 하지 않는다.
      DB에 "할 일"을 기록하고(outbox) 바로 응답한다. 실제 일은 Worker가 큐에서 꺼내서 한다.
WHY: HTTP 요청은 브라우저/프록시 타임아웃이 있고, API 프로세스가 재시작되면 진행 중이던 일이 날아간다.
"""
import asyncio
import json
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
from psycopg.errors import UndefinedTable
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from common import config, storage
from common.embed import embed, to_pg
from common.infra import make_db_pool, make_redis
from common.jobs import job_id_for, stream_fields
from common.obs import emit, log
from common.outbox import enqueue_job, enqueue_message

S = {}   # 프로세스 전역 자원 (풀, Redis 클라이언트) — 요청마다 만들지 않는다


@asynccontextmanager
async def lifespan(app):
    S["db"] = await make_db_pool(max_size=10)
    S["r"] = make_redis()
    await storage.ensure_bucket()
    await emit(S["r"], "API_STARTED", note="uvicorn 프로세스 시작, DB 풀/Redis 연결 준비 완료")
    yield
    await S["db"].close()


app = FastAPI(title="scholarship-lab api", lifespan=lifespan)


async def browser_event(request: Request, **data):
    await emit(S["r"], "HTTP_REQUEST", route=f"Browser → API", method=request.method, path=request.url.path, **data)


# =====================================================================  1) 가장 단순한 흐름: ping
class PingIn(BaseModel):
    message: str = "hello"
    chaos: str | None = None          # crash_after_read / crash_before_ack / always_fail / fail_xadd
    sleep_sec: int = 0                # 오래 걸리는 정상 작업 흉내 (시나리오 F)
    lease: bool = True                # False면 lease 갱신을 끔 (시나리오 F의 '문제 상황')


@app.post("/api/ping")
async def ping(body: PingIn, request: Request, enqueue: str = "outbox",
               idempotency_key: str | None = Header(default=None)):
    """API -> (PostgreSQL: jobs + outbox) -> outbox-relay -> Redis ai_jobs -> AI Worker -> PostgreSQL ping_results"""
    key = idempotency_key or f"ping:{uuid.uuid4()}"
    payload = body.model_dump()
    await browser_event(request, idempotency_key=key, body=payload, enqueue=enqueue)

    if enqueue == "direct":
        return await ping_direct(key, payload)

    async with S["db"].connection() as conn, conn.transaction():
        job_id, created = await enqueue_job(conn, type_="ping", payload=payload, idem_key=key,
                                            stream=config.AI_STREAM)
    if created:
        await emit(S["r"], "DB_COMMIT", route="API → PostgreSQL", job_id=job_id,
                   sql="INSERT jobs(status=created) + INSERT outbox (같은 트랜잭션)",
                   note="XADD는 API가 아니라 outbox-relay가 한다")
    else:
        await emit(S["r"], "DUPLICATE_REQUEST", route="API → PostgreSQL", job_id=job_id, idempotency_key=key,
                   note="UNIQUE(idempotency_key) 충돌 → 기존 job 반환, 새 메시지 없음 (멱등성 1차 방어)")
    return {"job_id": job_id, "duplicate": not created, "idempotency_key": key}


async def ping_direct(key, payload):
    """시나리오 G의 '나쁜 예': DB commit 후 API가 직접 XADD. 둘 사이에서 실패하면 job이 고아가 된다."""
    job_id = job_id_for(key)
    async with S["db"].connection() as conn, conn.transaction():
        row = await (await conn.execute(
            "INSERT INTO jobs(job_id, idempotency_key, type, payload, status) VALUES (%s,%s,'ping',%s,'created') "
            "ON CONFLICT (idempotency_key) DO NOTHING RETURNING job_id", (job_id, key, Jsonb(payload)))).fetchone()
    if row is None:
        return {"job_id": job_id, "duplicate": True}
    await emit(S["r"], "DB_COMMIT", route="API → PostgreSQL", job_id=job_id, sql="INSERT jobs(status=created)",
               mode="direct (outbox 없음)")
    if payload.get("chaos") == "fail_xadd":
        await emit(S["r"], "CHAOS_FAIL_XADD", job_id=job_id,
                   note="commit은 끝났는데 XADD 전에 실패 → 이 job은 큐에 영원히 안 들어간다 (status=created 고착)")
        raise HTTPException(500, f"simulated failure between DB commit and XADD (job {job_id} is now orphaned)")
    msg_id = await S["r"].xadd(config.AI_STREAM, stream_fields(job_id, "ping", payload, key))
    async with S["db"].connection() as conn:
        await conn.execute("UPDATE jobs SET status='queued', stream_msg_id=%s WHERE job_id=%s AND status='created'",
                           (msg_id, job_id))
    await emit(S["r"], "XADD", route="API → Redis", stream=config.AI_STREAM, msg_id=msg_id, job_id=job_id)
    return {"job_id": job_id, "duplicate": False, "msg_id": msg_id}


# =====================================================================  2) 수집 트리거
class CrawlIn(BaseModel):
    chaos: str | None = None          # Crawler/ingest 그래프로 전달됨 (crash_after_db_write, crash_after_notify ...)
    notify_idempotency: bool = True   # False면 '멱등 키를 지원하지 않는 알림 제공자' 흉내


@app.post("/api/crawl")
async def crawl(body: CrawlIn, request: Request, idempotency_key: str | None = Header(default=None)):
    key = idempotency_key or f"crawl:{uuid.uuid4()}"
    await browser_event(request, idempotency_key=key, body=body.model_dump())
    fields = {"job": json.dumps({"job_id": job_id_for(key), "type": "crawl", "payload": body.model_dump(),
                                 "idempotency_key": key})}
    async with S["db"].connection() as conn, conn.transaction():
        created = await enqueue_message(conn, dedup_key=key, stream=config.CRAWLER_STREAM, fields=fields)
    await emit(S["r"], "DB_COMMIT" if created else "DUPLICATE_REQUEST", route="API → PostgreSQL",
               sql="INSERT outbox(stream=crawler_jobs)", idempotency_key=key,
               note="Crawler는 DB를 쓰지 않으므로 jobs 행 없이 메시지만 보낸다")
    return {"queued": created, "idempotency_key": key}


# =====================================================================  3) 신청 준비 (LangGraph interrupt/resume)
class ApplyIn(BaseModel):
    student_id: int
    scholarship_id: int
    chaos: str | None = None          # crash_in_node / crash_after_upload
    draft_key_mode: str = "deterministic"   # random 이면 재실행 시 MinIO에 고아 파일이 생긴다


@app.post("/api/applications")
async def create_application(body: ApplyIn, request: Request):
    await browser_event(request, body=body.model_dump())
    async with S["db"].connection() as conn, conn.transaction():
        app_row = await (await conn.execute(
            "INSERT INTO applications(student_id, scholarship_id, options) VALUES (%s,%s,%s) "
            "ON CONFLICT (student_id, scholarship_id) DO NOTHING RETURNING id, thread_id",
            (body.student_id, body.scholarship_id,
             Jsonb({"chaos": body.chaos, "draft_key_mode": body.draft_key_mode})))).fetchone()
        if app_row is None:
            old = await (await conn.execute(
                "SELECT id, thread_id, status FROM applications WHERE student_id=%s AND scholarship_id=%s",
                (body.student_id, body.scholarship_id))).fetchone()
            dup = True
        else:
            old, dup = app_row, False
            job_id, _ = await enqueue_job(conn, type_="application_start",
                                          payload={"application_id": app_row["id"], "chaos": body.chaos},
                                          idem_key=f"app-start:{app_row['id']}", stream=config.AI_STREAM)
    if dup:
        await emit(S["r"], "DUPLICATE_REQUEST", route="API → PostgreSQL", application_id=old["id"],
                   note="UNIQUE(student_id, scholarship_id) → 기존 신청 건 반환")
        return {"application_id": old["id"], "thread_id": old["thread_id"], "duplicate": True}
    await emit(S["r"], "DB_COMMIT", route="API → PostgreSQL", application_id=old["id"], thread_id=old["thread_id"],
               job_id=job_id, sql="INSERT applications + jobs + outbox (1 tx)")
    return {"application_id": old["id"], "thread_id": old["thread_id"], "job_id": job_id, "duplicate": False}


class AnswerIn(BaseModel):
    answers: dict


@app.post("/api/applications/{app_id}/answer")
async def answer(app_id: int, body: AnswerIn, request: Request):
    """Resume Gate: '질문 대기 중'인 신청 건에만 resume 작업을 1번 넣는다."""
    await browser_event(request, application_id=app_id, body=body.model_dump())
    async with S["db"].connection() as conn, conn.transaction():
        # WHY 조건부 UPDATE: 학생이 답변 버튼을 두 번 눌러도, 두 번째 요청은 status가 이미 resuming이라 0행.
        row = await (await conn.execute(
            "UPDATE applications SET status='resuming', answer=%s, updated_at=now() "
            "WHERE id=%s AND status='waiting_input' RETURNING thread_id, pending_question",
            (Jsonb(body.answers), app_id))).fetchone()
        if row is None:
            cur = await (await conn.execute("SELECT status FROM applications WHERE id=%s", (app_id,))).fetchone()
            status = cur["status"] if cur else "not_found"
        else:
            interrupt_id = (row["pending_question"] or {}).get("interrupt_id", "x")
            job_id, _ = await enqueue_job(conn, type_="application_resume",
                                          payload={"application_id": app_id, "answers": body.answers},
                                          idem_key=f"app-resume:{app_id}:{interrupt_id}", stream=config.AI_STREAM)
    if row is None:
        await emit(S["r"], "RESUME_GATE_REJECTED", application_id=app_id, status=status,
                   note="waiting_input 상태가 아니므로 resume 작업을 만들지 않음")
        raise HTTPException(409, f"Resume Gate: application {app_id} is '{status}', not waiting_input")
    await emit(S["r"], "DB_COMMIT", route="API → PostgreSQL", application_id=app_id, thread_id=row["thread_id"],
               job_id=job_id, sql="UPDATE applications(status=resuming) + INSERT jobs + outbox (1 tx)")
    return {"application_id": app_id, "thread_id": row["thread_id"], "job_id": job_id}


@app.get("/api/applications/{app_id}/draft")
async def draft(app_id: int):
    async with S["db"].connection() as conn:
        row = await (await conn.execute("SELECT draft_s3_key FROM applications WHERE id=%s", (app_id,))).fetchone()
    if not row or not row["draft_s3_key"]:
        raise HTTPException(404, "draft not ready")
    # 실제 서비스라면 여기서 '이 학생의 신청 건이 맞는지' 권한 검사를 한다 (인증은 실습 범위 밖)
    url = storage.presign(row["draft_s3_key"])
    await emit(S["r"], "PRESIGNED_URL", route="API → Browser", key=row["draft_s3_key"],
               expires_sec=config.PRESIGN_EXPIRES_SEC, note="파일은 API를 거치지 않고 브라우저가 MinIO에서 직접 받는다")
    return {"url": url, "key": row["draft_s3_key"], "expires_sec": config.PRESIGN_EXPIRES_SEC}


# =====================================================================  4) 조회 (화면 복구의 근거)
async def fetch_all(conn, sql, args=()):
    return await (await conn.execute(sql, args)).fetchall()


@app.get("/api/state")
async def state():
    """화면 전체 스냅샷. SSE가 끊겼다가 다시 연결되면 화면은 이것으로 복구된다 (Pub/Sub 이벤트는 다시 오지 않음)."""
    r = S["r"]
    out = {}
    async with S["db"].connection() as conn:
        out["jobs"] = await fetch_all(conn, """
            SELECT j.job_id, j.type, j.status, j.attempts, j.worker_id, j.lease_token, j.stream_msg_id, j.error,
                   j.idempotency_key, j.result, j.created_at, j.updated_at,
                   (SELECT count(*) FROM job_runs r WHERE r.job_id=j.job_id) AS runs,
                   (j.status='created' AND j.created_at < now() - interval '10 seconds'
                     AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.job_id=j.job_id)) AS orphaned
            FROM jobs j ORDER BY j.created_at DESC LIMIT 40""")
        out["job_runs"] = await fetch_all(conn, "SELECT * FROM job_runs ORDER BY run_id DESC LIMIT 40")
        out["outbox"] = await fetch_all(conn, "SELECT id, dedup_key, job_id, stream, created_at, published_at, stream_msg_id, "
                                      "publish_count FROM outbox ORDER BY id DESC LIMIT 30")
        out["ping_results"] = await fetch_all(conn, "SELECT * FROM ping_results ORDER BY id DESC LIMIT 20")
        out["students"] = await fetch_all(conn, "SELECT * FROM students ORDER BY id")
        out["scholarships"] = await fetch_all(conn, "SELECT id, notice_id, source_url, content_hash, s3_key, title, conditions, "
                                            "status, problems, created_at FROM scholarships ORDER BY id")
        out["embeddings"] = (await fetch_all(conn, "SELECT count(*) AS n FROM scholarship_embeddings"))[0]["n"]
        out["recommendations"] = await fetch_all(conn, "SELECT * FROM recommendations ORDER BY scholarship_id, student_id")
        out["notifications"] = await fetch_all(conn, "SELECT * FROM notifications ORDER BY scholarship_id, student_id")
        out["provider_inbox"] = await fetch_all(conn, "SELECT * FROM mock_provider_inbox ORDER BY id")
        out["applications"] = await fetch_all(conn, "SELECT * FROM applications ORDER BY id")
    out["redis"] = await redis_snapshot()
    out["minio"] = await storage.list_objects()
    hb = []
    async for k in r.scan_iter("heartbeat:*"):
        v = await r.get(k)
        if v:
            hb.append(json.loads(v))
    out["heartbeats"] = sorted(hb, key=lambda x: x["worker_id"])
    return out


async def redis_snapshot():
    r = S["r"]
    snap = {}
    for stream, group in ((config.AI_STREAM, config.AI_GROUP), (config.CRAWLER_STREAM, config.CRAWLER_GROUP)):
        s = {"length": await r.xlen(stream), "groups": [], "consumers": [], "pending": [], "dlq": []}
        try:
            s["groups"] = await r.xinfo_groups(stream)
            s["consumers"] = await r.xinfo_consumers(stream, group)
            for p in await r.xpending_range(stream, group, min="-", max="+", count=50):
                e = await r.xrange(stream, p["message_id"], p["message_id"])
                job = json.loads(e[0][1]["job"]) if e else {}
                s["pending"].append({**p, "job_id": job.get("job_id"), "type": job.get("type")})
        except Exception:  # noqa: BLE001  스트림/그룹이 아직 없음
            pass
        for mid, f in await r.xrevrange(f"{stream}:dlq", count=20):
            s["dlq"].append({"msg_id": mid, **f})
        snap[stream] = s
    return snap


@app.get("/api/jobs/{job_id}")
async def job_detail(job_id: str):
    async with S["db"].connection() as conn:
        job = await (await conn.execute("SELECT * FROM jobs WHERE job_id=%s", (job_id,))).fetchone()
        runs = await fetch_all(conn, "SELECT * FROM job_runs WHERE job_id=%s ORDER BY run_id", (job_id,))
    if not job:
        raise HTTPException(404)
    return {"job": job, "runs": runs}


@app.get("/api/threads/{thread_id}")
async def thread(thread_id: str):
    """LangGraph checkpoint는 특별한 것이 아니라 PostgreSQL의 평범한 행이다 — 직접 SQL로 본다."""
    async with S["db"].connection() as conn:
        try:
            cps = await fetch_all(conn, """
                SELECT c.checkpoint_id, c.parent_checkpoint_id, c.metadata->>'step' AS step,
                       c.metadata->>'source' AS source,
                       (SELECT array_agg(DISTINCT w.task_path) FROM checkpoint_writes w
                         WHERE w.thread_id=c.thread_id AND w.checkpoint_id=c.checkpoint_id) AS write_tasks,
                       (SELECT array_agg(DISTINCT w.channel) FROM checkpoint_writes w
                         WHERE w.thread_id=c.thread_id AND w.checkpoint_id=c.checkpoint_id) AS write_channels,
                       jsonb_object_keys_agg.keys AS channel_versions
                FROM checkpoints c
                LEFT JOIN LATERAL (SELECT array_agg(k) AS keys FROM jsonb_object_keys(c.checkpoint->'channel_versions') k)
                     jsonb_object_keys_agg ON true
                WHERE c.thread_id=%s AND c.checkpoint_ns='' ORDER BY c.checkpoint_id""", (thread_id,))
        except UndefinedTable:
            cps = []
    node_runs = await S["r"].hgetall(f"node_exec:{thread_id}")
    return {"thread_id": thread_id, "checkpoints": cps, "node_exec_counts": node_runs}


@app.get("/api/search")
async def search(q: str = "", region: str | None = None):
    """pgvector: SQL 조건 필터 + 의미(벡터) 거리 정렬을 한 쿼리로 (설계 보완 ⑧)."""
    vec = to_pg(embed(q))
    async with S["db"].connection() as conn:
        rows = await fetch_all(conn, """
            SELECT s.id, s.title, e.sentence, round((e.embedding <=> %s::vector)::numeric, 4) AS distance
            FROM scholarship_embeddings e JOIN scholarships s ON s.id=e.scholarship_id
            WHERE s.status='approved' AND (%s::text IS NULL OR s.conditions::text LIKE '%%' || %s || '%%')
            ORDER BY e.embedding <=> %s::vector LIMIT 5""", (vec, region, region, vec))
    return {"query": q, "query_vector": embed(q), "results": rows}


# =====================================================================  5) Pub/Sub -> SSE
@app.get("/api/events")
async def events(request: Request):
    """브라우저 1개 연결 = Redis SUBSCRIBE 1개.
    구독 중이 아닐 때 발행된 메시지는 Redis가 보관하지 않는다 -> 끊긴 동안의 이벤트는 영원히 못 받는다."""
    async def gen():
        ps = S["r"].pubsub()
        await ps.subscribe(config.EVENTS_CHANNEL)
        log("SSE_CONNECTED", client=request.client.host if request.client else None)
        yield "retry: 3000\n\n"
        idle = 0.0
        try:
            while True:
                if await request.is_disconnected():
                    break
                msg = await ps.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg:
                    idle = 0.0
                    yield f"data: {msg['data']}\n\n"
                else:
                    idle += 1.0
                    if idle >= 15:
                        idle = 0.0
                        yield ": keepalive\n\n"
        finally:
            await ps.unsubscribe(config.EVENTS_CHANNEL)
            await ps.aclose()
            log("SSE_DISCONNECTED")
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# =====================================================================  6) 실습 초기화
@app.post("/api/admin/reset")
async def reset():
    """실험을 처음부터 다시 하기 위해 모든 저장소를 비운다 (학생 시드는 유지)."""
    async with S["db"].connection() as conn, conn.transaction():
        await conn.execute("TRUNCATE jobs, job_runs, outbox, ping_results, scholarships, scholarship_embeddings, "
                           "recommendations, notifications, mock_provider_inbox, applications RESTART IDENTITY CASCADE")
        for t in ("checkpoints", "checkpoint_writes", "checkpoint_blobs"):
            await conn.execute(f"DO $$ BEGIN IF to_regclass('{t}') IS NOT NULL THEN EXECUTE 'TRUNCATE {t}'; END IF; END $$")
    r = S["r"]
    await r.delete(config.AI_STREAM, config.CRAWLER_STREAM, f"{config.AI_STREAM}:dlq", f"{config.CRAWLER_STREAM}:dlq")
    for pat in ("chaos_fired:*", "node_exec:*"):
        async for k in r.scan_iter(pat):
            await r.delete(k)
    await storage.delete_all()
    await emit(r, "RESET", note="PostgreSQL/Redis/MinIO 비움. Worker는 NOGROUP을 만나면 그룹을 다시 만든다")
    return {"ok": True}


@app.get("/api/health")
async def health():
    return {"ok": True}
