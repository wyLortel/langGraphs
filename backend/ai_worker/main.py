"""AI Worker 프로세스 진입점.

프로세스 생명주기 (로그로 확인 가능):
  PROCESS_START      : docker compose up / restart 시 컨테이너 안에서 python 프로세스 1개가 시작
  CHECKPOINTER_READY : PostgreSQL checkpoint 테이블 준비
  GRAPH_COMPILED     : 그래프 2개를 '한 번만' 컴파일 (graph_obj_id 확인)
  CONSUMER_READY     : 이제부터 Redis ai_jobs를 무한 반복으로 기다림
  (작업마다) JOB_RECEIVED -> JOB_STARTED -> GRAPH_DECISION -> INVOKE_START -> NODE_ENTER... -> INVOKE_END -> XACK
  PROCESS_EXIT       : docker compose stop (SIGTERM) 시

invoke/astream은 새 프로세스나 스레드를 만들지 않는다. 같은 pid의 같은 이벤트 루프 안에서 코루틴으로 실행된다.
이 Worker는 작업을 최대 WORKER_CONCURRENCY개(기본 5)까지 동시에 처리한다.
  WHY: 작업 시간의 대부분은 DB·MCP·LLM 응답을 '기다리는' 시간이라, 기다리는 동안 다른 작업을 진행한다.
  그래프는 프로세스당 1번만 컴파일하고 모든 작업이 공유한다. 작업별 상태는 thread_id별 checkpoint에 따로 있다.
  더 많은 처리량이 필요하면 동시 실행 수를 측정하며 늘리거나 Worker 컨테이너를 늘린다.
  흐름을 관찰할 때는 .env에 WORKER_CONCURRENCY=1 을 두면 예전처럼 한 번에 1개씩 처리한다.
"""
import asyncio
import os
import platform
import signal
import time

from langgraph.types import Command
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from common import config, jobs, storage
from common.infra import make_db_pool, make_redis
from common.obs import CONTAINER, emit, log
from common.streams import StaleOwner, StreamConsumer

from . import graphs

G = {}


# ------------------------------------------------------------------ job handlers
async def handle_ping(msg, lease, pool, r):
    p = msg.job["payload"]
    if p.get("chaos") == "always_fail":
        raise RuntimeError("chaos=always_fail: 이 작업은 매번 실패한다 → 재시도 후 DLQ")
    for i in range(int(p.get("sleep_sec") or 0)):
        await asyncio.sleep(1)
        if (i + 1) % 5 == 0:
            await emit(r, "WORKING", job_id=lease.job_id, elapsed_sec=i + 1, total_sec=p["sleep_sec"],
                       lease_renewal=p.get("lease", True))
    async with pool.connection() as conn, conn.transaction():
        cur = await conn.execute(
            "INSERT INTO ping_results(job_id, message, worker_id) VALUES (%s,%s,%s) ON CONFLICT (job_id) DO NOTHING",
            (lease.job_id, p.get("message", ""), config.WORKER_ID))
        inserted = cur.rowcount
        # 결과 저장과 job 완료 표시를 같은 트랜잭션으로 -> 둘 다 되거나 둘 다 안 된다
        await jobs.complete(conn, lease, {"ping_results_inserted": inserted})
    await emit(r, "DB_COMMIT", route=f"{config.WORKER_ID} → PostgreSQL", job_id=lease.job_id,
               sql="INSERT ping_results ON CONFLICT DO NOTHING + UPDATE jobs SET status=done WHERE lease_token=내토큰",
               inserted=inserted, lease_token=lease.token)


async def run_graph(name, inp, cfg, r):
    graph = G[name]
    tid = cfg["configurable"]["thread_id"]
    await emit(r, "INVOKE_START", route="AI Worker → LangGraph", graph=name, graph_obj_id=hex(id(graph)),
               thread_id=tid, pid=os.getpid(), input_type=type(inp).__name__,
               note="새 프로세스/스레드 없음. 같은 프로세스 안에서 코루틴으로 실행")
    try:
        # durability="sync": 다음 노드로 넘어가기 전에 checkpoint를 DB에 확실히 쓴다 (크래시 실험에서 결과가 결정적이도록)
        async for chunk in graph.astream(inp, cfg, stream_mode="updates", durability="sync"):
            for node, upd in chunk.items():
                if node == "__interrupt__":
                    await emit(r, "INTERRUPT_RAISED", route="LangGraph", thread_id=tid,
                               value=[i.value for i in upd])
                else:
                    await emit(r, "NODE_DONE", route="LangGraph", thread_id=tid, node=node,
                               returned_keys=sorted((upd or {}).keys()))
    finally:
        graphs.CURRENT.pop(tid, None)
    snap = await graph.aget_state(cfg)
    await emit(r, "INVOKE_END", route="LangGraph → AI Worker", graph=name, thread_id=tid, next=list(snap.next),
               interrupted=bool(snap.interrupts), checkpoint_id=snap.config["configurable"].get("checkpoint_id"),
               note=("그래프는 멈췄지만 프로세스는 계속 산다. 멈춘 상태는 메모리가 아니라 PostgreSQL에 있다"
                     if snap.interrupts else None))
    return snap


async def decide(name, cfg, resume_value, r):
    """'Redis는 무엇을 다시 할지, Checkpointer는 어디서부터 할지'를 결정하는 단 하나의 진입점."""
    snap = await G[name].aget_state(cfg)
    has_cp = bool(snap.values) or bool(snap.next)
    if not has_cp:
        mode, reason = "start", "checkpoint 없음 → 처음부터 실행"
    elif snap.interrupts:
        if resume_value is not None:
            mode, reason = "resume", "interrupt 대기 중 + 답변 있음 → Command(resume=답변)"
        else:
            mode, reason = "noop", "이미 질문 대기 중 (중복 전달된 start 작업) → 아무것도 안 함"
    elif snap.next:
        mode, reason = "continue", f"중간에 끊긴 실행 발견(next={list(snap.next)}) → ainvoke(None)으로 마지막 checkpoint부터 이어감"
    else:
        mode, reason = "noop", "이미 끝까지 실행된 thread → 아무것도 안 함"
    await emit(r, "GRAPH_DECISION", graph=name, thread_id=cfg["configurable"]["thread_id"], mode=mode, reason=reason,
               last_checkpoint_id=snap.config["configurable"].get("checkpoint_id") if has_cp else None)
    return mode, snap


async def handle_ingest(msg, lease, pool, r):
    p = msg.job["payload"]
    tid = f"ingest:{p['notice_id']}:{p['content_hash'][:12]}"
    cfg = {"configurable": {"thread_id": tid, "job_id": lease.job_id}}
    mode, snap = await decide("ingest", cfg, None, r)
    if mode == "start":
        snap = await run_graph("ingest", {k: p.get(k) for k in ("notice_id", "source_url", "s3_key", "content_hash",
                                                                 "chaos", "notify_idempotency")}, cfg, r)
    elif mode == "continue":
        snap = await run_graph("ingest", None, cfg, r)
    async with pool.connection() as conn, conn.transaction():
        await jobs.complete(conn, lease, {"thread_id": tid, "mode": mode,
                                          "scholarship_id": snap.values.get("scholarship_id"),
                                          "valid": snap.values.get("valid")})


async def handle_application(msg, lease, pool, r):
    p = msg.job["payload"]
    app_id = p["application_id"]
    tid = f"app:{app_id}"
    cfg = {"configurable": {"thread_id": tid, "job_id": lease.job_id}}
    resume_value = p.get("answers") if msg.job["type"] == "application_resume" else None
    mode, snap = await decide("application", cfg, resume_value, r)

    async with pool.connection() as conn:
        app = await (await conn.execute(
            "UPDATE applications SET status='running', updated_at=now() WHERE id=%s "
            "AND status IN ('queued','resuming','running') RETURNING options", (app_id,))).fetchone()
    if mode == "start":
        opts = (app or {}).get("options") or {}
        snap = await run_graph("application", {"application_id": app_id, "chaos": opts.get("chaos"),
                                               "draft_key_mode": opts.get("draft_key_mode", "deterministic")}, cfg, r)
    elif mode == "resume":
        snap = await run_graph("application", Command(resume=resume_value), cfg, r)
    elif mode == "continue":
        snap = await run_graph("application", None, cfg, r)

    async with pool.connection() as conn, conn.transaction():
        if snap.interrupts:
            intr = snap.interrupts[0]
            question = {"interrupt_id": getattr(intr, "id", None) or "x", **intr.value}
            # 질문을 업무 DB에도 저장: 학생이 나중에 접속해도 API가 이 행을 읽어 질문을 다시 보여줄 수 있다
            await conn.execute("UPDATE applications SET status='waiting_input', pending_question=%s, updated_at=now() "
                               "WHERE id=%s", (Jsonb(question), app_id))
        await jobs.complete(conn, lease, {"thread_id": tid, "mode": mode, "interrupted": bool(snap.interrupts),
                                          "next": list(snap.next)})
    if snap.interrupts:
        await emit(r, "WAITING_FOR_STUDENT", route="AI Worker → Redis Pub/Sub → API → Browser",
                   application_id=app_id, thread_id=tid, question=snap.interrupts[0].value,
                   note="작업(job)은 완료·ACK된다. 그래프 상태만 checkpoint로 남아 답변을 기다린다")


HANDLERS = {"ping": handle_ping, "ingest": handle_ingest,
            "application_start": handle_application, "application_resume": handle_application}


# ------------------------------------------------------------------ main
async def main():
    log("PROCESS_START", worker_id=config.WORKER_ID, container=CONTAINER, pid=os.getpid(),
        python=platform.python_version(),
        note="컨테이너 1개 = 이 Python 프로세스 1개. 프로세스는 작업이 없어도 계속 살아서 큐를 기다린다")
    # WHY 풀 크기를 동시 실행 수에 맞춤: 작업마다 DB 연결을 잡으므로 풀이 작으면 작업들이 연결을 기다리며 줄을 선다
    db_max = max(5, config.WORKER_CONCURRENCY + 2)
    pool = await make_db_pool(max_size=db_max)
    r = make_redis()
    await storage.ensure_bucket()
    # Checkpointer 전용 풀: PostgresSaver는 autocommit + dict_row 연결을 요구한다
    saver_pool = AsyncConnectionPool(config.DATABASE_URL, min_size=1, max_size=db_max, open=False,
                                     kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row})
    await saver_pool.open(wait=True)
    graphs.DEPS.update(pool=pool, r=r)
    G.update(await graphs.build_graphs(saver_pool))

    async def handle(msg):
        lease = await jobs.begin(pool, r, msg)
        if lease is None:
            return                      # 이미 끝난 작업 -> 실행 없이 ACK
        try:
            await HANDLERS[msg.job["type"]](msg, lease, pool, r)
        except StaleOwner as e:
            await jobs.mark_run(pool, lease, "stale_rejected", str(e))
            raise
        except Exception as e:
            await jobs.fail(pool, lease, repr(e))
            raise

    async def on_dead(msg, reason):
        await jobs.mark_dead(pool, msg, reason)

    consumer = StreamConsumer(
        r, config.AI_STREAM, config.AI_GROUP, handle, on_dead=on_dead, concurrency=config.WORKER_CONCURRENCY,
        status_extra=lambda: {"graph_obj_ids": {k: hex(id(v)) for k, v in G.items() if k != "compiled_at"},
                              "graph_compiled_at": G.get("compiled_at"),
                              "current_thread": ", ".join(graphs.CURRENT) or None,
                              "current_node": ", ".join(graphs.CURRENT.values()) or None})

    def on_sigterm():
        log("SIGTERM_RECEIVED", note="처리 중인 작업들을 마치면 종료 (ACK 안 된 작업은 pending에 남아 다른 Worker가 회수)")
        consumer.stopping = True
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, on_sigterm)

    started = time.time()
    await consumer.run()
    log("PROCESS_EXIT", pid=os.getpid(), uptime_sec=round(time.time() - started, 1))


if __name__ == "__main__":
    asyncio.run(main())
