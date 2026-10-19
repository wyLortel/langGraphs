"""LangGraph 그래프 정의 2개 + 로그를 남기는 Checkpointer.

생명주기 핵심:
  - 이 모듈의 build_graphs()는 Worker 프로세스가 시작할 때 '딱 한 번' 호출된다 (main.py).
  - 컴파일된 그래프 객체는 '실행 방법(설계도)'일 뿐 학생별 상태를 갖지 않는다.
    학생 A와 B는 같은 그래프 객체를 쓰고, 상태는 thread_id별로 PostgreSQL checkpoint 테이블에 따로 저장된다.
  - 노드 함수의 지역 변수는 노드가 끝나면 사라진다. 다음 노드로 넘어가는 것은 '반환한 state'뿐이고,
    그 state는 노드가 끝날 때마다 checkpoint로 DB에 저장된다.
  - 노드 실행 '도중' 프로세스가 죽으면 그 노드는 처음부터 다시 실행된다 (부분 실행 결과는 저장되지 않음).
    -> 노드 안의 외부 부작용(DB, MinIO, 알림)은 다시 실행돼도 안전하게(멱등하게) 만들어야 한다.
"""
import os
import time
import uuid
from typing import TypedDict

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from psycopg.types.json import Jsonb

from common import chaos, storage
from common.embed import embed, to_pg
from common.obs import emit, log

from . import mock_llm
from .mcp_client import call_tool

DEPS = {}          # main.py가 채운다: pool(업무 DB), r(Redis)
CURRENT = {}       # heartbeat 표시용: {thread_id: 지금 실행 중인 node} (동시 실행이라 thread별로)


class LoggingSaver(AsyncPostgresSaver):
    """공식 PostgresSaver 그대로 + 저장될 때마다 로그 1줄. (저장 시점을 눈으로 보기 위함)"""

    async def aput(self, config_, checkpoint, metadata, new_versions):
        out = await super().aput(config_, checkpoint, metadata, new_versions)
        await emit(DEPS["r"], "CHECKPOINT_SAVED", route="LangGraph → PostgreSQL",
                   thread_id=config_["configurable"]["thread_id"],
                   checkpoint_id=out["configurable"]["checkpoint_id"],
                   parent=config_["configurable"].get("checkpoint_id"),
                   step=metadata.get("step"), source=metadata.get("source"),
                   changed_channels=sorted(k for k in new_versions.keys() if not k.startswith("branch:")))
        return out


async def enter(node: str, cfg: RunnableConfig) -> str:
    """노드 시작 로그 + Redis에 '이 thread에서 이 노드가 몇 번 실행됐나' 카운트 (재실행 관찰용)."""
    thread_id = cfg["configurable"]["thread_id"]
    n = await DEPS["r"].hincrby(f"node_exec:{thread_id}", node, 1)
    CURRENT[thread_id] = node
    await emit(DEPS["r"], "NODE_ENTER", route="LangGraph", node=node, thread_id=thread_id, exec_count=n,
               job_id=cfg["configurable"].get("job_id"),
               note=("이 노드가 이 thread에서 다시 실행되는 중 (이전 실행은 checkpoint에 저장되기 전에 끊겼거나 interrupt됨)"
                     if n > 1 else None))
    return thread_id


# ============================================================================ 1) 수집된 공고 처리 그래프
class IngestState(TypedDict, total=False):
    notice_id: str
    source_url: str
    s3_key: str
    content_hash: str
    chaos: str | None
    notify_idempotency: bool
    raw_text: str
    sections: list
    title: str
    conditions: list
    problems: list
    valid: bool
    scholarship_id: int
    verdicts: list


async def load_raw(state: IngestState, config: RunnableConfig):
    await enter("load_raw", config)
    text = await storage.get_text(state["s3_key"])
    await emit(DEPS["r"], "MINIO_GET", route="MinIO → AI Worker", key=state["s3_key"], bytes=len(text.encode()))
    return {"raw_text": text}


async def parse_via_mcp(state: IngestState, config: RunnableConfig):
    tid = await enter("parse_via_mcp", config)
    data = await call_tool(DEPS["r"], "parse_document", {"text": state["raw_text"]}, thread_id=tid)
    return {"sections": data["sections"]}


async def extract_conditions(state: IngestState, config: RunnableConfig):
    tid = await enter("extract_conditions", config)
    res = mock_llm.extract_conditions(state["sections"])
    await emit(DEPS["r"], "LLM_CALL", route="AI Worker → LLM API (MOCK)", thread_id=tid, model=res["model"],
               output={"title": res["title"], "conditions": res["conditions"]})
    return {"title": res["title"], "conditions": res["conditions"]}


async def validate_evidence(state: IngestState, config: RunnableConfig):
    """검증 후 색인 (설계 보완 ④): LLM이 준 근거 문장이 원문에 '실제로' 있는지 확인한다."""
    tid = await enter("validate_evidence", config)
    problems = [{"condition": c, "problem": "근거 문장이 원문에 없음 (LLM 환각 의심)"}
                for c in state["conditions"] if c["evidence"] not in state["raw_text"]]
    if not state["conditions"]:
        problems.append({"problem": "추출된 조건 없음"})
    await emit(DEPS["r"], "VALIDATED", thread_id=tid, valid=not problems, problems=problems)
    return {"problems": problems, "valid": not problems}


async def save_and_index(state: IngestState, config: RunnableConfig):
    tid = await enter("save_and_index", config)
    status = "approved" if state["valid"] else "needs_review"
    async with DEPS["pool"].connection() as conn, conn.transaction():
        # WHY ON CONFLICT: 이 노드가 재실행돼도(크래시 후) 같은 공고 버전은 1행. xmax=0이면 이번에 새로 INSERT된 것.
        row = await (await conn.execute(
            "INSERT INTO scholarships(notice_id, source_url, content_hash, s3_key, title, conditions, evidence, status, problems) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (source_url, content_hash) DO UPDATE SET status=EXCLUDED.status, problems=EXCLUDED.problems "
            "RETURNING id, (xmax = 0) AS inserted",
            (state["notice_id"], state["source_url"], state["content_hash"], state["s3_key"], state["title"],
             Jsonb(state["conditions"]), Jsonb([c["evidence"] for c in state["conditions"]]), status,
             Jsonb(state["problems"])))).fetchone()
        sid = row["id"]
        n_emb = 0
        if state["valid"]:
            # 공고 저장과 embedding 저장을 '같은 트랜잭션'으로: 승인된 공고인데 검색이 안 되는 상태가 생기지 않게
            sentences = [l.strip("-■# ").strip() for l in state["raw_text"].splitlines() if len(l.strip()) > 3]
            for i, s in enumerate(sentences):
                cur = await conn.execute(
                    "INSERT INTO scholarship_embeddings(scholarship_id, idx, sentence, embedding) "
                    "VALUES (%s,%s,%s,%s::vector) ON CONFLICT DO NOTHING", (sid, i, s, to_pg(embed(s))))
                n_emb += cur.rowcount
            # 같은 공고의 예전 버전은 superseded로 (공고 내용 변경 감지 시)
            await conn.execute("UPDATE scholarships SET status='superseded' WHERE source_url=%s AND id<>%s "
                               "AND status='approved'", (state["source_url"], sid))
    await emit(DEPS["r"], "DB_COMMIT", route="AI Worker → PostgreSQL", thread_id=tid, scholarship_id=sid,
               status=status, newly_inserted=row["inserted"], embeddings_inserted=n_emb,
               note=None if row["inserted"] else "이미 있던 행 → ON CONFLICT로 중복 생성 안 됨")
    # 시나리오 H: DB commit은 끝났는데 노드가 반환하기 전에 죽는다 -> checkpoint에는 이 노드 결과가 없다 -> 재실행
    await chaos.crash_if(DEPS["r"], state.get("chaos"), "crash_after_db_write", tid)
    return {"scholarship_id": sid}


def judge(conditions: list, st: dict):
    reasons, verdict = [], "eligible"
    for c in conditions:
        v = st.get(c["field"])
        if v is None:
            reasons.append(f"{c['field']}: UNKNOWN · 학생 정보 없음")
            verdict = "unknown" if verdict == "eligible" else verdict
            continue
        ok = {">=": lambda a, b: float(a) >= float(b), "<=": lambda a, b: float(a) <= float(b),
              "==": lambda a, b: str(a) == str(b)}[c["op"]](v, c["value"])
        reasons.append(f"{c['field']} {v} {c['op']} {c['value']}: {'OK' if ok else 'NO'}")
        if not ok:
            verdict = "not_eligible"
    return verdict, reasons


async def mock_provider_send(conn, idem_key: str | None, recipient: str, body: str) -> bool:
    """MOCK 외부 알림 서비스(FCM/이메일). 실제라면 HTTPS 호출이다.
    멱등 키를 지원하는 제공자는 같은 키를 두 번 받으면 두 번째는 무시한다 -> UNIQUE(idempotency_key)로 흉내."""
    cur = await conn.execute(
        "INSERT INTO mock_provider_inbox(idempotency_key, recipient, body) VALUES (%s,%s,%s) "
        "ON CONFLICT (idempotency_key) DO NOTHING", (idem_key, recipient, body))
    return cur.rowcount == 1


async def match_students(state: IngestState, config: RunnableConfig):
    tid = await enter("match_students", config)
    pool, r = DEPS["pool"], DEPS["r"]
    sid = state["scholarship_id"]
    verdicts = []
    async with pool.connection() as conn, conn.transaction():
        students = await (await conn.execute(
            "SELECT id, name, gpa::float AS gpa, income_decile, region, major, volunteer_hours FROM students")).fetchall()
        for st in students:
            verdict, reasons = judge(state["conditions"], st)
            await conn.execute(
                "INSERT INTO recommendations(student_id, scholarship_id, verdict, reasons) VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (student_id, scholarship_id) DO UPDATE SET verdict=EXCLUDED.verdict, "
                "reasons=EXCLUDED.reasons, updated_at=now()", (st["id"], sid, verdict, Jsonb(reasons)))
            verdicts.append({"student_id": st["id"], "name": st["name"], "verdict": verdict})
    await emit(r, "DB_COMMIT", route="AI Worker → PostgreSQL", thread_id=tid, table="recommendations",
               verdicts=verdicts)

    # 알림: eligible 학생에게만. 3단계 = (1) 멱등 레코드 확보 (2) 외부 발송 (3) sent 표시
    use_key = state.get("notify_idempotency", True)
    for v in [v for v in verdicts if v["verdict"] == "eligible"]:
        key = f"notify:{v['student_id']}:{sid}"
        # WHY 단계마다 별도 commit: 외부 발송은 DB 트랜잭션으로 되돌릴 수 없다.
        #   세 단계를 한 트랜잭션에 넣으면 '발송은 됐는데 기록은 롤백'되는 거짓 상태가 된다.
        async with pool.connection() as conn, conn.transaction():          # (1) 멱등 레코드 확보
            await conn.execute("INSERT INTO notifications(student_id, scholarship_id, idempotency_key) "
                               "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING", (v["student_id"], sid, key))
            row = await (await conn.execute("UPDATE notifications SET attempts=attempts+1, updated_at=now() "
                                            "WHERE idempotency_key=%s RETURNING status, attempts", (key,))).fetchone()
        if row["status"] == "sent":
            await emit(r, "NOTIFY_SKIP", thread_id=tid, key=key, note="이미 sent → 다시 보내지 않음")
            continue
        async with pool.connection() as conn, conn.transaction():          # (2) 외부 발송 (MOCK 제공자 쪽 commit)
            accepted = await mock_provider_send(conn, key if use_key else None, v["name"],
                                                f"새 장학금 '{state['title']}' 자격 충족!")
        await emit(r, "MOCK_NOTIFY", route="AI Worker → 알림 서비스 (MOCK)", thread_id=tid, key=key,
                   attempt=row["attempts"], sent_idempotency_key=use_key, provider_accepted=accepted,
                   note=(None if accepted else "제공자가 같은 멱등 키를 이미 받음 → 학생 휴대폰엔 1번만 감"))
        # 시나리오 H: 발송은 됐는데 'sent' 표시 전에 죽는다 -> 재실행 시 다시 발송 시도
        await chaos.crash_if(r, state.get("chaos"), "crash_after_notify", tid)
        async with pool.connection() as conn, conn.transaction():          # (3) 발송 완료 표시
            await conn.execute("UPDATE notifications SET status='sent', updated_at=now() WHERE idempotency_key=%s",
                               (key,))
    return {"verdicts": verdicts}


def route_after_save(state: IngestState):
    return "match_students" if state["valid"] else END


# ============================================================================ 2) 신청 준비 그래프 (interrupt 포함)
class AppState(TypedDict, total=False):
    application_id: int
    chaos: str | None
    draft_key_mode: str
    student: dict
    scholarship: dict
    missing: list
    answers: dict
    required_docs: list
    checklist: list
    draft_key: str


async def load_context(state: AppState, config: RunnableConfig):
    await enter("load_context", config)
    async with DEPS["pool"].connection() as conn:
        row = await (await conn.execute(
            "SELECT a.student_id, a.scholarship_id, s.name, s.gpa::float AS gpa, s.income_decile, s.region, s.major, "
            "s.volunteer_hours, sc.title, sc.source_url, sc.conditions "
            "FROM applications a JOIN students s ON s.id=a.student_id JOIN scholarships sc ON sc.id=a.scholarship_id "
            "WHERE a.id=%s", (state["application_id"],))).fetchone()
    student = {k: row[k] for k in ("name", "gpa", "income_decile", "region", "major", "volunteer_hours")}
    student["id"] = row["student_id"]
    scholarship = {"id": row["scholarship_id"], "title": row["title"], "source_url": row["source_url"],
                   "conditions": row["conditions"]}
    return {"student": student, "scholarship": scholarship}


async def check_missing(state: AppState, config: RunnableConfig):
    tid = await enter("check_missing", config)
    missing = [c["field"] for c in state["scholarship"]["conditions"] if state["student"].get(c["field"]) is None]
    missing.append("motivation")   # 지원 동기는 항상 학생에게 묻는다
    await emit(DEPS["r"], "UNKNOWN_FIELDS", thread_id=tid, missing=missing)
    return {"missing": missing}


async def ask_student(state: AppState, config: RunnableConfig):
    tid = await enter("ask_student", config)
    question = {"question": "신청서를 준비하려면 다음 정보가 필요해요.", "fields": state["missing"]}
    log("BEFORE_INTERRUPT", thread_id=tid,
        note="interrupt() 직전 코드. resume되면 이 노드가 처음부터 다시 실행되므로 이 로그가 한 번 더 찍힌다 "
             "→ 여기에 DB 저장/알림 같은 부작용을 두면 중복된다")
    # 처음 실행: 여기서 GraphInterrupt 예외가 나며 그래프가 멈춘다 (프로세스는 멈추지 않는다!)
    # resume 실행: 여기서 Command(resume=값)의 값이 반환된다
    answers = interrupt(question)
    await emit(DEPS["r"], "RESUMED_VALUE", route="LangGraph", thread_id=tid, node="ask_student", answers=answers)
    return {"answers": answers}


async def fetch_official_page(state: AppState, config: RunnableConfig):
    tid = await enter("fetch_official_page", config)
    page = await call_tool(DEPS["r"], "fetch_page", {"url": state["scholarship"]["source_url"]}, thread_id=tid)
    docs, on = [], False
    for line in page["text"].splitlines():
        if line.startswith("■"):
            on = "제출 서류" in line
        elif on and line.strip().startswith("-"):
            docs.append(line.strip("- ").strip())
    return {"required_docs": docs}


HOW_TO = {"재학증명서": "학교 포털", "성적증명서": "학교 포털", "소득분위 확인서": "한국장학재단",
          "주민등록초본": "정부24", "자기소개서": "AI 초안 제공", "학업계획서": "AI 초안 제공"}


async def build_checklist(state: AppState, config: RunnableConfig):
    tid = await enter("build_checklist", config)
    # 시나리오 C/H: 그래프 '중간' 노드에서 죽는다 -> 앞 노드들 결과는 checkpoint에서 복원, 이 노드부터 재실행
    await chaos.crash_if(DEPS["r"], state.get("chaos"), "crash_in_node", tid)
    return {"checklist": [f"{d} — 발급: {HOW_TO.get(d, '기관 확인 필요')}" for d in state["required_docs"]]}


async def write_draft(state: AppState, config: RunnableConfig):
    tid = await enter("write_draft", config)
    text = mock_llm.draft_essay(state["student"], state["scholarship"], state["answers"], state["checklist"])
    if state.get("draft_key_mode") == "random":
        key = f"drafts/app-{state['application_id']}/{uuid.uuid4()}.md"     # 나쁜 예: 재실행마다 새 파일
    else:
        key = f"drafts/app-{state['application_id']}/draft.md"              # 좋은 예: 재실행해도 같은 파일 덮어쓰기
    size = await storage.put_text(key, text, "text/markdown; charset=utf-8")
    await emit(DEPS["r"], "MINIO_PUT", route="AI Worker → MinIO", thread_id=tid, key=key, bytes=size,
               key_mode=state.get("draft_key_mode"))
    # 시나리오 H: 파일은 올라갔는데 노드 반환 전에 죽는다 -> checkpoint에 draft_key 없음 -> 노드 재실행 -> 다시 업로드
    await chaos.crash_if(DEPS["r"], state.get("chaos"), "crash_after_upload", tid)
    return {"draft_key": key}


async def finalize(state: AppState, config: RunnableConfig):
    tid = await enter("finalize", config)
    async with DEPS["pool"].connection() as conn:
        await conn.execute("UPDATE applications SET status='done', checklist=%s, draft_s3_key=%s, pending_question=NULL, "
                           "updated_at=now() WHERE id=%s",
                           (Jsonb(state["checklist"]), state["draft_key"], state["application_id"]))
    await emit(DEPS["r"], "DB_COMMIT", route="AI Worker → PostgreSQL", thread_id=tid,
               sql="UPDATE applications SET status=done, draft_s3_key", draft_key=state["draft_key"])
    return {}


# ============================================================================ 조립 (프로세스당 1번)
async def build_graphs(saver_pool):
    t0 = time.time()
    saver = LoggingSaver(saver_pool)
    # WHY advisory lock: Worker 2개가 동시에 시작하면 둘 다 setup()(테이블/인덱스 마이그레이션)을 실행해
    #   PostgreSQL deadlock이 실제로 발생했다. DB 수준 잠금으로 "한 번에 한 프로세스만" 마이그레이션하게 한다.
    async with saver_pool.connection() as lock_conn:
        await lock_conn.execute("SELECT pg_advisory_lock(424242)")
        try:
            await saver.setup()   # checkpoints / checkpoint_writes / checkpoint_blobs 테이블 생성 (이미 있으면 그대로)
        finally:
            await lock_conn.execute("SELECT pg_advisory_unlock(424242)")
    log("CHECKPOINTER_READY", type="AsyncPostgresSaver", tables="checkpoints, checkpoint_writes, checkpoint_blobs")

    g = StateGraph(IngestState)
    for fn in (load_raw, parse_via_mcp, extract_conditions, validate_evidence, save_and_index, match_students):
        g.add_node(fn.__name__, fn)
    g.add_edge(START, "load_raw")
    g.add_edge("load_raw", "parse_via_mcp")
    g.add_edge("parse_via_mcp", "extract_conditions")
    g.add_edge("extract_conditions", "validate_evidence")
    g.add_edge("validate_evidence", "save_and_index")
    g.add_conditional_edges("save_and_index", route_after_save, ["match_students", END])
    g.add_edge("match_students", END)
    ingest = g.compile(checkpointer=saver)

    a = StateGraph(AppState)
    order = [load_context, check_missing, ask_student, fetch_official_page, build_checklist, write_draft, finalize]
    for fn in order:
        a.add_node(fn.__name__, fn)
    a.add_edge(START, order[0].__name__)
    for x, y in zip(order, order[1:]):
        a.add_edge(x.__name__, y.__name__)
    a.add_edge(order[-1].__name__, END)
    application = a.compile(checkpointer=saver)

    for name, obj in (("ingest", ingest), ("application", application)):
        log("GRAPH_COMPILED", graph=name, graph_obj_id=hex(id(obj)), pid=os.getpid(),
            note="프로세스 시작 시 1번만. 이후 모든 학생/작업이 이 객체를 공유한다")
    log("GRAPHS_READY", seconds=round(time.time() - t0, 3))
    return {"ingest": ingest, "application": application, "compiled_at": time.time()}
