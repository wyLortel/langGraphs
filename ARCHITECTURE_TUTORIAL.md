# 아키텍처 튜토리얼 — 화살표 하나하나가 실제로 무엇인가

이 문서는 "외우기"용이 아니라 **직접 실행하고 로그로 확인하기** 위한 안내서입니다.
모든 설명에는 "어디서 확인하나"가 붙어 있습니다. 확인 방법이 없는 설명은 믿지 마세요.

---

## 0. 한 장 요약: 식당으로 비유하면

| 시스템 | 식당 | 핵심 |
|---|---|---|
| web (nginx + React) | 출입문과 메뉴판 | 손님에게 화면을 주고, 주문(`/api`)은 카운터로 넘김 |
| api (FastAPI) | 카운터 직원 | 주문을 **장부에 적고 바로 "접수됐어요"** 라고 답함. 요리는 안 함 |
| PostgreSQL | 장부 | 사라지면 안 되는 모든 것 (주문, 결과, 요리 중간 상태) |
| outbox-relay | 장부를 보고 주방 게시판에 붙이는 사람 | 장부에 적힌 주문을 빠짐없이 게시판으로 옮김 |
| Redis Stream | 주방 주문 게시판 | "아직 안 한 일"과 "누가 집어 갔는데 아직 완료 표시 안 한 일" |
| Redis Pub/Sub | 주방 확성기 | "지금 볶는 중!" — 듣는 사람이 없으면 그냥 사라짐 |
| ai-worker × 2 | 요리사 2명 | 게시판에서 주문을 집어 가서 요리 (LangGraph = 레시피) |
| LangGraph Checkpointer | 요리 단계별 메모 (장부에 기록) | "3단계까지 했음" — 요리사가 바뀌어도 4단계부터 이어서 |
| crawler | 시장 보는 사람 | 정해진 곳을 돌며 새 재료(공고)를 사 와서 창고에 넣음 |
| MinIO | 창고 | 큰 물건(원문 파일, 초안). 장부에는 "창고 몇 번 칸"만 적음 |
| mcp | 공구 대여점 (가게 밖 별도 건물) | 요리사가 요리 중 필요한 도구를 **전화(프로토콜)로** 빌림 |

---

## 1. 반드시 구분해야 할 세 가지 생명주기

많은 혼란은 이 셋을 섞어서 생깁니다.

### 1-1. 실행 흐름 (요청 하나가 지나가는 길)

```
[Browser] POST /api/ping
   │ HTTP
[web: nginx] ── /api 프록시 ──▶ [api]
                                  │ SQL: INSERT jobs + INSERT outbox  (한 트랜잭션)
                                  ▼
                              [PostgreSQL] ◀── 1초마다 SELECT ... FOR UPDATE SKIP LOCKED ── [outbox-relay]
                                                                                              │ XADD ai_jobs
                                                                                              ▼
                                                                                          [Redis Stream]
                                                                                              │ XREADGROUP (Consumer Group)
                                                                                              ▼
                                                                                    [ai-worker-1 또는 2]
                                                                                              │ SQL: 결과 INSERT + jobs done (한 트랜잭션)
                                                                                              │ 그 다음 XACK
                                                         모든 단계 ── PUBLISH events ──▶ [Redis Pub/Sub] ──▶ [api SSE] ──▶ [Browser 타임라인]
```

### 1-2. 프로세스 생명주기 (서버 프로그램이 언제 태어나고 죽는가)

| 단어 | 뜻 | 이 프로젝트에서 |
|---|---|---|
| 이미지 | 설계도 (파일 묶음) | `scholarship-lab-backend:dev` **1개**로 Python 서버 6개를 만듦 |
| 컨테이너 | 설계도로 만든 격리된 실행 환경 | `ai-worker-1` 컨테이너, `ai-worker-2` 컨테이너 … |
| 프로세스 | 컨테이너 안에서 실제로 CPU를 쓰는 프로그램 | 컨테이너마다 `python -m ai_worker.main` 1개 (pid=1) |
| 코루틴 / 태스크 | 프로세스 안의 가벼운 작업 단위 | `graph.astream(...)` 실행, heartbeat 루프, lease 갱신 루프 |

- `docker compose up` → 컨테이너 생성 → 그 안의 Python 프로세스 시작 → **작업이 없어도 계속 살아서** 큐를 기다림 (long-running).
- 요청이 올 때 서버가 "켜지는" 것이 아닙니다. 이미 켜져 있는 프로세스가 큐에서 일을 꺼냅니다.
- `docker compose restart ai-worker-1` → 컨테이너는 그대로(container ID 같음), 프로세스만 새로 시작 → **메모리는 전부 사라짐** (그래프 객체 id가 바뀌는 것으로 확인 가능).
- `docker compose down` + `up` → 컨테이너도 새로 만들어짐 (container ID가 바뀜).
- `docker compose pause` → 프로세스가 **살아 있지만 얼어 있음** (시나리오 F).

### 1-3. 데이터 생명주기 (데이터가 어디 있고 언제 사라지나)

| 데이터 | 저장 위치 | 언제 사라지나 | 사라지면? |
|---|---|---|---|
| 진행 이벤트 ("노드 실행 중") | Redis Pub/Sub | **발행 즉시** (구독자가 없으면 그 자리에서) | 괜찮음. 다음 이벤트가 오고, 상태는 DB에서 다시 읽음 |
| 아직 안 한 일 | Redis Stream (엔트리) | 실습에서는 계속 남음 (실서비스는 MAXLEN으로 정리) | 일이 누락됨 → 그래서 outbox가 DB에 원본을 가짐 |
| 누가 가져갔지만 완료 안 된 일 | Redis Stream PEL(pending) | XACK 할 때 | — |
| Worker 생존 신호 | Redis `heartbeat:*` (TTL 10초) | 10초 동안 갱신 없으면 | 화면 표시만 사라짐 (소유권 판단에 안 씀) |
| job의 최종 결과·상태 | PostgreSQL `jobs`, `job_runs` | 지우기 전까지 영구 | 큰일 |
| 업무 데이터 | PostgreSQL (`scholarships`, `applications` …) | 영구 | 큰일 |
| 그래프 중간 상태 | PostgreSQL `checkpoints*` | 영구 | interrupt된 학생의 진행 상황이 사라짐 |
| 원문·초안 파일 | MinIO | 영구 | 파일 유실 (DB에는 위치만 있으므로) |
| 그래프 객체, 노드 지역 변수 | Worker 프로세스 메모리 | **프로세스 종료 시** | 아무 문제 없음 (다시 컴파일하면 됨) |

> 확인: `docker compose restart redis` 후 화면 → Stream과 Consumer Group은 남아 있음 (AOF 켜 둠), Pub/Sub은 원래 저장 개념이 없음.

---

## 2. 구성요소별 10가지 질문

각 구성요소에 대해: ①왜 필요한가 ②없으면? ③언제 실행? ④항상 살아 있나? ⑤누구와 통신? ⑥어떤 데이터? ⑦어디에 저장? ⑧종료되면? ⑨코드 위치 ⑩직접 실험

### 2-1. web (React + nginx)
1. 브라우저에 화면 파일을 주고, `/api` 요청을 API로 넘기는 **유일한 출입문**.
2. 브라우저가 API·Redis에 직접 접근해야 함 → 내부 서버 주소/포트가 외부에 노출됨.
3. `docker compose up` 시. React는 **빌드 시점**에 정적 파일로 바뀌어 nginx에 복사됨 (실행 중 Node 없음).
4. 항상 살아 있음.
5. 브라우저 ↔ web ↔ api.
6. HTML/JS 파일, `/api/*` JSON, `/api/events` SSE 스트림.
7. 저장 안 함 (stateless).
8. 화면 접속 불가. 데이터에는 영향 없음.
9. `web/nginx.conf`(SSE 버퍼링 끄기), `web/src/App.jsx`, `web/Dockerfile`(2단계 빌드).
10. `docker compose stop web` → 화면 안 열림. 하지만 `curl localhost:8000/api/ping`은 여전히 동작 → 화면과 처리 시스템이 분리돼 있음을 확인.

### 2-2. api (FastAPI)
1. 모든 요청의 입구. 요청을 **기록하고 즉시 응답**.
2. 브라우저가 AI 작업 완료까지(수십 초~수 분) 기다려야 하고, API가 재시작되면 진행 중이던 일이 날아감.
3. 컨테이너 시작 시 uvicorn 프로세스 1개 (`API_STARTED` 로그).
4. 항상 살아 있음.
5. PostgreSQL(SQL), Redis(Pub/Sub 구독, 상태 조회), MinIO(presign 계산만, 네트워크 호출 없음).
6. 받는 것: JSON 요청 + `Idempotency-Key` 헤더. 쓰는 것: `jobs`, `outbox`, `applications` 행. 보내는 것: `{job_id, duplicate}`.
7. **직접 저장하지 않음.** 모든 상태는 PostgreSQL/Redis에. (그래서 API를 여러 개 띄워도 됨)
8. 새 요청 접수 불가. 이미 접수된 일은 Worker가 계속 처리함.
9. `backend/api/main.py` — `ping`, `create_application`, `answer`(Resume Gate), `state`, `events`.
10. `docker compose stop api` 상태에서 진행 중인 신청이 계속 완료되는지 Worker 로그로 확인.

### 2-3. PostgreSQL (+pgvector, LangGraph checkpoint)
1. **절대 잃어버리면 안 되는 상태**의 유일한 원본 (Single Source of Truth).
2. 재시작하면 모든 신청·결과·멈춘 그래프가 사라짐.
3. 컨테이너 첫 생성 시 `db/init.sql` 1회 실행, 이후 항상 실행.
4. 항상 살아 있음.
5. api, outbox-relay, ai-worker (crawler는 접근 안 함 — 아키텍처 연결표 기준).
6. 업무 행, `jobs`/`job_runs`/`outbox`, checkpoint(직렬화된 그래프 상태), `vector(8)`.
7. Docker 볼륨 `pgdata`.
8. 쓰기 불가 → API는 접수 실패, Worker는 예외 → ACK 안 함 → DB 복구 후 XAUTOCLAIM으로 재처리.
9. `db/init.sql`(UNIQUE 제약이 멱등성의 실체), `ai_worker/graphs.py: LoggingSaver`.
10. `docker compose exec postgres psql -U lab -d scholarship -c "select thread_id, checkpoint_id, metadata->>'step' from checkpoints order by 2"` — checkpoint는 그냥 DB 행이다.

### 2-4. Redis (Stream · Consumer Group · Pub/Sub)
1. Worker에게 일을 **나눠 주고**, 누가 무엇을 들고 있는지 추적하고, 진행 신호를 방송.
2. Worker가 DB를 계속 polling 해야 하고, "누가 가져갔다 죽은 일"을 직접 구현해야 함.
3~4. 항상 살아 있음.
5. api(구독·조회), outbox-relay(XADD), crawler·ai-worker(XREADGROUP/XACK/XAUTOCLAIM/PUBLISH).
6. Stream 엔트리 `{"job": "{job_id, type, payload, idempotency_key}"}` — **큰 데이터는 넣지 않음** (원문은 MinIO 위치만).
7. 메모리 + AOF 파일(볼륨 `redisdata`).
8. Worker는 `REDIS_DISCONNECTED` 로그 후 1초마다 재연결. 큐 작업은 멈췄다가 재개. Pub/Sub 이벤트는 그 동안 유실.
9. `backend/common/streams.py` (이 프로젝트에서 가장 중요한 파일).
10. ```
    docker compose exec redis redis-cli -a labredis XINFO GROUPS ai_jobs
    docker compose exec redis redis-cli -a labredis XPENDING ai_jobs ai_workers - + 10
    docker compose exec redis redis-cli -a labredis SUBSCRIBE events
    ```

### 2-5. outbox-relay
1. DB에 기록된 "보낼 메시지"를 Redis로 **빠짐없이** 옮김.
2. API가 commit 후 XADD 전에 죽으면 job이 영원히 처리되지 않음 (시나리오 G에서 실제로 재현).
3~4. 항상 살아 있음, 1초마다 확인.
5. PostgreSQL(outbox 읽기/표시), Redis(XADD).
6. outbox 행 → Stream 엔트리.
7. 저장 안 함 (상태는 outbox 테이블의 `published_at`).
8. 메시지가 outbox에 쌓이기만 함. 재시작하면 밀린 것부터 전부 발행 → **사용자가 다시 요청할 필요 없음**.
9. `backend/relay/main.py`, `backend/common/outbox.py`.
10. `docker compose stop outbox-relay` → 화면에서 ping → outbox 표에 ⏳ → `docker compose start outbox-relay` → 자동 처리.

### 2-6. crawler
1. 정해진 기관 사이트를 돌며 새/바뀐 공고를 찾아 원문을 저장하고 AI 작업을 요청.
2. 공고를 사람이 직접 넣어야 함. AI Worker에 넣으면 무거운 수집이 AI 처리를 막음.
3. 항상 살아 있음. `crawler_jobs` 스트림 대기 + (`CRAWL_INTERVAL_SEC>0`이면) 주기 실행.
4. 항상 살아 있음.
5. mock-sites(HTTP GET), MinIO(PUT), Redis(XADD `ai_jobs`).
6. 공고 텍스트 → `raw/{notice}/{hash}.txt`; 큐에는 `{s3_key, source_url, content_hash}`만.
7. MinIO (원문 + `.queued` 표시 파일).
8. 수집 작업이 pending에 남음 → 재시작 후 회수.
9. `backend/crawler/main.py` — `.queued` 표시를 **XADD 이후에** 쓰는 이유가 주석에 있음.
10. `mock-sites/html/notices/busan-2026.txt`의 평점을 `3.0` → `2.8`로 고치고 수집 실행 → 새 버전 행 생성, 이전 버전 `superseded`.

### 2-7. ai-worker × 2 (LangGraph 실행기)
1. 큐에서 일을 꺼내 그래프를 실행하고 결과를 저장.
2. AI 처리를 할 주체가 없음.
3. 컨테이너 시작 시 프로세스 1개 → checkpointer 준비 → **그래프 2개 1번만 컴파일** → 큐 대기.
4. 항상 살아 있음. 작업을 최대 `WORKER_CONCURRENCY`개(기본 5)까지 동시에 처리 (같은 프로세스 안에서 코루틴으로, 컴파일된 그래프 공유). `1`로 두면 1개씩 순서대로.
5. Redis, PostgreSQL, MinIO, MCP 서버, (Mock) LLM.
6. 입력: Stream 메시지. 출력: 업무 행, checkpoint, MinIO 파일, Pub/Sub 이벤트.
7. **아무것도 메모리에만 두지 않음.**
8. 처리 중이던 메시지는 pending에 남고 → 다른 Worker가 XAUTOCLAIM → checkpoint에서 이어감.
9. `backend/ai_worker/main.py`(`decide`, `run_graph`), `graphs.py`(노드), `common/jobs.py`(펜싱).
10. `docker compose logs -f ai-worker-1 | findstr "GRAPH_COMPILED INVOKE_START NODE_ENTER CHECKPOINT"`.

### 2-8. mcp (Tool 서버)
1. AI가 실행 중 필요할 때 쓰는 도구를 **표준 프로토콜**로 제공.
2. 도구 코드를 Worker에 직접 넣으면 도구 변경 = Worker 재배포, 다른 AI 클라이언트가 재사용 불가, 외부 접근 권한(인터넷·API Key)이 Worker 전체에 퍼짐.
3~4. 항상 살아 있음 (HTTP 서버, 요청 시 처리).
5. ai-worker(클라이언트), mock-sites(`fetch_page`가 실제 HTTP GET).
6. JSON-RPC 2.0: `initialize` → `notifications/initialized` → `tools/call`.
7. 저장 안 함 (`stateless_http=True`).
8. 도구 호출이 실패 → 노드 예외 → job 실패 → 재시도.
9. `backend/mcp_server/server.py`(`WireLogger`가 실제 송수신 JSON 기록), `backend/ai_worker/mcp_client.py`.
10. raw 프로토콜 직접 보내기 (API 컨테이너 안에서 실행):
    ```
    docker compose exec api python -c "import httpx;print(httpx.post('http://mcp:8100/mcp',headers={'Accept':'application/json, text/event-stream'},json={'jsonrpc':'2.0','id':1,'method':'tools/list'}).text)"
    ```
    (stateless 모드라 initialize 없이 `tools/list`도 응답합니다.)

### 2-9. MinIO (S3 대체)
1. 큰 파일 저장. DB에는 위치(키)만.
2. DB에 파일을 넣으면 DB가 무거워지고, 다운로드가 API를 통과해야 함.
3~4. 항상 살아 있음.
5. crawler(원문 PUT), ai-worker(원문 GET, 초안 PUT), api(presign 계산), **브라우저(presigned URL로 직접 GET)**.
6. 파일 바이트.
7. 볼륨 `miniodata`.
8. 파일 읽기/쓰기 실패 → 노드 예외 → 재시도.
9. `backend/common/storage.py` (presign용 클라이언트를 따로 둔 이유 주석).
10. http://localhost:9001 콘솔에서 `scholarship` 버킷 확인.

### 2-10. mock-sites
외부 기관 사이트 흉내 (MOCK). `mock-sites/html` 폴더의 파일을 고치면 곧바로 "공고가 바뀐 것"이 됩니다.

---

## 3. LangGraph 생명주기 — 질문마다 로그로 증명

로그 보는 법: `docker compose logs ai-worker-1 ai-worker-2` (또는 화면 타임라인, 필터에 thread_id 입력)

| 질문 | 답 | 증거 (로그/데이터) |
|---|---|---|
| AI Worker 서버는 언제 생성되나? | `docker compose up` 때 컨테이너가 생성되고 Python 프로세스가 시작된다. 요청과 무관. | `PROCESS_START worker_id=ai-worker-1 container=… pid=1` |
| 컨테이너와 프로세스는 같은가? | 아니다. 컨테이너는 격리된 환경, 프로세스는 그 안의 실행 중인 프로그램. `restart`하면 컨테이너는 같고 프로세스만 새로. | restart 전후 `container=` 값 동일, `started`·`graph_obj_id`는 바뀜 |
| 그래프 객체는 언제 만들어지나? | Worker 시작 시 1번 (`build_graphs`). | `GRAPH_COMPILED graph=application graph_obj_id=0x7e5f…` 가 시작 시에만 찍힘 |
| 요청마다 새로 컴파일하나? | 아니다. 모든 작업이 같은 객체 사용. | 여러 작업의 `INVOKE_START graph_obj_id=` 값이 같은 Worker 안에서 항상 같음 |
| `invoke/astream`이 새 프로세스를 만드나? | 아니다. 같은 프로세스·같은 이벤트 루프의 코루틴. | `INVOKE_START … pid=1` — pid 동일, 새 PROCESS_START 없음 |
| 학생 A·B가 같은 그래프 객체를 써도 되나? | 된다. 그래프는 "레시피"이고 상태를 갖지 않는다. 상태는 `thread_id`별 checkpoint에. | `app:1`, `app:3`의 INVOKE_START가 같은 graph_obj_id, 결과는 독립 |
| 공유되는 것 / 분리되는 것 | 공유: 그래프 객체, DB 풀, Redis 연결, checkpointer. 분리: state(thread_id별 checkpoint), 노드 실행 중 지역 변수. | `/api/threads/app:1` vs `app:2` checkpoint 목록이 별개 |
| `thread_id`는 무엇을 구분하나? | "어느 대화/신청 건의 상태인가". 이 프로젝트: `app:{신청 id}`, `ingest:{공고}:{해시}` | `checkpoints.thread_id` 컬럼 |
| Checkpoint는 언제 저장되나? | 입력 시(step -1) + **각 슈퍼스텝(노드) 완료 시마다**. `durability="sync"`라 다음 노드 시작 전에 DB에 씀. | `CHECKPOINT_SAVED step=0,1,2…` 가 `NODE_DONE` 사이에 찍힘 |
| `interrupt()`가 나면 프로세스가 종료되나? | 아니다. 그래프 실행만 끝나고(`astream` 반환), 프로세스는 다음 작업을 받는다. | `INTERRUPT_RAISED` → `INVOKE_END interrupted=True` → `XACK` → 곧바로 다음 `JOB_RECEIVED` (같은 pid) |
| 멈춘 그래프는 메모리에 살아 있나? | 아니다. DB checkpoint 행으로만 존재. | Worker 둘 다 `restart`해도 답변 후 정상 재개 (시나리오 B) |
| 재시작 후 어떻게 이어서 실행하나? | `aget_state(thread_id)`로 마지막 checkpoint를 읽고 `Command(resume=답변)` 또는 `ainvoke(None)`. | `GRAPH_DECISION mode=resume` / `mode=continue reason=중간에 끊긴 실행 발견(next=[…])` |
| Worker 1이 멈춘 일을 Worker 2가 이어받는 이유? | 두 Worker가 **같은 DB의 같은 checkpoint**와 **같은 Consumer Group**을 보기 때문. 어느 Worker에도 고유 상태가 없다. | 시나리오 B: start는 ai-worker-2, resume은 ai-worker-1 (`jobs.worker_id`) |
| resume 시 무엇이 다시 실행되나? | interrupt를 호출한 **노드 전체가 처음부터** 다시 실행된다. 그 앞 노드는 실행되지 않는다. | `BEFORE_INTERRUPT` 로그 2번, 노드 실행 횟수 `ask_student ×2`, `load_context ×1` |

### LangGraph Agent Server(LangGraph Platform) vs 자체 Worker

| | LangGraph Agent Server | 이 프로젝트: 자체 Worker |
|---|---|---|
| 형태 | 그래프를 배포하면 서버가 HTTP API(threads, runs, interrupt/resume)를 제공 | 우리 Python 프로세스가 라이브러리로 그래프를 실행 |
| 큐·재시도·동시성 | 서버가 내장 관리 | Redis Streams + 직접 구현 (`streams.py`) |
| Checkpoint | 서버가 관리 (Postgres) | 공식 `AsyncPostgresSaver`를 직접 연결 |
| 업무 DB와 원자성 | 그래프 상태와 우리 업무 DB가 다른 시스템 | 같은 PostgreSQL → 질문 저장 + job 완료를 한 트랜잭션 |
| 장점 | 코드 적음, 운영 기능(스트리밍, 스케일링, UI) 기본 제공 | 모든 동작이 보이고 통제됨, 다른 작업(crawler 등)과 같은 큐 체계, 추가 서비스/라이선스 없음 |
| 단점 | 내부 동작이 숨겨짐, 배포 형태·비용에 종속 | 재시도·lease·DLQ·펜싱을 직접 책임져야 함 (이 프로젝트 코드량의 대부분) |
| 이번 선택 이유 | — | **학습 목적**: 숨겨진 메커니즘을 직접 보기 위해. 원본 아키텍처도 "AI 작업 서버가 큐에서 꺼내 실행" 구조 |

---

## 4. 시나리오별 기대 로그 · 실패 로그 · 세 가지 '왜'

로그 형식: `[시각] [worker container=… pid=…] EVENT key=value`

### 시나리오 1 — API → Redis → Worker → PostgreSQL (가장 단순한 흐름)

성공 시:
```
api          HTTP_REQUEST route=Browser → API path=/api/ping idempotency_key=…
api          DB_COMMIT    route=API → PostgreSQL sql=INSERT jobs(status=created) + INSERT outbox (같은 트랜잭션)
outbox-relay XADD         route=outbox-relay → Redis stream=ai_jobs msg_id=…
ai-worker-1  JOB_RECEIVED route=Redis → ai-worker-1 how=XREADGROUP delivery=1
ai-worker-1  JOB_STARTED  lease_token=1 attempts=1
ai-worker-1  DB_COMMIT    sql=INSERT ping_results … + UPDATE jobs SET status=done WHERE lease_token=내토큰
ai-worker-1  XACK         note=처리 결과를 영구 저장한 뒤에 ACK
```
같은 `Idempotency-Key`로 다시 보내면: `DUPLICATE_REQUEST … (멱등성 1차 방어)`, 응답 `"duplicate": true`.
실패 신호: `RELAY_ERROR`(DB/Redis 연결), `JOB_FAILED_NO_ACK`, outbox 표에 ⏳가 계속 남음.

- **왜 발생?** (중복) 사용자가 버튼을 두 번 누르거나 네트워크 재시도로 같은 요청이 두 번 온다.
- **왜 이렇게 복구?** `jobs.idempotency_key UNIQUE` 제약이 두 번째 INSERT를 DB 수준에서 막는다. 애플리케이션 `if`문은 동시 요청 두 개가 동시에 "없음"을 볼 수 있어서 못 막는다.
- **이 설계를 빼면?** job 2개, 메시지 2개, 결과 2개.

### 시나리오 C — ACK 전 크래시 / ACK 전 크래시(commit 후) / DLQ

`crash_after_read` 성공 흐름:
```
ai-worker-2  JOB_RECEIVED delivery=1
ai-worker-2  CHAOS_CRASH  point=crash_after_read
(컨테이너 자동 재시작, 15초 후)
ai-worker-2  XAUTOCLAIM_RECLAIMED from_owner=ai-worker-2 idle_ms=17432 delivery=2
ai-worker-2  DB_COMMIT … → XACK
```
`crash_before_ack`:
```
ai-worker-1  DB_COMMIT … inserted=1
ai-worker-1  CHAOS_CRASH point=crash_before_ack
ai-worker-1  XAUTOCLAIM_RECLAIMED delivery=2
ai-worker-1  ALREADY_DONE_SKIP status=done note=중복 전달 감지: 실행하지 않고 ACK만
ai-worker-1  XACK
```
`always_fail`:
```
JOB_FAILED_NO_ACK delivery=1 max=3 → (15초) → delivery=2 → delivery=3
XAUTOCLAIM_RECLAIMED delivery=4
DEAD_LETTER reason=delivered 4 times > MAX_DELIVERIES=3
```
확인: `jobs.status=dead`, `XRANGE ai_jobs:dlq - +`.

| 질문 | 답 |
|---|---|
| 재시도 횟수는 어디서? | **Redis PEL의 `times_delivered`** (XREADGROUP=1, XAUTOCLAIM마다 +1). DB `jobs.attempts`는 관찰용 |
| 멱등성은 어디서 검사? | 1차 API(`UNIQUE idempotency_key`), 2차 Worker 시작 시(`jobs.status`가 done이면 실행 생략), 3차 결과 저장(`UNIQUE` + `ON CONFLICT`) |
| 왜 중복 저장이 안 되나? | 결과 INSERT와 `jobs.status=done`이 **한 트랜잭션**이라 "결과는 있는데 done이 아님" 상태가 없다 |

- **왜 발생?** Redis는 "ACK 받기 전까지 처리 안 된 것"으로 본다. 프로세스가 ACK 전에 죽으면 Redis는 처리 여부를 알 방법이 없다. → 그래서 Redis는 at-least-once(최소 1번)만 보장한다.
- **왜 이렇게 복구?** 다른 Worker가 idle 시간을 보고 XAUTOCLAIM으로 가져가고, DB의 jobs 상태가 "이미 끝났는지" 알려준다.
- **이 설계를 빼면?** ACK를 먼저 하면 → 크래시 시 작업 증발. 멱등성을 빼면 → commit 후 크래시 시 결과 2번. DLQ를 빼면 → 독이 든 메시지가 영원히 재시도되며 Worker를 계속 점유.

### 시나리오 F — 느린 정상 Worker vs 죽은 Worker

lease 없이 (`sleep_sec=40, lease=false`) — **실제로 관찰된 결과**:
```
ai-worker-1  JOB_STARTED lease_token=1
ai-worker-2  XAUTOCLAIM_RECLAIMED from_owner=ai-worker-1 idle_ms=16978 prev_owner_heartbeat_alive=True warning=…느린 것일 수 있다
ai-worker-2  TAKEOVER prev_worker_alive=True note=…DUPLICATE_EXECUTION 위험
(두 Worker 모두 WORKING …)
ai-worker-1  STALE_OWNER_NO_ACK 내 토큰 1이 더 이상 최신이 아님
ai-worker-1  XAUTOCLAIM_RECLAIMED from_owner=ai-worker-2 …   ← 서로 계속 빼앗음
…
DEAD_LETTER delivered 4 times
```
결과: `job_runs` = stale_rejected ×3, `ping_results` 0행, `jobs.status=dead`. **정상 작업이 끝내 실패했다 (livelock).**

lease 있음 (`lease=true`): 5초마다 `LEASE_RENEWED idle_before_ms=5003` → 아무도 회수하지 않고 1번에 완료.

`docker compose pause`로 얼리면:
```
ai-worker-2  XAUTOCLAIM_RECLAIMED from_owner=ai-worker-1 idle_ms=15250 prev_owner_heartbeat_alive=False
ai-worker-2  TAKEOVER prev_token=1 new_token=2 note=이전 실행이 중단된 작업을 이어받음
ai-worker-1  LEASE_LOST new_owner=ai-worker-2          ← unpause 직후
ai-worker-2  DB_COMMIT … XACK
ai-worker-1  STALE_OWNER_NO_ACK 내 토큰 1이 더 이상 최신이 아님
```
결과: `job_runs` = stale_rejected, committed / 결과 1행.

| 장치 | 무엇을 막나 | 무엇을 못 막나 |
|---|---|---|
| min-idle만 (XAUTOCLAIM) | 죽은 Worker의 일 영구 방치 | 느린 Worker를 죽은 것으로 오판 → 중복 실행, livelock |
| heartbeat (`heartbeat:*` TTL) | "프로세스가 살아 있나" 진단 | 프로세스는 살아도 그 **작업**은 멈춤/무한루프일 수 있음. 재시작이 빨라 같은 이름이면 구분 불가 (C 로그의 `prev_owner_heartbeat_alive=True` 참고) |
| lease 갱신 (`XCLAIM JUSTID`로 idle 리셋) | 살아서 일하는 동안 회수 방지 | 갱신 루프는 도는데 본 작업이 멈춘 경우, GC 정지·네트워크 분리로 갱신이 늦는 경우 |
| 펜싱 토큰 (`jobs.lease_token`) | 소유권을 잃은 Worker의 늦은 **DB 쓰기** | DB 밖 부작용 (MinIO, 알림, 외부 API) |
| 멱등성 (UNIQUE, 결정적 키) | 중복 실행의 **결과 중복** | 중복 실행 자체의 비용 (일은 2번 함) |

- **왜 발생?** XAUTOCLAIM은 "얼마나 오래 아무도 손대지 않았나"만 안다. "죽음"과 "느림"을 구분할 정보가 없다.
- **왜 이렇게 복구?** 소유자가 살아 있는 동안 스스로 idle을 리셋(lease)하고, 그래도 뺏겼다면 토큰으로 늦은 쓰기를 거부(fencing).
- **이 설계를 빼면?** lease 없음 → 위 livelock. 펜싱 없음 → 늦게 깨어난 Worker가 최신 결과를 옛 결과로 덮어쓸 수 있음. 그리고 소유권을 잃은 Worker가 XACK 하면, 지금 처리 중인 Worker의 pending 기록이 지워져 그 Worker가 죽을 때 작업이 증발한다(XACK는 소유자를 검사하지 않음).

### 시나리오 G — DB commit과 XADD 사이 장애

나쁜 예 (`?enqueue=direct`, `chaos=fail_xadd`):
```
api DB_COMMIT mode=direct (outbox 없음)
api CHAOS_FAIL_XADD note=commit은 끝났는데 XADD 전에 실패 → status=created 고착
```
화면 jobs 표에 빨간 `(고아!)`. 아무도 처리하지 않는다. 사용자가 같은 키로 재요청해도 "중복"으로 처리된다.

outbox (기본):
```
docker compose stop outbox-relay → ping → outbox ⏳, jobs=created
docker compose start outbox-relay
outbox-relay RELAY_STARTED → XADD outbox_id=8 → ai-worker DB_COMMIT
```
`crash_after_xadd`:
```
outbox-relay XADD outbox_id=9 → CHAOS_CRASH (published_at 표시 전) → RELAY_STARTED → XADD outbox_id=9 (같은 job 2번째)
ai-worker-1  ALREADY_DONE_SKIP
```
`job_runs` = committed, already_done.

- **왜 발생?** PostgreSQL과 Redis는 서로 다른 시스템이라 하나의 트랜잭션으로 묶을 수 없다 (분산 트랜잭션 없음).
- **왜 이렇게 복구?** "보낼 메시지"를 job과 **같은 DB 트랜잭션**에 저장하면 둘은 함께 존재하거나 함께 없다. relay가 성공할 때까지 반복하므로 언젠가 반드시 발행된다. 대신 중복 발행 가능 → Worker 멱등성 필요.
- **이 설계를 빼면?** 고아 job. 대안: reconciliation(주기적으로 `created`이면서 오래된 job을 찾아 재발행) — 더 단순하지만 "오래됨"의 기준이 애매하고 지연이 길다.

### 시나리오 A — 수집에서 추천까지 (+E MCP)

```
crawler     JOB_RECEIVED stream=crawler_jobs
crawler     CRAWL_INDEX route=Crawler → 기관 사이트 (MOCK)
crawler     MINIO_PUT key=raw/hope-2026/01b4fa5f2b055486.txt
crawler     XADD stream=ai_jobs fields={"s3_key": …} note=원문 내용이 아니라 파일 위치만
ai-worker-1 GRAPH_DECISION mode=start
ai-worker-1 NODE_ENTER load_raw → MINIO_GET
ai-worker-1 NODE_ENTER parse_via_mcp → MCP_REQUEST … → MCP_RESPONSE protocol=2025-11-25
mcp         MCP_WIRE_IN jsonrpc={"method":"initialize",…}  /  {"method":"tools/call","params":{"name":"parse_document",…}}
ai-worker-1 LLM_CALL route=AI Worker → LLM API (MOCK)
ai-worker-1 VALIDATED valid=True                  (science-2026은 valid=False, problems=[…LLM 환각 의심])
ai-worker-1 DB_COMMIT scholarship_id=1 status=approved embeddings_inserted=15
ai-worker-1 DB_COMMIT table=recommendations verdicts=[…]
ai-worker-2 MOCK_NOTIFY key=notify:2:2 provider_accepted=True
```
각 노드 사이에 `CHECKPOINT_SAVED step=n`. 두 Worker가 공고를 나눠 **병렬**로 처리하는 것도 보인다.
검색: `curl "localhost:8000/api/search?q=부산 지역"` (브라우저 주소창이 편함) → SQL 조건 + 벡터 거리 정렬.

- **왜 원문을 큐에 안 넣나?** Redis는 메모리. 큰 파일이 쌓이면 메모리를 잡아먹고, 재시도마다 큰 데이터가 오간다. 큐에는 "일의 이름표"만.
- **왜 validate_evidence가 필요?** LLM이 원문에 없는 조건을 지어냄(science-2026). 검증 없이 색인하면 틀린 추천이 나간다.
- **MCP를 왜 별도 서버로?** 도구 교체·권한·재사용을 Worker와 분리. 함수 import였다면 MCP 서버 로그에 `MCP_WIRE_IN`이 찍힐 수 없다 — 네트워크를 탔다는 증거.

### 시나리오 B — interrupt / resume / Worker 교대

```
ai-worker-2 GRAPH_DECISION thread_id=app:1 mode=start
ai-worker-2 BEFORE_INTERRUPT thread_id=app:1
ai-worker-2 INTERRUPT_RAISED value=[{"question":…,"fields":["volunteer_hours","motivation"]}]
ai-worker-2 INVOKE_END next=["ask_student"] interrupted=True note=그래프는 멈췄지만 프로세스는 계속 산다
ai-worker-2 WAITING_FOR_STUDENT … → XACK
(docker compose restart ai-worker-1 ai-worker-2 → PROCESS_EXIT, PROCESS_START, GRAPH_COMPILED graph_obj_id=새 값)
api         DB_COMMIT sql=UPDATE applications(status=resuming) + INSERT jobs + outbox (1 tx)
ai-worker-1 GRAPH_DECISION thread_id=app:1 mode=resume reason=interrupt 대기 중 + 답변 있음 → Command(resume=답변)
ai-worker-1 BEFORE_INTERRUPT thread_id=app:1                 ← 노드가 처음부터 다시 실행됨
ai-worker-1 NODE_ENTER ask_student exec_count=2
ai-worker-1 RESUMED_VALUE answers={…}
ai-worker-1 NODE_ENTER fetch_official_page → MCP fetch_page → build_checklist → write_draft → MINIO_PUT → finalize
```
이중 제출: `409 Resume Gate: application 2 is 'resuming', not waiting_input`.

- **왜 질문을 applications 테이블에도 저장?** 학생이 나중에 접속했을 때 API가 checkpoint를 해석하지 않고도 "지금 무엇을 묻는 중인지" 보여 주기 위해 (설계 보완 ⑦).
- **왜 start job은 interrupt 시 done 처리?** job은 "그래프를 여기까지 실행하라"는 일이고, 그 일은 끝났다. 답변 대기는 job이 아니라 **그래프 상태**다. job을 pending으로 두면 Worker를 점유하거나 XAUTOCLAIM이 계속 회수한다.
- **이 설계를 빼면?** (checkpointer 없음) 재시작 시 처음부터 다시 + 질문 재발송. (Resume Gate 없음) 같은 interrupt에 resume 2번 → 두 번째는 이미 진행된 그래프에 엉뚱한 값을 넣음.

### 시나리오 H — 노드 실행 중 크래시와 부작용

| chaos | 크래시 지점 | 복원된 것 (재실행 안 됨) | 재실행된 것 | 부작용 결과 |
|---|---|---|---|---|
| `crash_after_db_write` | save_and_index의 commit 후 | load_raw, parse_via_mcp, extract_conditions, validate_evidence | save_and_index 전체 | `ON CONFLICT` → `newly_inserted=False`, 행 1개 |
| `crash_after_notify` + 멱등 키 없음 | 발송 후 sent 표시 전 | 앞 노드 전부, recommendations 결과는 같은 값으로 다시 UPSERT | match_students 전체 | **제공자 inbox 2행 = 학생이 알림 2번 받음** |
| `crash_after_notify` + 멱등 키 | 〃 | 〃 | 〃 | 2번째 요청에 `provider_accepted=False`, inbox 1행 |
| `crash_after_upload` + random 키 | write_draft 업로드 후 | ask_student까지(답변 포함), fetch_official_page, build_checklist | write_draft | **MinIO 파일 2개 (1개는 고아)** |
| `crash_after_upload` + 결정적 키 | 〃 | 〃 | 〃 | 같은 키 덮어쓰기 → 파일 1개 |
| `crash_in_node` | build_checklist 시작 | ask_student까지 | build_checklist 이후 | — |

로그로 보는 법: `GRAPH_DECISION mode=continue reason=중간에 끊긴 실행 발견(next=['write_draft'])`, 화면 thread 뷰의 노드 실행 횟수 `write_draft ×2`.

- **왜 발생?** checkpoint는 노드가 **끝났을 때** 저장된다. 노드 안에서 일어난 외부 부작용은 checkpoint가 모른다.
- **왜 이렇게 복구?** 노드를 "다시 실행돼도 안전하게" 만든다: DB는 UNIQUE + UPSERT, 파일은 결정적 키, 외부 발송은 (우리 쪽 멱등 레코드 + 제공자 멱등 키).
- **이 설계를 빼면?** 재실행마다 행·파일·알림이 늘어난다. 멱등 키를 지원하지 않는 외부 서비스라면 "최소 1번(중복 가능)"과 "최대 1번(유실 가능)" 중 하나를 골라야 한다 — 완벽한 exactly-once는 없다.

### 시나리오 D — 데이터 저장 위치의 차이

1. 화면에서 `SSE + 화면 갱신 15초 끊기` → 바로 ③신청 준비 실행
2. 타임라인에 빨간 줄 "SSE 연결 끊음" → 15초 동안 이벤트 **0개**
3. 재연결 → 타임라인에는 그 사이 이벤트가 없음(유실), 하지만 applications 표는 `waiting_input` + 질문까지 정상 표시 (PostgreSQL에서 복구)

API 로그: `SSE_DISCONNECTED` → `SSE_CONNECTED`. Worker의 `emit()`은 구독자 수를 반환하므로, 아무도 안 듣는 동안 발행된 메시지는 0명에게 전달되고 끝.

- **왜 발생?** Pub/Sub은 "지금 듣고 있는 사람에게만 말하기". 저장·재전송 기능이 없다.
- **왜 이렇게 복구?** 화면이 진실을 이벤트에서 조립하지 않고, 재접속 시 DB 스냅샷(`/api/state`)을 다시 읽는다. 이벤트는 "빨리 보여주기"용 힌트일 뿐.
- **이 설계를 빼면?** (이벤트를 Stream에 저장) 재전송은 되지만 소비자 관리·정리 비용이 생긴다. (DB만 polling) 정확하지만 실시간성이 떨어지고 DB 부하가 는다. 이 프로젝트는 둘을 합쳐 쓴다.

---

## 5. 구현 중 실제로 발견된 문제 (교과서가 아니라 진짜)

1. **두 Worker가 동시에 checkpointer `setup()` → PostgreSQL deadlock.** `docker compose up`에서 실제로 발생했고, `restart` 정책 덕분에 재시작 후 정상화. 해결: `pg_advisory_lock`으로 마이그레이션을 한 번에 한 프로세스만 (`graphs.py: build_graphs`). 교훈: "같은 이미지를 여러 개 띄우면 시작 코드도 동시에 돈다."
2. **알림 3단계를 한 트랜잭션으로 묶었다가**, 크래시 시 "외부 제공자 수신 기록"까지 롤백되는 거짓 상태가 됨. 외부 부작용은 DB 트랜잭션으로 되돌릴 수 없으므로 단계별 독립 commit으로 수정.
3. **lease 없이 긴 작업은 정상인데도 DLQ로 감** (시나리오 F의 livelock). min-idle을 늘리는 것만으로는 "가장 긴 작업 시간"을 항상 알아야 하므로 근본 해결이 아니다.

---

## 6. 주요 설계 선택과 대안

| 선택 | 대안 | 트레이드오프 |
|---|---|---|
| Redis Streams + Consumer Group | RabbitMQ, SQS, Kafka, Celery | Streams는 Redis 하나로 큐+Pub/Sub+heartbeat 해결, pending/claim이 명시적이라 학습에 좋음. RabbitMQ는 ack 타임아웃·DLX가 내장, Kafka는 파티션 순서·재생에 강하지만 운영 복잡 |
| Postgres를 업무 DB + checkpoint로 공유 | 별도 checkpoint DB | 같은 DB라 트랜잭션·백업 단순. 부하가 커지면 분리 고려 |
| Transactional Outbox | 직접 XADD, reconciliation, CDC(Debezium) | outbox는 테이블 1개 + relay 1개로 해결. CDC는 더 강력하지만 인프라 큼 |
| lease = PEL idle 리셋 (`XCLAIM JUSTID`) | 별도 Redis lease 키, DB lease 컬럼+만료 시각 | 진실의 원천이 하나(PEL)라 불일치 없음. 단점: Redis 특정 기능에 의존 |
| 펜싱 토큰 = `jobs.lease_token` | 없음 | DB 쓰기는 보호, 외부 부작용은 별도 멱등 키 필요 |
| Worker 1개당 동시 작업 1개 | asyncio 동시 실행 N개 | 관찰 쉬움. 처리량은 Worker 수로 확장 |
| MCP stateless HTTP + 호출마다 세션 | 장기 세션 재사용 | 단순·재시작에 강함 vs 호출당 왕복 1회 추가 |
| pgvector | Qdrant 등 전용 Vector DB | 동기화 문제 없음, SQL 필터와 한 쿼리. 대규모 벡터 부하 시 분리 |

---

## 7. 스스로 답해 보기 (최종 점검)

1. 요청 하나가 들어오면 어떤 컨테이너들이 어떤 순서로 무엇을 주고받는가? (로그 이벤트 이름으로 말해 보기)
2. Redis가 사라지면 무엇을 잃고, PostgreSQL이 사라지면 무엇을 잃는가?
3. Worker가 ACK 직전에 죽었을 때 누가, 언제, 무엇을 근거로 복구하는가?
4. 왜 Redis는 "무엇을 다시 할지", Checkpointer는 "어디서부터 할지"를 맡는가?
5. 그래프 객체 1개를 학생 100명이 공유해도 되는 이유는?
6. Worker가 2개여야 하는 이유는 처리량 때문만인가?
7. MCP 도구를 함수 import로 바꾸면 무엇을 잃는가?
8. 이 아키텍처에서 exactly-once가 보장되는 곳이 있는가? 없다면 무엇으로 대신하는가?
