# Scholarship Lab — 장학금 AI 아키텍처 실습 랩

`Scholarship AI System Architecture v4`를 **직접 실행하며 관찰하기 위한** 작은 분산 시스템입니다.
장학금 기능은 최소한만 넣었고, 대신 서버 간 통신·실행 생명주기·상태 저장·장애 복구는 **진짜로** 동작합니다.

> 개념 설명은 [ARCHITECTURE_TUTORIAL.md](ARCHITECTURE_TUTORIAL.md)에 있습니다.

## 빠른 시작

```powershell
# Docker Desktop이 켜져 있어야 합니다
docker compose up -d --build
```

| 주소 | 내용 |
|---|---|
| http://localhost:8080 | React 관찰 화면 (nginx) |
| http://localhost:8000/docs | API (FastAPI Swagger) — 학습용으로 열어 둠 |
| http://localhost:9001 | MinIO 콘솔 (minioadmin / minioadmin123) |

전체 시나리오 자동 검증 (약 5분, **실행하면 데이터가 초기화됩니다**):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\e2e.ps1
```

로그 보기:

```powershell
docker compose logs -f ai-worker-1 ai-worker-2          # LangGraph·큐 처리
docker compose logs -f api outbox-relay crawler mcp
```

Worker 동시 실행 수 (`.env.example`의 `WORKER_CONCURRENCY`, 기본 5):

- Worker 1개가 작업을 최대 5개까지 동시에 처리합니다. 컴파일된 그래프는 공유하고, 상태는 `thread_id`별 checkpoint에 따로 있습니다.
- 측정 (5초 걸리는 작업 20개, Worker 2개): 동시 1개 **50.4초** → 동시 5개 **10.8초**
- 흐름을 한 줄씩 관찰하려면 `.env` 파일에 `WORKER_CONCURRENCY=1`을 넣고 `docker compose up -d`를 실행하세요 (로그가 섞이지 않음).

## 구성 (컨테이너 11개)

| 서비스 | 기술 | 역할 | 실제/Mock |
|---|---|---|---|
| web | React(Vite) + nginx | 화면, `/api` 프록시, SSE 전달 | 실제 |
| api | FastAPI | 요청 접수, outbox 기록, 조회, SSE, Resume Gate, presigned URL | 실제 |
| outbox-relay | Python | outbox 테이블 → Redis XADD | 실제 |
| redis | Redis 7.4 | Streams(`ai_jobs`, `crawler_jobs`, `*:dlq`), Consumer Group, Pub/Sub(`events`), heartbeat | 실제 |
| crawler | Python + httpx | 공고 수집 → MinIO 저장 → `ai_jobs`에 위치만 XADD | 실제 (대상 사이트는 Mock) |
| ai-worker-1, ai-worker-2 | Python + LangGraph 1.2 | 그래프 실행, interrupt/resume, XAUTOCLAIM, lease, 펜싱 | 실제 |
| mcp | MCP Python SDK 1.30 (Streamable HTTP) | `fetch_page`, `parse_document`, `web_search` 도구 | 프로토콜 실제, 도구 내용 Mock |
| postgres | PostgreSQL 17 + pgvector | 업무 데이터, jobs/outbox, LangGraph checkpoint, embedding | 실제 (RDS 대체) |
| minio | MinIO | 원문·초안 파일, presigned URL | 실제 S3 API (S3 대체) |
| mock-sites | nginx | 가짜 장학금 기관 사이트 (`mock-sites/html`) | **Mock** |

### Mock으로 대체한 것 (코드에 `MOCK` 표시)

| 원래 | 대체 | 위치 |
|---|---|---|
| LLM API (조건 추출, 초안) | 정규식 규칙 + 의도적 환각 1건 | `backend/ai_worker/mock_llm.py` |
| Embedding API | 8차원 해시 벡터 | `backend/common/embed.py` |
| 장학금 기관 사이트 | 정적 파일 nginx | `mock-sites/html/notices/*.txt` |
| 알림 서비스(FCM/이메일) | `mock_provider_inbox` 테이블 | `graphs.py: mock_provider_send` |
| 검색 API | 고정 결과 | `mcp_server/server.py: web_search` |
| AWS RDS / S3 | 로컬 PostgreSQL / MinIO | `docker-compose.yml` |

### 아키텍처 원본과 다른 점

| 원본 | 실습 | 이유 |
|---|---|---|
| 443 HTTPS만 외부 공개 | 8080(HTTP), 8000, 9000, 9001 공개 | 로컬 학습 편의 (curl로 API 직접 호출, MinIO 콘솔 보기) |
| JWT 로그인, 권한 검사 | 생략 | 요구사항상 인증 제외 |
| 관리자 검토 화면 / interrupt | `scholarships.status = needs_review` 표시만 | 학생 interrupt로 같은 메커니즘을 이미 체험 |
| Playwright 크롤러 | httpx GET | 정적 Mock 사이트라 브라우저 불필요 |
| API가 직접 XADD | **Transactional Outbox + outbox-relay 컨테이너 추가** | DB commit ↔ XADD 사이 장애 복구 (시나리오 G) |
| MinIO 공식 이미지 | `pgsty/minio` 고정 태그 | 공식 `minio/minio` 이미지가 더 이상 배포되지 않음 (같은 MinIO 바이너리) |

## 화면 사용법

1. **요청 보내기 ①** ping → 타임라인에서 `Browser → API → PostgreSQL → outbox-relay → Redis → ai-worker → PostgreSQL → XACK` 확인
2. **②수집 실행** → 공고 3건 처리 (1건은 Mock LLM 환각 때문에 `needs_review`)
3. **③신청 준비** → applications 표에 질문 입력칸이 뜸 → 답변 → 초안 다운로드
4. `thread_id` 링크 클릭 → checkpoint 행과 노드 실행 횟수
5. chaos 드롭다운으로 장애 주입, `SSE 15초 끊기`로 Pub/Sub 유실 체험

## 장애 주입 옵션 (`chaos`)

| 값 | 어디서 | 무엇을 |
|---|---|---|
| `crash_after_read` | Worker | 메시지 수신 직후 `os._exit(1)` |
| `crash_before_ack` | Worker | DB commit 후 XACK 전 종료 |
| `always_fail` | Worker(ping) | 매번 예외 → 재시도 3회 후 DLQ |
| `fail_xadd` + `?enqueue=direct` | API | commit 후 XADD 전 실패 (고아 job) |
| `crash_after_xadd` | outbox-relay | XADD 후 발행 표시 전 종료 (중복 XADD) |
| `crash_after_db_write` | ingest 그래프 `save_and_index` | DB commit 후 노드 반환 전 종료 |
| `crash_after_notify` | ingest 그래프 `match_students` | 알림 발송 후 `sent` 표시 전 종료 |
| `crash_in_node` | 신청 그래프 `build_checklist` | 그래프 중간 노드에서 종료 |
| `crash_after_upload` | 신청 그래프 `write_draft` | MinIO 업로드 후 노드 반환 전 종료 |
| `sleep_sec` + `lease=false` | Worker(ping) | lease 갱신 없이 오래 걸리는 작업 |

크래시는 같은 작업/thread에서 **한 번만** 발생합니다 (Redis `chaos_fired:*` 키). 컨테이너는 `restart: unless-stopped`로 자동 재시작됩니다.
그 밖의 장애: `docker compose kill ai-worker-1`, `docker compose pause ai-worker-1`, `docker compose stop outbox-relay`, `docker compose restart redis`.

## 검증 결과 (2026-10-10, Windows 11 + Docker Desktop 28.4)

`scripts/e2e.ps1` 27개 항목 전부 통과: ping 흐름과 멱등성, C(회수·ACK 전 크래시·DLQ), G(고아 job·relay 정지·중복 XADD), A+E(수집·환각 검증·추천·MCP), B(두 학생 interrupt → Worker 둘 다 재시작 → resume, Resume Gate, presigned 다운로드), H(노드 재실행·고아 파일), F(`docker pause` 후 펜싱).
수동으로 확인한 것: SSE 이벤트 수신(nginx 경유), Redis 재시작 후 Stream/Group 유지 및 Worker 재연결, 알림 멱등 키 유무 비교, React 화면 렌더링(Chromium 스크린샷).

**검증하지 않은 것**: 실제 LLM/S3/RDS/외부 사이트 연동, HTTPS·인증, 장시간 부하, Linux/macOS 호스트.

## 문제 해결

- **Git Bash의 curl로 한글 본문 전송 시 `error parsing the body`**: 터미널 인코딩 문제입니다. 파일로 보내세요 (`--data-binary @body.json`) 또는 화면/`e2e.ps1` 사용.
- **처음부터 다시**: 화면의 `전체 초기화` 또는 `curl -X POST localhost:8000/api/admin/reset`. 볼륨까지 지우려면 `docker compose down -v`.

## 디렉터리

```
docker-compose.yml  .env.example  db/init.sql
backend/  common/(obs, streams, jobs, outbox, storage, chaos, embed)  api/  relay/  crawler/  ai_worker/  mcp_server/
web/      src/App.jsx  nginx.conf
mock-sites/html/notices/
scripts/e2e.ps1
```
