-- PostgreSQL 초기 스키마 (컨테이너가 "처음" 만들어질 때 1번만 실행됨. 볼륨을 지우면 다시 실행됨)
-- 업무 데이터 + 작업 추적 테이블. LangGraph checkpoint 테이블은 AI Worker가 시작할 때 setup()으로 만든다.
CREATE EXTENSION IF NOT EXISTS vector;

-- ============ 작업(job) 추적 ============
-- WHY: Redis Stream 메시지는 ACK되면 사라진다. "그 작업이 결국 어떻게 됐는지"는 영구 저장소에 남겨야 한다.
CREATE TABLE jobs (
  job_id          UUID PRIMARY KEY,
  -- 멱등성 키: 같은 요청을 두 번 보내도 job은 1개. 검사는 애플리케이션 if문이 아니라 UNIQUE 제약이 한다.
  idempotency_key TEXT NOT NULL UNIQUE,
  type            TEXT NOT NULL,
  payload         JSONB NOT NULL DEFAULT '{}',
  -- created: DB에만 있음 / queued: Redis에 들어감 / running / done / failed(재시도 대기) / dead(DLQ)
  status          TEXT NOT NULL DEFAULT 'created',
  attempts        INT  NOT NULL DEFAULT 0,
  worker_id       TEXT,
  -- 펜싱 토큰: 새 Worker가 작업을 맡을 때마다 +1. 완료 저장은 "내 토큰이 최신일 때만" 성공한다.
  lease_token     BIGINT NOT NULL DEFAULT 0,
  stream_msg_id   TEXT,
  result          JSONB,
  error           TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 실행 시도 1번 = 1행. "작업 1개가 실제로 몇 번 실행됐나"를 데이터로 보기 위한 테이블.
CREATE TABLE job_runs (
  run_id      BIGSERIAL PRIMARY KEY,
  job_id      UUID NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  worker_id   TEXT NOT NULL,
  container   TEXT NOT NULL,
  pid         INT  NOT NULL,
  lease_token BIGINT NOT NULL,
  delivery    INT  NOT NULL,          -- Redis가 이 메시지를 몇 번째 전달했는가
  outcome     TEXT NOT NULL DEFAULT 'running',  -- running/committed/already_done/error/stale_rejected
  note        TEXT,
  started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ
);

-- Transactional Outbox: "job 생성"과 "큐에 넣을 메시지"를 같은 트랜잭션으로 저장한다.
-- 실제 XADD는 outbox-relay가 나중에 한다. (DB commit 후 XADD 전에 죽어도 메시지가 사라지지 않음)
CREATE TABLE outbox (
  id            BIGSERIAL PRIMARY KEY,
  dedup_key     TEXT NOT NULL UNIQUE,
  job_id        UUID,
  stream        TEXT NOT NULL,
  fields        JSONB NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ,
  stream_msg_id TEXT,
  publish_count INT NOT NULL DEFAULT 0
);

-- 가장 단순한 흐름(API -> Redis -> Worker -> PostgreSQL) 실습용 결과 테이블
CREATE TABLE ping_results (
  id         BIGSERIAL PRIMARY KEY,
  job_id     UUID NOT NULL UNIQUE,   -- WHY UNIQUE: 같은 job이 두 번 실행돼도 결과는 1행
  message    TEXT NOT NULL,
  worker_id  TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============ 업무 데이터 (장학금 도메인은 최소화) ============
CREATE TABLE students (
  id               INT PRIMARY KEY,
  name             TEXT NOT NULL,
  gpa              NUMERIC(3,2),
  income_decile    INT,
  region           TEXT,
  major            TEXT,
  volunteer_hours  INT          -- NULL = 모름(UNKNOWN) -> LangGraph가 학생에게 질문
);
INSERT INTO students VALUES
  (1, '학생A 김하나', 3.80, 4, '서울', '컴퓨터공학', NULL),
  (2, '학생B 이두리', 3.20, 8, '부산', '경영학', 30);

CREATE TABLE scholarships (
  id            BIGSERIAL PRIMARY KEY,
  notice_id     TEXT NOT NULL,
  source_url    TEXT NOT NULL,
  content_hash  TEXT NOT NULL,
  s3_key        TEXT NOT NULL,              -- 원문은 MinIO에. DB에는 위치만.
  title         TEXT,
  conditions    JSONB NOT NULL DEFAULT '[]',
  evidence      JSONB NOT NULL DEFAULT '[]',
  status        TEXT NOT NULL,              -- approved / needs_review / superseded
  problems      JSONB NOT NULL DEFAULT '[]',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- WHY: 같은 공고의 같은 내용(해시)은 몇 번 처리돼도 1행. 내용이 바뀌면 새 버전 행.
  UNIQUE (source_url, content_hash)
);

-- pgvector: 승인된 공고 문장의 embedding (MOCK: 8차원 해시 벡터)
CREATE TABLE scholarship_embeddings (
  scholarship_id BIGINT NOT NULL REFERENCES scholarships(id) ON DELETE CASCADE,
  idx            INT NOT NULL,
  sentence       TEXT NOT NULL,
  embedding      vector(8) NOT NULL,
  PRIMARY KEY (scholarship_id, idx)
);

CREATE TABLE recommendations (
  student_id     INT NOT NULL REFERENCES students(id),
  scholarship_id BIGINT NOT NULL REFERENCES scholarships(id) ON DELETE CASCADE,
  verdict        TEXT NOT NULL,     -- eligible / not_eligible / unknown
  reasons        JSONB NOT NULL,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (student_id, scholarship_id)
);

-- 알림의 "멱등 레코드": 우리 쪽에서 보내기로 한 알림 1건 = 1행
CREATE TABLE notifications (
  student_id      INT NOT NULL,
  scholarship_id  BIGINT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  status          TEXT NOT NULL DEFAULT 'pending',   -- pending -> sent
  attempts        INT NOT NULL DEFAULT 0,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (student_id, scholarship_id)
);

-- MOCK: 외부 알림 서비스(FCM/이메일) 쪽 "받은 편지함".
-- 실제 외부 서비스의 DB를 흉내 낸 것이다. idempotency_key를 지원하는 제공자라면 같은 키는 1번만 접수된다.
-- idempotency_key를 NULL로 보내면(멱등 키 미지원 제공자 흉내) 중복 발송이 그대로 쌓인다.
CREATE TABLE mock_provider_inbox (
  id              BIGSERIAL PRIMARY KEY,
  idempotency_key TEXT UNIQUE,
  recipient       TEXT NOT NULL,
  body            TEXT NOT NULL,
  received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE applications (
  id               BIGSERIAL PRIMARY KEY,
  student_id       INT NOT NULL REFERENCES students(id),
  scholarship_id   BIGINT NOT NULL REFERENCES scholarships(id) ON DELETE CASCADE,
  -- LangGraph thread_id = 신청 건 1개. 학생 A와 B의 그래프 상태는 이 값으로 분리된다.
  thread_id        TEXT GENERATED ALWAYS AS ('app:' || id) STORED,
  -- queued -> running -> waiting_input -> resuming -> running -> done
  status           TEXT NOT NULL DEFAULT 'queued',
  pending_question JSONB,          -- 재접속 시 화면 복구용: "지금 학생에게 무엇을 묻고 있나"
  answer           JSONB,
  checklist        JSONB,
  draft_s3_key     TEXT,
  options          JSONB NOT NULL DEFAULT '{}',   -- 실습용 chaos 옵션
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (student_id, scholarship_id)            -- 같은 학생이 같은 장학금에 신청 건 2개 생성 불가
);
