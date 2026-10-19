import os

DATABASE_URL = os.environ["DATABASE_URL"]
REDIS_URL = os.environ["REDIS_URL"]

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "minio:9000")
S3_PUBLIC_ENDPOINT = os.environ.get("S3_PUBLIC_ENDPOINT", "localhost:9000")
S3_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
S3_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")
S3_BUCKET = os.environ.get("S3_BUCKET", "scholarship")
PRESIGN_EXPIRES_SEC = int(os.environ.get("PRESIGN_EXPIRES_SEC", "300"))

MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://mcp:8100/mcp")
MOCK_SITE_URL = os.environ.get("MOCK_SITE_URL", "http://mock-sites")

CLAIM_MIN_IDLE_MS = int(os.environ.get("CLAIM_MIN_IDLE_MS", "15000"))
LEASE_RENEW_MS = int(os.environ.get("LEASE_RENEW_MS", "5000"))
MAX_DELIVERIES = int(os.environ.get("MAX_DELIVERIES", "3"))
CRAWL_INTERVAL_SEC = int(os.environ.get("CRAWL_INTERVAL_SEC", "0"))
# AI Worker 1개가 동시에 처리하는 작업 수 (1이면 예전처럼 한 번에 1개)
WORKER_CONCURRENCY = max(1, int(os.environ.get("WORKER_CONCURRENCY", "5")))

SERVICE_NAME = os.environ.get("SERVICE_NAME", "unknown")
WORKER_ID = os.environ.get("WORKER_ID", SERVICE_NAME)

# Stream / Consumer Group 이름
# WHY 두 개로 분리: 한 Consumer Group에 Crawler와 AI Worker가 섞이면 맞지 않는 Worker가 작업을 받는다 (설계 보완 ②)
CRAWLER_STREAM, CRAWLER_GROUP = "crawler_jobs", "crawlers"
AI_STREAM, AI_GROUP = "ai_jobs", "ai_workers"
EVENTS_CHANNEL = "events"   # Pub/Sub 채널 (저장되지 않는 진행 신호)
