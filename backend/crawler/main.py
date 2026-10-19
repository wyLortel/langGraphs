"""Crawler(수집 서버) — crawler_jobs 스트림을 소비해서 기관 사이트(MOCK)를 돌고, 원문을 MinIO에 저장하고,
'파일 위치'만 ai_jobs 스트림에 넘긴다. Crawler는 PostgreSQL을 쓰지 않는다 (아키텍처 연결표 기준).

변경 감지: 원문의 sha256 해시를 파일 이름에 넣는다. 같은 내용 = 같은 키.
  raw/{notice}/{hash}.txt     원문
  raw/{notice}/{hash}.queued  "AI 작업을 큐에 넣었음" 표시
WHY 표시를 따로 두나: 원문 저장 후 XADD 전에 죽으면, 원문만 보고 '이미 처리함'이라 판단해 작업이 영영 누락된다.
  XADD까지 끝난 뒤에 .queued를 쓰면, 그 사이에 죽어도 다음 수집 때 다시 XADD 한다 (중복은 AI Worker가 job_id로 걸러냄).
"""
import asyncio
import hashlib
import json
import os
import signal

import httpx

from common import config, storage
from common.infra import make_redis
from common.jobs import job_id_for, stream_fields
from common.obs import emit, log
from common.streams import StreamConsumer


async def crawl(msg, r):
    payload = msg.job.get("payload") or {}
    async with httpx.AsyncClient(timeout=5) as c:
        index = (await c.get(f"{config.MOCK_SITE_URL}/notices/index.json")).json()
        await emit(r, "CRAWL_INDEX", route="Crawler → 기관 사이트 (MOCK)", url=f"{config.MOCK_SITE_URL}/notices/index.json",
                   notices=[n["id"] for n in index])
        for n in index:
            url = f"{config.MOCK_SITE_URL}/notices/{n['file']}"
            text = (await c.get(url)).text
            h = hashlib.sha256(text.encode()).hexdigest()
            key = f"raw/{n['id']}/{h[:16]}.txt"
            marker = f"raw/{n['id']}/{h[:16]}.queued"
            if await storage.exists(marker):
                await emit(r, "UNCHANGED", notice=n["id"], hash=h[:16], note="같은 내용을 이미 큐에 넣음 → 건너뜀")
                continue
            size = await storage.put_text(key, text)
            await emit(r, "MINIO_PUT", route="Crawler → MinIO", key=key, bytes=size, notice=n["id"])
            idem = f"ingest:{n['id']}:{h}"
            job_id = job_id_for(idem)
            ingest_payload = {"notice_id": n["id"], "source_url": url, "s3_key": key, "content_hash": h,
                              "chaos": payload.get("chaos"),
                              "notify_idempotency": payload.get("notify_idempotency", True)}
            msg_id = await r.xadd(config.AI_STREAM, stream_fields(job_id, "ingest", ingest_payload, idem))
            await emit(r, "XADD", route="Crawler → Redis", stream=config.AI_STREAM, msg_id=msg_id, job_id=job_id,
                       fields={"s3_key": key, "notice_id": n["id"]},
                       note="원문 내용이 아니라 파일 위치만 큐에 넣는다")
            await storage.put_text(marker, msg_id)


async def scheduler(r):
    """CRAWL_INTERVAL_SEC > 0이면 주기적으로 crawler_jobs에 수집 작업을 넣는다 (아키텍처의 '정해진 시간마다')."""
    if config.CRAWL_INTERVAL_SEC <= 0:
        log("SCHEDULER_DISABLED", note="CRAWL_INTERVAL_SEC=0 → 버튼/API로만 수집")
        return
    while True:
        await asyncio.sleep(config.CRAWL_INTERVAL_SEC)
        key = f"crawl:schedule:{int(asyncio.get_running_loop().time())}"
        await r.xadd(config.CRAWLER_STREAM, {"job": json.dumps({"job_id": job_id_for(key), "type": "crawl",
                                                                "payload": {}, "idempotency_key": key})})
        log("SCHEDULED_CRAWL")


async def main():
    log("PROCESS_START", pid=os.getpid())
    r = make_redis()
    await storage.ensure_bucket()
    consumer = StreamConsumer(r, config.CRAWLER_STREAM, config.CRAWLER_GROUP, lambda m: crawl(m, r))

    def stop():
        consumer.stopping = True
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop)
    asyncio.create_task(scheduler(r))
    await consumer.run()
    log("PROCESS_EXIT", pid=os.getpid())


if __name__ == "__main__":
    asyncio.run(main())
