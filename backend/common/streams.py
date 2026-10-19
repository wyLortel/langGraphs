"""Redis Streams Consumer 루프 — Crawler와 AI Worker가 같이 쓰는 "큐에서 일 꺼내기" 엔진.

한 번의 루프에서 하는 일:
  0) 빈 자리(slot)가 날 때까지 기다린다 (동시 실행 수 = concurrency)
  1) XPENDING + XAUTOCLAIM : 오랫동안(min-idle) 아무도 손대지 않은 pending 메시지를 내가 가져온다 (죽은 Worker 복구)
  2) 전달 횟수가 MAX_DELIVERIES를 넘었으면 DLQ로 보내고 ACK
  3) XREADGROUP '>'      : 아직 아무에게도 전달되지 않은 새 메시지를 1개 가져온다
  4) 별도 task로 handler 실행 -> (DB commit은 handler 안에서) -> XACK. 루프는 기다리지 않고 바로 다음 자리를 채운다

동시 실행 (concurrency):
  WHY: LLM·DB·MCP 호출은 대부분 '응답 기다리는 시간'이다. 기다리는 동안 같은 프로세스가 다른 작업을 진행하면
  프로세스를 늘리지 않고도 처리량이 오른다. Semaphore로 상한을 둬서 DB 연결·LLM 호출 제한을 넘지 않게 한다.
  주의: 프로세스가 죽으면 그 안에서 돌던 작업 '전부'가 pending에 남고, 똑같이 XAUTOCLAIM으로 복구된다.

ACK 규칙 (가장 중요):
  - XACK는 "결과가 영구 저장소에 commit된 뒤"에만 한다. 반대로 하면 ACK 후 죽었을 때 작업이 영영 사라진다.
  - 예외가 나면 ACK하지 않는다. 메시지는 pending에 남고, min-idle이 지나면 XAUTOCLAIM으로 다시 실행된다.
    WHY: 재시도 경로를 "pending -> XAUTOCLAIM" 하나로 통일하면, 예외든 프로세스 사망이든 같은 방식으로 복구된다.
  - 소유권을 잃은 Worker(StaleOwner)는 ACK하면 안 된다. XACK는 소유자를 검사하지 않기 때문에,
    늦게 깨어난 Worker가 ACK하면 지금 그 메시지를 처리 중인 다른 Worker의 pending 기록을 지워버린다.
"""
import asyncio
import json
import time
from dataclasses import dataclass

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from . import chaos, config
from .obs import CONTAINER, STARTED_AT, emit, log


class StaleOwner(Exception):
    """내가 처리하던 작업의 소유권이 다른 Worker에게 넘어갔다 (펜싱 토큰 불일치)."""


@dataclass
class Msg:
    msg_id: str
    job: dict          # {"job_id", "type", "payload", "idempotency_key"}
    delivery: int      # Redis가 이 메시지를 몇 번째 전달했는가 (XPENDING의 times_delivered)
    how: str           # XREADGROUP(새 메시지) / XAUTOCLAIM(회수한 메시지)

    @property
    def chaos(self):
        return (self.job.get("payload") or {}).get("chaos")


class StreamConsumer:
    def __init__(self, redis, stream, group, handler, on_dead=None, status_extra=None, concurrency=1):
        self.r = redis
        self.stream, self.group = stream, group
        self.consumer = config.WORKER_ID     # Consumer 이름 = 논리적 Worker 이름 (재시작해도 같음)
        self.handler = handler               # async (Msg) -> None. 정상 반환 = ACK 해도 됨
        self.on_dead = on_dead               # async (Msg, reason) -> None. DLQ 이동 시 DB 표시 등
        self.status_extra = status_extra or (lambda: {})
        self.concurrency = concurrency
        self.slots = asyncio.Semaphore(concurrency)
        self.inflight: dict[str, Msg] = {}   # msg_id -> 지금 처리 중인 메시지
        self.tasks: set[asyncio.Task] = set()
        self.stopping = False

    # ---------------------------------------------------------------- setup
    async def ensure_group(self):
        try:
            # id="0": 그룹을 처음 만들 때 스트림에 이미 있던 메시지도 처리 대상으로 삼는다
            await self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
            log("XGROUP_CREATE", stream=self.stream, group=self.group)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def heartbeat_loop(self):
        """WHY heartbeat: 화면에 '지금 살아 있는 서버'를 보여주고, 회수 시 '이전 주인이 살아 있었나'를 진단하기 위함.
        소유권 판정에는 쓰지 않는다 (프로세스가 살아 있어도 그 작업은 멈춰 있을 수 있으므로)."""
        while True:
            running = list(self.inflight.values())
            info = {
                "worker_id": self.consumer, "container": CONTAINER, "pid": __import__("os").getpid(),
                "started_at": STARTED_AT, "stream": self.stream,
                "current_job": running[0].job["job_id"] if running else None,
                "current_msg": running[0].msg_id if running else None,
                "in_flight": f"{len(running)}/{self.concurrency}",
                "in_flight_jobs": [m.job["job_id"] for m in running],
                **self.status_extra(),
            }
            try:
                await self.r.set(f"heartbeat:{self.consumer}", json.dumps(info, default=str), ex=10)
            except Exception as e:  # noqa: BLE001
                log("HEARTBEAT_FAILED", error=repr(e))
            await asyncio.sleep(3)

    # ---------------------------------------------------------------- main loop
    async def run(self):
        await self.ensure_group()
        asyncio.create_task(self.heartbeat_loop())
        await emit(self.r, "CONSUMER_READY", stream=self.stream, group=self.group, consumer=self.consumer,
                   min_idle_ms=config.CLAIM_MIN_IDLE_MS, max_deliveries=config.MAX_DELIVERIES,
                   concurrency=self.concurrency)
        last_claim = 0.0
        while not self.stopping:
            # 빈 자리가 있을 때만 새 메시지를 가져온다. 자리가 다 찼으면 메시지는 Redis에 남아 다른 Worker가 가져간다
            await self.slots.acquire()
            started = False
            try:
                msg = None
                if time.monotonic() - last_claim > 3:
                    last_claim = time.monotonic()
                    handled, msg = await self.reclaim_one()
                    if handled and msg is None:
                        continue
                if msg is None:
                    # block=2000: 새 메시지가 없으면 최대 2초 기다렸다가 빈손으로 돌아온다 (그 사이 회수 검사도 해야 하므로)
                    resp = await self.r.xreadgroup(self.group, self.consumer, {self.stream: ">"}, count=1, block=2000)
                    for _stream, msgs in resp or []:
                        for msg_id, fields in msgs:
                            msg = Msg(msg_id, json.loads(fields["job"]), 1, "XREADGROUP")
                if msg is not None:
                    self.start(msg)
                    started = True
            except ResponseError as e:
                if "NOGROUP" in str(e):          # 실습 reset으로 스트림이 지워진 경우
                    await self.ensure_group()
                    continue
                log("REDIS_ERROR", error=repr(e))
                await asyncio.sleep(1)
            except (RedisConnectionError, OSError) as e:
                log("REDIS_DISCONNECTED", error=repr(e), note="1초 후 재연결 시도")
                await asyncio.sleep(1)
            finally:
                if not started:
                    self.slots.release()
        if self.tasks:
            log("CONSUMER_DRAINING", in_flight=len(self.tasks), note="처리 중인 작업이 끝날 때까지 기다린 뒤 종료")
            await asyncio.gather(*self.tasks, return_exceptions=True)
        log("CONSUMER_STOPPED")

    def start(self, msg: Msg):
        """자리 하나를 차지한 상태에서 호출. 메시지를 별도 task로 처리하고, 끝나면 자리를 반납한다."""
        self.inflight[msg.msg_id] = msg

        async def run():
            try:
                await self.process(msg)
            finally:
                self.inflight.pop(msg.msg_id, None)
                self.slots.release()

        task = asyncio.create_task(run())
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def reclaim_one(self) -> tuple[bool, Msg | None]:
        """반환: (무언가 처리했나, 실행할 메시지). DLQ로 보냈으면 (True, None)."""
        # 1) 관찰용: 누가 들고 있다가 멈춘 메시지인지 먼저 본다 (XAUTOCLAIM은 이전 주인을 알려주지 않음)
        idle = await self.r.xpending_range(self.stream, self.group, min="-", max="+", count=20,
                                           idle=config.CLAIM_MIN_IDLE_MS)
        if not idle:
            return False, None
        prev = {p["message_id"]: p for p in idle}
        # 2) 실제 회수: min-idle 이상 방치된 메시지의 소유자를 나로 바꾸고, 전달 횟수를 +1 한다 (원자적)
        res = await self.r.xautoclaim(self.stream, self.group, self.consumer,
                                      min_idle_time=config.CLAIM_MIN_IDLE_MS, start_id="0-0", count=1)
        claimed, deleted = res[1], (res[2] if len(res) > 2 else [])
        for d in deleted:                       # 스트림에서 이미 지워진(trim) 메시지는 ACK만
            await self.r.xack(self.stream, self.group, d)
        if not claimed:
            return False, None
        msg_id, fields = claimed[0]
        if msg_id in self.inflight:
            # lease 갱신을 끈 작업(lease=false)은 내가 처리 중인데도 idle이 쌓여 내가 다시 회수할 수 있다.
            # 같은 프로세스에서 같은 메시지를 두 번 돌리지 않는다 (지금 돌고 있는 쪽이 끝나면 ACK한다)
            log("RECLAIM_SKIPPED_SELF", msg_id=msg_id, note="이미 이 Worker 안에서 처리 중인 메시지")
            return True, None
        p = await self.r.xpending_range(self.stream, self.group, min=msg_id, max=msg_id, count=1)
        delivery = p[0]["times_delivered"] if p else 0
        old = prev.get(msg_id, {})
        old_owner = old.get("consumer")
        owner_alive = bool(old_owner and await self.r.exists(f"heartbeat:{old_owner}"))
        job = json.loads(fields["job"])
        await emit(self.r, "XAUTOCLAIM_RECLAIMED", route=f"Redis → {self.consumer}",
                   stream=self.stream, msg_id=msg_id, job_id=job.get("job_id"), from_owner=old_owner,
                   idle_ms=old.get("time_since_delivered"), delivery=delivery,
                   prev_owner_heartbeat_alive=owner_alive,
                   warning=("이전 주인의 heartbeat가 살아 있음 → 죽은 게 아니라 '느린' 것일 수 있다 (중복 실행 위험)"
                            if owner_alive and old_owner != self.consumer else None))
        msg = Msg(msg_id, job, delivery, "XAUTOCLAIM")
        if delivery > config.MAX_DELIVERIES:
            await self.dead_letter(msg, f"delivered {delivery} times > MAX_DELIVERIES={config.MAX_DELIVERIES}")
            return True, None
        return True, msg

    async def dead_letter(self, msg: Msg, reason: str):
        dlq = f"{self.stream}:dlq"
        # 순서: DLQ에 복사 -> DB에 dead 표시 -> 원본 ACK. 중간에 죽으면 DLQ에 2번 들어갈 수는 있어도 사라지지는 않는다.
        await self.r.xadd(dlq, {"job": json.dumps(msg.job, ensure_ascii=False), "orig_msg_id": msg.msg_id,
                                "deliveries": msg.delivery, "reason": reason, "dead_by": self.consumer})
        if self.on_dead:
            await self.on_dead(msg, reason)
        await self.r.xack(self.stream, self.group, msg.msg_id)
        await emit(self.r, "DEAD_LETTER", route=f"{self.consumer} → Redis {dlq}", msg_id=msg.msg_id,
                   job_id=msg.job.get("job_id"), reason=reason)

    # ---------------------------------------------------------------- one message
    async def process(self, msg: Msg):
        await emit(self.r, "JOB_RECEIVED", route=f"Redis → {self.consumer}", how=msg.how, stream=self.stream,
                   msg_id=msg.msg_id, job_id=msg.job.get("job_id"), type=msg.job.get("type"),
                   delivery=msg.delivery, payload=msg.job.get("payload"),
                   in_flight=f"{len(self.inflight)}/{self.concurrency}")
        lease = (msg.job.get("payload") or {}).get("lease", True)
        renew = asyncio.create_task(self.renew_lease(msg)) if lease else None
        try:
            await chaos.crash_if(self.r, msg.chaos, "crash_after_read", msg.job.get("job_id"))
            await self.handler(msg)
            # 여기까지 왔다 = handler 안에서 DB commit이 끝났다. 이제서야 ACK.
            await chaos.crash_if(self.r, msg.chaos, "crash_before_ack", msg.job.get("job_id"))
            await self.r.xack(self.stream, self.group, msg.msg_id)
            await emit(self.r, "XACK", route=f"{self.consumer} → Redis", stream=self.stream, msg_id=msg.msg_id,
                       job_id=msg.job.get("job_id"), note="처리 결과를 영구 저장한 뒤에 ACK → pending에서 제거")
        except StaleOwner as e:
            await emit(self.r, "STALE_OWNER_NO_ACK", job_id=msg.job.get("job_id"), msg_id=msg.msg_id, detail=str(e),
                       note="소유권을 잃었으므로 결과를 버리고 ACK도 하지 않는다 (지금 주인이 처리 중)")
        except Exception as e:  # noqa: BLE001
            await emit(self.r, "JOB_FAILED_NO_ACK", job_id=msg.job.get("job_id"), msg_id=msg.msg_id,
                       error=repr(e), delivery=msg.delivery, max=config.MAX_DELIVERIES,
                       note=f"pending에 남김 → {config.CLAIM_MIN_IDLE_MS}ms 후 XAUTOCLAIM으로 재시도")
        finally:
            if renew:
                renew.cancel()

    async def renew_lease(self, msg: Msg):
        """Lease 갱신: 처리 중인 동안 주기적으로 '내 메시지'의 idle 시간을 0으로 되돌린다.

        WHY: XAUTOCLAIM은 idle 시간만 본다. 정상적으로 40초 걸리는 작업도 15초가 지나면 '죽은 것'처럼 보인다.
        XCLAIM ... JUSTID (min-idle 0, 소유자는 나 그대로)로 idle만 리셋하면 살아 있는 동안은 아무도 못 가져간다.
        프로세스가 죽거나 멈추면(docker pause) 갱신이 끊기고, 그때부터 idle이 쌓여 회수 대상이 된다.
        """
        try:
            while True:
                await asyncio.sleep(config.LEASE_RENEW_MS / 1000)
                p = await self.r.xpending_range(self.stream, self.group, min=msg.msg_id, max=msg.msg_id, count=1)
                if not p:
                    return
                if p[0]["consumer"] != self.consumer:
                    await emit(self.r, "LEASE_LOST", job_id=msg.job.get("job_id"), msg_id=msg.msg_id,
                               new_owner=p[0]["consumer"], note="내가 멈춘 사이 다른 Worker가 회수함")
                    return
                await self.r.xclaim(self.stream, self.group, self.consumer, min_idle_time=0,
                                    message_ids=[msg.msg_id], justid=True)
                log("LEASE_RENEWED", msg_id=msg.msg_id, idle_before_ms=p[0]["time_since_delivered"])
        except asyncio.CancelledError:
            pass
