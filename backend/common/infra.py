"""PostgreSQL / Redis 연결 생성."""
import redis.asyncio as aioredis
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from . import config


async def make_db_pool(min_size=1, max_size=5) -> AsyncConnectionPool:
    # WHY 풀: 요청마다 TCP 연결 + 인증을 새로 하면 느리다. 프로세스가 시작될 때 연결 몇 개를 만들어 재사용한다.
    pool = AsyncConnectionPool(
        config.DATABASE_URL, min_size=min_size, max_size=max_size,
        kwargs={"row_factory": dict_row}, open=False,
    )
    await pool.open(wait=True, timeout=60)
    return pool


def make_redis() -> aioredis.Redis:
    return aioredis.from_url(config.REDIS_URL, decode_responses=True)
