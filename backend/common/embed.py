"""MOCK embedding (실제 LLM embedding API 대체).

단어를 해시해서 8칸 벡터에 더하고 길이를 1로 맞춘다. 의미를 이해하진 못하지만
"같은 단어를 많이 공유하는 문장이 가깝다"는 성질은 있어서 pgvector 검색 흐름을 실습하기엔 충분하다.
"""
import hashlib
import math
import re

DIM = 8


def embed(text: str) -> list[float]:
    v = [0.0] * DIM
    for tok in re.findall(r"[0-9A-Za-z가-힣]+", text.lower()):
        h = int(hashlib.md5(tok.encode()).hexdigest(), 16)
        v[h % DIM] += 1.0 if (h >> 8) % 2 else -1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [round(x / n, 6) for x in v]


def to_pg(vec: list[float]) -> str:
    return "[" + ",".join(str(x) for x in vec) + "]"
