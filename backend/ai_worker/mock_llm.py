"""MOCK LLM — 실제 LLM API(외부 HTTPS 호출)를 대체하는 규칙 기반 함수.

실제 서비스라면 여기서 LLM API를 호출한다. 실습 목적상 결과가 매번 같아야 관찰이 쉬우므로 정규식으로 흉내 낸다.
의도적으로 '환각(hallucination)'도 흉내 낸다: 원문에 기준 숫자가 없는데 "평점 4.0 이상"을 지어낸다.
-> 그래서 validate_evidence 노드(근거 검증)가 필요하다는 것을 보여준다.
"""
import re

RULES = [
    # (field, op, 정규식, 값 변환)
    ("gpa", ">=", r"평점\s*([0-9.]+)\s*이상", float),
    ("income_decile", "<=", r"소득\s*([0-9]+)\s*분위\s*이하", int),
    ("volunteer_hours", ">=", r"봉사활동\s*([0-9]+)\s*시간\s*이상", int),
    ("region", "==", r"지역\s*:\s*([가-힣]+)", str),
]


def extract_conditions(sections: list[dict]) -> dict:
    """원문 섹션 -> 자격 조건 목록. 각 조건은 '근거 문장(evidence)'을 같이 반환한다."""
    title = next((s["text"] for s in sections if s["heading"] == "title"), "제목 없음")
    elig = "\n".join(s["text"] for s in sections if "자격" in s["heading"])
    conditions = []
    for line in [l.strip("- ").strip() for l in elig.splitlines() if l.strip()]:
        for field, op, pat, conv in RULES:
            m = re.search(pat, line)
            if m:
                conditions.append({"field": field, "op": op, "value": conv(m.group(1)), "evidence": line})
        if "성적 우수" in line and not any(c["field"] == "gpa" for c in conditions):
            # MOCK 환각: 원문에 없는 기준을 그럴듯하게 지어낸다
            conditions.append({"field": "gpa", "op": ">=", "value": 4.0, "evidence": "직전 학기 평점 4.0 이상"})
    return {"title": title, "conditions": conditions, "model": "mock-llm-v1"}


def draft_essay(student: dict, scholarship: dict, answers: dict, checklist: list[str]) -> str:
    return (
        f"# {scholarship['title']} 지원서 초안 (MOCK LLM 생성)\n\n"
        f"지원자: {student['name']} / 전공: {student['major']} / 평점: {student['gpa']}\n\n"
        f"## 지원 동기\n{answers.get('motivation', '(미입력)')}\n\n"
        f"## 봉사 활동\n{answers.get('volunteer_hours', student.get('volunteer_hours') or 'UNKNOWN · 기관 확인 필요')} 시간\n\n"
        f"## 제출 서류 체크리스트\n" + "\n".join(f"- [ ] {d}" for d in checklist) + "\n\n"
        "※ 이 초안은 안내용입니다. 신청은 학생이 공식 사이트에서 직접 합니다.\n"
    )
