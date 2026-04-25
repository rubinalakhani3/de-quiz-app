import json
import math
import re
import streamlit as st
from db import fetch_cached_questions, count_cached_questions, insert_questions

# llama3.1-8b: fastest Cortex model, reliable JSON output, sufficient for MCQs
CORTEX_MODEL = "llama3.1-8b"
CACHE_MIN    = 10   # top up cache when a topic+difficulty combo drops below this


def _run_cortex(prompt: str) -> str:
    from db import cursor
    with cursor() as cur:
        cur.execute(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
            (CORTEX_MODEL, prompt),
        )
        return cur.fetchone()[0]


def _build_prompt(topic: str, difficulty: str, count: int) -> str:
    diff_guidance = {
        "Easy":   "basic definitions, simple concepts, beginner-friendly",
        "Medium": "intermediate concepts, common patterns, practical scenarios",
        "Hard":   "advanced internals, edge cases, performance tuning, architecture decisions",
    }
    return f"""You are a data engineering instructor. Generate exactly {count} multiple-choice quiz questions about "{topic}" at {difficulty} level ({diff_guidance[difficulty]}).

Rules:
- Each question must have EXACTLY ONE correct answer. The other three options must be clearly wrong or clearly inferior — never partially correct.
- If a concept has multiple valid aspects (e.g. MERGE can insert AND update), write the question to ask about ONE specific aspect so only one option is correct.
- Do not write questions where two or more options could both be considered correct.

Return ONLY a valid JSON array with no markdown, no extra text.
Required keys per element: "question_text", "option_a", "option_b", "option_c", "option_d", "correct_option" (one of A/B/C/D), "explanation" (1-2 sentences explaining why the correct answer is right and why the others are wrong).

JSON array:"""


def _parse_questions(raw: str, topic: str, difficulty: str) -> list[dict]:
    raw   = re.sub(r"```json|```", "", raw).strip()
    match = re.search(r"\[.*\]", raw, re.DOTALL)
    if match:
        raw = match.group(0)
    data     = json.loads(raw)
    required = {"question_text", "option_a", "option_b", "option_c", "option_d",
                "correct_option", "explanation"}
    valid = []
    for q in data:
        if not required.issubset(q.keys()):
            continue
        opts = [q["option_a"], q["option_b"], q["option_c"], q["option_d"]]
        if len(set(o.strip().lower() for o in opts)) < 4:
            continue  # discard questions with duplicate answer options
        q["topic"]          = topic
        q["difficulty"]     = difficulty
        q["correct_option"] = q["correct_option"].strip().upper()[0]
        valid.append(q)
    return valid


def _generate(topic: str, difficulty: str, count: int) -> list[dict]:
    prompt = _build_prompt(topic, difficulty, count)
    raw    = _run_cortex(prompt)
    qs     = _parse_questions(raw, topic, difficulty)
    if qs:
        insert_questions(qs)
    return qs


def get_questions(topics: list[str], difficulty: str, total: int) -> list[dict]:
    per_topic = max(1, math.ceil(total / len(topics)))
    questions = []
    seen_ids  = set()

    for topic in topics:
        available = count_cached_questions(topic, difficulty)
        needed    = per_topic - available

        if needed > 0:
            try:
                _generate(topic, difficulty, needed + 2)
            except Exception as e:
                print(f"[question_gen] Cortex error for {topic}/{difficulty}: {e}")
        elif available < CACHE_MIN:
            try:
                _generate(topic, difficulty, CACHE_MIN - available)
            except Exception:
                pass

        # Pass seen_ids so Snowflake excludes already-fetched questions
        fetched = fetch_cached_questions(topic, difficulty, per_topic,
                                         exclude_ids=seen_ids if seen_ids else None)
        for q in fetched:
            questions.append(q)
            seen_ids.add(q["question_id"])

    # Top up if still short, excluding everything already fetched
    if len(questions) < total:
        shortfall = total - len(questions)
        for topic in topics:
            extras = fetch_cached_questions(topic, difficulty, shortfall,
                                            exclude_ids=seen_ids)
            for q in extras:
                questions.append(q)
                seen_ids.add(q["question_id"])
                if len(questions) >= total:
                    break
            if len(questions) >= total:
                break

    import random
    random.shuffle(questions)
    return questions[:total]
