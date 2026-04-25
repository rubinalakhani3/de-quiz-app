import json
import math
import re
import streamlit as st
from db import fetch_cached_questions, count_cached_questions, insert_questions

# llama3.1-8b for Easy/Medium (fast), mistral-large2 for Hard (better quality)
MODEL_DEFAULT = "llama3.1-8b"
MODEL_HARD    = "mistral-large2"
CACHE_MIN     = 10


def _model_for(difficulty: str) -> str:
    return MODEL_HARD if difficulty == "Hard" else MODEL_DEFAULT


def _run_cortex(prompt: str, difficulty: str) -> str:
    from db import cursor
    model = _model_for(difficulty)
    with cursor() as cur:
        cur.execute(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
            (model, prompt),
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


def _build_weakness_prompt(topic: str, difficulty: str, results: list[dict]) -> str:
    lines = []
    for r in results:
        status = "CORRECT" if r["correct"] else f"WRONG (answered {r['user_ans']}, correct was {r['correct_ans']})"
        lines.append(f"- {r['question']} → {status}")
    summary = "\n".join(lines)
    return f"""You are a data engineering coach reviewing a quiz result.

The user took a {difficulty} difficulty quiz on: {topic}.
Here are their answers:
{summary}

Write a short, specific 2-3 sentence coaching note. Mention what they got right, identify any weak areas by concept name, and suggest one concrete thing to study next. Be direct and encouraging. No bullet points, just plain paragraph text."""


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
            continue
        q["topic"]          = topic
        q["difficulty"]     = difficulty
        q["correct_option"] = q["correct_option"].strip().upper()[0]
        valid.append(q)
    return valid


def _generate(topic: str, difficulty: str, count: int) -> list[dict]:
    prompt = _build_prompt(topic, difficulty, count)
    raw    = _run_cortex(prompt, difficulty)
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

        fetched = fetch_cached_questions(topic, difficulty, per_topic,
                                         exclude_ids=seen_ids if seen_ids else None)
        for q in fetched:
            questions.append(q)
            seen_ids.add(q["question_id"])

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


def generate_weakness_report(questions: list[dict], answers: dict,
                              topics: list[str], difficulty: str) -> str:
    results = [
        {
            "question":   q["question_text"],
            "user_ans":   answers.get(i, "?"),
            "correct_ans": q["correct_option"],
            "correct":    answers.get(i) == q["correct_option"],
        }
        for i, q in enumerate(questions)
    ]
    topic_str = ", ".join(topics)
    prompt    = _build_weakness_prompt(topic_str, difficulty, results)
    try:
        from db import cursor
        # Always use mistral-large2 for the report — quality matters here
        with cursor() as cur:
            cur.execute(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
                (MODEL_HARD, prompt),
            )
            return cur.fetchone()[0].strip()
    except Exception as e:
        print(f"[question_gen] Weakness report error: {e}")
        return ""
