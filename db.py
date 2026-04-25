import json
import streamlit as st
import snowflake.connector
from contextlib import contextmanager


@st.cache_resource
def get_connection():
    cfg = st.secrets["snowflake"]
    conn = snowflake.connector.connect(
        account   = cfg["account"].lower(),
        user      = cfg["user"],
        password  = cfg["password"],
        warehouse = cfg["warehouse"],
        database  = cfg["database"],
        schema    = cfg["schema"],
        role      = cfg["role"],
        # No network_timeout / login_timeout — defaults let Cortex LLM calls
        # complete without being killed mid-response (they can take 30-90s)
        session_parameters={"QUERY_TAG": "de_quiz_app"},
    )
    # Give every query in this session up to 5 minutes before Snowflake cancels it
    with conn.cursor() as cur:
        cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 300")
    return conn


def test_connection() -> tuple[bool, str]:
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True, ""
    except Exception as e:
        return False, str(e)


@contextmanager
def cursor():
    conn = get_connection()
    cur  = conn.cursor()
    try:
        yield cur
        conn.commit()
    except snowflake.connector.errors.OperationalError:
        # Connection dropped (e.g. VPN switch) — clear cache so next call reconnects
        get_connection.clear()
        raise
    finally:
        cur.close()


# ── Users ────────────────────────────────────────────────────

def upsert_user(email: str, display_name: str, picture_url: str) -> int:
    with cursor() as cur:
        cur.execute(
            """
            MERGE INTO USERS t
            USING (SELECT %s AS email, %s AS display_name, %s AS picture_url) s
            ON t.EMAIL = s.email
            WHEN NOT MATCHED THEN
                INSERT (EMAIL, DISPLAY_NAME, PICTURE_URL)
                VALUES (s.email, s.display_name, s.picture_url)
            """,
            (email, display_name, picture_url),
        )
        cur.execute("SELECT USER_ID FROM USERS WHERE EMAIL = %s", (email,))
        return cur.fetchone()[0]


# ── Question cache ───────────────────────────────────────────

def fetch_cached_questions(topic: str, difficulty: str, limit: int,
                           exclude_ids: set | None = None) -> list[dict]:
    if exclude_ids:
        placeholders = ",".join(["%s"] * len(exclude_ids))
        sql    = f"""
            SELECT QUESTION_ID, QUESTION_TEXT, OPTION_A, OPTION_B, OPTION_C, OPTION_D,
                   CORRECT_OPTION, EXPLANATION
            FROM QUESTION_CACHE
            WHERE TOPIC = %s AND DIFFICULTY = %s
              AND QUESTION_ID NOT IN ({placeholders})
            ORDER BY RANDOM()
            LIMIT %s
        """
        params = (topic, difficulty, *exclude_ids, limit)
    else:
        sql    = """
            SELECT QUESTION_ID, QUESTION_TEXT, OPTION_A, OPTION_B, OPTION_C, OPTION_D,
                   CORRECT_OPTION, EXPLANATION
            FROM QUESTION_CACHE
            WHERE TOPIC = %s AND DIFFICULTY = %s
            ORDER BY RANDOM()
            LIMIT %s
        """
        params = (topic, difficulty, limit)

    with cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def count_cached_questions(topic: str, difficulty: str) -> int:
    with cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM QUESTION_CACHE WHERE TOPIC = %s AND DIFFICULTY = %s",
            (topic, difficulty),
        )
        return cur.fetchone()[0]


def insert_questions(questions: list[dict]):
    if not questions:
        return
    with cursor() as cur:
        cur.executemany(
            """
            INSERT INTO QUESTION_CACHE
                (TOPIC, DIFFICULTY, QUESTION_TEXT, OPTION_A, OPTION_B, OPTION_C, OPTION_D,
                 CORRECT_OPTION, EXPLANATION)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    q["topic"], q["difficulty"], q["question_text"],
                    q["option_a"], q["option_b"], q["option_c"], q["option_d"],
                    q["correct_option"], q["explanation"],
                )
                for q in questions
            ],
        )


# ── Sessions / leaderboard ───────────────────────────────────

def save_session(user_id, email: str, display_name: str,
                 topics: list, difficulty: str, num_questions: int,
                 score: int, show_on_leaderboard: bool = True):
    score_pct = round(score / num_questions * 100, 1)
    with cursor() as cur:
        cur.execute(
            """
            INSERT INTO QUIZ_SESSIONS
                (USER_ID, EMAIL, DISPLAY_NAME, TOPICS, DIFFICULTY,
                 NUM_QUESTIONS, SCORE, SCORE_PCT, SHOW_ON_LEADERBOARD)
            SELECT %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s, %s
            """,
            (user_id, email, display_name,
             json.dumps(topics), difficulty, num_questions, score, score_pct,
             show_on_leaderboard),
        )


def fetch_leaderboard(limit: int = 50) -> list[dict]:
    with cursor() as cur:
        cur.execute(
            """
            SELECT DISPLAY_NAME, DIFFICULTY, NUM_QUESTIONS,
                   SCORE, SCORE_PCT, TOPICS, COMPLETED_AT
            FROM QUIZ_SESSIONS
            WHERE SHOW_ON_LEADERBOARD = TRUE
            ORDER BY SCORE_PCT DESC, COMPLETED_AT DESC
            LIMIT %s
            """,
            (limit,),
        )
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_user_history(email: str) -> list[dict]:
    with cursor() as cur:
        cur.execute(
            """
            SELECT DIFFICULTY, NUM_QUESTIONS, SCORE, SCORE_PCT, TOPICS, COMPLETED_AT
            FROM QUIZ_SESSIONS
            WHERE EMAIL = %s
            ORDER BY COMPLETED_AT DESC
            LIMIT 20
            """,
            (email,),
        )
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
