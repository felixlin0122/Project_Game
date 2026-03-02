import json
import re
from typing import Dict, Any, List, Tuple

from Project_storge import get_db_connection
from setting import GROQ_API_KEY

from concurrent.futures import ThreadPoolExecutor, as_completed
from groq import Groq
import itertools
import time
import random
import threading
from collections import defaultdict, deque

import sys

class ProgressBar:
    def __init__(self, total: int):
        self.total = total
        self.done = 0
        self.errors = 0
        self._lock = threading.Lock()
        self.start_time = time.time()

    def update(self, success: bool = True):
        with self._lock:
            self.done += 1
            if not success:
                self.errors += 1

            percent = (self.done / self.total) * 100
            elapsed = time.time() - self.start_time
            speed = self.done / elapsed if elapsed > 0 else 0

            bar_len = 30
            filled = int(bar_len * self.done / self.total)
            bar = "█" * filled + "-" * (bar_len - filled)

            sys.stdout.write(
                f"\r[{bar}] {self.done}/{self.total} "
                f"({percent:.1f}%) "
                f"| ERR: {self.errors} "
                f"| {speed:.2f} it/s"
            )
            sys.stdout.flush()

            if self.done == self.total:
                print()
# ---------------------------
# Text clip
# ---------------------------
def clip_text(text: str, max_chars: int = 800) -> str:
    t = (text or "").strip()
    if len(t) <= max_chars:
        return t
    head = t[:450]
    tail = t[-250:]
    return head + "\n...\n" + tail


# ---------------------------
# Prompt (shorter to save TPM)
# ---------------------------
SYSTEM_PROMPT = """
你是情緒分類器，只能輸出 JSON，不得輸出其他文字。
請根據文本判斷 8 類並給分與證據。

8 類：機率、金流、官方、社交、連線、更新、角色、媒體

規則：
- score: -1..+1
- confidence: 0..1
- 若無相關內容：score=0, confidence<=0.4, evidence=[]
- evidence 必須是原文短句或忠實改寫（短）
- 必須完整輸出所有欄位
""".strip()

USER_TEMPLATE = """文本：
{text}

輸出 JSON（不要 markdown / 不要多餘文字）。
欄位：
language="zh-TW"
overall_sentiment(-1..1)
overall_confidence(0..1)
categories：8 keys（機率/金流/官方/社交/連線/更新/角色/媒體）
每個類別：score(-1..1), confidence(0..1), evidence(list[str])
若無相關：score=0, confidence<=0.4, evidence=[]
""".strip()


# ---------------------------
# Config
# ---------------------------
GROQ_MODEL = "llama-3.1-8b-instant"

BATCH = 30                 # TPM=6000 下，建議先 20~30；你要 50 也行但排隊更久
RETRY = 4                  # 429 重試用
PER_KEY_WORKERS = 1        # 先穩住，避免過多並行導致 429 連發
INPUT_MAX_CHARS = 800      # 配合 clip_text

# 這是「粗略」字元節流（不是 TPM），可以放寬避免自我掐死
CHARS_PER_MINUTE_PER_KEY = 12000

PROMPT_VERSION = "v2_jsonmode_shortprompt"

SOURCE_TABLE = "project_datas"
SOURCE_PK = "bsn_sna_page"
SOURCE_TEXT_COL = "content"

TARGET_TABLE = "project_groq_nlp"


# ---------------------------
# DB queries
# ---------------------------
def pick_candidates(conn) -> List[Tuple[str, str]]:
    """
    - content not null / not empty
    - 且 project_groq_nlp 沒有 DONE（避免重做）
    - JOIN 用 collation workaround 避免 1267
    """
    sql = f"""
    SELECT s.{SOURCE_PK} AS k, s.{SOURCE_TEXT_COL} AS c
    FROM {SOURCE_TABLE} s
    LEFT JOIN {TARGET_TABLE} t
      ON t.bsn_sna_page COLLATE utf8mb4_0900_ai_ci
       = s.{SOURCE_PK}  COLLATE utf8mb4_0900_ai_ci
    WHERE s.{SOURCE_TEXT_COL} IS NOT NULL
      AND TRIM(s.{SOURCE_TEXT_COL}) <> ''
      AND (t.bsn_sna_page IS NULL OR t.status <> 'DONE')
    ORDER BY s.{SOURCE_PK} ASC
    LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (BATCH,))
        return [(r[0], r[1]) for r in cur.fetchall()]


def upsert_pending_many(conn, rows: List[Tuple[str, str]]):
    sql = f"""
    INSERT INTO {TARGET_TABLE} (bsn_sna_page, input_text, model, prompt_version, status)
    VALUES (%s, %s, %s, %s, 'PENDING')
    ON DUPLICATE KEY UPDATE
      input_text = IF(status='DONE', input_text, VALUES(input_text)),
      model = IF(status='DONE', model, VALUES(model)),
      prompt_version = IF(status='DONE', prompt_version, VALUES(prompt_version)),
      status = IF(status='DONE', 'DONE', 'PENDING'),
      error_msg = NULL;
    """
    data = [(k, txt, GROQ_MODEL, PROMPT_VERSION) for k, txt in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, data)


def mark_done_many(conn, rows: List[Tuple[Dict[str, Any], str, str]]):
    sql = f"""
    UPDATE {TARGET_TABLE}
    SET status='DONE',
        output_json=CAST(%s AS JSON),
        output_text=%s,
        error_msg=NULL
    WHERE bsn_sna_page=%s;
    """
    data = [(json.dumps(o, ensure_ascii=False), raw, k) for (o, raw, k) in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, data)


def mark_error_many(conn, rows: List[Tuple[str, str]]):
    sql = f"""
    UPDATE {TARGET_TABLE}
    SET status='ERROR',
        error_msg=%s
    WHERE bsn_sna_page=%s;
    """
    data = [(err[:2000], k) for (err, k) in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, data)


# ---------------------------
# Rate limiting helpers
# ---------------------------
class CharRateLimiter:
    """
    粗略以「字元」節流（不是 token），目的是減少 429 burst。
    真正的 TPM 控制仍交給 429 的 retry-after。
    """
    def __init__(self, per_minute: int, window_seconds: float = 60.0):
        self.per_minute = per_minute
        self.window_seconds = window_seconds
        self._lock = threading.Lock()
        self._buckets: dict[str, deque[tuple[float, int]]] = defaultdict(deque)

    def acquire(self, api_key: str, cost: int):
        while True:
            with self._lock:
                now = time.monotonic()
                q = self._buckets[api_key]

                while q and (now - q[0][0]) >= self.window_seconds:
                    q.popleft()

                used = sum(c for _, c in q)
                if used + cost <= self.per_minute:
                    q.append((now, cost))
                    return

                wait_seconds = self.window_seconds - (now - q[0][0]) if q else 0.2

            time.sleep(max(0.05, wait_seconds) + random.uniform(0, 0.15))


def estimate_cost_chars(input_text: str) -> int:
    # 粗估：system+user模板+文本 + 預留
    return len(SYSTEM_PROMPT) + len(USER_TEMPLATE) + len(input_text) + 400


def parse_retry_after_seconds(err_msg: str) -> float | None:
    m = re.search(r"try again in\s+([0-9]*\.?[0-9]+)s", err_msg, flags=re.IGNORECASE)
    return float(m.group(1)) if m else None


def sleep_retry_after(e: Exception, default: float = 5.0):
    msg = str(e)
    sec = parse_retry_after_seconds(msg) or default
    time.sleep(sec + random.uniform(0, 0.4))


def is_rate_limited(e: Exception) -> bool:
    s = str(e)
    return ("429" in s) or ("rate_limit" in s.lower()) or ("rate_limit_exceeded" in s.lower())


def is_non_retryable(e: Exception) -> bool:
    s = str(e)
    # 401 key/授權錯、model decommissioned、以及一般 400 都不該重試
    if ("401" in s) or ("invalid_api_key" in s) or ("model_decommissioned" in s):
        return True
    # 400 但不是 429 類型，一般也不重試
    if ("400" in s) and (not is_rate_limited(e)):
        return True
    return False


# ---------------------------
# Groq call (JSON mode)
# ---------------------------
def make_client(api_key: str) -> Groq:
    return Groq(api_key=api_key)


def infer_one(
    client: Groq,
    api_key: str,
    limiter: CharRateLimiter,
    input_text: str,
    max_retry: int = RETRY,
) -> Tuple[Dict[str, Any], str]:
    user_msg = USER_TEMPLATE.format(text=input_text)
    estimated_cost = estimate_cost_chars(input_text)

    last_err: Exception | None = None

    for attempt in range(1, max_retry + 1):
        try:
            limiter.acquire(api_key, estimated_cost)

            resp = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0,
                max_tokens=220,
                response_format={"type": "json_object"},  # ✅ 強制合法 JSON
            )

            raw = resp.choices[0].message.content or ""
            parsed = json.loads(raw)  # JSON mode 下直接 loads
            return parsed, raw

        except Exception as e:
            last_err = e

            if is_non_retryable(e):
                raise

            if is_rate_limited(e):
                sleep_retry_after(e, default=5.0)
                continue

            # 其他不明錯誤：做輕量 backoff 再試一次
            time.sleep(min(8.0, 1.2 * (2 ** (attempt - 1))) + random.uniform(0, 0.3))
            continue

    raise RuntimeError(f"Failed after retries: {type(last_err).__name__}: {last_err}")


# ---------------------------
# Main
# ---------------------------
def normalize_keys(v) -> List[str]:
    """
    支援：
    - 單一字串 key
    - list/tuple keys
    - 逗號分隔字串: "k1,k2"
    """
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return [str(k).strip() for k in v if k and str(k).strip()]

    s = str(v).strip()
    if not s:
        return []
    # 若是逗號分隔
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def main():
    keys = normalize_keys(GROQ_API_KEY)
    if not keys:
        raise RuntimeError("Missing GROQ_API_KEY / GROQ_API_KEYS")

    # 防呆：避免 ["k1,k2"] 被當成一把
    for k in keys:
        if not k.startswith("gsk_"):
            raise RuntimeError(f"Bad key format detected: {repr(k)}")

    conn = get_db_connection()

    candidates = pick_candidates(conn)
    if not candidates:
        print("No candidates.")
        return
    print(f"Picked {len(candidates)} candidates.")

    # 1) clip + PENDING 批次寫入
    pending_rows: List[Tuple[str, str]] = []
    row_keys: List[str] = []
    texts: List[str] = []

    for k, content in candidates:
        input_text = clip_text(content, INPUT_MAX_CHARS)
        pending_rows.append((k, input_text))
        row_keys.append(k)
        texts.append(input_text)

    upsert_pending_many(conn, pending_rows)
    conn.commit()

    # 2) 並行呼叫：每 key 配 PER_KEY_WORKERS 個 client slot
    per_key_workers = PER_KEY_WORKERS
    client_slots = [(api_key, make_client(api_key)) for api_key in keys for _ in range(per_key_workers)]
    rr = itertools.cycle(client_slots)
    limiter = CharRateLimiter(CHARS_PER_MINUTE_PER_KEY)

    done_rows: List[Tuple[Dict[str, Any], str, str]] = []
    err_rows: List[Tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=len(client_slots)) as ex:
        futs = {}
        for i, input_text in enumerate(texts):
            api_key, client = next(rr)
            fut = ex.submit(infer_one, client, api_key, limiter, input_text)
            futs[fut] = row_keys[i]
            progress = ProgressBar(len(texts))

        for fut in as_completed(futs):
            k = futs[fut]
            try:
                parsed, raw = fut.result()
                done_rows.append((parsed, raw, k))
                progress.update(success=True)
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                err_rows.append((err, k))
                progress.update(success=False)

    # 3) 批次寫回 DB
    if done_rows:
        mark_done_many(conn, done_rows)
    if err_rows:
        mark_error_many(conn, err_rows)
    conn.commit()


if __name__ == "__main__":
    main()