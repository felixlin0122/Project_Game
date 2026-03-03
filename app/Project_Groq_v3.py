import json
import re
import sys
import time
import random
import threading
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import itertools

from groq import Groq
from Project_storge import get_db_connection
from setting import GROQ_API_KEY

# ============================================================
# 0 Config
# ============================================================
GROQ_MODEL = "llama-3.1-8b-instant"

# 一次抓幾筆做（做完再抓下一批，直到清空）
BATCH = 30

# 每把 key 同時開幾個 worker（目前因TPM=6000所以抓1）
PER_KEY_WORKERS = 1

# 非 429 的重試次數
RETRY = 4

# 文本截斷長度：越小越省 TPM、越不容易 429，但資訊也少
INPUT_MAX_CHARS = 800

# JSON mode 的輸出上限（太小容易輸出不完整；太大浪費 TPM）
MAX_TOKENS = 300

# 粗略字元節流（不是 TPM）：用來減少 burst
CHARS_PER_MINUTE_PER_KEY = 12000

# 資料表設定
SOURCE_TABLE = "project_datas"
SOURCE_PK = "bsn_sna_page"
SOURCE_TEXT_COL = "content"
TARGET_TABLE = "project_groq_nlp"

# 進度條顯示用（目前 org TPM=6000）
TPM_LIMIT = 6000

PROMPT_VERSION = "v3_jsonmode_tpm_progress"

# ============================================================
# 1 Clip text
# ============================================================
def clip_text(text: str, max_chars: int = INPUT_MAX_CHARS) -> str:
    """
    以 head+tail 保留判斷情緒的上下文，降低 tokens/min。
    """
    t = (text or "").strip()
    if len(t) <= max_chars:
        return t
    head = t[:450]
    tail = t[-250:]
    return head + "\n...\n" + tail

def clip_text_more(text: str, max_chars: int = 500) -> str:
    """
    遇到 400/context/too long 等情況時，做一次更激進的縮短再重送。
    """
    t = (text or "").strip()
    if len(t) <= max_chars:
        return t
    head = t[:320]
    tail = t[-180:]
    return head + "\n...\n" + tail

# ============================================================
# 2 Prompt (short to save TPM)
# ============================================================
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

# ============================================================
# 3 Key normalization
# ============================================================
def normalize_keys(v) -> List[str]:
    """
    支援：
    - 單一字串 key
    - list/tuple keys
    - 逗號分隔字串: "key1,key2"
    """
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        keys = [str(k).strip() for k in v if k and str(k).strip()]
    else:
        s = str(v).strip()
        if not s:
            return []
        keys = [p.strip() for p in s.split(",") if p.strip()]

    for k in keys:
        if not k.startswith("gsk_"):
            raise RuntimeError(f"Bad key format detected: {repr(k)}")
    return keys

# ============================================================
# 4 DB helpers (只保留 output_json，一種就夠)
# ============================================================
def pick_candidates(conn) -> List[Tuple[str, str]]:
    """
    從來源表挑要做 NLP 的資料：
    - content not null / not empty
    - 且目標表沒有 DONE
    - JOIN 做 collation workaround 避免 1267
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


def mark_done_many(conn, rows: List[Tuple[Dict[str, Any], str]]):
    """
    只存 output_json；
    rows: (out_json, key)
    """
    sql = f"""
    UPDATE {TARGET_TABLE}
    SET status='DONE',
        output_json=CAST(%s AS JSON),
        error_msg=NULL
    WHERE bsn_sna_page=%s;
    """
    data = [(json.dumps(o, ensure_ascii=False), k) for (o, k) in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, data)


def mark_error_many(conn, rows: List[Tuple[str, str]]):
    """
    rows: (err, key)
    """
    sql = f"""
    UPDATE {TARGET_TABLE}
    SET status='ERROR',
        error_msg=%s
    WHERE bsn_sna_page=%s;
    """
    data = [(err[:2000], k) for (err, k) in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, data)

# ============================================================
# 5 Rate limit + retry helpers
# ============================================================
class CharRateLimiter:
    """
    粗略以「字元」節流（不是 token），用來減少 burst。
    真正的 TPM 控制交給 429 的 retry-after。
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


def parse_retry_after_seconds(err_msg: str) -> Optional[float]:
    m = re.search(r"try again in\s+([0-9]*\.?[0-9]+)s", err_msg, flags=re.IGNORECASE)
    return float(m.group(1)) if m else None


def sleep_retry_after(e: Exception, default: float = 5.0):
    msg = str(e)
    sec = parse_retry_after_seconds(msg) or default
    time.sleep(sec + random.uniform(0, 0.4))


def is_rate_limited(e: Exception) -> bool:
    s = str(e).lower()
    return ("429" in s) or ("rate_limit" in s) or ("rate_limit_exceeded" in s)


def is_non_retryable(e: Exception) -> bool:
    s = str(e).lower()
    if ("401" in s) or ("invalid_api_key" in s) or ("model_decommissioned" in s):
        return True
    # 非 429 的一般 400：通常不重試
    if ("error code: 400" in s) and ("rate_limit" not in s):
        return True
    return False


def looks_like_context_too_long(e: Exception) -> bool:
    s = str(e).lower()
    return ("context" in s and "length" in s) or ("maximum context" in s) or ("too long" in s)

# ============================================================
# 6 TPM-aware progress
# ============================================================
class TokenWindow:
    """
    rolling 60s token estimate window，用於顯示 estTPM
    """
    def __init__(self, window_seconds: float = 60.0):
        self.w = window_seconds
        self.q = deque()  # (t, tokens)
        self.lock = threading.Lock()

    def add(self, tokens: int):
        with self.lock:
            now = time.monotonic()
            self.q.append((now, tokens))
            while self.q and (now - self.q[0][0]) > self.w:
                self.q.popleft()

    def used(self) -> int:
        with self.lock:
            now = time.monotonic()
            while self.q and (now - self.q[0][0]) > self.w:
                self.q.popleft()
            return sum(t for _, t in self.q)


def estimate_tokens(system_prompt: str, user_msg: str, out_max: int) -> int:
    """
    粗估 token：中文大略 1 char ~ 1 token（僅用於進度條估算）
    user_msg 已包含 input_text，不要重複加。
    """
    return len(system_prompt) + len(user_msg) + out_max


class ProgressTPM:
    def __init__(self, total: int, tpm_limit: int, token_window: TokenWindow):
        self.total = total
        self.done = 0
        self.err = 0
        self.start = time.time()
        self.lock = threading.Lock()
        self.tpm_limit = tpm_limit
        self.tw = token_window

    def update(self, success: bool, est_tokens: int):
        self.tw.add(est_tokens)

        with self.lock:
            self.done += 1
            if not success:
                self.err += 1

            used = self.tw.used()
            elapsed = time.time() - self.start
            speed = self.done / elapsed if elapsed > 0 else 0

            remaining = self.total - self.done
            # 以目前速度估 ETA（實際會受 429 影響）
            eta_sec = remaining / max(0.1, speed)

            percent = (self.done / self.total) * 100
            bar_len = 26
            filled = int(bar_len * self.done / self.total)
            bar = "█" * filled + "-" * (bar_len - filled)

            sys.stdout.write(
                f"\r[{bar}] {self.done}/{self.total} ({percent:5.1f}%) "
                f"| ERR:{self.err} "
                f"| estTPM:{used:4d}/{self.tpm_limit} "
                f"| ETA:{eta_sec:5.0f}s"
            )
            sys.stdout.flush()
            if self.done == self.total:
                print()
# ============================================================
# 7 Groq call (JSON mode) + retry
# ============================================================
def make_client(api_key: str) -> Groq:
    return Groq(api_key=api_key)


def infer_one(
    client: Groq,
    api_key: str,
    limiter: CharRateLimiter,
    input_text: str,
    max_retry: int = RETRY,
) -> Dict[str, Any]:
    """
    成功回傳 parsed_json
    - 使用 JSON mode：避免 JSONDecodeError
    - 429：照 retry-after 等再重試
    - 400/context too long：縮短一次後再試（僅一次）
    """
    user_msg = USER_TEMPLATE.format(text=input_text)
    estimated_cost = estimate_cost_chars(input_text)

    shortened_once = False
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
                max_tokens=MAX_TOKENS,
                response_format={"type": "json_object"},  #  強制 JSON
            )
            raw = resp.choices[0].message.content or ""
            return json.loads(raw)

        except Exception as e:
            last_err = e

            # 429：照 retry-after 等
            if is_rate_limited(e):
                sleep_retry_after(e, default=5.0)
                continue

            # 400 / context too long：縮短一次再試
            if (not shortened_once) and looks_like_context_too_long(e):
                shortened_once = True
                input_text = clip_text_more(input_text, 500)
                user_msg = USER_TEMPLATE.format(text=input_text)
                estimated_cost = estimate_cost_chars(input_text)
                time.sleep(0.3)
                continue

            # 不可重試類型：直接丟出
            if is_non_retryable(e):
                raise

            # 其他：指數退避
            backoff = min(10.0, 1.2 * (2 ** (attempt - 1))) + random.uniform(0, 0.3)
            time.sleep(backoff)

    raise RuntimeError(f"Failed after retries: {type(last_err).__name__}: {last_err}")

# ============================================================
# 8 One batch runner
# ============================================================
def run_one_batch(conn, keys: List[str]) -> Tuple[int, int]:
    """
    跑一批 candidates：
    - upsert pending
    - ThreadPool 並行 call
    - 批次寫回 DONE/ERROR
    回傳 (done_count, err_count)
    """
    candidates = pick_candidates(conn)
    if not candidates:
        return 0, 0

    print(f"\nPicked {len(candidates)} candidates...")

    # clip + pending
    pending_rows: List[Tuple[str, str]] = []
    row_keys: List[str] = []
    texts: List[str] = []
    user_msgs: List[str] = []
    est_tokens: List[int] = []

    for k, content in candidates:
        input_text = clip_text(content, INPUT_MAX_CHARS)
        pending_rows.append((k, input_text))
        row_keys.append(k)
        texts.append(input_text)
        um = USER_TEMPLATE.format(text=input_text)
        user_msgs.append(um)
        est_tokens.append(estimate_tokens(SYSTEM_PROMPT, um, MAX_TOKENS))

    upsert_pending_many(conn, pending_rows)
    conn.commit()

    # slots + limiter + progress
    client_slots = [(api_key, make_client(api_key)) for api_key in keys for _ in range(PER_KEY_WORKERS)]
    rr = itertools.cycle(client_slots)
    limiter = CharRateLimiter(CHARS_PER_MINUTE_PER_KEY)

    token_window = TokenWindow()
    progress = ProgressTPM(total=len(texts), tpm_limit=TPM_LIMIT, token_window=token_window)

    done_rows: List[Tuple[Dict[str, Any], str]] = []      # (parsed_json, key)
    err_rows: List[Tuple[str, str]] = []                  # (err, key)

    with ThreadPoolExecutor(max_workers=len(client_slots)) as ex:
        futs = {}
        for i, input_text in enumerate(texts):
            api_key, client = next(rr)
            fut = ex.submit(infer_one, client, api_key, limiter, input_text)
            futs[fut] = i  # 存 index，方便取 key / est_tokens

        for fut in as_completed(futs):
            i = futs[fut]
            k = row_keys[i]
            try:
                parsed = fut.result()
                done_rows.append((parsed, k))
                progress.update(success=True, est_tokens=est_tokens[i])
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                err_rows.append((err, k))
                progress.update(success=False, est_tokens=est_tokens[i])

    if done_rows:
        mark_done_many(conn, done_rows)
    if err_rows:
        mark_error_many(conn, err_rows)
    conn.commit()

    return len(done_rows), len(err_rows)

# ============================================================
# 9 Main loop: run until empty
# ============================================================
def main():
    keys = normalize_keys(GROQ_API_KEY)
    if not keys:
        raise RuntimeError("Missing GROQ_API_KEY / GROQ_API_KEYS")

    conn = get_db_connection()

    total_done = 0
    total_err = 0

    while True:
        done_cnt, err_cnt = run_one_batch(conn, keys)
        if done_cnt == 0 and err_cnt == 0:
            print(f"\nAll done. total_done={total_done}, total_err={total_err}")
            break

        total_done += done_cnt
        total_err += err_cnt

        # 每批之間稍微喘一下（避免 burst）
        time.sleep(0.8)


if __name__ == "__main__":
    main()
