# -*- coding: utf-8 -*-
import os
import json
from datetime import datetime
from typing import Dict, List, Any

import pymysql
from pymysql.cursors import DictCursor

from tqdm import tqdm
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from dotenv import load_dotenv
from groq import Groq
import time

load_dotenv()

# -----------------------------
# 8 維度定義
# -----------------------------
DIMENSIONS: Dict[str, List[str]] = {
    "機率": ["機率", "抽率", "掉落率", "保底", "歐", "非", "出貨"],
    "金流": ["課金", "消費", "課長", "交易", "儲值", "禮包", "月卡", "通行證"],
    "官方": ["官方", "客服", "外掛", "封號", "福利", "公告", "營運"],
    "社交": ["公會", "好友", "組隊", "聊天", "社群", "聊天室"],
    "連線": ["平台", "卡頓", "閃退", "卡bug", "延遲", "lag", "斷線", "當機", "BUG", "bug"],
    "更新": ["更新", "劇情", "玩法", "活動", "地圖", "改版", "版本"],
    "角色": ["角色", "強度", "操作", "立繪", "寵物", "職業", "平衡", "nerf", "buff"],
    "媒體": ["流暢度", "畫面", "順暢", "解析度", "幀", "掉幀", "特效", "音樂"],
}

SYSTEM_PROMPT = """
你是一個嚴格的情緒分析器。請對輸入文本的整體情緒打分，範圍 -1 到 1：
-1 = 明顯消極、負面、沮喪、憤怒
 0 = 中性、客觀、資訊為主
 1 = 明顯積極、正面、開心、鼓舞

規則：
1) score 必須是浮點數，保留最多兩位小數，且必須落在 [-1, 1]
2) confidence 必須是 0 到 1 的浮點數
3) label 只能是 negative / neutral / positive
4) 只輸出 JSON，不要有任何多餘文字

輸出格式固定如下：
{
  "score": <float>,
  "label": "negative|neutral|positive",
  "confidence": <float>,
  "reasons": ["...","..."]
}
""".strip()

def clip_text(text: str, max_chars: int = 1200) -> str:
    t = (text or "").strip()
    if len(t) <= max_chars:
        return t
    # 保留前後重點（前 900 + 後 300）
    head = t[:900]
    tail = t[-300:]
    return head + "\n...\n" + tail

# -----------------------------
# 設定讀取
# -----------------------------
def env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    return default if not v else int(v)

def get_cfg() -> Dict[str, Any]:
    c = {
        "groq_api_key": os.environ.get("GROQ_API_KEY"),
        "groq_model": os.environ.get("GROQ_MODEL", "llama-3.1-8b-instant"),

        "mysql_host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
        "mysql_port": env_int("MYSQL_PORT", 3306),
        "mysql_user": os.environ.get("MYSQL_USER"),
        "mysql_password": os.environ.get("MYSQL_PASSWORD"),
        "mysql_db": os.environ.get("MYSQL_DB"),

        "source_table": os.environ.get("MYSQL_SOURCE_TABLE"),
        "target_table": os.environ.get("MYSQL_TARGET_TABLE"),
        "key_col": os.environ.get("MYSQL_KEY_COL", "bsn_sna_page"),
        "content_col": os.environ.get("MYSQL_CONTENT_COL", "content"),

        "score_col": os.environ.get("MYSQL_SCORE_COL", "sentiment_score"),
        "label_col": os.environ.get("MYSQL_LABEL_COL", "sentiment_label"),
        "conf_col": os.environ.get("MYSQL_CONF_COL", "sentiment_confidence"),
        "dim_json_col": os.environ.get("MYSQL_DIM_JSON_COL", "dimensions_json"),
        "hit_json_col": os.environ.get("MYSQL_HIT_JSON_COL", "hit_words_json"),
        "reasons_json_col": os.environ.get("MYSQL_REASONS_JSON_COL", "reasons_json"),
        "updated_at_col": os.environ.get("MYSQL_UPDATED_AT_COL", "sentiment_updated_at"),

        "batch_size": env_int("BATCH_SIZE", 200),
        "max_items": env_int("MAX_ITEMS", 0),
    }

    must = ["groq_api_key", "mysql_user", "mysql_password", "mysql_db", "source_table", "target_table", "key_col"]
    missing = [k for k in must if not c.get(k)]
    if missing:
        raise RuntimeError(f"缺少必要環境變數：{missing}")
    return c

# -----------------------------
# 維度分類
# -----------------------------
def classify_dimensions(text: str) -> Dict[str, Any]:
    t_low = (text or "").lower()
    matched: List[str] = []
    hit_words: Dict[str, List[str]] = {}

    for dim, keywords in DIMENSIONS.items():
        hits = [kw for kw in keywords if kw.lower() in t_low]
        if hits:
            matched.append(dim)
            hit_words[dim] = hits

    if not matched:
        matched = ["其他"]
        hit_words["其他"] = []

    return {"dimensions": matched, "hit_words": hit_words}

# -----------------------------
# Groq 呼叫 + 重試
# -----------------------------
class TransientGroqError(Exception):
    pass

def _is_retryable(exc: Exception) -> bool:
    msg = (str(exc) or "").lower()

    # 有些 SDK 把 status_code 掛在不同地方
    status = (
        getattr(exc, "status_code", None)
        or getattr(getattr(exc, "response", None), "status_code", None)
        or getattr(getattr(exc, "http_response", None), "status_code", None)
    )

    # 常見可重試的 HTTP 狀態
    if status in (408, 409, 425, 429, 500, 502, 503, 504):
        return True

    # 文字訊息判斷（Groq 常見）
    retry_keywords = [
        "timeout", "timed out",
        "rate limit", "too many requests",
        "connection error", "connection", "connect",
        "network", "dns", "name resolution",
        "temporarily unavailable", "service unavailable",
        "bad gateway", "gateway timeout",
        "server disconnected", "remote end closed",
    ]
    if any(k in msg for k in retry_keywords):
        return True

    return False


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=30),
    stop=stop_after_attempt(6),
    retry=retry_if_exception_type(TransientGroqError),
    reraise=True,
)
def groq_sentiment(client: Groq, text: str, model: str) -> Dict[str, Any]:
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            max_tokens=300,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": text},
            ],
        )
        data = json.loads(resp.choices[0].message.content)

        score = float(data.get("score", 0.0))
        score = max(-1.0, min(1.0, score))
        data["score"] = round(score, 2)

        label = data.get("label", "neutral")
        if label not in ("negative", "neutral", "positive"):
            label = "negative" if score <= -0.15 else ("positive" if score >= 0.15 else "neutral")
        data["label"] = label

        conf = float(data.get("confidence", 0.5))
        conf = max(0.0, min(1.0, conf))
        data["confidence"] = round(conf, 2)

        reasons = data.get("reasons", [])
        if not isinstance(reasons, list):
            reasons = [str(reasons)]
        data["reasons"] = [str(r) for r in reasons][:5]

        return data

    except Exception as e:
    # 讓你看得到到底是哪一種例外（之後排查很有用）
    # print(f"[Groq error] {type(e).__name__}: {e}")
        if _is_retryable(e):
            raise TransientGroqError(f"{type(e).__name__}: {e}")
    raise


# -----------------------------
# MySQL
# -----------------------------
def mysql_connect(c: Dict[str, Any]):
    return pymysql.connect(
        host=c["mysql_host"],
        port=c["mysql_port"],
        user=c["mysql_user"],
        password=c["mysql_password"],
        database=c["mysql_db"],
        charset="utf8mb4",
        cursorclass=DictCursor,
        autocommit=False,
    )

def count_todo(cur, c: Dict[str, Any]) -> int:
    # 未處理：target 沒有這個 key，或 target 的 score 仍為 NULL（可允許重跑）
    sql = f"""
    SELECT COUNT(*) AS c
    FROM `{c['source_table']}` s
    LEFT JOIN `{c['target_table']}` t
      ON t.`{c['key_col']}` = s.`{c['key_col']}`
    WHERE s.`{c['content_col']}` IS NOT NULL
      AND TRIM(s.`{c['content_col']}`) <> ''
      AND (t.`{c['key_col']}` IS NULL OR t.`{c['score_col']}` IS NULL)
    """
    cur.execute(sql)
    return int(cur.fetchone()["c"])

def fetch_batch(cur, c: Dict[str, Any], last_key: str, limit: int) -> List[Dict[str, Any]]:
    # 用 key 做游標分頁（字串 key 也可以用 > 來做，但需確保 key 排序一致）
    sql = f"""
    SELECT s.`{c['key_col']}` AS k, s.`{c['content_col']}` AS content
    FROM `{c['source_table']}` s
    LEFT JOIN `{c['target_table']}` t
      ON t.`{c['key_col']}` = s.`{c['key_col']}`
    WHERE s.`{c['content_col']}` IS NOT NULL
      AND TRIM(s.`{c['content_col']}`) <> ''
      AND (t.`{c['key_col']}` IS NULL OR t.`{c['score_col']}` IS NULL)
      AND s.`{c['key_col']}` > %s
    ORDER BY s.`{c['key_col']}` ASC
    LIMIT %s
    """
    cur.execute(sql, (last_key, limit))
    return cur.fetchall()

def upsert_result(cur, c: Dict[str, Any], key: str,
                  score: float, label: str, confidence: float,
                  dimensions: List[str], hit_words: Dict[str, List[str]],
                  reasons: List[str]):
    dim_json = json.dumps(dimensions, ensure_ascii=False)
    hit_json = json.dumps(hit_words, ensure_ascii=False)
    reasons_json = json.dumps(reasons, ensure_ascii=False)
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 需要 target key 有 UNIQUE/PK，才能 ON DUPLICATE KEY UPDATE
    sql = f"""
    INSERT INTO `{c['target_table']}` (
      `{c['key_col']}`,
      `{c['score_col']}`, `{c['label_col']}`, `{c['conf_col']}`,
      `{c['dim_json_col']}`, `{c['hit_json_col']}`, `{c['reasons_json_col']}`,
      `{c['updated_at_col']}`
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      `{c['score_col']}`=VALUES(`{c['score_col']}`),
      `{c['label_col']}`=VALUES(`{c['label_col']}`),
      `{c['conf_col']}`=VALUES(`{c['conf_col']}`),
      `{c['dim_json_col']}`=VALUES(`{c['dim_json_col']}`),
      `{c['hit_json_col']}`=VALUES(`{c['hit_json_col']}`),
      `{c['reasons_json_col']}`=VALUES(`{c['reasons_json_col']}`),
      `{c['updated_at_col']}`=VALUES(`{c['updated_at_col']}`)
    """
    cur.execute(sql, (key, score, label, confidence, dim_json, hit_json, reasons_json, updated_at))

# -----------------------------
# 主流程
# -----------------------------
def run():
    c = get_cfg()
    groq_client = Groq(api_key=c["groq_api_key"])

    with mysql_connect(c) as conn:
        with conn.cursor() as cur:
            total = count_todo(cur, c)
            if c["max_items"] and c["max_items"] > 0:
                total = min(total, c["max_items"])

            pbar = tqdm(total=total, desc="Groq analyze (datas -> posts)", unit="row")

            last_key = ""   # 字串游標
            processed = 0

            while processed < total:
                take = min(c["batch_size"], total - processed)
                rows = fetch_batch(cur, c, last_key=last_key, limit=take)
                if not rows:
                    break

                try:
                    for r in rows:
                        key = str(r["k"])
                        text = (r.get("content") or "").strip()
                        last_key = key

                        dim = classify_dimensions(text)
                        text_for_llm = clip_text(text, max_chars=1200)
                        try:
                            sent = groq_sentiment(groq_client, text_for_llm, model=c["groq_model"])
                        except Exception as e:
                        # 真的連線不穩就先略過這筆，避免整批 rollback
                        # 你也可以把錯誤寫到 log 或另外一張表
                        # print(f"[Skip] key={key} groq_failed: {type(e).__name__}: {e}")
                            continue

                        upsert_result(
                            cur, c, key=key,
                            score=float(sent["score"]),
                            label=str(sent["label"]),
                            confidence=float(sent["confidence"]),
                            dimensions=dim["dimensions"],
                            hit_words=dim["hit_words"],
                            reasons=sent.get("reasons", []),
                        )
                        time.sleep(0.5)


                        processed += 1
                        pbar.update(1)

                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    pbar.close()
                    raise RuntimeError(f"批次處理失敗，已 rollback。錯誤：{e}")

            pbar.close()

    print("Done.")
    print(f"Processed: {processed}")

if __name__ == "__main__":
    run()
