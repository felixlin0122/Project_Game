import os
import json
from datetime import datetime
from typing import Dict, List, Any

from Project_storge import get_db_connection

import pymysql
from pymysql.cursors import DictCursor

from tqdm import tqdm
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from groq import Groq
import time

from setting import (
    Groq_api_keys
    )

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
你是情緒分類器，只能輸出 JSON，不得輸出其他文字。
請根據 8 類分類規則輸出分數與證據。

8 類包含：
機率、金流、官方、社交、連線、更新、角色、媒體

規則：
- score 範圍為 -1 到 +1
- confidence 範圍為 0 到 1
- 若無相關內容，score=0, confidence<=0.4, evidence=[]
- evidence 必須是原文短句或忠實改寫
- 必須完整輸出所有欄位
"""

USER_TEMPLATE = """
請分析以下文本：

{text}

請輸出以下 JSON 結構：

{
  "language": "zh-TW",
  "overall_sentiment": 0,
  "overall_confidence": 0.0,
  "categories": {
    "機率":   {"score": 0, "confidence": 0.0, "evidence": []},
    "金流":   {"score": 0, "confidence": 0.0, "evidence": []},
    "官方":   {"score": 0, "confidence": 0.0, "evidence": []},
    "社交":   {"score": 0, "confidence": 0.0, "evidence": []},
    "連線":   {"score": 0, "confidence": 0.0, "evidence": []},
    "更新":   {"score": 0, "confidence": 0.0, "evidence": []},
    "角色":   {"score": 0, "confidence": 0.0, "evidence": []},
    "媒體":   {"score": 0, "confidence": 0.0, "evidence": []}
  }
}
"""

MODEL_NAME = "llama-3.3-70b-versatile"
PROMPT_VERSION = "v1.0"
class GroqKeyManager:
    def __init__(self, keys):
        self.keys = keys
        self.index = 0

    def get_client(self):
        key = self.keys[self.index]
        return Groq(api_key=key)

    def rotate(self):
        self.index = (self.index + 1) % len(self.keys)
        print(f"🔄 切換到 key index: {self.index}")
key_manager = GroqKeyManager(Groq_api_keys)

def clip_text(text: str, max_chars: int = 1200) -> str:
    t = (text or "").strip()
    if len(t) <= max_chars:
        return t
    part = max_chars // 3  # 400
    head = t[:part]
    mid_start = max(0, len(t)//2 - part//2)
    mid = t[mid_start: mid_start + part]
    tail = t[-part:]
    return head + "\n...\n" + mid + "\n...\n" + tail

def call_groq(text: str, max_retry: int = 10):

    backoff = 2  # 初始等待秒數

    for attempt in range(max_retry):

        client = key_manager.get_client()

        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                temperature=0,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": USER_TEMPLATE.format(text=text)}
                ]
            )

            content = response.choices[0].message.content.strip()

            start = content.find("{")
            end = content.rfind("}")
            json_str = content[start:end + 1]

            return json.loads(json_str)

        except Exception as e:

            error_str = str(e)

            # ===== 429 or quota =====
            if "429" in error_str or "rate" in error_str.lower():
                print("⚠️ 遇到限速，等待中...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            # ===== token 超過 =====
            if "context" in error_str.lower() or "token" in error_str.lower():
                raise RuntimeError("Token 過長，請分段處理")

            # ===== 可能 key 用盡 =====
            print("⚠️ 可能 key 用盡，嘗試切換")
            key_manager.rotate()
            time.sleep(2)

    raise RuntimeError("多次嘗試仍失敗")

def save_to_mysql(result: Dict[str, Any], bsn: int, sna: int, page: int):

    bsn_sna_page = f"{bsn}_{sna}_{page}"

    cat = result["categories"]

    sql = """
    INSERT INTO project_nlp_sentiment (
        bsn_sna_page, bsn, sna, page,
        model, prompt_version,
        overall_sentiment, overall_confidence,
        score_prob, score_pay, score_official,
        score_social, score_conn, score_update,
        score_role, score_media,
        result_json
    ) VALUES (
        %s,%s,%s,%s,
        %s,%s,
        %s,%s,
        %s,%s,%s,
        %s,%s,%s,
        %s,%s,
        %s
    )
    ON DUPLICATE KEY UPDATE
        overall_sentiment=VALUES(overall_sentiment),
        overall_confidence=VALUES(overall_confidence),
        score_prob=VALUES(score_prob),
        score_pay=VALUES(score_pay),
        score_official=VALUES(score_official),
        score_social=VALUES(score_social),
        score_conn=VALUES(score_conn),
        score_update=VALUES(score_update),
        score_role=VALUES(score_role),
        score_media=VALUES(score_media),
        result_json=VALUES(result_json),
        extracted_at=NOW()
    """

    values = (
        bsn_sna_page, bsn, sna, page,
        MODEL_NAME, PROMPT_VERSION,
        result["overall_sentiment"],
        Decimal(str(result["overall_confidence"])),

        cat["機率"]["score"],
        cat["金流"]["score"],
        cat["官方"]["score"],
        cat["社交"]["score"],
        cat["連線"]["score"],
        cat["更新"]["score"],
        cat["角色"]["score"],
        cat["媒體"]["score"],

        json.dumps(result, ensure_ascii=False)
    )

    conn = get_db_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, values)
        conn.commit()
    finally:
        conn.close()

def main():

    # 模擬一筆資料（之後改成從 article table 撈）
    bsn = 23805
    sna = 123456
    page = 1

    text = """
    這次更新真的很棒，角色強度調整合理。
    但是伺服器一直斷線，真的很煩。
    """

    result = call_groq(text)
    print("解析結果:", result)

    save_to_mysql(result, bsn, sna, page)
    print("已寫入資料庫")


if __name__ == "__main__":
    main()

