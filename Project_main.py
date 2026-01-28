from typing import Optional, Dict, Iterable
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import time
import random

import pymysql
import requests

from Project_crawler import parse_article_title_link, parse_content_message, parse_max_page
from Project_crawler import parse_Great_Bad_point,parse_post_time
from setting.setting import (
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_DB,
    Basehtml,
    Bsn,
    page,
    max_set_page,
)

def get_db_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        port=MYSQL_PORT,
        charset="utf8mb4",
        autocommit=True
    )


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}


def fetch_text(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 15, fetch: int = 3) -> str:
    merged_headers = dict(DEFAULT_HEADERS)
    if headers:
        merged_headers.update(headers)
    for i in range(fetch):
        try :
            resp = requests.get(url, headers=merged_headers, timeout=timeout)
            if resp.status_code== 200 :
                return resp.text
            print(f"[WARN] {resp.status_code} {url}")
        except Exception as e :
            print(f"ERROR:{e}")
        time.sleep(7)
    return None



def save_article(conn, bsn: int, sna: int, title: str, article_page: int , 
                 article_create_time : str , great_point : int , bad_point : int) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO project_article (bsn, sna, title, article_page, article_create_time, great_point, bad_point)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title=VALUES(title),
                article_page=VALUES(article_page),
                article_create_time=VALUES(article_create_time),
                great_point=VALUES(great_point),
                bad_point=VALUES(bad_point)
            """,
            (bsn, sna, title, article_page,article_create_time,great_point,bad_point),
        )
        


def save_nlp_page(conn, sna_article_page: str, article_url: str, content: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO project_NLP
                (sna_article_page, NLP_item_code, NLP_content, article_url, extract_time, NLP_item, NLP_score)
            VALUES
                ( %s, %s, %s, %s, NOW(), %s, %s)
            ON DUPLICATE KEY UPDATE
                NLP_content=VALUES(NLP_content),
                article_url=VALUES(article_url),
                extract_time=VALUES(extract_time)
            """,
            (sna_article_page, 0, content, article_url, "", 0.0),
        )


def build_article_page_url(url: str, page_no: int) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    keep = {}
    for k in ("bsn", "snA"):
        if k in q and q[k]:
            keep[k] = q[k][0]

    keep["page"] = str(page_no)

    new_query = urlencode(keep, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

def parse_sna(url: str) -> Optional[int]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    sna = query.get("snA")
    if not sna:
        return None
    try:
        return int(sna[0])
    except ValueError:
        return None


def crawl_and_save(list_page_html: str, base_url: str) -> int:
    items = parse_article_title_link(list_page_html, base_url)
    if items:
        print("FIRST_URL:", items[0].get("url"))

    if not items:
        return 0
    conn = get_db_connection()
    saved = 0
    count = 0
    for item in items:
        url = item.get("url")
        title = item.get("title") or ""
        if not url:
            continue
        sna = parse_sna(url)
        if sna is None:
            continue
        count +=1
        print(f"sna={sna},Article={count}/30")
        first_html = fetch_text(url)
        GP,BP = parse_Great_Bad_point(first_html) or (0,0)
        post_time = parse_post_time(first_html)
        max_page = parse_max_page(first_html) or 1
        save_article(conn, Bsn, sna, title, max_page,post_time,GP,BP)
        pages = min(max_page,int(max_set_page))
        for page_no in range(1, pages + 1):
            page_url = build_article_page_url(url, page_no)
            page_html = first_html if page_no == 1 else fetch_text(page_url)
            content = parse_content_message(page_html) or ""
            sna_page_no = str(sna)+ "_" +str(page_no)
            save_nlp_page(conn, sna_page_no, page_url, content)
            saved += 1
            time.sleep(random.uniform(2.0, 4.0))
            print(f"Page={page_no}/{max_page}")
        
    
    time.sleep(random.uniform(3.0, 6.0))
    return saved




def main() -> None:
    base_url = Basehtml
    total = 0
    for outer_page in range(1, int(page) + 1):
        print(f"bsn={Bsn},Page={outer_page}/{page}")
        list_url = f"{Basehtml}B.php?page={outer_page}&bsn={Bsn}"
        list_html = fetch_text(list_url)
        total += crawl_and_save(list_html, base_url)   
    print(f"Saved {total} pages.")


if __name__ == "__main__":
    start_time = time.time()
    main()
    alltime = time.time()-start_time
    print(f"程式耗時：{int(alltime/3600)}小時，{int((alltime%3600)/60)}分鐘，{int(alltime%60)}秒")
