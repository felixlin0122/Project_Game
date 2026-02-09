from typing import Optional, Dict, Iterable
from pathlib import Path
import csv

import time
import random

import pymysql
import requests

from Project_units   import dayapart
from Project_crawler import build_article_page_url,parse_article_title_link, parse_content_message
from Project_crawler import parse_Great_Bad_point,parse_post_time,parse_sna, parse_max_page
from setting import (
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_DB,
    Basehtml,
    page,
    bsn_,
    game_name_
)
## 使用PyMySQL連線到MySQL
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
##定義HEADERS
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
##讀取取遊戲名稱列表
def gamename():
    BASE_DIR = Path(__file__).resolve().parents
    p = Path( "game.csv")
    bsn=[]
    game_name=[]
    with p.open("r",encoding="utf-8",newline="") as f :
        reader = csv.DictReader(f)
        for row in reader :
            bsn.append( row["Bsn"])
            game_name.append(row["Gamename"])           
    return bsn,game_name
##測試爬蟲連線狀況
def fetch_text(url: str, headers=None, timeout: int = 15, fetch: int = 3) -> str | None:
    merged_headers = dict(DEFAULT_HEADERS)
    if headers:
        merged_headers.update(headers)
    for i in range(fetch):
        try:
            resp = requests.get(url, headers=merged_headers, timeout=timeout, allow_redirects=True)
            if resp.status_code == 200:
                return resp.text

            snippet = resp.text[:300].replace("\n", " ")
            print(f"[WARN] {resp.status_code} final_url={resp.url} len={len(resp.text)} snippet={snippet}")

        except Exception as e:
            print(f"[ERROR] {type(e).__name__}: {e} url={url}")

        time.sleep(7)

##儲存資料進MySQL
def save_data(conn,bsn_sna_page:str ,bsn: int, sna: int, pages :int, game_name:str ,title: str,content: str
              , max_page: int , url: str ,article_create_time : str , great_point : int , bad_point : int):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO project_datas
                (bsn_sna_page,bsn,sna,pages,game_name,title,content,max_page,url,article_create_time,extract_time,great_point, bad_point)
            VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s)
            ON DUPLICATE KEY UPDATE
                title=VALUES(title),
                content=VALUES(content),
                max_page=VALUES(max_page),
                url=VALUES(url),
                extract_time=VALUES(extract_time),
                great_point=VALUES(great_point),
                bad_point=VALUES(bad_point)
            """,
            (bsn_sna_page,bsn,sna,pages,game_name,title,content,max_page,url,article_create_time,great_point,bad_point)
        )
##爬取及儲存總整理
def crawl_and_save(list_page_html: str, base_url: str , Bsn :str ,game_name :str) -> int:
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
        first_html = fetch_text(url)
        title = item.get("title") or ""
        post_time = parse_post_time(first_html)
        if dayapart(post_time)< -180 :
            continue
        if not url:
            continue
        sna = parse_sna(url)
        if sna is None:
            continue
        count +=1
        print(f"sna={sna},Article={count}/30")
        
        Great_point,Bad_point = parse_Great_Bad_point(first_html) or (0,0)

        max_page = parse_max_page(first_html) or 1
        for page_no in range(1, max_page + 1):
            page_url = build_article_page_url(url, page_no)
            page_html = first_html if page_no == 1 else fetch_text(page_url)
            content = parse_content_message(page_html) or ""
            bsn_sna_page = str(Bsn)+"_"+str(sna)+ "_" +str(page_no)
            save_data(conn,bsn_sna_page,Bsn,sna,page_no,game_name,title,content,max_page,page_url
                      ,post_time,Great_point,Bad_point)
            saved += 1
            time.sleep(random.uniform(2.0, 4.0))
            print(f"Page = {page_no}/{max_page} ")
    
    time.sleep(random.uniform(3.0, 4.0))
    return saved

##串接爬取、儲存、連接SQL
def storge() -> None:
    base_url = Basehtml
    total = 0
    
    # Bsn_,game_name_ = gamename()
    for i in range(len(bsn_)) :
        bsn = bsn_[i]
        game_name = game_name_[i]
        for outer_page in range(1, int(page) + 1):
            print(f"bsn={bsn},Page={outer_page}/{page}")
            list_url = f"{Basehtml}B.php?page={outer_page}&bsn={bsn}"
            list_html = fetch_text(list_url)
            total += crawl_and_save(list_html, base_url,bsn,game_name)   
    print(f"Saved {total} pages.")

if __name__ == "__main__":
    start_time = time.time()
    storge()
    alltime = time.time()-start_time
    print(f"程式耗時：{int(alltime/3600)}小時，{int((alltime%3600)/60)}分鐘，{int(alltime%60)}秒")
