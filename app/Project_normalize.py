from Project_storge import get_db_connection
import time

def save_game_name(conn ,Bsn:int , game_name:str , source :str) :
    with conn.cursor() as cursor :
        cursor.execute(
            """
            INSERT INTO game_info
                (Bsn,game_name,source)
            VALUES 
                (%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                source = VALUES(source) 
            """ ,
            (Bsn,game_name,source)
        )

def save_article(conn ,bsn:int ,sna:int ,title:str ,article_create_time:str ,great_point:int ,bad_point:int ,max_page:int):
    with conn.cursor() as cursor :
        cursor.execute(
            """
            INSERT INTO article
                (article_id,bsn,sna,title,article_create_time,great_point,bad_point,max_page)
            VALUES
                (UUID(),%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                title       = VALUES(title),
                great_point = VALUES(great_point),
                bad_point   = VALUES(bad_point),
                max_page    = VALUES(max_page) 
            """,
            (bsn,sna,title,article_create_time,great_point,bad_point,max_page)
        )

def save_content(conn ,article_id:str ,pages:int ,content:str ,url:str ,extract_time :str) :
    with conn.cursor() as cursor :
        cursor.execute(
            """
            INSERT INTO content_
                (content_id,article_id,pages,content,url,extract_time)
            VALUES
                (UUID(),%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                content      = VALUES(content),
                url          = VALUES(url),
                extract_time = VALUES(extract_time)
            """,
            (article_id,pages,content,url,extract_time)
        )

def select_distinct(conn,table:str,columns:list[str]) ->list[dict]:
    cols = " , ".join(columns)#(f"`{c}`" for c in columns)
    sql = f"SELECT DISTINCT {cols} FROM {table}"
    with conn.cursor() as cursor :
        cursor.execute(sql)
        rows = cursor.fetchall()

    return [dict(zip(columns,row))for row in rows]

def load_article_id_map(conn) -> dict[tuple[int, int], str]:
    sql = "SELECT bsn, sna, article_id FROM article"
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    return {(int(bsn), int(sna)): article_id for (bsn, sna, article_id) in rows}


def normalize(conn,data_table,crawler) :
    game_info_cols = ["bsn","game_name"]
    game_info = select_distinct(conn,data_table,game_info_cols)
    for row in game_info :
        save_game_name(conn ,row["bsn"] , row["game_name"] , crawler)
    article_cols = ["bsn" ,"sna" ,"title" ,"article_create_time" ,"great_point" ,"bad_point" ,"max_page"]
    artice = select_distinct(conn,data_table,article_cols)
    for row in artice :
        save_article(conn,row["bsn"],row["sna"],row["title"],row["article_create_time"],row["great_point"]
                     ,row["bad_point"],row["max_page"])
    article_id_map = load_article_id_map(conn)
    content_cols = ["bsn", "sna", "pages", "content", "url", "extract_time"]
    contents = select_distinct(conn, data_table, content_cols)
    for row in contents:
        key = (int(row["bsn"]), int(row["sna"]))
        article_id = article_id_map.get(key)
        if not article_id:
            continue
        save_content(conn, article_id, row["pages"], row["content"], row["url"], row["extract_time"])


if __name__ == "__main__" :
    start_time = time.time()
    conn = get_db_connection()
    crawler = "Felix"
    data_table = "project_datas"
    normalize(conn,data_table,crawler)
    dt = time.time()-start_time
    print(dt)

