import pymysql
from Project_storge import get_db_connection
import time


def split_text(text, chunk_size=600, overlap=100):
    chunks = []
    start = 0

    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end].strip()

        if chunk:
            chunks.append(chunk)

        start += chunk_size - overlap

    return chunks

def build_chunks(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT bsn_sna_page, bsn, sna, pages, game_name, title, content
            FROM project_datas
            WHERE content IS NOT NULL
              AND content != ''
        """)
        rows = cur.fetchall()

        for row in rows:
            bsn_sna_page, bsn, sna, pages, game_name, title, content = row
            chunks = split_text(content)

            for i, chunk_text in enumerate(chunks):
                chunk_id = f"{bsn_sna_page}_chunk_{i:03d}"

                cur.execute("""
                    INSERT INTO rag_chunks (
                        chunk_id, bsn, sna, pages, bsn_sna_page,
                        game_name, title, chunk_order, chunk_text
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        chunk_text = VALUES(chunk_text),
                        title = VALUES(title),
                        game_name = VALUES(game_name)
                """, (
                    chunk_id, bsn, sna, pages, bsn_sna_page,
                    game_name, title, i, chunk_text
                ))

    conn.commit()

if __name__ =="__main__" :
    st_time = time.time()
    conn = get_db_connection()
    build_chunks(conn)
    ed_time = time.time()-st_time
    print (ed_time)
    

