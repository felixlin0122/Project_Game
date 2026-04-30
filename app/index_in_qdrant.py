from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

from Project_storge import get_db_connection

from sentence_transformers import SentenceTransformer
from qdrant_client.models import PointStruct
from datetime import datetime
import uuid

from groq import Groq
import os
from setting import GROQ_API_KEY

def embed_and_upsert(conn, batch_size=500):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.chunk_id, c.chunk_text, c.bsn, c.sna, c.pages,
                   c.bsn_sna_page, c.game_name, c.title
            FROM rag_chunks c
            LEFT JOIN rag_embedding_jobs j
              ON c.chunk_id = j.chunk_id
            WHERE j.chunk_id IS NULL
               OR j.status = 'failed'
            LIMIT %s
        """, (batch_size,))

        rows = cur.fetchall()

        if not rows:
            print("沒有需要索引的 chunk")
            return 0

        points = []

        for row in rows:
            chunk_id, chunk_text, bsn, sna, pages, bsn_sna_page, game_name, title = row

            vector = model.encode(chunk_text).tolist()
            point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, chunk_id))

            points.append(
                PointStruct(
                    id=point_id,
                    vector=vector,
                    payload={
                        "chunk_id": chunk_id,
                        "bsn": bsn,
                        "sna": sna,
                        "pages": pages,
                        "bsn_sna_page": bsn_sna_page,
                        "game_name": game_name,
                        "title": title,
                        "text": chunk_text
                    }
                )
            )

        client.upsert(
            collection_name=COLLECTION_NAME,
            points=points
        )

        for row in rows:
            chunk_id = row[0]

            cur.execute("""
                INSERT INTO rag_embedding_jobs (
                    chunk_id, embedding_model, status, embedded_at
                )
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    embedded_at = VALUES(embedded_at),
                    error_message = NULL
            """, (
                chunk_id,
                "paraphrase-multilingual-MiniLM-L12-v2",
                "success",
                datetime.now()
            ))

    conn.commit()
    print(f"本批完成：{len(rows)} 筆")
    return len(rows)

def embed_and_upsert_all(conn, batch_size=500):
    total = 0

    while True:
        count = embed_and_upsert(conn, batch_size=batch_size)

        if count == 0:
            break

        total += count
        print(f"累計完成：{total} 筆")

    print("全部 chunk 已索引完成")



def ensure_collection():
    collections = client.get_collections().collections
    collection_names = [c.name for c in collections]

    if COLLECTION_NAME not in collection_names:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(
                size=384,
                distance=Distance.COSINE
            )
        )





if __name__ == "__main__" :
    COLLECTION_NAME = "bahamut_forum_chunks"
    client = QdrantClient(host="qdrant", port=6333)
    ensure_collection()
    model = SentenceTransformer(
        "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    )
    groq_client = Groq(api_key=GROQ_API_KEY)
    conn = get_db_connection()
    embed_and_upsert_all(conn, batch_size=500)
    conn.close()


