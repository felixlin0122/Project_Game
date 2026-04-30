from qdrant_client import QdrantClient

from sentence_transformers import SentenceTransformer

from groq import Groq
from setting import GROQ_API_KEY
from Project_storge import get_db_connection
import json

from qdrant_client.models import Filter, FieldCondition, MatchValue


def save_rag_query_log(conn, question, contexts, answer, llm_model="llama-3.1-8b-instant"):
    """
    將 RAG 問答紀錄寫入 MySQL
    question: 使用者問題
    contexts: retrieve() 回傳的 list
    answer: Groq 回答結果
    """

    retrieved_chunk_ids = [
        c.get("chunk_id")
        for c in contexts
        if c.get("chunk_id")
    ]

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO rag_query_logs (
                user_question,
                retrieved_chunk_ids,
                final_answer,
                llm_model
            )
            VALUES (%s, %s, %s, %s)
        """, (
            question,
            json.dumps(retrieved_chunk_ids, ensure_ascii=False),
            answer,
            llm_model
        ))

    conn.commit()

def retrieve(question, model, client, top_k=5):
    query_vector = model.encode(question).tolist()

    results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vector,
        limit=top_k
    )

    contexts = []

    for r in results.points:
        contexts.append({
            "score": r.score,
            "chunk_id": r.payload.get("chunk_id"),
            "title": r.payload.get("title"),
            "game_name": r.payload.get("game_name"),
            "text": r.payload.get("text")
        })

    return contexts

def answer_with_rag(question, model, client, groq_client):
    contexts = retrieve(question, model, client, top_k=5)

    if not contexts:
        return "目前沒有檢索到相關資料。", contexts

    context_text = "\n\n".join([
        f"【資料{i+1}】\n"
        f"chunk_id：{c['chunk_id']}\n"
        f"標題：{c['title']}\n"
        f"遊戲：{c['game_name']}\n"
        f"內容：{c['text']}"
        for i, c in enumerate(contexts)
    ])

    prompt = f"""
你是一個遊戲論壇輿情分析助理。
請只根據下方資料回答問題，不要自行編造。
如果資料不足，請回答「目前資料不足以判斷」。

問題：
{question}

可參考資料：
{context_text}
"""

    response = groq_client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": "請使用繁體中文回答，並根據提供資料進行分析。"},
            {"role": "user", "content": prompt}
        ],
        temperature=0.2
    )

    answer = response.choices[0].message.content

    return answer, contexts





if __name__=="__main__" :
    COLLECTION_NAME = "bahamut_forum_chunks"
    client = QdrantClient(host="qdrant", port=6333)
    model = SentenceTransformer(
        "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    )
    groq_client = Groq(api_key=GROQ_API_KEY[0])
    query_filter=Filter(
        must=[
            FieldCondition(
                key="game_name",
                match=MatchValue(value="某遊戲")
            )
        ]
    )

    while True:
        question = input("請輸入問題，輸入 q 離開：")
        conn = get_db_connection()
        if question.lower() == "q":
            break

        answer , contexts = answer_with_rag(question, model, client, groq_client)
        save_rag_query_log(conn, question, contexts, answer)
        print("\n===== RAG 回答 =====")
        print(answer)
    conn.close()
    


