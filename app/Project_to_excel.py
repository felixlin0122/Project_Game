from Project_storge import get_db_connection

import pandas as pd

SOURCE_TABLE = "project_groq_nlp"
SOURCE_PK = "bsn_sna_page"
SOURCE_TEXT_COL = "output_json"
TARGET_TABLE = "project_datas"  


sql = f"""
SELECT
  s.{SOURCE_PK} AS bsn_sna_page,
  s.{SOURCE_TEXT_COL} AS output_json,
  t.article_create_time,
  t.great_point,
  t.bad_point,
  t.game_name

FROM {SOURCE_TABLE} s
LEFT JOIN {TARGET_TABLE} t
  ON t.bsn_sna_page COLLATE utf8mb4_0900_ai_ci
   = s.{SOURCE_PK}  COLLATE utf8mb4_0900_ai_ci
WHERE s.{SOURCE_TEXT_COL} IS NOT NULL
  AND TRIM(s.{SOURCE_TEXT_COL}) <> ''
ORDER BY s.{SOURCE_PK} ASC
"""

conn = get_db_connection()

df = pd.read_sql(sql, conn)
from datetime import date
da = date.today()
filename =f"groq_init_data{da}.xlsx"
df.to_excel(filename, index=False)
