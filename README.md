遊戲營運輿情智能資料管線系統
專案簡介
本專案實作了一套可擴展的端到端資料管線（End-to-End Data Pipeline），用於擷取遊戲論壇資料、處理多維度情緒特徵，提供後續分析與視覺化使用。
本專案的核心不僅是情緒分析，而是：
• 資料管線架構設計
• 工作流程調度（Workflow Orchestration）
• 特徵工程設計
• 可擴展儲存架構
• 營運監控機制
資料來源（Source）
目標網站：forum.gamer.com.tw
資料類型：
文章列表頁（標題、連結、頁碼）
文章內文頁（主文、留言、時間、GP/BP）  
特性：
使用 QueryString 分頁（page, bPage, snA）
內文格式混合（<div>、<br>、<b>、影片、圖片）
系統架構
[Crawler 服務]
↓
[Raw Data Layer - MySQL]
↓
[Feature Engineering Layer]
↓
[Sentiment Processing Layer]
↓
[Analytics Data Mart]
↓
[BI Layer - Tableau]
核心組件
1️⃣ 資料擷取層（Data Ingestion Layer）
• 網頁爬蟲擷取結構化論壇內容
• 處理分頁與動態 URL 組裝
• 實作指數退避重試機制（Exponential Backoff）
• 將原始資料存入 MySQL
設計原則：
• 將「原始資料層」與「處理資料層」分離
• 保留原始資料以利未來重新處理
2️⃣ 資料儲存設計（Data Storage Design）
採用雙層結構：
Raw Table（原始層）
• 儲存原始文章內容
• 保留留言 JSON 結構
• 不進行轉換
Processed Table（Data Mart 分析層）
• 情緒特徵欄位
• 風險指標
• 聚合後指標
• 優化 BI 查詢效率
索引策略（Index Strategy）
• 建立 game_name 索引
• 建立 created_time 索引
• 建立複合索引 (game_name, created_time)
________________________________________
3️⃣ 工作流程調度（Airflow）
Pipeline DAG：
crawl_data
↓
store_raw
↓
run_sentiment_engine
↓
generate_features
↓
update_data_mart
特點：
• 任務具備冪等性設計（Idempotent Tasks）
• 支援增量更新（Incremental Processing）
• 失敗重試策略
• 日誌與監控機制
4️⃣ 特徵工程層（Feature Engineering Layer）
不同於單純正負情緒分類，本系統產生結構化特徵：
• 8 維度情緒向量
• 爭議指數（Controversy Index）
• 金流壓力指數（Monetization Pressure Index）
上述特徵皆實體化（Materialized）於分析資料表中。
資料建模策略（Data Modeling Strategy）
採用星型類分析架構（Star-like Structure）：
Fact Table（事實表）
• article_id
• game_name
• created_time
• comment_count
• GP
• BP
• sentiment_scores
Dimension Tables（維度表）
• dim_game
• dim_time
設計目的：
• 優化聚合查詢效能
• 提供 BI 工具高效分析支援
解決的工程問題
1️⃣ JSON 留言儲存處理
• 儲存原始 JSON
• 額外抽取 comment_count
• 避免過度複雜的關聯式 Join
2️⃣ 增量處理機制
使用條件：
WHERE crawl_time > last_processed_time
確保 Pipeline 僅處理新增資料。
3️⃣ 容錯設計（Fault Tolerance）
• 資料庫交易控制（Transaction Handling）
• 部分失敗隔離設計
4️⃣ 可重現環境（Reproducible Environment）
• Docker Compose
• MySQL Container
• Airflow Container
• Python App Container
確保系統具備可攜式部署能力。
可擴展性設計（Scalability Considerations）
當資料量成長時：
• 依 created_time 進行分區（Partitioning）
• 引入訊息佇列（Kafka）
• 遷移至資料倉儲（BigQuery / GCP Compute Engine VM）
技術棧（Tech Stack）
• Python 3.11
• MySQL 8.0
• Docker / Docker Compose
• Apache Airflow
• Pandas
• PyMySQL
• GROQ
• Tableau
________________________________________
本專案展現能力
✔ 端到端資料管線設計
✔ 分析導向特徵工程
✔ 工作流調度實作
✔ 生產環境級容器化設計
✔ Raw 與 Analytics 層分離架構
✔ 可擴展資料建模設計

