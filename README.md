# Project_Game
團隊專題

1️⃣ 資料來源（Source）

目標網站：forum.gamer.com.tw
資料類型：
文章列表頁（標題、連結、頁碼）
文章內文頁（主文、留言、時間、GP/BP）
特性：
使用 QueryString 分頁（page, bPage, snA）
內文格式混合（<div>、<br>、<b>、影片、圖片）

2️⃣ 抓取層（Ingestion Layer）

2.1 List Page Crawler
輸入：看板 URL + page range
輸出：title、article_url、sna、article_page
功能：
分頁 URL 組裝（urllib.parse）
基礎 Header / Referer 設定
請求間隔（避免被封鎖）

2.2 Article Page Crawler
逐筆請求文章頁
抓取：
主文內容
留言內容
發文 / 編輯時間（data-mtime）
GP / BP 數值

3️⃣ 解析層（Parsing Layer）

HTML Parsing
工具：BeautifulSoup
處理策略：
<br> 統一轉為空白
移除圖片 / 影片節點
保留純文字內容
結構化輸出
主文與留言分段標記