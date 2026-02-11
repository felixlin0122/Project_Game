FROM python:3.11-slim

WORKDIR /app

# 先裝常用套件；你需要什麼再加（pymysql 幾乎一定要）
RUN pip install --no-cache-dir pymysql requests pandas beautifulsoup4 lxml

# 把程式放進容器
COPY . /app



