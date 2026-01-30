import os
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

def require(name: str) -> str:
    if name not in os.environ:
        raise RuntimeError(f"Missing env var: {name}")
    return os.environ[name]

MYSQL_USER = require("MYSQL_USER")
MYSQL_PASSWORD = require("MYSQL_PASSWORD")
MYSQL_HOST = require("MYSQL_HOST")
MYSQL_PORT = int(require("MYSQL_PORT"))
MYSQL_DB = require("MYSQL_DB")

Basehtml= require("Basehtml")
page = require("page")
max_set_page = require("Max_set_page")



