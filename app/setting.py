import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


def _load_dotenv_if_exists():
    """
    本機開發：如果專案根目錄有 .env 就載入
    Docker/Airflow：通常已由 env_file / 環境變數注入，不依賴檔案
    """
    if load_dotenv is None:
        return

    # 以目前檔案位置往上找 .env
    base_dir = Path(__file__).resolve().parents[1]
    env_path = base_dir / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def require(name: str) -> str:
    v = os.getenv(name)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


def getenv_any(names: list[str], default: str | None = None) -> str | None:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default


_load_dotenv_if_exists()

# ---- MySQL (容器內 host 應為 mysql；本機可用 127.0.0.1) ----
MYSQL_USER = getenv_any(["MYSQL_USER"], "root")
MYSQL_PASSWORD = getenv_any(["MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD"])  # 兩者擇一
MYSQL_HOST = getenv_any(["MYSQL_HOST"], "mysql")
MYSQL_PORT = int(getenv_any(["MYSQL_PORT"], "3306"))

# 同時支援 MYSQL_DB / MYSQL_DATABASE
MYSQL_DB = getenv_any(["MYSQL_DB", "MYSQL_DATABASE"], "bahamut")

# 初始值
Basehtml = "https://forum.gamer.com.tw/"
page = 20

# 測試用的小清單
game_name_ = ["神魔之塔"]
bsn_ = [23805]

words = ["朋友", "兌換", "邀請碼", "指南", "進板圖", "曬卡", "集中串"]
