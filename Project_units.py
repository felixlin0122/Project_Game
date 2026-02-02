from datetime import datetime
import re
from pathlib import Path
import csv

def parse_dt(dt_str:str) ->str :
    for fmt in ("%Y-%m-%d %H:%M:%S" ,"%Y/%m/%d %H:%M:%S","%Y-%b-%d %H:%M") :
        
        try :
            dt = datetime.strptime(dt_str,fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None


def words_() -> None :
    p = Path("pratece_P/forbiddenword.csv")
    words= []
    with p.open("r",encoding="utf-8",newline=" ")  as f:
        reader = csv.DictReader(f)
        for row in reader :
            words.append(row["words"])
    return words

def clean_text(text:str) ->str:
    URL_PATTERN = re.compile(r"https?://\S+|www\.\S+", re.I)
    EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
    HASH_BLOCK_PATTERN = re.compile(r"##.*?##")
    if not text : return ""
    text = URL_PATTERN.sub("",text)
    text = EMAIL_PATTERN.sub("",text)
    text = HASH_BLOCK_PATTERN.sub("",text)
    return text



