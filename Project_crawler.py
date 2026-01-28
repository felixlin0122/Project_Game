from Project_units import parse_dt

from pathlib import Path
from typing import Optional,Dict
import re


from bs4 import BeautifulSoup
from urllib.parse import  urljoin

def parse_article_title_link(html:str,base_html:str)->dict :
    soup =BeautifulSoup(html,"html.parser")
    items : list[Dict] = []

    for td in soup.select(".b-list__main") :
        title_el = td.select_one("p.b-list__main__title")
        title = title_el.get_text(strip=True) if title_el else None

        href=None
        if title_el :
            href = title_el.get("href")
        if not href :
            a = td.select_one("a[href*='C.php']")
            if not a:
                continue
            href = a.get("href")
            title = a.get_text(strip=True) or title
        items.append({"title": title, "url": urljoin(base_html, href)})
    return items

def parse_max_page(html:str) ->int :
    soup = BeautifulSoup(html,"html.parser")
    page = []
    for a in soup.select("a[href*='page=']") :
        href =  a.get("href","")
        b = re.search(r"[?&]page=(\d+)",href)
        if b :
            page.append(int(b.group(1)))
    return max(page) if page else None


def parse_content_message(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    lines: list[str] = []

    content_blocks = soup.select("div.c-article__content")

    for idx, content_el in enumerate(content_blocks, start=1):
        for br in content_el.select("br"):
            br.replace_with(" ")

        text = content_el.get_text(" ", strip=True)
        if text:
            lines.append(f"【內文】{idx}")
            lines.append(text)

    comment_blocks = soup.select("span.comment_content")

    for idx, c in enumerate(comment_blocks, start=1):
        for br in c.select("br"):
            br.replace_with(" ")
        txt = c.get_text(" ", strip=True)
        if txt:
            lines.append(f"【留言】{idx}【{txt}")
    # print("content_blocks:", len(content_blocks))
    # print("comment_blocks:", len(comment_blocks))
    

    final_text = " ".join(lines).strip()
    return final_text if final_text else None


def parse_Great_Bad_point(html:str) ->tuple[int,int] | None :
    soup = BeautifulSoup(html,"html.parser")
    GP_el = soup.select_one("span.postgp > span")
    BP_el = soup.select_one("span.postbp > span")
    if not GP_el and not BP_el :
        return None
    def to_int(el):
        if not el : return 0
        text = el.get_text(strip=True)
        return 0 if text=="-" else int(text)
    return to_int(GP_el),to_int(BP_el)
        
    
def parse_post_time(html:str) -> str :
    soup = BeautifulSoup(html, "html.parser")
    edit_el = soup.select_one("a.edittime")
    if not edit_el :
        return None
    mtime = edit_el.get("data-mtime")
    if not mtime :
        return None
    return parse_dt(mtime)

    
        
    
    





