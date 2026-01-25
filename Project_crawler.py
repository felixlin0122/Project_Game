from Project_units import parse_dt

from pathlib import Path
import re


from bs4 import BeautifulSoup
from urllib.parse import  urljoin

def parse_article_title_link(html:str,base_html:str)->str :
    soup =BeautifulSoup(html,"html.parser")
    items = list[dict] = []

    for td in soup.select("b_list__main") :
        title_el = td.select_one("p.b_list__main__title")
        title = title_el.get_text(strip=True) if title_el else None

        href=None
        if title_el :
            href = title_el.get("href")
        if not href :
            a= td.select_one("a[href*='C.php']")
            if a :
                href = a.get("href")
                if not title :
                    title = a.get_text(strip=True) or None
        items.append({ "title" : title ,"url" : urljoin(base_html,href) })
    return items

def parse_max_page(html:str) ->str :
    soup = BeautifulSoup(html,"html.parser")
    page = []
    for a in soup.select("a[href*= 'page=']") :
        href =  a.get("href","")
        b = re.search(r"[?&]page=(\d+)",href)
        if b :
            page.append(int(b.group(1)))
    return max(page) if max(page) else None

def parse_content_message(html:str)->str :
    soup = BeautifulSoup(html,"html.parser")
    content_el = soup.select("div.c-article__content")
    if not content_el:
        return None

    # Use newline as separator so <br> turns into line breaks.
    content_text = content_el.get_text("\n", strip=True)
    lines = [s for s in content_text.split("\n") if s.strip()]

    # Collect comment text (if any) and append to content lines.
    for el in soup.select("div.comment_content"):
        txt = el.get_text(" ", strip=True)
        if txt:
            lines.append(txt)

    text = "\n".join(lines).strip()
    return text

def parse_Great_Bad_point(html:str) ->dict | None :
    soup = BeautifulSoup(html,"html.parser")
    GP_el = soup.select_one("span.postgp > span")
    BP_el = soup.select_one("span.postbp > span")
    if not GP_el and not BP_el :
        return None
    def to_int(el):
        if not el : return 0
        text = el.get_text(strip=True)
        return 0 if text=="-" else int(text)
    return {"GP":to_int(GP_el),"BP":to_int(BP_el)}
        
    
def parse_post_time(html:str) ->str :
    soup = BeautifulSoup(html, "html.parser")
    edit_el = soup.select_one("a.edittime")
    if not edit_el :
        return None
    mtime = edit_el.get("data-mtime")
    if not mtime :
        return None
    return parse_dt(mtime)

    
        
    
    





