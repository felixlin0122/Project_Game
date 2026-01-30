from Project_units import parse_dt

from typing import Optional,Dict
from urllib.parse import urlparse, parse_qs,urljoin, urlencode, urlunparse ,parse_qs
import re

from bs4 import BeautifulSoup

def build_article_page_url(url: str, page_no: int) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    keep = {}
    for k in ("bsn", "snA"):
        if k in q and q[k]:
            keep[k] = q[k][0]

    keep["page"] = str(page_no)

    new_query = urlencode(keep, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

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

PAGE_RE = re.compile(r"[?&]page=(\d+)")

def parse_max_page(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    pager = soup.select_one("p.BH-pagebtnA")
    if not pager:
        return 1

    pages = [1]
    for a in pager.select("a[href*='page=']"):
        href = a.get("href", "")
        m = PAGE_RE.search(href)
        if m:
            pages.append(int(m.group(1)))

    return max(pages)

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
    final_text = " ".join(lines).strip()
    return final_text if final_text else None

def parse_Great_Bad_point(html: str) -> tuple[int, int] | None:
    soup = BeautifulSoup(html, "html.parser")
    GP_el = soup.select_one("span.postgp > span")
    BP_el = soup.select_one("span.postbp > span")

    if not GP_el and not BP_el:
        return None
    def to_int(el) -> int:
        if not el:
            return 0

        text = el.get_text(strip=True)
        if text == "-" or text == "":
            return 0
        if text == "爆":
            return 999   
        if text.isdigit():
            return int(text)
        return 0

    return to_int(GP_el), to_int(BP_el)

        
    
def parse_post_time(html:str) -> str :
    soup = BeautifulSoup(html, "html.parser")
    edit_el = soup.select_one("a.edittime")
    if not edit_el :
        return None
    mtime = edit_el.get("data-mtime")
    if not mtime :
        return None
    return parse_dt(mtime)

def parse_sna(url: str) -> Optional[int]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    sna = query.get("snA")
    if not sna:
        return None
    try:
        return int(sna[0])
    except ValueError:
        return None