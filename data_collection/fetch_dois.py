import requests
import json
import time
import random
import os
import re
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config.api_keys import API_KEYS, API_EMAIL

STATE_DIR = Path("data/raw/cursors")
RAW_DIR = Path("data/raw")

def _norm_doi(doi: str) -> str:
    if not doi:
        return ""
    doi = doi.strip()
    doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    return doi.lower()

def _sanitize(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", name)[:80]

def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def create_retry_session(retries=8, backoff_factor=1.5,
                         status_forcelist=(429, 500, 502, 503, 504)):
    """创建带重试机制的会话"""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({"User-Agent": "SteelOA/1.0 (+mailto:%s)" % API_EMAIL})
    return session

def _load_cursor(source: str, key: str) -> str:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    p = STATE_DIR / f"{source}_{_sanitize(key)}.txt"
    if p.exists():
        return p.read_text(encoding="utf-8").strip() or "*"
    return "*"

def _save_cursor(source: str, key: str, cursor: str):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    p = STATE_DIR / f"{source}_{_sanitize(key)}.txt"
    p.write_text(cursor or "", encoding="utf-8")

def _load_seen():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    return set(_load_json(RAW_DIR/"seen_dois.json", []))

def _save_seen(seen_set):
    _save_json(RAW_DIR/"seen_dois.json", sorted(list(seen_set)))

def fetch_crossref_dois(keywords, start_year, end_year, max_results=300):
    """从Crossref获取DOI信息（支持游标持久化，跨次继续）"""
    BASE_URL = "https://api.crossref.org/works"
    dois = []
    session = create_retry_session()
    api_counter = 0
    seen = _load_seen()

    for keyword in keywords:
        cursor = _load_cursor("crossref", keyword)
        items_collected = 0

        while items_collected < max_results and cursor:
            params = {
                "query": keyword,
                "filter": f"from-pub-date:{start_year},until-pub-date:{end_year}",
                "rows": 50,
                "cursor": cursor,
                "mailto": API_EMAIL,
                "select": "DOI,title,created,publisher,container-title,URL"
            }

            try:
                response = session.get(BASE_URL, params=params, timeout=30)
                api_counter += 1
                if response.status_code >= 400:
                    # 如果速率受限，等待后继续
                    if response.status_code == 429:
                        time.sleep(60)
                        continue
                    # 其他错误，跳出本关键字循环
                    break

                data = response.json()

                # 下一页游标
                next_cursor = data["message"].get("next-cursor")
                if next_cursor:
                    _save_cursor("crossref", keyword, next_cursor)
                    cursor = next_cursor
                else:
                    cursor = None

                for item in data["message"].get("items", []):
                    doi_raw = item.get("DOI", "")
                    doi = _norm_doi(doi_raw)
                    if not doi or doi in seen:
                        continue
                    record = {
                        "doi": doi,
                        "title": (item.get("title") or [""])[0] if isinstance(item.get("title"), list) else (item.get("title") or ""),
                        "year": item.get("created",{}).get("date-parts", [[None]])[0][0],
                        "publisher": item.get("publisher", ""),
                        "journal": (item.get("container-title") or [""])[0] if isinstance(item.get("container-title"), list) else (item.get("container-title") or ""),
                        "url": item.get("URL","")
                    }
                    dois.append(record)
                    seen.add(doi)
                    items_collected += 1
                    if items_collected >= max_results:
                        break

                time.sleep(random.uniform(1.0, 2.0))

            except Exception:
                # 不要卡死在某个关键字
                break

    return dois

def fetch_openalex_dois(keywords, start_year, end_year, max_results=100):
    """从OpenAlex获取DOI信息（持久化游标 + 仅返回未见过的）"""
    BASE_URL = "https://api.openalex.org/works"
    session = create_retry_session()
    dois = []
    seen = _load_seen()

    for keyword in keywords:
        cursor = _load_cursor("openalex", keyword)
        collected = 0

        while collected < max_results and cursor:
            params = {
                "search": f'"{keyword}"',
                "filter": f"publication_year:{start_year}-{end_year},is_oa:true,type:journal-article",
                "per-page": min(200, max_results - collected),
                "cursor": cursor,
                "mailto": API_EMAIL
            }

            try:
                response = session.get(BASE_URL, params=params, timeout=30)
                if response.status_code == 403:
                    time.sleep(60)
                    continue
                if response.status_code >= 400:
                    break

                data = response.json()
                next_cursor = data.get("meta", {}).get("next_cursor")
                if next_cursor:
                    _save_cursor("openalex", keyword, next_cursor)
                    cursor = next_cursor
                else:
                    cursor = None

                for work in data.get("results", []):
                    doi_raw = work.get("doi", "")
                    doi = _norm_doi(doi_raw)
                    if not doi or doi in seen:
                        continue

                    # 抓取OA PDF
                    pdf_url = None
                    for loc in work.get("locations", []):
                        if loc.get("pdf_url"):
                            pdf_url = loc["pdf_url"]
                            break

                    record = {
                        "doi": doi,
                        "title": work.get("title", ""),
                        "year": str(work.get("publication_year", "")),
                        "publisher": work.get("host_venue", {}).get("publisher", ""),
                        "journal": work.get("host_venue", {}).get("display_name", ""),
                        "oa_pdf_url": pdf_url,
                        "url": doi_raw
                    }
                    dois.append(record)
                    seen.add(doi)
                    collected += 1
                    if collected >= max_results:
                        break

                time.sleep(random.uniform(0.8, 1.5))

            except Exception:
                break

    return dois

def fetch_all_dois(start_year=2012, end_year=2025, per_source_limit=300):
    """从多来源获取新的DOI；自动跳过 seen_dois.json 中已有的"""
    keywords = [
        "bainitic steel",
        "steel heat treatment",
        "mechanical properties steel",
        "isothermal treatment steel",
        "steel composition",
        "steel microstructure",
        "steel mechanical properties",
        "steel alloy design"
    ]

    print("从Crossref获取DOI...")
    crossref_dois = fetch_crossref_dois(keywords, start_year, end_year, per_source_limit)
    print(f"Crossref: 新增 {len(crossref_dois)} 个")

    print("从OpenAlex获取DOI...")
    openalex_dois = fetch_openalex_dois(keywords, start_year, end_year, max_results=max(50, per_source_limit//3))
    print(f"OpenAlex: 新增 {len(openalex_dois)} 个")

    # TODO: 可扩展WoS/Scopus等（需合法API密钥）

    # 合并（OpenAlex优先提供oa_pdf_url）
    merged = {}
    for item in crossref_dois + openalex_dois:
        doi = item["doi"]
        if doi not in merged or ("oa_pdf_url" in item and item["oa_pdf_url"]):
            merged[doi] = item

    return list(merged.values())

def save_dois_incremental(new_items, filename="data/raw/doi_list.json", snapshot=True):
    """将新的DOI增量写入主文件；可额外生成快照文件"""
    filename = Path(filename)
    filename.parent.mkdir(parents=True, exist_ok=True)

    # 读取旧文件并合并去重
    existing = _load_json(filename, [])
    by_doi = { _norm_doi(x.get("doi","")): x for x in existing if x.get("doi") }

    added = 0
    for item in new_items:
        doi = _norm_doi(item.get("doi",""))
        if not doi:
            continue
        if doi in by_doi:
            # 合并信息（保留更丰富字段）
            old = by_doi[doi]
            if item.get("oa_pdf_url"):
                old["oa_pdf_url"] = item["oa_pdf_url"]
            for k in ("title","year","publisher","journal","url"):
                if not old.get(k) and item.get(k):
                    old[k] = item[k]
        else:
            by_doi[doi] = item
            added += 1

    merged_list = list(by_doi.values())
    _save_json(filename, merged_list)
    print(f"写入主DOI文件: {filename}（新增 {added} 条，合计 {len(merged_list)} 条）")

    # 更新 seen_dois.json
    seen = set(_load_json(RAW_DIR/"seen_dois.json", []))
    for d in by_doi.keys():
        if d:
            seen.add(d)
    _save_seen(seen)

    # 生成快照
    if snapshot:
        snap_path = filename.parent / f"doi_list_{time.strftime('%Y%m%d_%H%M%S')}.json"
        _save_json(snap_path, new_items)
        print(f"保存本次抓取快照: {snap_path}")

    return added, len(merged_list)

if __name__ == "__main__":
    new_dois = fetch_all_dois()
    save_dois_incremental(new_dois)