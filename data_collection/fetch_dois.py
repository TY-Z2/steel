import requests
import json
import time
import random
import os
import re
from urllib.parse import quote_plus
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
    session = requests.Session()
    retry = Retry(
        total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
        status_forcelist=status_forcelist, allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True, raise_on_status=False,
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

def build_keywords():
    kw_en = ["bainitic steel","martensitic steel","quenched and tempered steel","austempering steel",
             "steel heat treatment","tempering behavior steel","quenching steel","isothermal transformation steel",
             "microstructure evolution steel","EBSD steel","steel composition","alloy design steel",
             "mechanical properties steel","impact toughness steel","yield strength steel","tensile strength steel",
             "hardness steel","fracture toughness steel","pipeline steel","structural steel","low alloy high strength steel"]
    kw_zh = ["钢 热处理","淬火 回火 钢","等温转变 钢","贝氏体 钢","马氏体 钢","微观组织 钢","组织演化 钢","EBSD 钢",
             "合金成分 钢","合金设计 钢","力学性能 钢","冲击韧性 钢","屈服强度 钢","抗拉强度 钢",
             "硬度 钢","断裂韧性 钢","管线钢","结构钢","低合金高强钢","高强水电钢"]
    kws = list(dict.fromkeys(kw_en + kw_zh))
    random.shuffle(kws)
    return kws

def fetch_crossref_dois(keywords, start_year, end_year, max_results=300, rows=20, per_window_limit=200):
    BASE_URL = "https://api.crossref.org/works"
    dois, session, seen = [], create_retry_session(), _load_seen()
    for keyword in keywords:
        for year in range(start_year, end_year + 1):
            window_key = f"{keyword}_{year}"
            cursor = _load_cursor("crossref", window_key)
            items_collected = 0; consecutive_fail = 0
            while items_collected < per_window_limit and cursor and len(dois) < max_results:
                params = {"query": keyword,
                          "filter": f"type:journal-article,from-pub-date:{year}-01-01,until-pub-date:{year}-12-31",
                          "rows": rows, "cursor": cursor, "mailto": API_EMAIL,
                          "select": "DOI,title,issued,publisher,container-title,URL"}
                try:
                    r = session.get(BASE_URL, params=params, timeout=30)
                    if r.status_code == 429:
                        retry_after = int(r.headers.get("Retry-After", "20")); time.sleep(max(20, retry_after)); continue
                    if r.status_code >= 500:
                        time.sleep(5); consecutive_fail += 1
                        if consecutive_fail >= 3: break
                        continue
                    if r.status_code >= 400: break
                    data = r.json(); consecutive_fail = 0
                    next_cursor = data["message"].get("next-cursor")
                    _save_cursor("crossref", window_key, next_cursor or "")
                    cursor = next_cursor
                    items = data["message"].get("items", []) or []
                    if not items: break
                    for it in items:
                        doi = _norm_doi(it.get("DOI",""))
                        if not doi or doi in seen: continue
                        rec = {"doi": doi,
                               "title": (it.get("title") or [""])[0] if isinstance(it.get("title"), list) else (it.get("title") or ""),
                               "year": (it.get("issued",{}).get("date-parts", [[None]])[0][0]),
                               "publisher": it.get("publisher",""),
                               "journal": (it.get("container-title") or [""])[0] if isinstance(it.get("container-title"), list) else (it.get("container-title") or ""),
                               "url": it.get("URL","")}
                        dois.append(rec); seen.add(doi); items_collected += 1
                        if items_collected >= per_window_limit or len(dois) >= max_results: break
                    time.sleep(random.uniform(0.5,1.2))
                except requests.exceptions.SSLError:
                    time.sleep(random.uniform(3,6)); consecutive_fail+=1
                    if consecutive_fail>=4: break
                except requests.exceptions.ConnectionError:
                    time.sleep(random.uniform(3,6)); consecutive_fail+=1
                    if consecutive_fail>=4: break
                except Exception:
                    break
            if len(dois) >= max_results: break
        if len(dois) >= max_results: break
    return dois

def fetch_openalex_dois(keywords, start_year, end_year, max_results=120):
    BASE_URL = "https://api.openalex.org/works"
    session, dois, seen = create_retry_session(), [], _load_seen()
    for keyword in keywords:
        cursor = _load_cursor("openalex", keyword); collected = 0
        while collected < max_results and cursor:
            params = {"search": f'"{keyword}"',
                      "filter": f"publication_year:{start_year}-{end_year},is_oa:true,type:journal-article",
                      "per-page": min(200, max_results-collected),
                      "cursor": cursor, "mailto": API_EMAIL}
            try:
                r = session.get(BASE_URL, params=params, timeout=30)
                if r.status_code == 403: time.sleep(60); continue
                if r.status_code >= 400: break
                data = r.json(); next_cursor = data.get("meta", {}).get("next_cursor")
                _save_cursor("openalex", keyword, next_cursor or ""); cursor = next_cursor
                for w in data.get("results", []) or []:
                    doi = _norm_doi(w.get("doi",""))
                    if not doi or doi in seen: continue
                    pdf_url = None
                    for loc in w.get("locations", []) or []:
                        if loc.get("pdf_url"): pdf_url = loc["pdf_url"]; break
                    rec = {"doi": doi, "title": w.get("title",""),
                           "year": str(w.get("publication_year","")),
                           "publisher": w.get("host_venue",{}).get("publisher",""),
                           "journal": w.get("host_venue",{}).get("display_name",""),
                           "oa_pdf_url": pdf_url, "url": w.get("doi",""), "source": "openalex"}
                    dois.append(rec); seen.add(doi); collected += 1
                    if collected >= max_results: break
                time.sleep(random.uniform(0.6,1.2))
            except Exception: break
    return dois

def fetch_doaj_dois(keywords, start_year, end_year, max_results=120):
    BASE_URL = "https://doaj.org/api/v2/search/articles/"
    session, dois, seen = create_retry_session(), [], _load_seen()
    for keyword in keywords:
        for year in range(start_year, end_year+1):
            page, page_size, collected = 1, 100, 0
            while collected < max_results:
                query = f'(title:"{keyword}" OR abstract:"{keyword}") AND year:{year}'
                url = BASE_URL + quote_plus(query)
                params = {"page": page, "pageSize": page_size}
                try:
                    r = session.get(url, params=params, timeout=30)
                    if r.status_code >= 400: break
                    data = r.json(); results = data.get("results", []) or []
                    if not results: break
                    for rec in results:
                        bib = rec.get("bibjson", {}) or {}
                        doi = ""
                        for ident in bib.get("identifier", []) or []:
                            if (ident.get("type","") or "").lower() == "doi":
                                doi = _norm_doi(ident.get("id","")); break
                        if not doi or doi in seen: continue
                        pdf_url = None
                        for lk in bib.get("link", []) or []:
                            if (lk.get("type","") or "").lower() in ("fulltext","pdf"):
                                pdf_url = lk.get("url"); break
                        item = {"doi": doi, "title": bib.get("title",""),
                                "year": str(bib.get("year","")),
                                "publisher": (bib.get("publisher","") or ""),
                                "journal": (bib.get("journal",{}) or {}).get("title",""),
                                "oa_pdf_url": pdf_url, "url": f"https://doi.org/{doi}", "source": "doaj"}
                        dois.append(item); seen.add(doi); collected += 1
                        if collected >= max_results: break
                    if len(results) < page_size or collected >= max_results: break
                    page += 1; time.sleep(random.uniform(0.6,1.0))
                except Exception: break
    return dois

def fetch_core_dois(keywords, start_year, end_year, max_results=120):
    api_key = API_KEYS.get("core")
    if not api_key: return []
    BASE_URL = "https://core.ac.uk:443/api-v2/articles/search/"
    session, dois, seen = create_retry_session(), [], _load_seen()
    for keyword in keywords:
        for year in range(start_year, end_year+1):
            page, page_size, collected = 1, 100, 0
            while collected < max_results:
                query = f'"{keyword}" AND year:{year}'
                url = f"{BASE_URL}{quote_plus(query)}"
                params = {"page": page, "pageSize": page_size, "apiKey": api_key}
                try:
                    r = session.get(url, params=params, timeout=30)
                    if r.status_code in (401,403): return []
                    if r.status_code >= 400: break
                    data = r.json(); items = data.get("data", []) or []
                    if not items: break
                    for it in items:
                        doi = _norm_doi(it.get("doi") or "")
                        if not doi or doi in seen: continue
                        pdf_url = it.get("downloadUrl") or it.get("oaiPdfUrl") or None
                        item = {"doi": doi, "title": it.get("title",""),
                                "year": str(it.get("year","")), "publisher": it.get("publisher",""),
                                "journal": "", "oa_pdf_url": pdf_url,
                                "url": f"https://doi.org/{doi}", "source": "core"}
                        dois.append(item); seen.add(doi); collected += 1
                        if collected >= max_results: break
                    if len(items) < page_size or collected >= max_results: break
                    page += 1; time.sleep(random.uniform(0.6,1.0))
                except Exception: break
    return dois

def fetch_all_dois(start_year=2012, end_year=2025, per_source_limit=300):
    keywords = build_keywords()
    print("从Crossref获取DOI（年窗口 + 游标）...")
    crossref_dois = fetch_crossref_dois(keywords, start_year, end_year, max_results=per_source_limit, rows=20, per_window_limit=200)
    print(f"Crossref: 新增 {len(crossref_dois)} 个")
    print("从OpenAlex获取DOI...")
    openalex_dois = fetch_openalex_dois(keywords, start_year, end_year, max_results=max(60, per_source_limit//2))
    print(f"OpenAlex: 新增 {len(openalex_dois)} 个")
    print("从DOAJ获取DOI（OA优先）...")
    doaj_dois = fetch_doaj_dois(keywords, start_year, end_year, max_results=max(60, per_source_limit//3))
    print(f"DOAJ: 新增 {len(doaj_dois)} 个")
    print("从CORE获取DOI（OA优先）...")
    core_dois = fetch_core_dois(keywords, start_year, end_year, max_results=max(60, per_source_limit//3))
    print(f"CORE: 新增 {len(core_dois)} 个")
    merged = {}
    for item in crossref_dois + openalex_dois + doaj_dois + core_dois:
        doi = item["doi"]
        if doi not in merged:
            merged[doi] = item
        else:
            if item.get("oa_pdf_url") and not merged[doi].get("oa_pdf_url"):
                merged[doi]["oa_pdf_url"] = item["oa_pdf_url"]
            for k in ("title","year","publisher","journal","url","source"):
                if not merged[doi].get(k) and item.get(k):
                    merged[doi][k] = item[k]
    return list(merged.values())

def save_dois_incremental(new_items, filename="data/raw/doi_list.json", snapshot=True):
    filename = Path(filename); filename.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_json(filename, [])
    by_doi = { _norm_doi(x.get("doi","")): x for x in existing if x.get("doi") }
    added = 0
    for item in new_items:
        doi = _norm_doi(item.get("doi",""));
        if not doi: continue
        if doi in by_doi:
            old = by_doi[doi]
            if item.get("oa_pdf_url"): old["oa_pdf_url"] = item["oa_pdf_url"]
            for k in ("title","year","publisher","journal","url","source"):
                if not old.get(k) and item.get(k): old[k] = item[k]
        else:
            by_doi[doi] = item; added += 1
    merged_list = list(by_doi.values())
    _save_json(filename, merged_list)
    print(f"写入主DOI文件: {filename}（新增 {added} 条，合计 {len(merged_list)} 条）")
    seen = set(_load_json(RAW_DIR/"seen_dois.json", []))
    for d in by_doi.keys():
        if d: seen.add(d)
    _save_seen(seen)
    if snapshot:
        from time import strftime
        snap_path = filename.parent / f"doi_list_{strftime('%Y%m%d_%H%M%S')}.json"
        _save_json(snap_path, new_items)
        print(f"保存本次抓取快照: {snap_path}")
    return added, len(merged_list)

if __name__ == "__main__":
    new_dois = fetch_all_dois()
    save_dois_incremental(new_dois)
