import os
import requests
import re
import json
import time
import random
from config.api_keys import API_KEYS, API_EMAIL
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

# 出版商PDF下载策略（保持原有并增强）
PDF_STRATEGIES = {
    "elsevier": {
        "url": "https://api.elsevier.com/content/article/doi/{doi}",
        "headers": {
            "X-ELS-APIKey": API_KEYS.get("elsevier"),
            "Accept": "application/pdf",
            "User-Agent": "AcademicResearchBot/1.0"
        },
        "params": {"view": "FULL"}
    },
    "springer": {
        "url": "https://link.springer.com/content/pdf/{doi}.pdf",
        "headers": {"User-Agent": "AcademicResearchBot/1.0"}
    },
    "mdpi": {
        "url": "https://www.mdpi.com/{doi}/pdf",
        "headers": {"User-Agent": "AcademicResearchBot/1.0"}
    },
    "wiley": {
        "url": "https://onlinelibrary.wiley.com/doi/pdfdirect/{doi}",
        "headers": {"User-Agent": "AcademicResearchBot/1.0"}
    },
    "tandfonline": {
        "url": "https://www.tandfonline.com/doi/pdf/{doi}",
        "headers": {"User-Agent": "AcademicResearchBot/1.0"}
    },
    "sciencedirect": {
        "url": "https://www.sciencedirect.com/science/article/pii/{pii}/pdfft",
        "headers": {"User-Agent": "AcademicResearchBot/1.0"}
    },
    "ieee": {
        "url": "https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber={arnumber}",
        "headers": {"User-Agent": "AcademicResearchBot/1.0"}
    },
    "openalex": {
        "url": "{pdf_url}",
        "headers": {"User-Agent": "AcademicResearchBot/1.0"}
    }
}

def create_retry_session(retries=5, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504)):
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
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({"User-Agent": "SteelOA/1.0 (+mailto:%s)" % API_EMAIL})
    return session

def get_publisher_key(publisher):
    """将出版商名称映射到策略键"""
    publisher = (publisher or "").lower()
    if "elsevier" in publisher or "sciencedirect" in publisher:
        return "elsevier"
    elif "springer" in publisher or "nature" in publisher:
        return "springer"
    elif "mdpi" in publisher:
        return "mdpi"
    elif "wiley" in publisher:
        return "wiley"
    elif "taylor" in publisher or "francis" in publisher:
        return "tandfonline"
    elif "ieee" in publisher:
        return "ieee"
    return None

def _norm_doi(doi: str) -> str:
    if not doi:
        return ""
    doi = doi.strip()
    doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
    return doi

def _save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def _ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def try_unpaywall(session, doi):
    """通过 Unpaywall 获取合法 OA PDF 链接"""
    base = f"https://api.unpaywall.org/v2/{doi}"
    params = {"email": API_EMAIL}
    try:
        r = session.get(base, params=params, timeout=20)
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            return None
        data = r.json()

        # 首选 best_oa_location.url_for_pdf
        best = data.get("best_oa_location") or {}
        if best.get("url_for_pdf"):
            return best["url_for_pdf"]

        # 其次看 oa_locations 中的可下载 PDF
        for loc in (data.get("oa_locations") or []):
            if loc.get("url_for_pdf"):
                return loc["url_for_pdf"]
    except Exception:
        return None
    return None

def stream_to_file(resp, filepath):
    with open(filepath, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    # 简单校验
    return os.path.getsize(filepath) > 2048

def download_pdf(session, doi_info, output_dir):
    """尝试下载PDF文件（OpenAlex OA → Unpaywall → 出版商策略 → 通用解析）"""
    doi = _norm_doi(doi_info["doi"])

    # 0. OpenAlex提供的OA PDF链接
    if doi_info.get("oa_pdf_url"):
        try:
            url = doi_info["oa_pdf_url"]
            resp = session.get(url, timeout=25, stream=True)
            if resp.status_code == 200 and "pdf" in resp.headers.get("Content-Type","").lower():
                filepath = os.path.join(output_dir, f"{doi.replace('/', '_')}.pdf")
                if stream_to_file(resp, filepath):
                    return filepath
        except Exception:
            pass

    # 1. Unpaywall OA
    try:
        pdf_url = try_unpaywall(session, doi)
        if pdf_url:
            resp = session.get(pdf_url, timeout=25, stream=True, allow_redirects=True)
            if resp.status_code == 200 and "pdf" in resp.headers.get("Content-Type","").lower():
                filepath = os.path.join(output_dir, f"{doi.replace('/', '_')}.pdf")
                if stream_to_file(resp, filepath):
                    return filepath
    except Exception:
        pass

    # 2. 出版商特定策略
    publisher_key = get_publisher_key(doi_info.get("publisher", ""))
    if publisher_key and publisher_key in PDF_STRATEGIES:
        strategy = PDF_STRATEGIES[publisher_key]
        url = strategy["url"]

        # 处理需要特殊参数的URL
        if publisher_key == "sciencedirect":
            if re.search(r'/science/article/pii/(\w+)', doi_info.get("url", "")):
                pii = re.search(r'/science/article/pii/(\w+)', doi_info.get("url", "")).group(1)
                url = url.format(pii=pii)
            else:
                url = None
        elif publisher_key == "ieee":
            if re.search(r'arnumber=(\d+)', doi_info.get("url", "")):
                arnumber = re.search(r'arnumber=(\d+)', doi_info.get("url", "")).group(1)
                url = url.format(arnumber=arnumber)
            else:
                url = None
        else:
            url = url.format(doi=doi)

        if url:
            try:
                resp = session.get(url, headers=strategy.get("headers", {}),
                                   params=strategy.get("params", {}),
                                   timeout=25, stream=True)
                if resp.status_code == 200 and "pdf" in resp.headers.get("Content-Type","").lower():
                    filepath = os.path.join(output_dir, f"{doi.replace('/', '_')}.pdf")
                    if stream_to_file(resp, filepath):
                        return filepath
            except Exception:
                pass

    # 3. 通用DOI解析
    try:
        url = f"https://doi.org/{doi}"
        resp = session.get(url, allow_redirects=True, timeout=20)
        final_url = resp.url

        # 直接PDF响应
        if "application/pdf" in resp.headers.get("Content-Type","").lower():
            filepath = os.path.join(output_dir, f"{doi.replace('/', '_')}.pdf")
            with open(filepath, "wb") as f:
                f.write(resp.content)
            if os.path.getsize(filepath) > 2048:
                return filepath

        # 从HTML寻找PDF
        if "text/html" in resp.headers.get("Content-Type","").lower():
            pdf_patterns = [
                r'href="([^"]+\.pdf)"',
                r'"(https?://[^"]+download=true[^"]*)"',
                r'"(https?://[^"]+/pdf[^"]*)"',
                r'"(https?://[^"]+/full[^"]*\.pdf)"'
            ]
            html = resp.text
            for pattern in pdf_patterns:
                m = re.search(pattern, html, re.IGNORECASE)
                if not m:
                    continue
                pdf_url = m.group(1)
                if not pdf_url.startswith("http"):
                    base_url = "/".join(final_url.split("/")[:3])
                    pdf_url = base_url + pdf_url
                pr = session.get(pdf_url, timeout=25, stream=True)
                if pr.status_code == 200 and "pdf" in pr.headers.get("Content-Type","").lower():
                    filepath = os.path.join(output_dir, f"{doi.replace('/', '_')}.pdf")
                    if stream_to_file(pr, filepath):
                        return filepath
    except Exception:
        pass

    return None

def download_paper(doi_info, output_dir="data/raw/papers"):
    """下载单篇论文"""
    doi = _norm_doi(doi_info["doi"])
    _ensure_dir(output_dir)
    session = create_retry_session()

    # 检查是否已下载
    filename = f"{doi.replace('/', '_')}.pdf"
    filepath = os.path.join(output_dir, filename)
    if os.path.exists(filepath):
        print(f"文件已存在: {filename}")
        return filepath

    # 尝试下载PDF
    pdf_path = download_pdf(session, doi_info, output_dir)
    if pdf_path:
        return pdf_path

    print(f"所有下载策略均失败: {doi}")
    return None

def batch_download(doi_list_file="data/raw/doi_list.json", output_dir="data/raw/papers"):
    """批量下载论文（增量，保留日志和失败列表）"""
    if not os.path.exists(doi_list_file):
        print(f"未找到DOI列表文件: {doi_list_file}")
        return []

    with open(doi_list_file, encoding="utf-8") as f:
        doi_list = json.load(f)

    downloaded = []
    failed = []
    download_log = []

    print(f"开始下载 {len(doi_list)} 篇论文...")

    # 创建下载日志目录
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    for idx, doi_info in enumerate(doi_list):
        doi = _norm_doi(doi_info["doi"])
        print(f"[{idx + 1}/{len(doi_list)}] 下载: {doi}")

        start_time = time.time()
        filepath = download_paper(doi_info, output_dir)
        elapsed = time.time() - start_time

        log_entry = {
            "doi": doi,
            "timestamp": datetime.now().isoformat(),
            "duration": round(elapsed, 2),
            "success": filepath is not None
        }

        if filepath:
            doi_info["filepath"] = filepath
            downloaded.append(doi_info)
            print(f"  ✓ 成功: {os.path.basename(filepath)} ({elapsed:.2f}秒)")
            log_entry["filepath"] = filepath
        else:
            failed.append(doi)
            print(f"  ✗ 下载失败")

        download_log.append(log_entry)

        # 保存进度日志
        with open(log_file, "w", encoding="utf-8") as f:
            json.dump(download_log, f, indent=2, ensure_ascii=False)

        # 随机延迟避免触发反爬
        time.sleep(random.uniform(2.0, 5.0))

    # 读取旧的下载记录并合并
    os.makedirs("data/raw", exist_ok=True)
    downloaded_file = "data/raw/downloaded_papers.json"
    if os.path.exists(downloaded_file):
        try:
            with open(downloaded_file, "r", encoding="utf-8") as f:
                old = json.load(f)
        except Exception:
            old = []
    else:
        old = []
    # 合并去重（按 DOI）
    by_doi = { (_norm_doi(x.get("doi",""))): x for x in old if x.get("doi") }
    for item in downloaded:
        by_doi[_norm_doi(item.get("doi",""))] = item

    with open(downloaded_file, "w", encoding="utf-8") as f:
        json.dump(list(by_doi.values()), f, indent=2, ensure_ascii=False)

    print(f"\n下载总结:")
    print(f"  本次成功: {len(downloaded)}")
    print(f"  本次失败: {len(failed)}")
    print(f"  累计成功: {len(by_doi)}")

    if failed:
        fail_file = "data/raw/failed_dois.json"
        with open(fail_file, "w", encoding="utf-8") as f:
            json.dump(sorted(list(set(failed))), f, indent=2, ensure_ascii=False)
        print(f"\n失败的DOI保存至 {fail_file}")

    return list(by_doi.values())

if __name__ == "__main__":
    downloaded = batch_download()
