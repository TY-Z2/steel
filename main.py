import sys
import os
import json
import time
import signal
import argparse
from pathlib import Path
from datetime import datetime

# 兼容两种导入方式：包内/同目录
try:
    from data_collection import fetch_dois, download_papers
except Exception:
    import fetch_dois, download_papers

try:
    from data_extraction import extract_steel_data
except Exception:
    import extract_steel_data

def signal_handler(sig, frame):
    """处理中断信号"""
    print("\n程序被用户中断，正在退出...")
    sys.exit(0)

def newest_mtime_in_dir(directory, ext=".pdf"):
    newest = None
    if not os.path.isdir(directory):
        return None
    for name in os.listdir(directory):
        if name.lower().endswith(ext):
            p = os.path.join(directory, name)
            m = os.path.getmtime(p)
            if newest is None or m > newest:
                newest = m
    return newest

def main():
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(description='钢铁材料数据爬取系统（增量 & 自动提取）')
    parser.add_argument('--start-year', type=int, default=2012, help='起始年份')
    parser.add_argument('--end-year', type=int, default=datetime.now().year, help='结束年份')
    parser.add_argument('--per-source-limit', type=int, default=300, help='每个来源抓取上限')
    parser.add_argument('--force-download', action='store_true', help='强制重新下载论文')
    parser.add_argument('--force-extract', action='store_true', help='强制重新提取数据')
    parser.add_argument('--skip-fetch', action='store_true', help='跳过DOI收集')
    args = parser.parse_args()

    project_root = Path(__file__).parent
    sys.path.append(str(project_root))

    os.makedirs(project_root / "data/raw/papers", exist_ok=True)
    os.makedirs(project_root / "data/processed", exist_ok=True)

    dois_file = project_root / "data/raw/doi_list.json"

    # 步骤1: 获取DOI（默认每次运行都会增量更新，无需删除旧文件）
    print("=" * 60)
    print("DOI收集阶段（增量）")
    print("=" * 60)

    if not args.skip_fetch:
        try:
            new_dois = fetch_dois.fetch_all_dois(args.start_year, args.end_year, args.per_source_limit)
            fetch_dois.save_dois_incremental(new_dois, str(dois_file), snapshot=True)
        except Exception as e:
            print(f"DOI收集失败（将继续后续步骤）: {str(e)}")
    else:
        print("已按要求跳过 DOI 收集")

    # 步骤2: 下载文献（默认增量合并，不会覆盖旧的 downloaded_papers.json）
    print("\n" + "=" * 60)
    print("论文下载阶段（增量）")
    print("=" * 60)

    downloaded_file = project_root / "data/raw/downloaded_papers.json"
    try:
        downloaded = download_papers.batch_download(
            str(dois_file),
            str(project_root / "data/raw/papers")
        )
        print(f"累计已下载论文: {len(downloaded)} 篇")
    except Exception as e:
        print(f"论文下载失败（将继续后续步骤）: {str(e)}")

    # 步骤3: 提取钢铁数据（默认自动触发：首次或有新PDF或 --force-extract）
    print("\n" + "=" * 60)
    print("钢铁数据提取阶段（自动判断是否需要运行）")
    print("=" * 60)

    output_dir = project_root / "data/processed/steel_data"
    papers_dir = project_root / "data/raw/papers"
    json_out = output_dir / "steel_data.json"

    need_extract = args.force_extract
    newest_pdf_mtime = newest_mtime_in_dir(str(papers_dir), ".pdf")
    existing_out_mtime = os.path.getmtime(json_out) if json_out.exists() else None

    if not json_out.exists():
        need_extract = True
    elif newest_pdf_mtime and (existing_out_mtime is None or newest_pdf_mtime > existing_out_mtime):
        need_extract = True

    if need_extract:
        try:
            start_time = time.time()
            steel_dataset = extract_steel_data.process_papers_from_directory(str(papers_dir))
            extraction_time = time.time() - start_time

            print(f"成功从 {len(steel_dataset)} 篇文献中提取数据")
            print(f"数据提取耗时: {extraction_time:.2f}秒")

            json_path, excel_path = extract_steel_data.save_steel_data(
                steel_dataset,
                str(output_dir)
            )
            print(f"钢铁数据已保存至: {json_path}")
            print(f"Excel格式数据已保存至: {excel_path}")
        except Exception as e:
            print(f"钢铁数据提取失败: {str(e)}")
    else:
        print("数据提取已跳过（未检测到新增PDF且已有输出）。如需强制提取请使用 --force-extract")

    print("\n" + "=" * 60)
    print("钢铁材料数据爬取完成!")
    print("=" * 60)

if __name__ == "__main__":
    main()