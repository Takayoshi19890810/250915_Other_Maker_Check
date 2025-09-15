# -*- coding: utf-8 -*-
import os
import re
import io
import time
import argparse
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


# ===== 共通ユーティリティ =====
def jst_now():
    return datetime.now(timezone(timedelta(hours=9)))

def jst_str(fmt="%Y/%m/%d %H:%M"):
    return jst_now().strftime(fmt)

DEFAULT_KEYWORD = "ホンダ"   # NEWS_KEYWORD 環境変数 or --keyword で上書き
RELEASE_TAG = "news-latest"
ASSET_NAME = "yahoo_news.xlsx"
SHEET_NAME = "news"


# ===== Chrome（headless） =====
def make_driver() -> webdriver.Chrome:
    opts = Options()
    chrome_path = os.getenv("CHROME_PATH")  # Actionsで注入
    if chrome_path:
        opts.binary_location = chrome_path
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1280,2000")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)


# ===== 引用元のクリーンアップ =====
DATE_RE = re.compile(r"(?:\d{4}/\d{1,2}/\d{1,2}|\d{1,2}/\d{1,2})\s*\d{1,2}[:：]\d{2}")

def clean_source_text(text: str) -> str:
    if not text:
        return ""
    t = text
    t = re.sub(r"[（(][^）)]+[）)]", "", t)      # （）内削除
    t = DATE_RE.sub("", t)                       # 日付+時刻削除
    t = re.sub(r"^\d+\s*", "", t)                # 先頭の番号＋空白
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t


# ===== Yahoo!ニュース検索 =====
def get_yahoo_news(keyword: str) -> pd.DataFrame:
    """
    Yahoo!ニュース（検索）から タイトル/URL/投稿日/引用元 を取得（1ページ）
    """
    driver = make_driver()
    url = (
        f"https://news.yahoo.co.jp/search?p={keyword}"
        f"&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    )
    driver.get(url)
    time.sleep(5)  # 初期描画待ち

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    items = soup.find_all("li", class_=re.compile("sc-1u4589e-0"))
    rows = []
    for li in items:
        try:
            title_tag = li.find("div", class_=re.compile("sc-3ls169-0"))
            link_tag = li.find("a", href=True)
            time_tag = li.find("time")

            title = title_tag.get_text(strip=True) if title_tag else ""
            url = link_tag["href"] if link_tag else ""
            date_str = time_tag.get_text(strip=True) if time_tag else ""

            # 投稿日：YYYY/MM/DD HH:MM に揃えられる場合は揃え、それ以外は原文
            pub_date = "取得不可"
            if date_str:
                ds = re.sub(r'\([月火水木金土日]\)', '', date_str).strip()
                try:
                    dt = datetime.strptime(ds, "%Y/%m/%d %H:%M")
                    pub_date = dt.strftime("%Y/%m/%d %H:%M")
                except Exception:
                    pub_date = ds

            # 引用元（媒体＋カテゴリ）を抽出してクリーン
            source = ""
            for sel in [
                "div.sc-n3vj8g-0.yoLqH div.sc-110wjhy-8.bsEjY span",
                "div.sc-n3vj8g-0.yoLqH",
                "span",
                "div"
            ]:
                el = li.select_one(sel)
                if not el:
                    continue
                raw = el.get_text(" ", strip=True)
                txt = clean_source_text(raw)
                if txt and not txt.isdigit():
                    source = txt
                    break

            if title and url:
                rows.append({
                    "タイトル": title,
                    "URL": url,
                    "投稿日": pub_date,
                    "引用元": source or "Yahoo",
                    "取得日時": jst_str(),            # いつ取得したか（追記運用のため）
                    "検索キーワード": keyword,        # 将来マルチキーワード時に役立つ
                })
        except Exception:
            continue

    return pd.DataFrame(rows, columns=["タイトル", "URL", "投稿日", "引用元", "取得日時", "検索キーワード"])


# ===== Releaseから既存Excelを取得 =====
def download_existing_from_release(repo: str, tag: str, asset_name: str, token: str) -> pd.DataFrame:
    """Release(tag)に存在すればExcelをDLしてDFで返す。無ければ空DF。"""
    if not (repo and tag and token):
        return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元", "取得日時", "検索キーワード"])

    base = "https://api.github.com"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}

    r = requests.get(f"{base}/repos/{repo}/releases/tags/{tag}", headers=headers)
    if r.status_code != 200:
        return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元", "取得日時", "検索キーワード"])
    rel = r.json()

    asset = next((a for a in rel.get("assets", []) if a.get("name") == asset_name), None)
    if not asset:
        return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元", "取得日時", "検索キーワード"])

    headers_dl = headers | {"Accept": "application/octet-stream"}
    dr = requests.get(asset["url"], headers_dl)
    if dr.status_code != 200:
        return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元", "取得日時", "検索キーワード"])

    with io.BytesIO(dr.content) as bio:
        try:
            df = pd.read_excel(bio, sheet_name=SHEET_NAME)
            return df[["タイトル", "URL", "投稿日", "引用元", "取得日時", "検索キーワード"]].copy()
        except Exception:
            return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元", "取得日時", "検索キーワード"])


# ===== 保存（オートフィルター/列幅/フリーズ対応） =====
def save_with_format(df: pd.DataFrame, path: str):
    from openpyxl.utils import get_column_letter
    from openpyxl.styles import Font, Alignment
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=SHEET_NAME)
        ws = w.book[SHEET_NAME]

        # オートフィルター（ヘッダーに並べ替え・フィルターのボタン）
        max_col = ws.max_column
        max_row = ws.max_row
        ws.auto_filter.ref = f"A1:{get_column_letter(max_col)}{max_row}"

        # ヘッダー太字＆中央寄せ
        header_font = Font(bold=True)
        for cell in ws[1]:
            cell.font = header_font
            cell.alignment = Alignment(vertical="center")

        # 列幅の軽調整
        widths = {
            "A": 50,  # タイトル
            "B": 60,  # URL
            "C": 16,  # 投稿日
            "D": 24,  # 引用元
            "E": 16,  # 取得日時
            "F": 16,  # 検索キーワード
        }
        for col, wdt in widths.items():
            if ws.max_column >= ord(col) - 64:
                ws.column_dimensions[col].width = wdt

        # ヘッダー固定（1行目）
        ws.freeze_panes = "A2"


# ===== メイン =====
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keyword", type=str, default=None, help="検索キーワード（未指定なら環境変数NEWS_KEYWORD、なければホンダ）")
    args = ap.parse_args()

    keyword = args.keyword or os.getenv("NEWS_KEYWORD") or DEFAULT_KEYWORD
    print(f"🔎 キーワード: {keyword}")

    # 1) 最新取得
    df_new = get_yahoo_news(keyword)

    # 2) 既存（固定Release資産）とマージ（既存優先＝新規は末尾に付く）
    token = os.getenv("GITHUB_TOKEN", "")
    repo = os.getenv("GITHUB_REPOSITORY", "")
    df_old = download_existing_from_release(repo, RELEASE_TAG, ASSET_NAME, token)

    df_all = pd.concat([df_old, df_new], ignore_index=True)
    if not df_all.empty:
        df_all = df_all.dropna(subset=["URL"]).drop_duplicates(subset=["URL"], keep="first")
        # 並べ替えはしない：既存の順序を保持し、新規は末尾に追記される

    # 3) 保存（単一シート news, オートフィルター付き）
    os.makedirs("output", exist_ok=True)
    out_path = os.path.join("output", ASSET_NAME)
    save_with_format(df_all, out_path)

    print(f"✅ Excel出力: {out_path}（合計 {len(df_all)} 件、うち新規 {len(df_new)} 件）")
    print(f"🔗 固定DL: https://github.com/<OWNER>/<REPO>/releases/download/{RELEASE_TAG}/{ASSET_NAME}")


if __name__ == "__main__":
    main()
