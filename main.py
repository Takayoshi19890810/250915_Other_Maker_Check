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

def ym_tag():
    return jst_now().strftime("%Y-%m")

def monthly_excel_name():
    return f"yahoo_news_{jst_now().strftime('%Y-%m')}.xlsx"

DEFAULT_KEYWORD = "ホンダ"   # NEWS_KEYWORD 環境変数 or --keyword で上書き


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


# ===== Yahoo!ニュース検索 =====
def format_dt(dt: datetime) -> str:
    return dt.strftime("%Y/%m/%d %H:%M")

def get_yahoo_news(keyword: str) -> pd.DataFrame:
    """
    Yahoo!ニュース（検索）から タイトル/URL/投稿日/引用元 を取得
    1ページ分（必要ならページ送り処理を追加可能）
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

            # 投稿日正規化
            pub_date = "取得不可"
            if date_str:
                ds = re.sub(r'\([月火水木金土日]\)', '', date_str).strip()
                try:
                    dt = datetime.strptime(ds, "%Y/%m/%d %H:%M")
                    pub_date = format_dt(dt)
                except Exception:
                    pub_date = ds

            # 引用元クリーンアップ
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
                txt = el.get_text(" ", strip=True)

                # 日付（YYYY/MM/DD HH:MM または M/D H:MM）を削除
                txt = re.sub(r"\d{1,4}/\d{1,2}/\d{1,2}\s+\d{1,2}:\d{2}", "", txt)
                txt = re.sub(r"\d{1,2}/\d{1,2}\s+\d{1,2}:\d{2}", "", txt)

                # 丸括弧内を削除
                txt = re.sub(r"\([^)]+\)", "", txt)

                # 先頭の数字＋空白を削除（例: "2 Merkmal" → "Merkmal"）
                txt = re.sub(r"^\d+\s*", "", txt)

                txt = txt.strip()
                if txt and not txt.isdigit():
                    source = txt
                    break

            if title and url:
                rows.append({"タイトル": title, "URL": url, "投稿日": pub_date, "引用元": source or "Yahoo"})
        except Exception:
            continue

    return pd.DataFrame(rows, columns=["タイトル", "URL", "投稿日", "引用元"])


# ===== Releaseから既存Excelを取得 =====
def download_existing_from_release(repo: str, tag: str, asset_name: str, token: str) -> pd.DataFrame:
    """Release(tag)に存在すればExcelをDLしてDFで返す。無ければ空DF。"""
    if not (repo and tag and token):
        return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元"])

    base = "https://api.github.com"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}

    r = requests.get(f"{base}/repos/{repo}/releases/tags/{tag}", headers=headers)
    if r.status_code != 200:
        return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元"])
    rel = r.json()

    asset = next((a for a in rel.get("assets", []) if a.get("name") == asset_name), None)
    if not asset:
        return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元"])

    headers_dl = headers | {"Accept": "application/octet-stream"}
    dr = requests.get(asset["url"], headers=headers_dl)
    if dr.status_code != 200:
        return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元"])

    with io.BytesIO(dr.content) as bio:
        try:
            df = pd.read_excel(bio, sheet_name="news")
            return df[["タイトル", "URL", "投稿日", "引用元"]].copy()
        except Exception:
            return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元"])


# ===== メイン =====
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keyword", type=str, default=None, help="検索キーワード（未指定なら環境変数NEWS_KEYWORD、なければホンダ）")
    args = ap.parse_args()

    keyword = args.keyword or os.getenv("NEWS_KEYWORD") or DEFAULT_KEYWORD
    print(f"🔎 キーワード: {keyword}")

    # 1) 最新取得
    df_new = get_yahoo_news(keyword)

    # 2) 既存（同月Release資産）とマージ
    token = os.getenv("GITHUB_TOKEN", "")
    repo = os.getenv("GITHUB_REPOSITORY", "")
    tag = f"news-{ym_tag()}"
    asset = monthly_excel_name()

    df_old = download_existing_from_release(repo, tag, asset, token)
    df_all = pd.concat([df_old, df_new], ignore_index=True)

    if not df_all.empty:
        df_all = df_all.dropna(subset=["URL"]).drop_duplicates(subset=["URL"], keep="last")
        # 投稿日で降順（変換できない値は末尾）
        try:
            dt = pd.to_datetime(df_all["投稿日"], errors="coerce", format="%Y/%m/%d %H:%M")
            df_all = df_all.assign(_dt=dt).sort_values("_dt", ascending=False).drop(columns=["_dt"])
        except Exception:
            pass

    # 3) 保存（単一シート news）
    os.makedirs("output", exist_ok=True)
    out_path = os.path.join("output", monthly_excel_name())
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df_all.to_excel(w, index=False, sheet_name="news")

    print(f"✅ Excel出力: {out_path}（合計 {len(df_all)} 件、うち新規 {len(df_new)} 件）")


if __name__ == "__main__":
    main()
