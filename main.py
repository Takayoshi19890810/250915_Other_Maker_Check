# -*- coding: utf-8 -*-
import os
import re
import io
import json
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


# =========================
# 設定
# =========================
def jst_now():
    return datetime.now(timezone(timedelta(hours=9)))

def ym_tag():
    return jst_now().strftime("%Y-%m")

def monthly_excel_name():
    return f"yahoo_news_{jst_now().strftime('%Y-%m')}.xlsx"

DEFAULT_KEYWORD = "ホンダ"   # 環境変数 NEWS_KEYWORD / 引数 --keyword で上書き可


# =========================
# ヘッドレスChrome用Driver
# =========================
def make_driver() -> webdriver.Chrome:
    options = Options()
    # GitHub Actions用にheadless(new) + Chromeパス（setup-chromeで注入）に対応
    chrome_path = os.getenv("CHROME_PATH")
    if chrome_path:
        options.binary_location = chrome_path
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1280,2000")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


# =========================
# Yahooニュース スクレイパ（添付版をベースに調整）
# =========================
def format_datetime(dt_obj: datetime) -> str:
    return dt_obj.strftime("%Y/%m/%d %H:%M")

def get_yahoo_news(keyword: str) -> pd.DataFrame:
    """
    Yahoo!ニュース検索結果を取得して DataFrame で返す
    カラム: タイトル, URL, 投稿日, 引用元
    """
    driver = make_driver()
    search_url = (
        f"https://news.yahoo.co.jp/search?p={keyword}"
        f"&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    )
    driver.get(search_url)
    time.sleep(5)  # 初期描画待ち（添付プログラムに準拠）:contentReference[oaicite:5]{index=5}

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    # li.sc-1u4589e-0 系のコンテナから各項目を抽出（添付プログラムでの選択ロジックを踏襲）:contentReference[oaicite:6]{index=6}
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

            # 投稿日の正規化（"YYYY/MM/DD HH:MM" を優先。失敗時は原文残し）
            pub_date = "取得不可"
            if date_str:
                ds = re.sub(r'\([月火水木金土日]\)', '', date_str).strip()
                try:
                    dt = datetime.strptime(ds, "%Y/%m/%d %H:%M")
                    pub_date = format_datetime(dt)
                except Exception:
                    pub_date = ds

            # 媒体名（周辺テキストから推定）
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
                # 投稿日の文字列などを除去（添付プログラムに準拠）:contentReference[oaicite:7]{index=7}
                txt = re.sub(r"\d{4}/\d{1,2}/\d{1,2} \d{2}:\d{2}", "", txt)
                txt = re.sub(r"\([^)]+\)", "", txt)
                txt = txt.strip()
                if txt and not txt.isdigit():
                    source = txt
                    break

            if title and url:
                rows.append({"タイトル": title, "URL": url, "投稿日": pub_date, "引用元": source or "Yahoo"})
        except Exception:
            continue

    df = pd.DataFrame(rows, columns=["タイトル", "URL", "投稿日", "引用元"])
    return df


# =========================
# Release(同月タグ)の既存Excelを取得して結合
# =========================
def try_download_existing_from_release(repo: str, tag: str, asset_name: str, token: str) -> pd.DataFrame:
    """
    既存の月次Excel（asset_name）を Release(tag) から取得してDFで返す。
    見つからない場合は空DF。
    """
    if not token or not repo or not tag:
        return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元"])

    base = "https://api.github.com"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}

    # tagからreleaseを取得
    r = requests.get(f"{base}/repos/{repo}/releases/tags/{tag}", headers=headers)
    if r.status_code != 200:
        return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元"])
    rel = r.json()

    # アセット探索
    target = None
    for a in rel.get("assets", []):
        if a.get("name") == asset_name:
            target = a
            break

    if not target:
        return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元"])

    # ダウンロード（アセットURLはapplication/octet-streamでリダイレクト）
    asset_url = target.get("url")
    headers_dl = headers | {"Accept": "application/octet-stream"}
    dr = requests.get(asset_url, headers=headers_dl)
    if dr.status_code != 200:
        return pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元"])

    # バイト→Excel読み込み
    with io.BytesIO(dr.content) as bio:
        try:
            df = pd.read_excel(bio, sheet_name="news")
            # 型/列名の安全化
            df = df[["タイトル", "URL", "投稿日", "引用元"]].copy()
        except Exception:
            df = pd.DataFrame(columns=["タイトル", "URL", "投稿日", "引用元"])

    return df


# =========================
# メイン：スクレイプ→結合→重複排除→保存
# =========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword", type=str, default=None)
    args = parser.parse_args()

    keyword = args.keyword or os.getenv("NEWS_KEYWORD") or DEFAULT_KEYWORD
    print(f"🔎 キーワード: {keyword}")

    # 1) Yahooニュース取得
    df_new = get_yahoo_news(keyword)

    # 2) 既存の月次Excel（Release）を取得して結合
    token = os.getenv("GITHUB_TOKEN", "")
    repo = os.getenv("GITHUB_REPOSITORY", "")  # 例: owner/repo (Actions内で自動注入)
    tag = f"news-{ym_tag()}"
    asset = monthly_excel_name()

    df_old = try_download_existing_from_release(repo, tag, asset, token)

    # 3) 結合 & URLで重複排除（新しい方を優先）
    df_all = pd.concat([df_old, df_new], ignore_index=True)
    if not df_all.empty:
        # 投稿日が同じでもURLがキー。URL欠損は落とす
        df_all = df_all.dropna(subset=["URL"]).drop_duplicates(subset=["URL"], keep="last")
        # 並び（新しい方が上）: 投稿日を文字列から並べ替え。失敗時はそのまま。
        try:
            dt = pd.to_datetime(df_all["投稿日"], errors="coerce", format="%Y/%m/%d %H:%M")
            df_all = df_all.assign(_dt=dt).sort_values("_dt", ascending=False).drop(columns=["_dt"])
        except Exception:
            pass

    # 4) 保存（単一シート news）
    os.makedirs("output", exist_ok=True)
    out_path = os.path.join("output", monthly_excel_name())
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df_all.to_excel(w, index=False, sheet_name="news")

    print(f"✅ Excel出力: {out_path}（{len(df_all)}件、うち新規 {len(df_new)}件）")


if __name__ == "__main__":
    main()
