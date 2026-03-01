"""
CFTC COTレポート 週次LINE通知スクリプト
データソース: https://www.cftc.gov/dea/newcot/FinFutWk.txt
毎週金曜 21:30 UTC（土曜 06:30 JST）にGitHub Actionsで実行される

対象: Leveraged Funds（ヘッジファンド・CTA等の投機筋）ポジション
"""

import os
import io
import csv
import requests
from datetime import datetime
import pytz

# ── 設定 ──────────────────────────────────────────────
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_USER_ID = os.environ["LINE_USER_ID"]
JST = pytz.timezone("Asia/Tokyo")

# ── CFTCデータのカラムインデックス（0始まり）──────────
COL_MARKET = 0          # 市場名
COL_DATE = 2            # 集計基準日（YYYY-MM-DD）
COL_LEV_LONG = 14       # Leveraged Funds ロング枚数
COL_LEV_SHORT = 15      # Leveraged Funds ショート枚数
COL_CHG_LEV_LONG = 32   # 前週比ロング変化
COL_CHG_LEV_SHORT = 33  # 前週比ショート変化

# ── 対象市場（CFTCの市場名の先頭部分で部分一致） ─────
TARGET_MARKETS = {
    "JAPANESE YEN":      {"currency": "JPY", "flag": "🇯🇵"},
    "EURO FX -":         {"currency": "EUR", "flag": "🇪🇺"},
    "BRITISH POUND":     {"currency": "GBP", "flag": "🇬🇧"},
    "AUSTRALIAN DOLLAR": {"currency": "AUD", "flag": "🇦🇺"},
    "CANADIAN DOLLAR":   {"currency": "CAD", "flag": "🇨🇦"},
    "NZ DOLLAR":         {"currency": "NZD", "flag": "🇳🇿"},
    "SWISS FRANC":       {"currency": "CHF", "flag": "🇨🇭"},
}


def fetch_cot_data() -> list:
    """CFTCからFinancial Futures週次データを取得"""
    url = "https://www.cftc.gov/dea/newcot/FinFutWk.txt"
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        return list(csv.reader(io.StringIO(resp.text)))
    except Exception as e:
        print(f"[ERR] COTデータ取得失敗: {e}")
        return []


def parse_cot(rows: list) -> list:
    """対象通貨のLeveraged Fundsポジションを抽出"""
    results = []
    for row in rows:
        if len(row) < 34:
            continue

        market_name = row[COL_MARKET].strip().strip('"').upper()

        # 対象市場を検索（部分一致）
        matched = None
        for key, info in TARGET_MARKETS.items():
            if market_name.startswith(key):
                matched = info
                break
        if not matched:
            continue

        try:
            date_str = row[COL_DATE].strip()
            lev_long = int(row[COL_LEV_LONG].strip())
            lev_short = int(row[COL_LEV_SHORT].strip())
            chg_long = int(row[COL_CHG_LEV_LONG].strip())
            chg_short = int(row[COL_CHG_LEV_SHORT].strip())

            net = lev_long - lev_short
            net_change = chg_long - chg_short

            results.append({
                "currency": matched["currency"],
                "flag": matched["flag"],
                "date": date_str,
                "long": lev_long,
                "short": lev_short,
                "net": net,
                "net_change": net_change,
            })
        except (ValueError, IndexError) as e:
            print(f"[WARN] パースエラー ({market_name}): {e}")

    # 通貨の表示順を固定
    order = ["JPY", "EUR", "GBP", "AUD", "CAD", "NZD", "CHF"]
    results.sort(key=lambda x: order.index(x["currency"]) if x["currency"] in order else 99)
    return results


def build_message(data: list) -> str:
    """LINEメッセージを作成"""
    if not data:
        return "[ERR] COTデータが取得できませんでした"

    date_str = data[0]["date"]
    now_jst = datetime.now(JST).strftime("%Y/%m/%d")

    lines = [
        f"📊 COTレポート（{now_jst}）",
        f"集計基準日：{date_str}",
        f"対象：投機筋（Leveraged Funds）",
        "━━━━━━━━━━━━━━",
    ]

    for item in data:
        net = item["net"]
        chg = item["net_change"]

        # ポジション方向
        direction = "買い越し" if net > 0 else "売り越し"

        # 前週比の矢印
        if chg > 0:
            arrow = "△"
            chg_label = "買い方向へ"
        elif chg < 0:
            arrow = "▼"
            chg_label = "売り方向へ"
        else:
            arrow = "→"
            chg_label = "変化なし"

        lines += [
            f"{item['flag']} {item['currency']}  {direction}",
            f"  ネット: {net:+,}枚",
            f"  前週比: {arrow}{abs(chg):,}枚（{chg_label}）",
            "",
        ]

    lines.append("━━━━━━━━━━━━━━")
    return "\n".join(lines).strip()


def send_line_message(message: str):
    """LINE Messaging API でプッシュ通知を送信"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
    }
    body = {
        "to": LINE_USER_ID,
        "messages": [{"type": "text", "text": message}],
    }
    resp = requests.post(
        "https://api.line.me/v2/bot/message/push",
        headers=headers,
        json=body,
        timeout=10,
    )
    if resp.status_code == 200:
        print("[OK] LINE通知送信成功")
    else:
        print(f"[ERR] LINE送信エラー: {resp.status_code} {resp.text}")


def main():
    print("COTレポート取得開始")
    rows = fetch_cot_data()
    print(f"取得行数: {len(rows)}")

    data = parse_cot(rows)
    print(f"対象通貨数: {len(data)}")

    if not data:
        print("[WARN] 対象データなし")
        return

    message = build_message(data)
    print("--- 送信メッセージ ---")
    print(message)
    print("-------------------")
    send_line_message(message)


if __name__ == "__main__":
    main()
