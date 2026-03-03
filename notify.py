"""
FX経済指標 15分前 LINE通知スクリプト
データソース: みんかぶFX (https://fx.minkabu.jp/indicators)
GitHub Actions で5分ごとに実行される
"""

import os
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone

import pytz

# ── 設定 ──────────────────────────────────────────────
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_USER_ID = os.environ["LINE_USER_ID"]
JST = pytz.timezone("Asia/Tokyo")

# 監視する国コード（みんかぶの data_country 属性）
# US=USD, JP=JPY, EU=EUR, GB=GBP, AU=AUD, CA=CAD, CH=CHF, NZ=NZD
WATCHED_COUNTRIES = {"US", "JP", "EU", "GB"}

# 通知対象の最低重要度（1〜5の星の数。3以上を対象）
MIN_IMPORTANCE = 3

# 通知タイミング設定
NOTIFY_MINUTES_BEFORE = 15  # 発表の何分前に通知するか
WINDOW_MINUTES = 5           # ±何分の誤差を許容するか（GitHub Actionsの遅延対策）

# ── 国コード → 通貨・表示名マッピング ─────────────────
COUNTRY_INFO = {
    "US": {"currency": "USD", "flag": "🇺🇸"},
    "JP": {"currency": "JPY", "flag": "🇯🇵"},
    "EU": {"currency": "EUR", "flag": "🇪🇺"},
    "GB": {"currency": "GBP", "flag": "🇬🇧"},
    "AU": {"currency": "AUD", "flag": "🇦🇺"},
    "CA": {"currency": "CAD", "flag": "🇨🇦"},
    "CH": {"currency": "CHF", "flag": "🇨🇭"},
    "NZ": {"currency": "NZD", "flag": "🇳🇿"},
}


def fetch_calendar() -> list:
    """みんかぶFXの経済指標カレンダーをスクレイピングして取得"""
    url = "https://fx.minkabu.jp/indicators"
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
    except Exception as e:
        print(f"[ERR] カレンダー取得失敗: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    events = []

    for table in soup.find_all("table"):
        # テーブルのキャプション（日付）を取得
        caption = table.find("caption")
        if not caption:
            continue

        # 日付をパース（例: "2026年03月02日(月)"）
        date_text = caption.get_text(strip=True)
        date_match = re.search(r"(\d{4})年(\d{2})月(\d{2})日", date_text)
        if not date_match:
            continue
        year, month, day = int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))

        for tr in table.find_all("tr"):
            country = tr.get("data_country", "")
            importance_str = tr.get("data_importance", "0")

            # 時刻を取得
            time_td = tr.find("td", class_=lambda c: c and "eilist__time" in c)
            if not time_td:
                continue
            time_span = time_td.find("span")
            if not time_span:
                continue
            time_text = time_span.get_text(strip=True)
            if not re.match(r"^\d{1,2}:\d{2}$", time_text):
                continue

            # 指標名を取得
            name_p = tr.find("p", class_=lambda c: c and "flexbox__grow" in c)
            if not name_p:
                continue
            title = name_p.get_text(strip=True)

            # 予想値・前回値を取得（eilist__data クラスのtd）
            data_tds = tr.find_all("td", class_=lambda c: c and "eilist__data" in c)
            forecast = data_tds[0].get_text(strip=True) if len(data_tds) > 0 else ""
            previous = data_tds[1].get_text(strip=True) if len(data_tds) > 1 else ""
            forecast = "" if forecast in ("---", "") else forecast
            previous = "" if previous in ("---", "") else previous

            # 時刻をJSTのdatetimeに変換してUTCにする
            hour, minute = map(int, time_text.split(":"))
            event_dt_jst = JST.localize(datetime(year, month, day, hour, minute))
            event_dt_utc = event_dt_jst.astimezone(timezone.utc)

            events.append({
                "country": country,
                "importance": int(importance_str),
                "title": title,
                "time_utc": event_dt_utc,
                "time_jst": event_dt_jst,
                "forecast": forecast,
                "previous": previous,
            })

    return events


def is_target_event(event: dict) -> bool:
    """通知対象の指標かどうかを判定"""
    if event["country"] not in WATCHED_COUNTRIES:
        return False
    if event["importance"] < MIN_IMPORTANCE:
        return False
    return True


def should_notify(event_time_utc: datetime, now_utc: datetime) -> bool:
    """発表のNOTIFY_MINUTES_BEFORE分前の±WINDOW/2分以内かどうかを判定"""
    target = event_time_utc - timedelta(minutes=NOTIFY_MINUTES_BEFORE)
    delta_sec = (target - now_utc).total_seconds()
    half = WINDOW_MINUTES * 60 / 2
    return -half <= delta_sec <= half


def build_message(event: dict) -> str:
    """LINE通知メッセージを作成"""
    country = event["country"]
    info = COUNTRY_INFO.get(country, {"currency": country, "flag": ""})
    time_str = event["time_jst"].strftime("%H:%M")
    stars = "★" * event["importance"] + "☆" * (5 - event["importance"])

    lines = [
        f"⚠️ 経済指標 {NOTIFY_MINUTES_BEFORE}分前",
        f"{info['flag']} {info['currency']} - {event['title']}",
        f"発表: {time_str} JST  {stars}",
    ]
    if event["forecast"]:
        lines.append(f"予想: {event['forecast']}")
    if event["previous"]:
        lines.append(f"前回: {event['previous']}")
    return "\n".join(lines)


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
    # テストモード：TEST_MODE=1 の場合はLINEにテストメッセージを送って終了
    if os.environ.get("TEST_MODE") == "1":
        send_line_message("✅ テスト通知\nFX経済指標通知システムが正常に動作しています。")
        print("テストメッセージを送信しました")
        return

    now_utc = datetime.now(timezone.utc)
    print(f"実行時刻(UTC): {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")

    events = fetch_calendar()
    print(f"取得イベント数: {len(events)}")

    notified = 0
    for event in events:
        if not is_target_event(event):
            continue

        if should_notify(event["time_utc"], now_utc):
            message = build_message(event)
            print(f"通知対象: {event['title']} / {event['country']}")
            send_line_message(message)
            notified += 1

    print(f"通知送信数: {notified}")


if __name__ == "__main__":
    main()
