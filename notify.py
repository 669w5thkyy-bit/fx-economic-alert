"""
FX経済指標 15分前 LINE通知スクリプト
GitHub Actions で5分ごとに実行される
"""

import os
import json
import requests
from datetime import datetime, timedelta, timezone

import pytz

# ── 設定 ──────────────────────────────────────────────
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_USER_ID = os.environ["LINE_USER_ID"]
JST = pytz.timezone("Asia/Tokyo")

# 監視する通貨（Forex Factory の country コード）
WATCHED_COUNTRIES = {"USD", "JPY", "EUR", "GBP"}

# 通知対象の最低インパクト（"High" のみ）
MIN_IMPACT = "High"

# 通知タイミング設定
NOTIFY_MINUTES_BEFORE = 15  # 発表の何分前に通知するか
WINDOW_MINUTES = 4           # ±何分の誤差を許容するか（GitHub Actionsの遅延対策）

# ── 厳選した経済指標リスト ────────────────────────────
# ※ Highインパクトのみでも十分だが、以下を明示的にホワイトリスト管理
# ※ Trueの場合はHigh全部、Falseの場合は下記リストのみ
USE_ALL_HIGH_IMPACT = True

INDICATOR_WHITELIST = {
    "USD": [
        "Non-Farm Employment Change",
        "Unemployment Rate",
        "FOMC Statement",
        "Federal Funds Rate",
        "CPI m/m",
        "Core CPI m/m",
        "GDP q/q",
        "Prelim GDP q/q",
        "Retail Sales m/m",
        "ISM Manufacturing PMI",
        "ISM Services PMI",
        "Core PCE Price Index m/m",
        "ADP Non-Farm Employment Change",
        "JOLTS Job Openings",
        "Trade Balance",
        "Consumer Confidence",
        "PPI m/m",
    ],
    "JPY": [
        "BOJ Policy Rate",
        "Monetary Policy Statement",
        "Tankan Manufacturing Index",
        "Tokyo Core CPI y/y",
        "CPI y/y",
        "GDP q/q",
        "Retail Sales y/y",
    ],
    "EUR": [
        "Main Refinancing Rate",
        "ECB Press Conference Starts",
        "German Prelim CPI m/m",
        "Flash GDP q/q",
        "CPI Flash Estimate y/y",
    ],
    "GBP": [
        "Official Bank Rate",
        "MPC Official Bank Rate Votes",
        "CPI y/y",
        "GDP m/m",
        "Retail Sales m/m",
    ],
}

# ── 国旗マッピング ────────────────────────────────────
COUNTRY_FLAG = {
    "USD": "🇺🇸",
    "JPY": "🇯🇵",
    "EUR": "🇪🇺",
    "GBP": "🇬🇧",
    "AUD": "🇦🇺",
    "CAD": "🇨🇦",
    "CHF": "🇨🇭",
    "NZD": "🇳🇿",
}


def fetch_calendar() -> list:
    """Forex Factory APIから今週・来週のカレンダーを取得"""
    events = []
    for week in ["thisweek", "nextweek"]:
        url = f"https://nfs.faireconomy.media/ff_calendar_{week}.json"
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            events.extend(resp.json())
        except Exception as e:
            print(f"[WARN] カレンダー取得エラー ({week}): {e}")
    return events


def parse_event_time(event: dict):
    """
    イベントのdate + timeフィールドをUTCのdatetimeに変換する。
    date例: "2024-01-05T00:00:00-05:00"
    time例: "8:30am" / "All Day" / "Tentative"
    """
    date_str = event.get("date", "")
    time_str = event.get("time", "").strip()

    if not time_str or time_str in ("All Day", "Tentative", ""):
        return None

    try:
        # タイムゾーンオフセットを取得（例: -05:00 または -04:00）
        tz_part = date_str[19:]  # "T00:00:00-05:00" → "-05:00"
        sign = 1 if tz_part[0] == "+" else -1
        tz_hours = int(tz_part[1:3])
        tz_minutes = int(tz_part[4:6])
        tz_offset = timezone(timedelta(hours=sign * tz_hours, minutes=sign * tz_minutes))

        # 日付を取得（"2024-01-05"）
        year = int(date_str[0:4])
        month = int(date_str[5:7])
        day = int(date_str[8:10])

        # 時刻をパース（"8:30am" → hour, minute）
        t = time_str.lower()
        is_pm = t.endswith("pm")
        t = t.replace("am", "").replace("pm", "").strip()
        hour, minute = map(int, t.split(":"))
        if is_pm and hour != 12:
            hour += 12
        elif not is_pm and hour == 12:
            hour = 0

        event_dt = datetime(year, month, day, hour, minute, tzinfo=tz_offset)
        return event_dt.astimezone(timezone.utc)

    except Exception as e:
        print(f"[WARN] 時刻パースエラー: {e}  date={date_str!r}  time={time_str!r}")
        return None


def is_target_event(event: dict) -> bool:
    """通知対象の指標かどうかを判定"""
    country = event.get("country", "")
    impact = event.get("impact", "")
    title = event.get("title", "")

    if country not in WATCHED_COUNTRIES:
        return False
    if impact != MIN_IMPACT:
        return False
    if USE_ALL_HIGH_IMPACT:
        return True
    # ホワイトリストモード
    return title in INDICATOR_WHITELIST.get(country, [])


def should_notify(event_time_utc: datetime, now_utc: datetime) -> bool:
    """発表のNOTIFY_MINUTES_BEFORE分前の±WINDOW/2分以内かどうかを判定"""
    target = event_time_utc - timedelta(minutes=NOTIFY_MINUTES_BEFORE)
    delta_sec = (target - now_utc).total_seconds()
    half = WINDOW_MINUTES * 60 / 2
    return -half <= delta_sec <= half


def build_message(event: dict, event_time_utc: datetime) -> str:
    """LINE通知メッセージを作成"""
    event_time_jst = event_time_utc.astimezone(JST)
    country = event.get("country", "")
    flag = COUNTRY_FLAG.get(country, "")
    time_str = event_time_jst.strftime("%H:%M")
    forecast = event.get("forecast", "")
    previous = event.get("previous", "")

    lines = [
        f"⚠️ 経済指標 {NOTIFY_MINUTES_BEFORE}分前",
        f"{flag} {country} - {event.get('title', '')}",
        f"発表: {time_str} JST",
    ]
    if forecast:
        lines.append(f"予想: {forecast}")
    if previous:
        lines.append(f"前回: {previous}")
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
        print(f"[OK] LINE通知送信成功")
    else:
        print(f"[ERR] LINE送信エラー: {resp.status_code} {resp.text}")


def main():
    now_utc = datetime.now(timezone.utc)
    print(f"実行時刻(UTC): {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")

    events = fetch_calendar()
    print(f"取得イベント数: {len(events)}")

    notified = 0
    for event in events:
        if not is_target_event(event):
            continue

        event_time_utc = parse_event_time(event)
        if event_time_utc is None:
            continue

        if should_notify(event_time_utc, now_utc):
            message = build_message(event, event_time_utc)
            print(f"通知対象: {event.get('title')} / {event.get('country')}")
            send_line_message(message)
            notified += 1

    print(f"通知送信数: {notified}")


if __name__ == "__main__":
    main()
