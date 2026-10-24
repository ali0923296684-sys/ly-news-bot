import os
import time
import html
import sqlite3
import logging
import hashlib
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import requests
import feedparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ==================================================
# الإعدادات: تم وضع بياناتك مباشرة هنا
# ==================================================

BOT_TOKEN = "8854476880:AAFsZklgrEvgFeVnbL4kViO0aXfIcuGEpsk"
CHAT_ID = "8257896938"

DB_PATH = "news_seen.sqlite3"
POLL_SECONDS = 900
MAX_AGE_HOURS = 36
MAX_PER_CYCLE = 5

# مصادر RSS: تم تنظيفها والاحتفاظ بالمصادر النشطة فقط
DEFAULT_FEEDS = [
    {
        "name": "الاحتياطي الفيدرالي الأمريكي",
        "url": "https://www.federalreserve.gov/feeds/press_monetary.xml",
        "official": True,
    },
    {
        "name": "الفيدرالي الأمريكي - جميع البيانات الصحفية",
        "url": "https://www.federalreserve.gov/feeds/press_all.xml",
        "official": True,
    },
    {
        "name": "إدارة معلومات الطاقة الأمريكية EIA",
        "url": "https://www.eia.gov/rss/press_rss.xml",
        "official": True,
    },
    {
        "name": "مصرف ليبيا المركزي - أخبار الموقع",
        "url": "https://cbl.gov.ly/feed/",
        "official": True,
    },
    {
        "name": "عين ليبيا",
        "url": "https://www.eanlibya.com/feed",
        "official": False,
    },
]

# الكلمات الاقتصادية المستهدفة (مصرف ليبيا المركزي والفيدرالي الأمريكي والنفط).
KEYWORDS = {
    "مصرف ليبيا المركزي": 5,
    "المركزي الليبي": 5,
    "محافظ المصرف المركزي": 5,
    "سعر الصرف": 5,
    "تخفيض قيمة الدينار": 5,
    "تعديل سعر الصرف": 5,
    "بيع العملة الأجنبية": 5,
    "مخصصات الأغراض الشخصية": 4,
    "احتياطي النقد الأجنبي": 5,
    "الاحتياطيات الأجنبية": 5,
    "الاحتياطي الأجنبي": 5,
    "السياسة النقدية": 4,
    "الدولار مقابل الدينار": 5,
    "السوق الموازي": 4,
    "سوق الصرف الموازي": 4,
    "الاحتياطي الفيدرالي": 5,
    "الفيدرالي الأمريكي": 5,
    "Federal Reserve": 5,
    "FOMC": 5,
    "Jerome Powell": 4,
    "جيروم باول": 4,
    "سعر الفائدة": 5,
    "أسعار الفائدة": 5,
    "خفض الفائدة": 5,
    "رفع الفائدة": 5,
    "التضخم الأمريكي": 4,
    "مؤشر أسعار المستهلك": 4,
    "النفط الليبي": 5,
    "إنتاج النفط": 4,
    "صادرات النفط": 4,
    "إغلاق الحقول النفطية": 5,
    "إغلاق الموانئ النفطية": 5,
    "أسعار النفط": 4,
    "خام برنت": 4,
    "OPEC": 4,
    "أوبك": 4,
    "النفط": 2,
    "الدولار": 2,
}

# كلمات عامة لا تكفي وحدها لإرسال خبر.
IMPORTANT_TERMS = {
    "قرار", "قرارات", "رفع", "خفض", "تعديل", "إلغاء",
    "تعليق", "استئناف", "اعتماد", "احتياطي", "احتياطيات",
    "فائدة", "إنتاج", "صادرات", "إغلاق", "فتح",
    "سعر الصرف", "مخصصات", "بيان", "عاجل", "اجتماع",
    "inflation", "interest rate", "rate cut", "rate hike",
    "oil production", "oil exports", "supply disruption",
    "foreign reserves", "exchange rate", "monetary policy",
}

# النطاقات الرسمية التي نعتمد عليها في تصنيف المصدر.
OFFICIAL_DOMAINS = {
    "cbl.gov.ly",
    "www.cbl.gov.ly",
    "federalreserve.gov",
    "www.federalreserve.gov",
    "eia.gov",
    "www.eia.gov",
    "opec.org",
    "www.opec.org",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

session = requests.Session()
retry_policy = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
session.mount("https://", HTTPAdapter(max_retries=retry_policy))
session.headers.update({
    "User-Agent": "LibyaEconomicNewsBot/1.0"
})


# ==================================================
# قاعدة البيانات: حفظ الأخبار ومنع تكرارها
# ==================================================

def init_db():
    with sqlite3.connect(DB_PATH, timeout=20) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sent_news (
                news_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                link TEXT NOT NULL,
                source TEXT NOT NULL,
                sent_at TEXT NOT NULL
            )
        """)
        conn.commit()


def already_sent(news_id):
    with sqlite3.connect(DB_PATH, timeout=20) as conn:
        row = conn.execute(
            "SELECT 1 FROM sent_news WHERE news_id = ?",
            (news_id,),
        ).fetchone()
        return row is not None


def mark_sent(news_id, title, link, source):
    with sqlite3.connect(DB_PATH, timeout=20) as conn:
        conn.execute("""
            INSERT OR IGNORE INTO sent_news
            (news_id, title, link, source, sent_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            news_id,
            title,
            link,
            source,
            datetime.now(timezone.utc).isoformat(),
        ))
        conn.commit()


# ==================================================
# الأدوات المساعدة
# ==================================================

def normalize_text(value):
    return " ".join(str(value or "").split())


def parse_date(entry):
    for field in ("published", "updated", "created"):
        raw = entry.get(field)
        if not raw:
            continue
        try:
            dt = parsedate_to_datetime(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except (TypeError, ValueError, OverflowError):
            pass

    for field in ("published_parsed", "updated_parsed"):
        value = entry.get(field)
        if value:
            try:
                return datetime(*value[:6], tzinfo=timezone.utc)
            except (TypeError, ValueError, OverflowError):
                pass

    return None


def source_is_official(source_url, configured_official):
    host = urlparse(source_url).hostname or ""
    return configured_official and host.lower() in OFFICIAL_DOMAINS


def score_news(title, summary):
    text = f"{title} {summary}".casefold()
    score = 0
    matched = []

    for keyword, weight in KEYWORDS.items():
        if keyword.casefold() in text:
            score += weight
            matched.append(keyword)

    important = any(term.casefold() in text for term in IMPORTANT_TERMS)

    if score < 4:
        return 0, matched

    if any(term in text for term in [
        "تخفيض قيمة الدينار", "تعديل سعر الصرف",
        "سعر الفائدة", "خفض الفائدة", "رفع الفائدة",
        "إغلاق الحقول النفطية", "إغلاق الموانئ النفطية",
        "احتياطي النقد الأجنبي", "الاحتياطيات الأجنبية",
        "rate cut", "rate hike", "exchange rate",
        "foreign reserves", "oil production",
    ]):
        score += 3

    if score <= 2 or not important:
        return 0, matched

    return score, matched


def make_news_id(link, title):
    base = normalize_text(link) or normalize_text(title)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


# ==================================================
# إرسال تيليجرام
# ==================================================

def send_telegram_message(title, link, source, official, published, score):
    label = "مصدر رسمي" if official else "مصدر صحفي - ليس بياناً رسمياً"

    if published:
        published_text = published.strftime("%Y-%m-%d %H:%M UTC")
    else:
        published_text = "غير متوفر من المصدر"

    message = (
        "🏦 <b>تنبيه اقتصادي مهم</b>\n\n"
        f"📌 <b>العنوان:</b> {html.escape(title)}\n\n"
        f"📰 <b>المصدر:</b> {html.escape(source)}\n"
        f"🔎 <b>تصنيف المصدر:</b> {label}\n"
        f"🕒 <b>تاريخ النشر:</b> {published_text}\n"
        f"📊 <b>درجة الأهمية الآلية:</b> {score}\n\n"
        f'🔗 <a href="{html.escape(link, quote=True)}">اقرأ الخبر الأصلي</a>\n\n'
        "⚠️ هذا تنبيه آلي، وليس تأكيداً مستقلاً لصحة الخبر أو توصية مالية."
    )

    api_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    response = session.post(
        api_url,
        json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=(10, 25),
    )

    response.raise_for_status()
    result = response.json()

    if not result.get("ok"):
        raise RuntimeError("Telegram API returned ok=false")

    return True


# ==================================================
# جلب الأخبار ومعالجتها
# ==================================================

def process_feed(feed_info):
    name = feed_info["name"]
    url = feed_info["url"]

    try:
        response = session.get(url, timeout=(10, 25))
        response.raise_for_status()

        feed = feedparser.parse(response.content)

        if feed.bozo and not feed.entries:
            logging.warning("تعذر تحليل RSS من %s", name)
            return

        for entry in feed.entries[:30]:
            title = normalize_text(entry.get("title"))
            link = normalize_text(entry.get("link"))

            if not title or not link:
                continue

            parsed_link = urlparse(link)
            if parsed_link.scheme not in ("http", "https"):
                continue

            summary = normalize_text(
                entry.get("summary") or entry.get("description")
            )

            published = parse_date(entry)

            if published:
                age_hours = (
                    datetime.now(timezone.utc) - published
                ).total_seconds() / 3600

                if age_hours > MAX_AGE_HOURS:
                    continue

            score, matched = score_news(title, summary)

            if score == 0:
                continue

            news_id = make_news_id(link, title)

            if already_sent(news_id):
                continue

            official = source_is_official(
                url, bool(feed_info.get("official"))
            )

            try:
                send_telegram_message(
                    title=title,
                    link=link,
                    source=name,
                    official=official,
                    published=published,
                    score=score,
                )

                mark_sent(news_id, title, link, name)
                logging.info("تم إرسال: %s | %s", name, title)

                time.sleep(1.5)

            except Exception:
                logging.exception("فشل إرسال الخبر: %s", title)

    except requests.RequestException:
        logging.exception("خطأ اتصال بمصدر: %s", name)
    except Exception:
        logging.exception("خطأ غير متوقع في مصدر: %s", name)


def main():
    init_db()
    logging.info("بدأ بوت الأخبار الاقتصادية.")

    while True:
        started = time.monotonic()

        for feed_info in DEFAULT_FEEDS:
            process_feed(feed_info)

        elapsed = time.monotonic() - started
        wait_seconds = max(1, POLL_SECONDS - int(elapsed))

        logging.info(
            "اكتمل الفحص. الفحص القادم بعد %s ثانية.",
            wait_seconds,
        )
        time.sleep(wait_seconds)


if __name__ == "__main__":
    main()