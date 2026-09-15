from flask import Flask, jsonify, render_template_string
import requests
import os
import io
import csv
import json
import gzip
import zipfile
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

app = Flask(__name__)

# =========================================================
# SETTINGS
# =========================================================

UPSTOX_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()

UPSTOX_BASE = "https://api.upstox.com"

INSTRUMENT_URL = (
    "https://assets.upstox.com/market-quote/"
    "instruments/exchange/complete.json.gz"
)

NSE_HOME = "https://www.nseindia.com"

NSE_ARCHIVE_BASE = "https://nsearchives.nseindia.com"

NSE_UDIFF_URL = (
    NSE_ARCHIVE_BASE +
    "/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{ymd}_F_0000.csv.zip"
)

NSE_REPORTS_URL = (
    NSE_HOME + "/api/reports"
)

MIN_PRICE = 50.0
MIN_AVG_TURNOVER = 100000000.0
LIQUIDITY_DAYS = 20

# FINAL WEIGHTS
W_GAP = 0.05
W_RECOVERY = 0.20
W_GAIN = 0.35
W_LIVE = 0.30
W_AVG = 0.10

IST = ZoneInfo("Asia/Kolkata")

UPSTOX_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {UPSTOX_TOKEN}"
}

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive"
}

STATE = {
    "running": False,
    "status": "Ready",
    "last_scan": None,
    "results": [],
    "error": None
}

LOCK = threading.Lock()

TURNOVER_CACHE = {
    "date": None,
    "data": {}
}


# =========================================================
# BASIC HELPERS
# =========================================================

def now_ist():
    return datetime.now(IST)


def set_status(text):
    with LOCK:
        STATE["status"] = text


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


# =========================================================
# SCORING
# =========================================================

def score_gap(open_price, low_price):

    if open_price <= 0:
        return 0.0

    gap = (
        (open_price - low_price)
        / open_price
    ) * 100.0

    return clamp(
        (1.0 - gap / 0.50) * 100.0
    )


def score_recovery(close_price, low_price):

    if low_price <= 0:
        return 0.0

    recovery = (
        (close_price - low_price)
        / low_price
    ) * 100.0

    return clamp(
        (recovery / 1.50) * 100.0
    )


def score_gain(close_price, prev_close):

    if prev_close <= 0:
        return 0.0

    gain = (
        (close_price - prev_close)
        / prev_close
    ) * 100.0

    return clamp(
        (gain / 3.00) * 100.0
    )


def score_live_turnover(
    live_turnover,
    average_turnover
):

    if average_turnover <= 0:
        return 0.0

    return clamp(
        live_turnover
        / (average_turnover * 2.5)
        * 100.0
    )


def score_avg_turnover(
    average_turnover
):

    return clamp(
        average_turnover
        / 100000000.0
        * 100.0
    )


# =========================================================
# NSE SESSION
# =========================================================

def create_nse_session():

    s = requests.Session()

    s.headers.update(NSE_HEADERS)

    try:

        r = s.get(
            NSE_HOME,
            timeout=25
        )

        if r.status_code not in (200, 403):

            raise RuntimeError(
                f"NSE home status {r.status_code}"
            )

    except Exception as e:

        raise RuntimeError(
            f"NSE connection failed: {e}"
        )

    return s


# =========================================================
# DOWNLOAD UDIFF ARCHIVE
# =========================================================

def download_udiff(
    session,
    date_obj
):

    ymd = date_obj.strftime("%Y%m%d")

    url = NSE_UDIFF_URL.format(
        ymd=ymd
    )

    headers = {
        "User-Agent": NSE_HEADERS["User-Agent"],
        "Accept": "*/*",
        "Referer": "https://www.nseindia.com/all-reports"
    }

    r = session.get(
        url,
        headers=headers,
        timeout=30
    )

    if r.status_code != 200:
        return None, (
            f"UDiFF HTTP {r.status_code}"
        )

    if len(r.content) < 1000:
        return None, "UDiFF file बहुत छोटी है"

    try:

        with zipfile.ZipFile(
            io.BytesIO(r.content)
        ) as z:

            names = z.namelist()

            csv_name = None

            for name in names:

                if name.lower().endswith(".csv"):

                    csv_name = name
                    break

            if not csv_name:

                return None, (
                    "ZIP में CSV नहीं मिली"
                )

            with z.open(csv_name) as f:

                text = f.read().decode(
                    "utf-8-sig",
                    errors="replace"
                )

                return text, None

    except zipfile.BadZipFile:

        return None, (
            "NSE response ZIP नहीं है"
        )

    except Exception as e:

        return None, str(e)


# =========================================================
# NSE REPORTS API FALLBACK
# =========================================================

def download_reports_api(
    session,
    date_obj
):

    archives = [{
        "name":
            "CM-UDiFF Common Bhavcopy Final (zip)",
        "type":
            "daily-reports",
        "category":
            "capital-market",
        "section":
            "equities"
    }]

    params = {
        "archives": json.dumps(
            archives,
            separators=(",", ":")
        ),
        "date":
            date_obj.strftime("%d-%b-%Y"),
        "type":
            "equities",
        "mode":
            "single"
    }

    headers = {
        "User-Agent":
            NSE_HEADERS["User-Agent"],
        "Accept":
            "*/*",
        "Referer":
            "https://www.nseindia.com/all-reports",
        "X-Requested-With":
            "XMLHttpRequest"
    }

    r = session.get(
        NSE_REPORTS_URL,
        params=params,
        headers=headers,
        timeout=30
    )

    if r.status_code != 200:

        return None, (
            f"NSE reports HTTP {r.status_code}"
        )

    content_type = (
        r.headers.get(
            "content-type",
            ""
        ).lower()
    )

    # Sometimes API directly returns ZIP
    if (
        "zip" in content_type
        or r.content[:2] == b"PK"
    ):

        try:

            with zipfile.ZipFile(
                io.BytesIO(r.content)
            ) as z:

                names = z.namelist()

                csv_name = None

                for name in names:

                    if name.lower().endswith(
                        ".csv"
                    ):

                        csv_name = name
                        break

                if not csv_name:
                    return None, (
                        "Reports ZIP में CSV नहीं"
                    )

                with z.open(csv_name) as f:

                    return (
                        f.read().decode(
                            "utf-8-sig",
                            errors="replace"
                        ),
                        None
                    )

        except Exception as e:

            return None, str(e)

    # Some NSE responses contain JSON metadata
    try:

        data = r.json()

        file_url = None

        if isinstance(data, dict):

            if "filePath" in data:
                file_url = data["filePath"]

            if "fileUrl" in data:
                file_url = data["fileUrl"]

            if "url" in data:
                file_url = data["url"]

            if "data" in data:
                d = data["data"]

                if isinstance(d, list) and d:

                    first = d[0]

                    if isinstance(first, dict):

                        for k in (
                            "filePath",
                            "fileUrl",
                            "url"
                        ):

                            if first.get(k):
                                file_url = first[k]
                                break

        if file_url:

            if file_url.startswith("/"):
                file_url = NSE_HOME + file_url

            rr = session.get(
                file_url,
                headers=headers,
                timeout=30
            )

            if rr.status_code == 200:

                try:

                    with zipfile.ZipFile(
                        io.BytesIO(rr.content)
                    ) as z:

                        names = z.namelist()

                        for name in names:

                            if name.lower().endswith(
                                ".csv"
                            ):

                                with z.open(name) as f:

                                    return (
                                        f.read().decode(
                                            "utf-8-sig",
                                            errors="replace"
                                        ),
                                        None
                                    )

                except Exception:
                    pass

    except Exception:
        pass

    return None, (
        "NSE reports API से file नहीं मिली"
    )


# =========================================================
# PARSE UDIFF / LEGACY CSV
# =========================================================

def parse_turnover_csv(text):

    if not text:
        return {}

    lines = text.splitlines()

    if not lines:
        return {}

    reader = csv.DictReader(
        io.StringIO(text)
    )

    if not reader.fieldnames:
        return {}

    headers = [
        str(x).strip()
        for x in reader.fieldnames
    ]

    # -----------------------------------------
    # UDIFF
    # -----------------------------------------

    if (
        "TckrSymb" in headers
        and "SctySrs" in headers
    ):

        symbol_col = "TckrSymb"
        series_col = "SctySrs"

        turnover_col = None

        for c in (
            "TtlTrfVal",
            "TtlTrdVal",
            "Turnover"
        ):

            if c in headers:
                turnover_col = c
                break

        if not turnover_col:
            return {}

        result = {}

        for row in reader:

            symbol = str(
                row.get(
                    symbol_col,
                    ""
                )
            ).strip().upper()

            series = str(
                row.get(
                    series_col,
                    ""
                )
            ).strip().upper()

            if not symbol:
                continue

            if series != "EQ":
                continue

            raw = str(
                row.get(
                    turnover_col,
                    ""
                )
            ).strip()

            try:

                value = float(
                    raw.replace(",", "")
                )

            except Exception:

                continue

            if value > 0:
                result[symbol] = value

        return result

    # -----------------------------------------
    # OLD FORMAT FALLBACK
    # -----------------------------------------

    if (
        "SYMBOL" in headers
        and "SERIES" in headers
    ):

        symbol_col = "SYMBOL"
        series_col = "SERIES"

        turnover_col = None

        for c in (
            "TOTTRDVAL",
            "TURNOVER_LACS"
        ):

            if c in headers:
                turnover_col = c
                break

        if not turnover_col:
            return {}

        result = {}

        for row in reader:

            symbol = str(
                row.get(
                    symbol_col,
                    ""
                )
            ).strip().upper()

            series = str(
                row.get(
                    series_col,
                    ""
                )
            ).strip().upper()

            if not symbol or series != "EQ":
                continue

            raw = str(
                row.get(
                    turnover_col,
                    ""
                )
            ).strip()

            try:

                value = float(
                    raw.replace(",", "")
                )

            except Exception:

                continue

            # Old NSE TURNOVER_LACS
            if turnover_col == "TURNOVER_LACS":
                value *= 100000.0

            if value > 0:
                result[symbol] = value

        return result

    return {}


# =========================================================
# ONE TRADING DAY
# =========================================================

def get_one_day_turnover(
    session,
    date_obj
):

    # First UDiFF archive
    text, error1 = download_udiff(
        session,
        date_obj
    )

    if text:

        data = parse_turnover_csv(
            text
        )

        if data:
            return data, "UDiFF"

    # Then NSE reports API
    text, error2 = download_reports_api(
        session,
        date_obj
    )

    if text:

        data = parse_turnover_csv(
            text
        )

        if data:
            return data, "Reports API"

    return {}, (
        f"{date_obj.strftime('%d-%m-%Y')}: "
        f"{error1}; {error2}"
    )


# =========================================================
# 20 DAY AVERAGE TURNOVER
# =========================================================

def get_average_turnover():

    today = now_ist().date()

    if (
        TURNOVER_CACHE["date"] == today
        and TURNOVER_CACHE["data"]
    ):

        set_status(
            "20-Day turnover cache से लिया जा रहा है..."
        )

        return TURNOVER_CACHE["data"]

    session = create_nse_session()

    sums = {}
    counts = {}

    valid_days = 0
    checked_days = 0

    errors = []

    date_cursor = (
        today - timedelta(days=1)
    )

    set_status(
        "NSE से 20 trading days निकाले जा रहे हैं..."
    )

    while valid_days < LIQUIDITY_DAYS:

        if checked_days >= 60:
            break

        checked_days += 1

        day_data, source = (
            get_one_day_turnover(
                session,
                date_cursor
            )
        )

        if day_data:

            valid_days += 1

            set_status(
                f"NSE turnover: "
                f"{valid_days}/{LIQUIDITY_DAYS} "
                f"days • {source}"
            )

            for symbol, turnover in (
                day_data.items()
            ):

                sums[symbol] = (
                    sums.get(symbol, 0.0)
                    + turnover
                )

                counts[symbol] = (
                    counts.get(symbol, 0)
                    + 1
                )

        else:

            errors.append(source)

        date_cursor -= timedelta(days=1)

        time.sleep(0.20)

    if valid_days < LIQUIDITY_DAYS:

        detail = ""

        if errors:
            detail = (
                " | Last: "
                + errors[-1][:350]
            )

        raise RuntimeError(
            f"NSE से केवल {valid_days} "
            f"valid trading days मिले, "
            f"{LIQUIDITY_DAYS} चाहिए."
            + detail
        )

    averages = {}

    for symbol, total in sums.items():

        if (
            counts.get(symbol, 0)
            >= LIQUIDITY_DAYS
        ):

            averages[symbol] = (
                total
                / LIQUIDITY_DAYS
            )

    if not averages:

        raise RuntimeError(
            "20-Day turnover averages खाली हैं."
        )

    TURNOVER_CACHE["date"] = today
    TURNOVER_CACHE["data"] = averages

    return averages


# =========================================================
# UPSTOX INSTRUMENTS
# =========================================================

def get_instruments():

    set_status(
        "NSE Equity list download हो रही है..."
    )

    r = requests.get(
        INSTRUMENT_URL,
        timeout=45
    )

    r.raise_for_status()

    raw = gzip.decompress(
        r.content
    )

    data = json.loads(
        raw.decode("utf-8")
    )

    stocks = []

    for x in data:

        if x.get("segment") != "NSE_EQ":
            continue

        if x.get("instrument_type") != "EQ":
            continue

        key = x.get(
            "instrument_key"
        )

        symbol = x.get(
            "trading_symbol"
        )

        if not key or not symbol:
            continue

        stocks.append({
            "key": key,
            "symbol": symbol
        })

    return stocks


# =========================================================
# UPSTOX LIVE QUOTES
# =========================================================

def get_live_quotes(
    stocks
):

    quotes = {}

    total = len(stocks)

    for start in range(
        0,
        total,
        500
    ):

        batch = stocks[
            start:start + 500
        ]

        set_status(
            f"Live market data: "
            f"{min(start + 500, total)}"
            f"/{total}"
        )

        keys = ",".join(
            x["key"]
            for x in batch
        )

        url = (
            f"{UPSTOX_BASE}"
            f"/v3/market-quote/quotes"
        )

        r = requests.get(
            url,
            headers=UPSTOX_HEADERS,
            params={
                "instrument_key": keys
            },
            timeout=40
        )

        if r.status_code != 200:

            raise RuntimeError(
                "Upstox Live Quote error "
                f"{r.status_code}: "
                f"{r.text[:300]}"
            )

        payload = r.json()

        data = payload.get(
            "data",
            {}
        )

        for response_key, q in (
            data.items()
        ):

            if not isinstance(
                q,
                dict
            ):
                continue

            quotes[
                response_key
            ] = q

        time.sleep(0.15)

    return quotes


# =========================================================
# MAIN SCAN
# =========================================================

def perform_scan():

    with LOCK:

        STATE["running"] = True
        STATE["results"] = []
        STATE["error"] = None
        STATE["last_scan"] = None
        STATE["status"] = (
            "Scan शुरू हो रहा है..."
        )

    try:

        if not UPSTOX_TOKEN:

            raise RuntimeError(
                "UPSTOX_ACCESS_TOKEN Render "
                "Environment में नहीं मिला."
            )

        # -----------------------------------------
        # 1. Instruments
        # -----------------------------------------

        stocks = get_instruments()

        if not stocks:

            raise RuntimeError(
                "NSE Equity instruments नहीं मिले."
            )

        # -----------------------------------------
        # 2. Live quotes
        # -----------------------------------------

        quotes = get_live_quotes(
            stocks
        )

        if not quotes:

            raise RuntimeError(
                "Upstox से Live Quotes नहीं मिले."
            )

        # -----------------------------------------
        # 3. NSE 20-day turnover
        # -----------------------------------------

        avg_turnover = (
            get_average_turnover()
        )

        # -----------------------------------------
        # 4. Scoring
        # -----------------------------------------

        set_status(
            "Strength Score calculate हो रहा है..."
        )

        results = []

        for stock in stocks:

            symbol = stock[
                "symbol"
            ]

            key = stock[
                "key"
            ]

            response_key = (
                "NSE_EQ:"
                + symbol
            )

            q = quotes.get(
                response_key
            )

            if not q:
                continue

            ltp = q.get(
                "last_price"
            )

            prev_close = q.get(
                "prev_close_price"
            )

            volume = q.get(
                "volume",
                0
            )

            ohlc = q.get(
                "ohlc",
                {}
            ) or {}

            open_price = ohlc.get(
                "open"
            )

            low_price = ohlc.get(
                "low"
            )

            try:

                ltp = float(ltp)
                prev_close = float(
                    prev_close
                )
                volume = float(
                    volume or 0
                )
                open_price = float(
                    open_price
                )
                low_price = float(
                    low_price
                )

            except Exception:

                continue

            if ltp < MIN_PRICE:
                continue

            average_turnover = (
                avg_turnover.get(
                    symbol
                )
            )

            if not average_turnover:
                continue

            if (
                average_turnover
                < MIN_AVG_TURNOVER
            ):
                continue

            if open_price <= 0:
                continue

            if low_price <= 0:
                continue

            if prev_close <= 0:
                continue

            # ---------------------------------
            # Scores
            # ---------------------------------

            gap_score = score_gap(
                open_price,
                low_price
            )

            recovery_score = (
                score_recovery(
                    ltp,
                    low_price
                )
            )

            gain_score = score_gain(
                ltp,
                prev_close
            )

            live_turnover = (
                volume * ltp
            )

            live_score = (
                score_live_turnover(
                    live_turnover,
                    average_turnover
                )
            )

            avg_score = (
                score_avg_turnover(
                    average_turnover
                )
            )

            # ---------------------------------
            # FINAL RANK
            # ---------------------------------

            strength = (
                gap_score * W_GAP
                + recovery_score
                * W_RECOVERY
                + gain_score
                * W_GAIN
                + live_score
                * W_LIVE
                + avg_score
                * W_AVG
            )

            gain_percent = (
                (ltp - prev_close)
                / prev_close
            ) * 100.0

            results.append({

                "symbol":
                    symbol,

                "strength":
                    round(
                        strength,
                        2
                    ),

                "ltp":
                    round(
                        ltp,
                        2
                    ),

                "open":
                    round(
                        open_price,
                        2
                    ),

                "low":
                    round(
                        low_price,
                        2
                    ),

                "gain":
                    round(
                        gain_percent,
                        2
                    ),

                "avg_turnover":
                    round(
                        average_turnover,
                        0
                    ),

                "gap_score":
                    round(
                        gap_score,
                        2
                    ),

                "recovery_score":
                    round(
                        recovery_score,
                        2
                    ),

                "gain_score":
                    round(
                        gain_score,
                        2
                    ),

                "live_score":
                    round(
                        live_score,
                        2
                    ),

                "avg_score":
                    round(
                        avg_score,
                        2
                    )
            })

        results.sort(
            key=lambda x:
                x["strength"],
            reverse=True
        )

        for i, item in enumerate(
            results,
            1
        ):

            item["rank"] = i

        with LOCK:

            STATE["results"] = results

            STATE["last_scan"] = (
                now_ist().strftime(
                    "%d-%m-%Y %H:%M:%S"
                )
            )

            STATE["status"] = (
                f"Scan complete — "
                f"{len(results)} stocks found"
            )

    except Exception as e:

        with LOCK:

            STATE["error"] = str(e)

            STATE["status"] = (
                "ERROR: " + str(e)
            )

    finally:

        with LOCK:

            STATE["running"] = False


# =========================================================
# HTML
# =========================================================

HTML = """
<!DOCTYPE html>

<html lang="hi">

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width,initial-scale=1.0">

<title>
Early Strength Rank Scanner
</title>

<style>

*{
box-sizing:border-box;
}

body{
margin:0;
padding:12px;
background:#0e141b;
color:#e8edf3;
font-family:Arial,Helvetica,sans-serif;
}

.container{
max-width:1000px;
margin:auto;
}

.header,
.panel{
background:#151d26;
border:1px solid #27313c;
border-radius:18px;
padding:20px;
margin-bottom:18px;
}

h1{
margin:0 0 8px;
font-size:28px;
}

.subtitle{
color:#9ca8b5;
font-size:17px;
}

.status{
background:#1c2732;
border-radius:12px;
padding:16px;
font-size:18px;
margin-bottom:12px;
}

button{
background:#2563b9;
color:white;
border:0;
border-radius:10px;
padding:14px 24px;
font-size:17px;
}

button:disabled{
opacity:.5;
}

.weights{
display:grid;
grid-template-columns:repeat(2,1fr);
gap:12px;
margin-bottom:18px;
}

.box{
background:#1b2631;
border-radius:12px;
padding:16px;
}

.label{
color:#9ca8b5;
margin-bottom:6px;
}

.value{
font-size:22px;
font-weight:bold;
}

.results-title{
font-size:25px;
font-weight:bold;
margin-bottom:18px;
}

.table-wrap{
overflow-x:auto;
}

table{
width:100%;
border-collapse:collapse;
min-width:650px;
}

th,td{
padding:12px 10px;
border-bottom:1px solid #27313c;
text-align:left;
}

th{
color:#aeb8c3;
}

.error{
color:#ff8d8d;
background:#321b1b;
padding:12px;
border-radius:10px;
margin-top:12px;
}

</style>

</head>

<body>

<div class="container">

<div class="header">

<h1>
Early Strength Rank Scanner
</h1>

<div class="subtitle">
NSE EQ • Live Strength Ranking • Strongest First
</div>

</div>


<div class="panel">

<div id="status"
class="status">
Ready
</div>

<div>
Last Scan:
<strong id="lastScan">--</strong>
</div>

<br>

<button id="scanBtn"
onclick="startScan()">
Scan Now
</button>

<div id="error"></div>

</div>


<div class="weights">

<div class="box">
<div class="label">Price</div>
<div class="value">≥ ₹50</div>
</div>

<div class="box">
<div class="label">Gap Weight</div>
<div class="value">5%</div>
</div>

<div class="box">
<div class="label">Recovery</div>
<div class="value">20%</div>
</div>

<div class="box">
<div class="label">Gain</div>
<div class="value">35%</div>
</div>

<div class="box">
<div class="label">Live Turnover</div>
<div class="value">30%</div>
</div>

<div class="box">
<div class="label">Average Turnover</div>
<div class="value">10%</div>
</div>

</div>


<div class="panel">

<div class="results-title">
Results:
<span id="count">0</span>
</div>

<div class="table-wrap">

<table>

<thead>

<tr>
<th>Rank</th>
<th>Share</th>
<th>Strength</th>
<th>LTP</th>
<th>Open</th>
<th>Low</th>
<th>Gain</th>
</tr>

</thead>

<tbody id="tbody">

<tr>
<td colspan="7">
कोई result नहीं
</td>
</tr>

</tbody>

</table>

</div>

</div>


<div class="panel">

<h2>
Ranking Logic
</h2>

<p>
• NSE Equity shares only
</p>

<p>
• Price ≥ ₹50
</p>

<p>
• 20-Day Average Turnover ≥ ₹10 Crore
</p>

<p>
• Open-Low Gap = 5%
</p>

<p>
• Recovery = 20%
</p>

<p>
• Gain = 35%
</p>

<p>
• Live Turnover = 30%
</p>

<p>
• Average Turnover = 10%
</p>

</div>

</div>


<script>

async function loadResults(){

try{

const r =
await fetch(
"/api/results",
{cache:"no-store"}
);

const data =
await r.json();

document.getElementById(
"status"
).innerText =
data.status || "Ready";

document.getElementById(
"lastScan"
).innerText =
data.last_scan || "--";

document.getElementById(
"count"
).innerText =
data.results
? data.results.length
: 0;

const errorBox =
document.getElementById(
"error"
);

if(data.error){

errorBox.innerHTML =
'<div class="error">'
+ data.error +
'</div>';

}else{

errorBox.innerHTML = "";

}

document.getElementById(
"scanBtn"
).disabled =
data.running;

const tbody =
document.getElementById(
"tbody"
);

if(
!data.results ||
data.results.length === 0
){

tbody.innerHTML =
'<tr><td colspan="7">'
+
'कोई result नहीं'
+
'</td></tr>';

return;
}

tbody.innerHTML =
data.results.map(
x => `

<tr>

<td>
<strong>${x.rank}</strong>
</td>

<td>
<strong>${x.symbol}</strong>
</td>

<td>
<strong>${x.strength}</strong>
</td>

<td>
₹${x.ltp}
</td>

<td>
₹${x.open}
</td>

<td>
₹${x.low}
</td>

<td>
${x.gain}%
</td>

</tr>

`
).join("");

}catch(e){

document.getElementById(
"status"
).innerText =
"Server response नहीं मिला";

}

}


async function startScan(){

const btn =
document.getElementById(
"scanBtn"
);

btn.disabled = true;

document.getElementById(
"status"
).innerText =
"Scan शुरू हो रहा है...";

try{

await fetch(
"/api/scan",
{
method:"POST"
}
);

}catch(e){

document.getElementById(
"status"
).innerText =
"Scan request failed";

}

}


loadResults();

setInterval(
loadResults,
1500
);

</script>

</body>

</html>
"""


# =========================================================
# ROUTES
# =========================================================

@app.route("/")
def home():

    return render_template_string(
        HTML
    )


@app.route(
    "/api/scan",
    methods=["POST"]
)
def api_scan():

    with LOCK:

        if STATE["running"]:

            return jsonify({
                "ok": True,
                "message":
                    "Scan already running"
            })

    thread = threading.Thread(
        target=perform_scan,
        daemon=True
    )

    thread.start()

    return jsonify({
        "ok": True
    })


@app.route(
    "/api/results"
)
def api_results():

    with LOCK:

        return jsonify({

            "running":
                STATE["running"],

            "status":
                STATE["status"],

            "last_scan":
                STATE["last_scan"],

            "results":
                STATE["results"],

            "error":
                STATE["error"]
        })


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "5000"
            )
        )
    )
