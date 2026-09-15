from flask import Flask, jsonify, render_template_string
import requests
import os
import io
import csv
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

NSE_BHAV_URL = (
    "https://nsearchives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
)

MIN_PRICE = 50.0
MIN_AVG_TURNOVER = 100000000.0   # ₹10 Crore
LIQUIDITY_DAYS = 20

# FINAL WEIGHTS
W_GAP = 0.05
W_RECOVERY = 0.20
W_GAIN = 0.35
W_LIVE = 0.30
W_AVG = 0.10

IST = ZoneInfo("Asia/Kolkata")

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {UPSTOX_TOKEN}",
    "User-Agent": "Mozilla/5.0"
}

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/"
}


# =========================================================
# STATE
# =========================================================

STATE = {
    "running": False,
    "status": "Ready",
    "last_scan": None,
    "results": [],
    "error": None
}

LOCK = threading.Lock()

# Same-day turnover cache.
TURNOVER_CACHE = {
    "date": None,
    "data": {}
}


# =========================================================
# HELPERS
# =========================================================

def now_ist():
    return datetime.now(IST)


def set_status(text):
    with LOCK:
        STATE["status"] = text


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


def score_gap(open_price, low_price):
    if open_price <= 0:
        return 0.0

    gap = ((open_price - low_price) / open_price) * 100.0

    return clamp((1.0 - gap / 0.50) * 100.0)


def score_recovery(close_price, low_price):
    if low_price <= 0:
        return 0.0

    recovery = ((close_price - low_price) / low_price) * 100.0

    return clamp((recovery / 1.50) * 100.0)


def score_gain(close_price, prev_close):
    if prev_close <= 0:
        return 0.0

    gain = ((close_price - prev_close) / prev_close) * 100.0

    return clamp((gain / 3.00) * 100.0)


def score_live_turnover(live_turnover, avg_turnover):
    if avg_turnover <= 0:
        return 0.0

    return clamp(
        live_turnover / (avg_turnover * 2.5) * 100.0
    )


def score_avg_turnover(avg_turnover):
    return clamp(
        avg_turnover / 100000000.0 * 100.0
    )


# =========================================================
# UPSTOX INSTRUMENTS
# =========================================================

def get_instruments():

    set_status("NSE Equity list download हो रही है...")

    r = requests.get(
        INSTRUMENT_URL,
        timeout=40
    )

    r.raise_for_status()

    import gzip
    raw = gzip.decompress(r.content)

    import json
    data = json.loads(raw.decode("utf-8"))

    stocks = []

    for x in data:

        if x.get("segment") != "NSE_EQ":
            continue

        if x.get("instrument_type") != "EQ":
            continue

        key = x.get("instrument_key")
        symbol = x.get("trading_symbol")

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

def get_live_quotes(stocks):

    quotes = {}

    total = len(stocks)

    for start in range(0, total, 500):

        batch = stocks[start:start + 500]

        set_status(
            f"Live market data: "
            f"{min(start + 500, total)}/{total}"
        )

        keys = ",".join(x["key"] for x in batch)

        url = f"{UPSTOX_BASE}/v2/market-quote/quotes"

        # V2 fallback is intentionally NOT used.
        # Use V3 endpoint below.
        url = f"{UPSTOX_BASE}/v3/market-quote/quotes"

        try:
            r = requests.get(
                url,
                headers=HEADERS,
                params={"instrument_key": keys},
                timeout=35
            )

            if r.status_code != 200:
                raise RuntimeError(
                    f"Upstox quote error {r.status_code}: "
                    f"{r.text[:300]}"
                )

            payload = r.json()
            data = payload.get("data", {})

            for response_key, q in data.items():

                if not isinstance(q, dict):
                    continue

                instrument_key = q.get("instrument_token")

                quotes[response_key] = q

                if instrument_key:
                    quotes[instrument_key] = q

            time.sleep(0.12)

        except Exception as e:
            raise RuntimeError(
                f"Live quote batch failed: {e}"
            )

    return quotes


# =========================================================
# NSE BHAVCOPY
# =========================================================

def download_bhavcopy(session, date_obj):

    date_str = date_obj.strftime("%Y%m%d")

    url = NSE_BHAV_URL.format(date=date_str)

    try:

        r = session.get(
            url,
            headers=NSE_HEADERS,
            timeout=25
        )

        if r.status_code != 200:
            return None

        if len(r.content) < 1000:
            return None

        return r.content

    except Exception:
        return None


def parse_bhavcopy(content):

    result = {}

    try:

        with zipfile.ZipFile(io.BytesIO(content)) as z:

            names = z.namelist()

            csv_name = None

            for name in names:
                if name.lower().endswith(".csv"):
                    csv_name = name
                    break

            if not csv_name:
                return result

            with z.open(csv_name) as f:

                text = io.TextIOWrapper(
                    f,
                    encoding="utf-8-sig",
                    errors="replace"
                )

                reader = csv.DictReader(text)

                if not reader.fieldnames:
                    return result

                headers = [
                    str(x).strip()
                    for x in reader.fieldnames
                ]

                # Actual UDiFF names
                sym_col = (
                    "TckrSymb"
                    if "TckrSymb" in headers
                    else "SYMBOL"
                )

                series_col = (
                    "SctySrs"
                    if "SctySrs" in headers
                    else "SERIES"
                )

                turnover_col = None

                for c in [
                    "TtlTrfVal",
                    "TOTTRDVAL",
                    "TtlTrdVal",
                    "TURNOVER"
                ]:
                    if c in headers:
                        turnover_col = c
                        break

                if not turnover_col:
                    return result

                for row in reader:

                    symbol = str(
                        row.get(sym_col, "")
                    ).strip().upper()

                    series = str(
                        row.get(series_col, "")
                    ).strip().upper()

                    if not symbol:
                        continue

                    # ONLY NSE EQ
                    if series != "EQ":
                        continue

                    raw_turnover = str(
                        row.get(turnover_col, "")
                    ).strip()

                    try:
                        turnover = float(
                            raw_turnover.replace(",", "")
                        )
                    except Exception:
                        continue

                    if turnover <= 0:
                        continue

                    result[symbol] = turnover

    except Exception:
        return {}

    return result


# =========================================================
# 20 DAY AVERAGE TURNOVER
# =========================================================

def get_average_turnover():

    today = now_ist().date()

    # Use today's already calculated cache.
    if (
        TURNOVER_CACHE["date"] == today
        and TURNOVER_CACHE["data"]
    ):
        set_status(
            "20-Day turnover cache से लिया जा रहा है..."
        )
        return TURNOVER_CACHE["data"]

    session = requests.Session()

    sums = {}
    counts = {}

    found_days = 0
    checked_days = 0

    date_cursor = today - timedelta(days=1)

    set_status(
        "20-Day NSE turnover data तैयार हो रहा है..."
    )

    while found_days < LIQUIDITY_DAYS:

        # Safety limit
        if checked_days > 60:
            break

        checked_days += 1

        data = download_bhavcopy(
            session,
            date_cursor
        )

        if data:

            parsed = parse_bhavcopy(data)

            if parsed:

                found_days += 1

                set_status(
                    f"20-Day turnover: "
                    f"{found_days}/{LIQUIDITY_DAYS} trading days"
                )

                for symbol, turnover in parsed.items():

                    sums[symbol] = (
                        sums.get(symbol, 0.0)
                        + turnover
                    )

                    counts[symbol] = (
                        counts.get(symbol, 0)
                        + 1
                    )

        date_cursor -= timedelta(days=1)

        time.sleep(0.08)

    if found_days < LIQUIDITY_DAYS:

        raise RuntimeError(
            f"NSE से केवल {found_days} valid trading days "
            f"मिले, {LIQUIDITY_DAYS} चाहिए।"
        )

    averages = {}

    for symbol, total in sums.items():

        if counts.get(symbol, 0) >= LIQUIDITY_DAYS:

            averages[symbol] = (
                total / LIQUIDITY_DAYS
            )

    TURNOVER_CACHE["date"] = today
    TURNOVER_CACHE["data"] = averages

    return averages


# =========================================================
# MAIN SCAN
# =========================================================

def perform_scan():

    with LOCK:
        STATE["running"] = True
        STATE["results"] = []
        STATE["error"] = None
        STATE["last_scan"] = None
        STATE["status"] = "Scan शुरू हो रहा है..."

    try:

        if not UPSTOX_TOKEN:
            raise RuntimeError(
                "UPSTOX_ACCESS_TOKEN Render Environment "
                "में नहीं मिला।"
            )

        # ---------------------------------------------
        # 1. NSE EQUITY LIST
        # ---------------------------------------------

        stocks = get_instruments()

        if not stocks:
            raise RuntimeError(
                "NSE Equity instruments नहीं मिले।"
            )

        # ---------------------------------------------
        # 2. LIVE QUOTES
        # ---------------------------------------------

        quotes = get_live_quotes(stocks)

        if not quotes:
            raise RuntimeError(
                "Upstox से Live Quotes नहीं मिले।"
            )

        # ---------------------------------------------
        # 3. 20 DAY TURNOVER
        # ---------------------------------------------

        avg_turnover = get_average_turnover()

        if not avg_turnover:
            raise RuntimeError(
                "20-Day Average Turnover data नहीं मिला।"
            )

        # ---------------------------------------------
        # 4. CALCULATE RANK
        # ---------------------------------------------

        set_status(
            "Strength Score calculate हो रहा है..."
        )

        results = []

        for stock in stocks:

            symbol = stock["symbol"]
            key = stock["key"]

            q = quotes.get(key)

            if not q:
                q = quotes.get(symbol)

            if not q:
                continue

            # -----------------------------
            # LIVE VALUES
            # -----------------------------

            ltp = q.get("last_price")

            prev_close = q.get(
                "prev_close_price"
            )

            volume = q.get("volume", 0)

            ohlc = q.get("ohlc", {}) or {}

            open_price = ohlc.get("open")
            low_price = ohlc.get("low")

            try:
                ltp = float(ltp)
                prev_close = float(prev_close)
                volume = float(volume or 0)
                open_price = float(open_price)
                low_price = float(low_price)
            except Exception:
                continue

            # -----------------------------
            # BASIC FILTERS
            # -----------------------------

            if ltp < MIN_PRICE:
                continue

            average_turnover = (
                avg_turnover.get(symbol)
            )

            if not average_turnover:
                continue

            if average_turnover < MIN_AVG_TURNOVER:
                continue

            if open_price <= 0:
                continue

            if low_price <= 0:
                continue

            if prev_close <= 0:
                continue

            # -----------------------------
            # SCORES
            # -----------------------------

            gap_score = score_gap(
                open_price,
                low_price
            )

            recovery_score = score_recovery(
                ltp,
                low_price
            )

            gain_score = score_gain(
                ltp,
                prev_close
            )

            live_turnover = volume * ltp

            live_turnover_score = (
                score_live_turnover(
                    live_turnover,
                    average_turnover
                )
            )

            avg_turnover_score = (
                score_avg_turnover(
                    average_turnover
                )
            )

            # -----------------------------
            # FINAL WEIGHTED SCORE
            # -----------------------------

            strength = (
                gap_score * W_GAP
                + recovery_score * W_RECOVERY
                + gain_score * W_GAIN
                + live_turnover_score * W_LIVE
                + avg_turnover_score * W_AVG
            )

            results.append({
                "symbol": symbol,
                "strength": round(strength, 2),
                "ltp": round(ltp, 2),
                "open": round(open_price, 2),
                "low": round(low_price, 2),
                "gain": round(
                    ((ltp - prev_close) /
                     prev_close) * 100.0,
                    2
                ),
                "avg_turnover": round(
                    average_turnover,
                    0
                ),
                "gap_score": round(
                    gap_score,
                    2
                ),
                "recovery_score": round(
                    recovery_score,
                    2
                ),
                "gain_score": round(
                    gain_score,
                    2
                ),
                "live_score": round(
                    live_turnover_score,
                    2
                ),
                "avg_score": round(
                    avg_turnover_score,
                    2
                )
            })

        # ---------------------------------------------
        # SORT STRONGEST FIRST
        # ---------------------------------------------

        results.sort(
            key=lambda x: x["strength"],
            reverse=True
        )

        # Add rank
        for i, item in enumerate(results, 1):
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
# ROUTES
# =========================================================

@app.route("/")
def home():

    return render_template_string("""
<!DOCTYPE html>
<html lang="hi">
<head>
<meta charset="UTF-8">
<meta name="viewport"
content="width=device-width, initial-scale=1.0">

<title>Early Strength Rank Scanner</title>

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

.header{
    background:#151d26;
    border:1px solid #27313c;
    border-radius:18px;
    padding:22px;
    margin-bottom:20px;
}

h1{
    margin:0 0 8px 0;
    font-size:28px;
}

.subtitle{
    color:#9ca8b5;
    font-size:17px;
}

.panel{
    background:#151d26;
    border:1px solid #27313c;
    border-radius:18px;
    padding:20px;
    margin-bottom:18px;
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
    min-width:620px;
}

th,td{
    padding:12px 10px;
    border-bottom:1px solid #27313c;
    text-align:left;
}

th{
    color:#aeb8c3;
}

.rank{
    font-weight:bold;
}

.error{
    color:#ff8d8d;
    background:#321b1b;
    padding:12px;
    border-radius:10px;
    margin-top:12px;
}

.small{
    color:#9ca8b5;
    font-size:13px;
    margin-top:10px;
}

@media(max-width:600px){

    h1{
        font-size:25px;
    }

    .weights{
        grid-template-columns:1fr 1fr;
    }

    .panel{
        padding:14px;
    }

}

</style>
</head>

<body>

<div class="container">

<div class="header">

<h1>Early Strength Rank Scanner</h1>

<div class="subtitle">
NSE EQ • Live Strength Ranking • Strongest First
</div>

</div>


<div class="panel">

<div id="status" class="status">
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
Results: <span id="count">0</span>
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

<h2>Ranking Logic</h2>

<p>• NSE Equity shares only</p>
<p>• Price ≥ ₹50</p>
<p>• Average 20-Day Turnover ≥ ₹10 Crore</p>
<p>• Open-Low Gap = 5%</p>
<p>• Recovery = 20%</p>
<p>• Gain = 35%</p>
<p>• Live Turnover = 30%</p>
<p>• Average Turnover = 10%</p>

</div>

</div>


<script>

let timer = null;

async function loadResults(){

    try{

        const r = await fetch(
            "/api/results",
            {cache:"no-store"}
        );

        const data = await r.json();

        document.getElementById(
            "status"
        ).innerText = data.status || "Ready";

        document.getElementById(
            "lastScan"
        ).innerText = data.last_scan || "--";

        document.getElementById(
            "count"
        ).innerText =
            data.results ? data.results.length : 0;

        const errorBox =
            document.getElementById("error");

        if(data.error){

            errorBox.innerHTML =
                '<div class="error">' +
                data.error +
                '</div>';

        }else{

            errorBox.innerHTML = "";

        }

        const btn =
            document.getElementById("scanBtn");

        btn.disabled = data.running;

        const tbody =
            document.getElementById("tbody");

        if(!data.results ||
           data.results.length === 0){

            tbody.innerHTML =
                '<tr><td colspan="7">' +
                'कोई result नहीं' +
                '</td></tr>';

            return;
        }

        tbody.innerHTML =
            data.results.map(x => `

<tr>

<td class="rank">
${x.rank}
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

`).join("");

    }catch(e){

        document.getElementById(
            "status"
        ).innerText =
            "Server response नहीं मिला";

    }

}


async function startScan(){

    const btn =
        document.getElementById("scanBtn");

    btn.disabled = true;

    document.getElementById(
        "status"
    ).innerText =
        "Scan शुरू हो रहा है...";

    document.getElementById(
        "error"
    ).innerHTML = "";

    try{

        await fetch(
            "/api/scan",
            {method:"POST"}
        );

    }catch(e){

        document.getElementById(
            "status"
        ).innerText =
            "Scan request failed";

    }

}


loadResults();

timer = setInterval(
    loadResults,
    1500
);

</script>

</body>
</html>
""")


@app.route("/api/scan", methods=["POST"])
def api_scan():

    with LOCK:

        if STATE["running"]:
            return jsonify({
                "ok": True,
                "message": "Scan already running"
            })

    thread = threading.Thread(
        target=perform_scan,
        daemon=True
    )

    thread.start()

    return jsonify({
        "ok": True
    })


@app.route("/api/results")
def api_results():

    with LOCK:

        return jsonify({
            "running": STATE["running"],
            "status": STATE["status"],
            "last_scan": STATE["last_scan"],
            "results": STATE["results"],
            "error": STATE["error"]
        })


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv("PORT", "5000")
        )
    )
