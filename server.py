from flask import Flask, jsonify, render_template_string
import requests
import os
import json
import gzip
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote

app = Flask(__name__)

# =========================================================
# SETTINGS
# =========================================================

UPSTOX_TOKEN = os.getenv(
    "UPSTOX_ACCESS_TOKEN", ""
).strip()

BASE_URL = "https://api.upstox.com"

INSTRUMENT_URL = (
    "https://assets.upstox.com/"
    "market-quote/instruments/"
    "exchange/complete.json.gz"
)

MIN_PRICE = 50.0

LIQUIDITY_DAYS = 20

# FINAL WEIGHTS
GAP_WEIGHT = 0.05
RECOVERY_WEIGHT = 0.20
GAIN_WEIGHT = 0.35
LIVE_TURNOVER_WEIGHT = 0.30
AVERAGE_TURNOVER_WEIGHT = 0.10

IST = ZoneInfo("Asia/Kolkata")

HEADERS = {
    "Accept": "application/json",
    "Authorization":
        f"Bearer {UPSTOX_TOKEN}"
}

# Keep requests comfortably below
# Upstox standard API rate limits.
HISTORY_DELAY = 0.20

# Retry settings for temporary API problems.
MAX_RETRIES = 4

# =========================================================
# STATE
# =========================================================

STATE = {
    "running": False,
    "status": "Ready",
    "last_scan": None,
    "results": [],
    "error": None,
    "progress": 0,
    "total": 0
}

LOCK = threading.Lock()

# Same-day average turnover cache.
TURNOVER_CACHE = {
    "date": None,
    "data": {}
}


# =========================================================
# BASIC
# =========================================================

def now_ist():
    return datetime.now(IST)


def set_status(text):
    with LOCK:
        STATE["status"] = text


def set_progress(done, total):
    with LOCK:
        STATE["progress"] = done
        STATE["total"] = total


def clamp(value, low=0.0, high=100.0):
    return max(low, min(high, value))


# =========================================================
# SCORE FUNCTIONS
# =========================================================

def gap_score(open_price, low_price):

    if open_price <= 0:
        return 0.0

    gap = (
        (open_price - low_price)
        / open_price
    ) * 100.0

    return clamp(
        (1.0 - gap / 0.50) * 100.0
    )


def recovery_score(
    current_price,
    low_price
):

    if low_price <= 0:
        return 0.0

    recovery = (
        (current_price - low_price)
        / low_price
    ) * 100.0

    return clamp(
        (recovery / 1.50) * 100.0
    )


def gain_score(
    current_price,
    previous_close
):

    if previous_close <= 0:
        return 0.0

    gain = (
        (current_price - previous_close)
        / previous_close
    ) * 100.0

    return clamp(
        (gain / 3.00) * 100.0
    )


def live_turnover_score(
    live_turnover,
    average_turnover
):

    if average_turnover <= 0:
        return 0.0

    return clamp(
        (
            live_turnover
            / (average_turnover * 2.5)
        ) * 100.0
    )


def average_turnover_score(
    average_turnover
):

    return clamp(
        (
            average_turnover
            / 100000000.0
        ) * 100.0
    )


# =========================================================
# UPSTOX INSTRUMENTS
# =========================================================

def load_instruments():

    set_status(
        "NSE Equity list Upstox से आ रही है..."
    )

    r = requests.get(
        INSTRUMENT_URL,
        timeout=60
    )

    if r.status_code != 200:

        raise RuntimeError(
            "Instrument list error "
            f"{r.status_code}"
        )

    try:

        raw = gzip.decompress(
            r.content
        )

        data = json.loads(
            raw.decode("utf-8")
        )

    except Exception as e:

        raise RuntimeError(
            "Instrument list पढ़ने में error: "
            + str(e)
        )

    stocks = []

    for item in data:

        if item.get("segment") != "NSE_EQ":
            continue

        if item.get("instrument_type") != "EQ":
            continue

        key = item.get(
            "instrument_key"
        )

        symbol = item.get(
            "trading_symbol"
        )

        if not key or not symbol:
            continue

        stocks.append({
            "key": key,
            "symbol": symbol
        })

    if not stocks:

        raise RuntimeError(
            "NSE EQ instruments नहीं मिले."
        )

    return stocks


# =========================================================
# LIVE QUOTES
# =========================================================

def get_live_quotes(stocks):

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

        end_number = min(
            start + 500,
            total
        )

        set_status(
            f"Live data: "
            f"{end_number}/{total}"
        )

        keys = ",".join(
            x["key"]
            for x in batch
        )

        url = (
            BASE_URL
            + "/v3/market-quote/quotes"
        )

        success = False

        for attempt in range(
            MAX_RETRIES
        ):

            try:

                response = requests.get(
                    url,
                    headers=HEADERS,
                    params={
                        "instrument_key": keys
                    },
                    timeout=45
                )

                if response.status_code == 200:

                    payload = response.json()

                    data = payload.get(
                        "data",
                        {}
                    )

                    for response_key, value in (
                        data.items()
                    ):

                        if isinstance(
                            value,
                            dict
                        ):

                            quotes[
                                response_key
                            ] = value

                    success = True
                    break

                if response.status_code == 429:

                    wait = 5 + (
                        attempt * 5
                    )

                    set_status(
                        "Upstox rate limit — "
                        f"{wait} sec wait..."
                    )

                    time.sleep(wait)
                    continue

                raise RuntimeError(
                    "Upstox quote error "
                    f"{response.status_code}: "
                    f"{response.text[:300]}"
                )

            except requests.RequestException as e:

                if attempt == (
                    MAX_RETRIES - 1
                ):

                    raise RuntimeError(
                        "Upstox connection error: "
                        + str(e)
                    )

                time.sleep(
                    3 + attempt * 2
                )

        if not success:

            raise RuntimeError(
                "Live quote batch complete "
                "नहीं हो पाया."
            )

        time.sleep(0.15)

    if not quotes:

        raise RuntimeError(
            "Upstox से कोई live quote नहीं मिला."
        )

    return quotes


# =========================================================
# HISTORICAL DAILY DATA
# =========================================================

def get_history_for_stock(
    instrument_key
):

    today = now_ist().date()

    # We deliberately go back enough days
    # to collect at least 20 trading sessions.
    to_date = (
        today - timedelta(days=1)
    )

    from_date = (
        today - timedelta(days=55)
    )

    key_encoded = quote(
        instrument_key,
        safe=""
    )

    url = (
        BASE_URL
        + "/v3/historical-candle/"
        + key_encoded
        + "/days/1/"
        + to_date.strftime("%Y-%m-%d")
        + "/"
        + from_date.strftime("%Y-%m-%d")
    )

    for attempt in range(
        MAX_RETRIES
    ):

        try:

            response = requests.get(
                url,
                headers=HEADERS,
                timeout=45
            )

            if response.status_code == 200:

                payload = response.json()

                candles = (
                    payload
                    .get("data", {})
                    .get("candles", [])
                )

                return candles

            if response.status_code == 429:

                wait = 5 + (
                    attempt * 5
                )

                time.sleep(wait)
                continue

            # A single stock failure should
            # not stop the complete scanner.
            return []

        except requests.RequestException:

            if attempt == (
                MAX_RETRIES - 1
            ):

                return []

            time.sleep(
                3 + attempt * 2
            )

    return []


# =========================================================
# 20-DAY AVERAGE TURNOVER
# =========================================================

def calculate_average_turnover(
    candles
):

    if not candles:
        return None

    rows = []

    for candle in candles:

        if not isinstance(
            candle,
            list
        ):
            continue

        if len(candle) < 6:
            continue

        try:

            timestamp = candle[0]

            close_price = float(
                candle[4]
            )

            volume = float(
                candle[5]
            )

            if close_price <= 0:
                continue

            if volume <= 0:
                continue

            turnover = (
                close_price * volume
            )

            rows.append({
                "timestamp":
                    timestamp,
                "turnover":
                    turnover
            })

        except Exception:

            continue

    if not rows:
        return None

    # Sort by date newest first.
    rows.sort(
        key=lambda x:
            x["timestamp"],
        reverse=True
    )

    rows = rows[
        :LIQUIDITY_DAYS
    ]

    if len(rows) < LIQUIDITY_DAYS:
        return None

    total = sum(
        x["turnover"]
        for x in rows
    )

    return (
        total
        / LIQUIDITY_DAYS
    )


def get_all_average_turnovers(
    candidates
):

    today = now_ist().date()

    # Use same-day cache.
    if (
        TURNOVER_CACHE["date"] == today
        and TURNOVER_CACHE["data"]
    ):

        set_status(
            "20-Day turnover cache से लिया जा रहा है..."
        )

        return TURNOVER_CACHE["data"]

    result = {}

    total = len(candidates)

    set_status(
        f"20-Day data शुरू: "
        f"0/{total}"
    )

    for index, stock in enumerate(
        candidates,
        1
    ):

        symbol = stock["symbol"]
        key = stock["key"]

        candles = get_history_for_stock(
            key
        )

        average = (
            calculate_average_turnover(
                candles
            )
        )

        if average is not None:

            result[symbol] = average

        set_progress(
            index,
            total
        )

        set_status(
            f"20-Day turnover: "
            f"{index}/{total} • "
            f"{symbol}"
        )

        # Keep well below the
        # 500/minute standard limit.
        time.sleep(
            HISTORY_DELAY
        )

    if not result:

        raise RuntimeError(
            "किसी भी stock का "
            "20-Day historical data नहीं मिला."
        )

    TURNOVER_CACHE["date"] = today
    TURNOVER_CACHE["data"] = result

    return result


# =========================================================
# MAIN SCAN
# =========================================================

def perform_scan():

    with LOCK:

        STATE["running"] = True
        STATE["status"] = (
            "Scan शुरू हो रहा है..."
        )
        STATE["last_scan"] = None
        STATE["results"] = []
        STATE["error"] = None
        STATE["progress"] = 0
        STATE["total"] = 0

    try:

        if not UPSTOX_TOKEN:

            raise RuntimeError(
                "UPSTOX_ACCESS_TOKEN Render "
                "Environment में नहीं मिला."
            )

        # -----------------------------------------
        # 1. INSTRUMENTS
        # -----------------------------------------

        stocks = load_instruments()

        # -----------------------------------------
        # 2. LIVE QUOTES
        # -----------------------------------------

        quotes = get_live_quotes(
            stocks
        )

        # -----------------------------------------
        # 3. FIRST FILTER
        # -----------------------------------------
        #
        # Only Price >= ₹50.
        #
        # Average turnover is NOT a hard filter.
        # It contributes only 10% score.
        #

        candidates = []

        for stock in stocks:

            symbol = stock["symbol"]

            quote_key = (
                "NSE_EQ:"
                + symbol
            )

            q = quotes.get(
                quote_key
            )

            if not q:
                continue

            try:

                ltp = float(
                    q.get(
                        "last_price"
                    )
                )

            except Exception:

                continue

            if ltp < MIN_PRICE:
                continue

            candidates.append(
                stock
            )

        if not candidates:

            raise RuntimeError(
                "₹50 या उससे ऊपर का "
                "कोई NSE EQ stock नहीं मिला."
            )

        set_status(
            f"₹50+ candidates: "
            f"{len(candidates)}"
        )

        # -----------------------------------------
        # 4. 20-DAY HISTORICAL TURNOVER
        # -----------------------------------------

        average_turnovers = (
            get_all_average_turnovers(
                candidates
            )
        )

        # -----------------------------------------
        # 5. FINAL SCORE
        # -----------------------------------------

        set_status(
            "Final Strength Rank calculate हो रही है..."
        )

        results = []

        total_candidates = len(
            candidates
        )

        for index, stock in enumerate(
            candidates,
            1
        ):

            symbol = stock[
                "symbol"
            ]

            quote_key = (
                "NSE_EQ:"
                + symbol
            )

            q = quotes.get(
                quote_key
            )

            if not q:
                continue

            try:

                ltp = float(
                    q.get(
                        "last_price"
                    )
                )

                previous_close = float(
                    q.get(
                        "prev_close_price"
                    )
                )

                ohlc = q.get(
                    "ohlc",
                    {}
                ) or {}

                open_price = float(
                    ohlc.get(
                        "open"
                    )
                )

                low_price = float(
                    ohlc.get(
                        "low"
                    )
                )

                volume = float(
                    q.get(
                        "volume",
                        0
                    ) or 0
                )

            except Exception:

                continue

            average_turnover = (
                average_turnovers.get(
                    symbol
                )
            )

            if not average_turnover:
                continue

            if (
                open_price <= 0
                or low_price <= 0
                or previous_close <= 0
            ):
                continue

            # ---------------------------------
            # COMPONENT SCORES
            # ---------------------------------

            s_gap = gap_score(
                open_price,
                low_price
            )

            s_recovery = recovery_score(
                ltp,
                low_price
            )

            s_gain = gain_score(
                ltp,
                previous_close
            )

            live_turnover = (
                volume * ltp
            )

            s_live = (
                live_turnover_score(
                    live_turnover,
                    average_turnover
                )
            )

            s_average = (
                average_turnover_score(
                    average_turnover
                )
            )

            # ---------------------------------
            # FINAL SCORE
            # ---------------------------------

            strength = (

                s_gap
                * GAP_WEIGHT

                + s_recovery
                * RECOVERY_WEIGHT

                + s_gain
                * GAIN_WEIGHT

                + s_live
                * LIVE_TURNOVER_WEIGHT

                + s_average
                * AVERAGE_TURNOVER_WEIGHT
            )

            gain_percent = (
                (
                    ltp
                    - previous_close
                )
                / previous_close
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

                "gap_score":
                    round(
                        s_gap,
                        2
                    ),

                "recovery_score":
                    round(
                        s_recovery,
                        2
                    ),

                "gain_score":
                    round(
                        s_gain,
                        2
                    ),

                "live_score":
                    round(
                        s_live,
                        2
                    ),

                "average_score":
                    round(
                        s_average,
                        2
                    ),

                "average_turnover":
                    round(
                        average_turnover,
                        0
                    )
            })

            set_progress(
                index,
                total_candidates
            )

        # -----------------------------------------
        # 6. SORT
        # -----------------------------------------

        results.sort(
            key=lambda x:
                x["strength"],
            reverse=True
        )

        # Add rank
        for rank, item in enumerate(
            results,
            1
        ):

            item["rank"] = rank

        # -----------------------------------------
        # 7. SAVE
        # -----------------------------------------

        with LOCK:

            STATE["results"] = results

            STATE["last_scan"] = (
                now_ist().strftime(
                    "%d-%m-%Y %H:%M:%S"
                )
            )

            STATE["status"] = (
                "Scan complete — "
                f"{len(results)} stocks found"
            )

            STATE["progress"] = (
                total_candidates
            )

            STATE["total"] = (
                total_candidates
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

HTML = r"""
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

.progress{
height:8px;
background:#27313c;
border-radius:10px;
overflow:hidden;
margin-top:12px;
}

.progressbar{
height:100%;
width:0%;
background:#3b82f6;
transition:width .4s;
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
min-width:700px;
}

th,
td{
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

.note{
color:#9ca8b5;
font-size:14px;
line-height:1.5;
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

<div class="progress">

<div id="progressbar"
class="progressbar">
</div>

</div>

<br>

<button
id="scanBtn"
onclick="startScan()">

Scan Now

</button>

<div id="error"></div>

</div>


<div class="weights">


<div class="box">

<div class="label">
Price
</div>

<div class="value">
≥ ₹50
</div>

</div>


<div class="box">

<div class="label">
Gap Weight
</div>

<div class="value">
5%
</div>

</div>


<div class="box">

<div class="label">
Recovery
</div>

<div class="value">
20%
</div>

</div>


<div class="box">

<div class="label">
Gain
</div>

<div class="value">
35%
</div>

</div>


<div class="box">

<div class="label">
Live Turnover
</div>

<div class="value">
30%
</div>

</div>


<div class="box">

<div class="label">
Average Turnover
</div>

<div class="value">
10%
</div>

</div>


</div>


<div class="panel">

<div class="results-title">

Results:
<span id="count">
0
</span>

</div>


<div class="table-wrap">

<table>

<thead>

<tr>

<th>
Rank
</th>

<th>
Share
</th>

<th>
Strength
</th>

<th>
LTP
</th>

<th>
Open
</th>

<th>
Low
</th>

<th>
Gain
</th>

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

<p class="note">
20-Day Average Turnover अब अलग hard filter नहीं है।
यह केवल 10% score component है।
</p>

</div>


</div>


<script>

async function loadResults(){

try{

const response =
await fetch(
"/api/results",
{
cache:"no-store"
}
);

const data =
await response.json();


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
+
data.error
+
'</div>';

}else{

errorBox.innerHTML =
"";

}


const button =
document.getElementById(
"scanBtn"
);

button.disabled =
data.running;


let percent = 0;

if(
data.total &&
data.total > 0
){

percent =
(
data.progress
/
data.total
) * 100;

}

document.getElementById(
"progressbar"
).style.width =
percent + "%";


const tbody =
document.getElementById(
"tbody"
);


if(
!data.results ||
data.results.length === 0
){

tbody.innerHTML =
'<tr>' +
'<td colspan="7">' +
'कोई result नहीं' +
'</td>' +
'</tr>';

return;

}


tbody.innerHTML =
data.results.map(
x => `

<tr>

<td>
<strong>
${x.rank}
</strong>
</td>

<td>
<strong>
${x.symbol}
</strong>
</td>

<td>
<strong>
${x.strength}
</strong>
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


}catch(error){

document.getElementById(
"status"
).innerText =
"Server response नहीं मिला.";

}

}


async function startScan(){

const button =
document.getElementById(
"scanBtn"
);

button.disabled = true;


document.getElementById(
"status"
).innerText =
"Scan शुरू हो रहा है...";


document.getElementById(
"error"
).innerHTML =
"";


try{

await fetch(
"/api/scan",
{
method:"POST"
}
);

}catch(error){

document.getElementById(
"status"
).innerText =
"Scan request failed.";

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
                STATE["error"],

            "progress":
                STATE["progress"],

            "total":
                STATE["total"]
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
