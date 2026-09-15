from flask import Flask, jsonify, render_template_string
import requests, os, json, gzip, threading, time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
BASE = "https://api.upstox.com"
INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"

MIN_PRICE = 50.0
TOP_HISTORY = 250
DAYS = 20

WGAP = 0.05
WREC = 0.20
WGAIN = 0.35
WLIVE = 0.30
WAVG = 0.10

IST = ZoneInfo("Asia/Kolkata")

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {TOKEN}"
}

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

# Same-day cache.
HISTORY_CACHE = {
    "date": None,
    "data": {}
}


def now():
    return datetime.now(IST)


def status(text):
    with LOCK:
        STATE["status"] = text


def progress(done, total):
    with LOCK:
        STATE["progress"] = done
        STATE["total"] = total


def clamp(x):
    return max(0.0, min(100.0, x))


# ---------------------------------------------------------
# SCORE FUNCTIONS
# ---------------------------------------------------------

def gap_score(o, l):
    if o <= 0:
        return 0
    gap = ((o - l) / o) * 100
    return clamp((1 - gap / 0.50) * 100)


def recovery_score(c, l):
    if l <= 0:
        return 0
    recovery = ((c - l) / l) * 100
    return clamp((recovery / 1.50) * 100)


def gain_score(c, pc):
    if pc <= 0:
        return 0
    gain = ((c - pc) / pc) * 100
    return clamp((gain / 3.00) * 100)


def live_score(live_turnover, avg_turnover):
    if avg_turnover <= 0:
        return 0
    return clamp(
        live_turnover /
        (avg_turnover * 2.5) * 100
    )


def avg_score(avg_turnover):
    return clamp(
        avg_turnover /
        100000000 * 100
    )


# ---------------------------------------------------------
# INSTRUMENTS
# ---------------------------------------------------------

def instruments():

    status("Upstox NSE Equity list download हो रही है...")

    r = requests.get(
        INSTRUMENT_URL,
        timeout=60
    )

    if r.status_code != 200:
        raise RuntimeError(
            f"Instrument list error {r.status_code}"
        )

    raw = gzip.decompress(r.content)
    data = json.loads(raw.decode("utf-8"))

    stocks = []

    for x in data:

        if x.get("segment") != "NSE_EQ":
            continue

        if x.get("instrument_type") != "EQ":
            continue

        key = x.get("instrument_key")
        symbol = x.get("trading_symbol")

        if key and symbol:
            stocks.append({
                "key": key,
                "symbol": symbol
            })

    if not stocks:
        raise RuntimeError(
            "NSE EQ instruments नहीं मिले"
        )

    return stocks


# ---------------------------------------------------------
# LIVE QUOTES
# ---------------------------------------------------------

def live_quotes(stocks):

    result = {}
    total = len(stocks)

    for start in range(0, total, 500):

        batch = stocks[start:start + 500]

        status(
            f"Live market data: "
            f"{min(start + 500,total)}/{total}"
        )

        keys = ",".join(
            x["key"] for x in batch
        )

        url = (
            BASE +
            "/v3/market-quote/quotes"
        )

        for attempt in range(4):

            r = requests.get(
                url,
                headers=HEADERS,
                params={
                    "instrument_key": keys
                },
                timeout=45
            )

            if r.status_code == 200:
                data = r.json().get(
                    "data", {}
                )

                result.update(data)
                break

            if r.status_code == 429:
                time.sleep(5 + attempt * 5)
                continue

            raise RuntimeError(
                f"Upstox quote error "
                f"{r.status_code}: "
                f"{r.text[:250]}"
            )

        time.sleep(0.15)

    if not result:
        raise RuntimeError(
            "Live quotes नहीं मिले"
        )

    return result


# ---------------------------------------------------------
# HISTORICAL 20 DAY DATA
# ---------------------------------------------------------

def history_one(stock):

    key = stock["key"]
    symbol = stock["symbol"]

    today = now().date()

    to_date = today - timedelta(days=1)
    from_date = today - timedelta(days=55)

    encoded = quote(
        key,
        safe=""
    )

    url = (
        BASE +
        "/v3/historical-candle/" +
        encoded +
        "/days/1/" +
        to_date.strftime("%Y-%m-%d") +
        "/" +
        from_date.strftime("%Y-%m-%d")
    )

    try:

        for attempt in range(4):

            r = requests.get(
                url,
                headers=HEADERS,
                timeout=40
            )

            if r.status_code == 200:

                candles = (
                    r.json()
                    .get("data", {})
                    .get("candles", [])
                )

                rows = []

                for candle in candles:

                    if len(candle) < 6:
                        continue

                    try:

                        close = float(candle[4])
                        volume = float(candle[5])

                        if close > 0 and volume > 0:

                            rows.append(
                                close * volume
                            )

                    except:
                        continue

                if len(rows) >= DAYS:

                    rows = rows[:DAYS]

                    average = (
                        sum(rows) / DAYS
                    )

                    return symbol, average

                return symbol, None

            if r.status_code == 429:

                time.sleep(
                    5 + attempt * 5
                )

                continue

            return symbol, None

    except Exception:

        return symbol, None

    return symbol, None


def historical_top(candidates):

    today = now().date()

    if (
        HISTORY_CACHE["date"] == today
        and HISTORY_CACHE["data"]
    ):

        status(
            "20-Day turnover cache से लिया जा रहा है..."
        )

        return HISTORY_CACHE["data"]

    total = len(candidates)

    status(
        f"Top {total} shares का 20-Day data..."
    )

    result = {}
    done = 0

    # Six parallel workers.
    # This keeps request speed high while
    # staying around normal API capacity.
    with ThreadPoolExecutor(
        max_workers=6
    ) as executor:

        futures = [
            executor.submit(
                history_one,
                stock
            )
            for stock in candidates
        ]

        for future in as_completed(
            futures
        ):

            symbol, average = (
                future.result()
            )

            if average:
                result[symbol] = average

            done += 1

            progress(
                done,
                total
            )

            status(
                f"20-Day turnover: "
                f"{done}/{total} • {symbol}"
            )

    if not result:
        raise RuntimeError(
            "20-Day historical data नहीं मिला"
        )

    HISTORY_CACHE["date"] = today
    HISTORY_CACHE["data"] = result

    return result


# ---------------------------------------------------------
# MAIN SCAN
# ---------------------------------------------------------

def scan():

    with LOCK:

        STATE["running"] = True
        STATE["status"] = "Scan शुरू हो रहा है..."
        STATE["results"] = []
        STATE["error"] = None
        STATE["last_scan"] = None
        STATE["progress"] = 0
        STATE["total"] = 0

    try:

        if not TOKEN:

            raise RuntimeError(
                "UPSTOX_ACCESS_TOKEN Render Environment में नहीं मिला"
            )

        # 1. Instruments
        stocks = instruments()

        # 2. Live quotes
        quotes = live_quotes(stocks)

        # -------------------------------------------------
        # 3. PRELIMINARY SCORE
        # -------------------------------------------------

        status(
            "Fast preliminary ranking बन रही है..."
        )

        preliminary = []

        for stock in stocks:

            symbol = stock["symbol"]

            q = quotes.get(
                "NSE_EQ:" + symbol
            )

            if not q:
                continue

            try:

                ltp = float(
                    q["last_price"]
                )

                pc = float(
                    q["prev_close_price"]
                )

                ohlc = q.get(
                    "ohlc", {}
                ) or {}

                op = float(
                    ohlc["open"]
                )

                low = float(
                    ohlc["low"]
                )

                vol = float(
                    q.get(
                        "volume",
                        0
                    )
                )

            except:

                continue

            if ltp < MIN_PRICE:
                continue

            if op <= 0 or low <= 0 or pc <= 0:
                continue

            sg = gap_score(
                op,
                low
            )

            sr = recovery_score(
                ltp,
                low
            )

            sgain = gain_score(
                ltp,
                pc
            )

            # Average-turnover component is
            # temporarily neutral.
            #
            # It will be added after historical
            # data is obtained for the strongest
            # candidates.

            live_turnover = vol * ltp

            # Preliminary score = 90%
            preliminary_score = (
                sg * WGAP
                + sr * WREC
                + sgain * WGAIN
                + clamp(
                    live_turnover /
                    250000000 * 100
                ) * WLIVE
            )

            preliminary.append({

                "stock": stock,

                "symbol": symbol,

                "ltp": ltp,

                "pc": pc,

                "open": op,

                "low": low,

                "volume": vol,

                "gap": sg,

                "recovery": sr,

                "gain": sgain,

                "live_turnover":
                    live_turnover,

                "preliminary":
                    preliminary_score
            })

        if not preliminary:

            raise RuntimeError(
                "₹50+ वाले live candidates नहीं मिले"
            )

        preliminary.sort(
            key=lambda x:
                x["preliminary"],
            reverse=True
        )

        # -------------------------------------------------
        # 4. ONLY TOP 250 HISTORICAL DATA
        # -------------------------------------------------

        top = preliminary[
            :TOP_HISTORY
        ]

        candidates = [
            x["stock"]
            for x in top
        ]

        average_turnovers = (
            historical_top(
                candidates
            )
        )

        # -------------------------------------------------
        # 5. FINAL SCORE
        # -------------------------------------------------

        status(
            "Final Strength Ranking तैयार हो रही है..."
        )

        results = []

        for x in top:

            symbol = x["symbol"]

            average = (
                average_turnovers.get(
                    symbol
                )
            )

            if not average:
                continue

            savg = avg_score(
                average
            )

            slive = live_score(
                x["live_turnover"],
                average
            )

            final_score = (

                x["gap"]
                * WGAP

                + x["recovery"]
                * WREC

                + x["gain"]
                * WGAIN

                + slive
                * WLIVE

                + savg
                * WAVG
            )

            gain_percent = (
                (x["ltp"] - x["pc"])
                / x["pc"]
            ) * 100

            results.append({

                "symbol":
                    symbol,

                "strength":
                    round(
                        final_score,
                        2
                    ),

                "ltp":
                    round(
                        x["ltp"],
                        2
                    ),

                "open":
                    round(
                        x["open"],
                        2
                    ),

                "low":
                    round(
                        x["low"],
                        2
                    ),

                "gain":
                    round(
                        gain_percent,
                        2
                    ),

                "avg_turnover":
                    round(
                        average,
                        0
                    )
            })

        results.sort(
            key=lambda x:
                x["strength"],
            reverse=True
        )

        for i, x in enumerate(
            results,
            1
        ):

            x["rank"] = i

        with LOCK:

            STATE["results"] = results

            STATE["last_scan"] = (
                now().strftime(
                    "%d-%m-%Y %H:%M:%S"
                )
            )

            STATE["status"] = (
                f"Scan complete — "
                f"{len(results)} stocks found"
            )

            STATE["progress"] = (
                len(candidates)
            )

            STATE["total"] = (
                len(candidates)
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


# ---------------------------------------------------------
# HTML
# ---------------------------------------------------------

HTML = """
<!DOCTYPE html>
<html lang="hi">

<head>

<meta charset="UTF-8">

<meta name="viewport"
content="width=device-width,initial-scale=1.0">

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

.header,.panel{
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
transition:width .3s;
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

<div id="bar"
class="progressbar">
</div>

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
<p>• Open-Low Gap = 5%</p>
<p>• Recovery = 20%</p>
<p>• Gain = 35%</p>
<p>• Live Turnover = 30%</p>
<p>• Average Turnover = 10%</p>

<p class="note">
Fast scan में पहले strongest live candidates
निकाले जाते हैं। 20-Day Average Turnover
केवल Top 250 candidates पर calculate होता है,
जिससे scanner बहुत तेज चलता है।
</p>

</div>

</div>


<script>

async function load(){

try{

const r=await fetch(
"/api/results",
{cache:"no-store"}
);

const d=await r.json();

document.getElementById(
"status"
).innerText=
d.status||"Ready";

document.getElementById(
"lastScan"
).innerText=
d.last_scan||"--";

document.getElementById(
"count"
).innerText=
d.results?d.results.length:0;

document.getElementById(
"scanBtn"
).disabled=d.running;

if(d.error){

document.getElementById(
"error"
).innerHTML=
'<div class="error">'+
d.error+
'</div>';

}else{

document.getElementById(
"error"
).innerHTML="";
}

let p=0;

if(d.total>0){

p=(d.progress/d.total)*100;

}

document.getElementById(
"bar"
).style.width=
p+"%";


const tbody=
document.getElementById(
"tbody"
);

if(!d.results||
d.results.length===0){

tbody.innerHTML=
'<tr><td colspan="7">'+
'कोई result नहीं'+
'</td></tr>';

return;
}

tbody.innerHTML=
d.results.map(x=>`

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

`).join("");

}catch(e){

document.getElementById(
"status"
).innerText=
"Server response नहीं मिला";

}

}


async function startScan(){

document.getElementById(
"scanBtn"
).disabled=true;

document.getElementById(
"status"
).innerText=
"Scan शुरू हो रहा है...";

document.getElementById(
"error"
).innerHTML="";

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
).innerText=
"Scan request failed";

}

}


load();

setInterval(
load,
1500
);

</script>

</body>

</html>
"""


# ---------------------------------------------------------
# ROUTES
# ---------------------------------------------------------

@app.route("/")
def home():
    return render_template_string(HTML)


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

    threading.Thread(
        target=scan,
        daemon=True
    ).start()

    return jsonify({
        "ok": True
    })


@app.route("/api/results")
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
