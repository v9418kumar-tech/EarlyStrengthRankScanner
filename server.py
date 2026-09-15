from flask import Flask, jsonify, render_template_string
import requests
import gzip
import json
import os
import time
import threading
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

# ============================================================
# SETTINGS
# ============================================================

UPSTOX_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()

BASE = "https://api.upstox.com"
INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"

MIN_PRICE = 50.0
LIQUIDITY_DAYS = 20
MAX_HIST_DAYS = 50

# Minimum average turnover filter
MIN_AVG_TURNOVER = 100_000_000.0   # ₹10 Crore

# ============================================================
# FINAL WEIGHTS
# ============================================================

WEIGHT_GAP = 0.05
WEIGHT_RECOVERY = 0.20
WEIGHT_GAIN = 0.35
WEIGHT_LIVE = 0.30
WEIGHT_AVG = 0.10

# ============================================================
# GLOBAL DATA
# ============================================================

INSTRUMENTS = []
BY_KEY = {}

LIVE_RESULTS = []
LAST_SCAN_TIME = "--"
LAST_SCAN_ERROR = ""
SCAN_RUNNING = False
SCAN_LOCK = threading.Lock()

HIST_CACHE = {}
CACHE_DATE = ""


# ============================================================
# HELPERS
# ============================================================

def ist_now():
    return datetime.now(timezone.utc).astimezone(
        timezone(timedelta(hours=5, minutes=30))
    )


def headers():
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {UPSTOX_TOKEN}"
    }


def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def log(msg):
    print(f"[{ist_now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ============================================================
# LOAD NSE EQUITY INSTRUMENTS
# ============================================================

def load_instruments():
    global INSTRUMENTS, BY_KEY

    if INSTRUMENTS:
        return

    log("Downloading Upstox complete instrument list...")

    r = requests.get(INSTRUMENT_URL, timeout=30)
    r.raise_for_status()

    data = json.loads(gzip.decompress(r.content).decode("utf-8"))

    selected = []

    for x in data:
        try:
            if x.get("segment") != "NSE_EQ":
                continue

            if x.get("instrument_type") != "EQ":
                continue

            key = x.get("instrument_key")
            symbol = x.get("trading_symbol")

            if not key or not symbol:
                continue

            selected.append({
                "instrument_key": key,
                "trading_symbol": symbol,
                "name": x.get("name", "")
            })

        except Exception:
            continue

    INSTRUMENTS = selected
    BY_KEY = {x["instrument_key"]: x for x in selected}

    log(f"NSE EQ instruments loaded: {len(INSTRUMENTS)}")


# ============================================================
# FULL MARKET QUOTES V3
# ============================================================

def fetch_live_quotes():
    results = []

    keys = [x["instrument_key"] for x in INSTRUMENTS]

    for i in range(0, len(keys), 500):
        batch = keys[i:i + 500]

        try:
            params = {
                "instrument_key": ",".join(batch)
            }

            url = BASE + "/v3/market-quote/quotes"

            r = requests.get(
                url,
                headers=headers(),
                params=params,
                timeout=20
            )

            r.raise_for_status()

            data = r.json().get("data", {})

            for response_key, q in data.items():

                instrument_key = q.get("instrument_token")

                if not instrument_key:
                    meta = BY_KEY.get(
                        response_key.replace(":", "|"),
                        {}
                    )
                    instrument_key = meta.get("instrument_key")

                if not instrument_key:
                    continue

                q["_instrument_key"] = instrument_key
                results.append(q)

        except Exception as e:
            log(f"Quote batch error: {repr(e)}")

        time.sleep(0.15)

    log(f"Live quotes received: {len(results)}")

    return results


# ============================================================
# HISTORICAL 20-DAY TURNOVER
# ============================================================

def historical_20day_turnover(key, today):
    """
    Gets up to 50 calendar days of daily candles and
    calculates the average of the latest 20 valid trading days.

    Turnover = Close × Volume
    """

    yesterday = today - timedelta(days=1)
    start = today - timedelta(days=MAX_HIST_DAYS)

    url = (
        BASE
        + "/v3/historical-candle/"
        + quote(key, safe="|")
        + "/days/1/"
        + yesterday.isoformat()
        + "/"
        + start.isoformat()
    )

    try:
        r = requests.get(
            url,
            headers=headers(),
            timeout=15
        )

        r.raise_for_status()

        candles = r.json().get("data", {}).get("candles", [])

        valid = []

        for c in candles:

            if len(c) < 6:
                continue

            try:
                close = safe_float(c[4])
                volume = safe_float(c[5])

                if close > 0 and volume > 0:
                    turnover = close * volume
                    valid.append(turnover)

            except Exception:
                continue

        if len(valid) < LIQUIDITY_DAYS:
            return None

        # Historical response is normally newest first,
        # but sorting is safer.
        valid = valid[:LIQUIDITY_DAYS]

        return sum(valid) / len(valid)

    except Exception as e:
        log(f"Historical error {key}: {repr(e)}")
        return None


# ============================================================
# CACHE 20-DAY TURNOVER
# ============================================================

def get_average_turnovers():

    global HIST_CACHE, CACHE_DATE

    today = ist_now().date()
    today_key = today.isoformat()

    # New day = fresh cache
    if CACHE_DATE != today_key:
        HIST_CACHE = {}
        CACHE_DATE = today_key

    result = {}

    pending = []

    for item in INSTRUMENTS:

        key = item["instrument_key"]

        if key in HIST_CACHE:
            result[key] = HIST_CACHE[key]
        else:
            pending.append(key)

    log(
        f"20D turnover cache: {len(result)} ready, "
        f"{len(pending)} pending"
    )

    if not pending:
        return result

    # Upstox standard rate limit is large enough for this
    # controlled parallel approach.
    #
    # We deliberately keep workers limited so the API
    # is not flooded.

    completed = 0

    def worker(key):
        return key, historical_20day_turnover(key, today)

    with ThreadPoolExecutor(max_workers=8) as executor:

        futures = {
            executor.submit(worker, key): key
            for key in pending
        }

        for future in as_completed(futures):

            key = futures[future]

            try:
                k, value = future.result()

                if value is not None:
                    HIST_CACHE[k] = value
                    result[k] = value

            except Exception as e:
                log(f"Turnover worker error: {repr(e)}")

            completed += 1

            if completed % 50 == 0:
                log(
                    f"20D turnover progress: "
                    f"{completed}/{len(pending)}"
                )

    log(
        f"20D turnover completed: "
        f"{len(result)} stocks"
    )

    return result


# ============================================================
# LIVE CANDIDATES
# ============================================================

def build_candidates(quotes):

    output = []

    rejected_price = 0
    rejected_data = 0

    for q in quotes:

        try:

            key = q.get("_instrument_key")

            meta = BY_KEY.get(key, {})

            symbol = (
                meta.get("trading_symbol")
                or q.get("symbol")
                or ""
            )

            price = safe_float(q.get("last_price"))

            prev_close = safe_float(
                q.get("prev_close_price")
            )

            ohlc = q.get("ohlc") or {}

            opening_price = safe_float(
                ohlc.get("open")
            )

            low_price = safe_float(
                ohlc.get("low")
            )

            volume = safe_float(
                q.get("volume")
                or ohlc.get("volume")
            )

            average_price = safe_float(
                q.get("average_price")
            )

            # Only Price >= ₹50
            if price < MIN_PRICE:
                rejected_price += 1
                continue

            if opening_price <= 0 or low_price <= 0:
                rejected_data += 1
                continue

            # ------------------------------------------------
            # EXACT CHARTINK-STYLE COMPONENTS
            # ------------------------------------------------

            gap = (
                (opening_price - low_price)
                / opening_price
            ) * 100.0

            recovery = (
                (price - low_price)
                / low_price
            ) * 100.0

            gain = 0.0

            if prev_close > 0:
                gain = (
                    (price - prev_close)
                    / prev_close
                ) * 100.0

            live_price = (
                average_price
                if average_price > 0
                else price
            )

            live_turnover = volume * live_price

            output.append({
                "key": key,
                "symbol": symbol,
                "price": price,
                "open": opening_price,
                "low": low_price,
                "gap": gap,
                "recovery": recovery,
                "gain": gain,
                "volume": volume,
                "live_turnover": live_turnover
            })

        except Exception:
            continue

    log(
        f"Candidates: {len(output)} | "
        f"Price rejected: {rejected_price} | "
        f"Missing OHLC: {rejected_data}"
    )

    return output


# ============================================================
# APPLY EARLY STRENGTH SCORE
# ============================================================

def apply_strength_score(items, avg_turnovers):

    final = []

    for x in items:

        key = x["key"]

        avg_turnover = avg_turnovers.get(key)

        if avg_turnover is None:
            continue

        # ₹10 Crore average turnover filter
        if avg_turnover < MIN_AVG_TURNOVER:
            continue

        # ----------------------------------------------------
        # 1. OPEN-LOW GAP SCORE
        # Maximum reference gap = 0.50%
        # ----------------------------------------------------

        gap_score = max(
            0.0,
            min(
                100.0,
                (1.0 - x["gap"] / 0.50) * 100.0
            )
        )

        # ----------------------------------------------------
        # 2. RECOVERY SCORE
        # 1.50% recovery = 100
        # ----------------------------------------------------

        recovery_score = max(
            0.0,
            min(
                100.0,
                (x["recovery"] / 1.50) * 100.0
            )
        )

        # ----------------------------------------------------
        # 3. GAIN SCORE
        # 3.00% gain = 100
        # ----------------------------------------------------

        gain_score = max(
            0.0,
            min(
                100.0,
                (x["gain"] / 3.00) * 100.0
            )
        )

        # ----------------------------------------------------
        # 4. LIVE TURNOVER SCORE
        # 2.5 × average turnover = 100
        # ----------------------------------------------------

        live_score = 0.0

        if avg_turnover > 0:

            live_score = max(
                0.0,
                min(
                    100.0,
                    (
                        x["live_turnover"]
                        / (avg_turnover * 2.5)
                    ) * 100.0
                )
            )

        # ----------------------------------------------------
        # 5. AVERAGE TURNOVER SCORE
        # ₹10 Crore = 10
        # ₹100 Crore = 100
        # ----------------------------------------------------

        avg_score = max(
            0.0,
            min(
                100.0,
                (avg_turnover / 100_000_000.0) * 100.0
            )
        )

        # ----------------------------------------------------
        # FINAL WEIGHTING
        #
        # Gap       5%
        # Recovery 20%
        # Gain      35%
        # Live      30%
        # Average   10%
        # ----------------------------------------------------

        strength = (
            gap_score * WEIGHT_GAP
            + recovery_score * WEIGHT_RECOVERY
            + gain_score * WEIGHT_GAIN
            + live_score * WEIGHT_LIVE
            + avg_score * WEIGHT_AVG
        )

        x["gap_score"] = gap_score
        x["recovery_score"] = recovery_score
        x["gain_score"] = gain_score
        x["live_score"] = live_score
        x["avg_score"] = avg_score
        x["avg_turnover"] = avg_turnover
        x["strength"] = strength

        final.append(x)

    # Strongest first
    final.sort(
        key=lambda x: (
            -x["strength"],
            -x["gain"],
            -x["live_turnover"],
            -x["recovery"]
        )
    )

    return final


# ============================================================
# SCAN
# ============================================================

def perform_scan():

    global LIVE_RESULTS
    global LAST_SCAN_TIME
    global LAST_SCAN_ERROR
    global SCAN_RUNNING

    with SCAN_LOCK:

        if SCAN_RUNNING:
            return

        SCAN_RUNNING = True

    try:

        LAST_SCAN_ERROR = ""
        LIVE_RESULTS = []

        log("==========================================")
        log("EARLY STRENGTH SCAN STARTED")
        log("==========================================")

        if not UPSTOX_TOKEN:
            raise RuntimeError(
                "UPSTOX_ACCESS_TOKEN Render Environment "
                "Variables में नहीं मिला।"
            )

        load_instruments()

        # ----------------------------------------------------
        # LIVE DATA
        # ----------------------------------------------------

        quotes = fetch_live_quotes()

        candidates = build_candidates(quotes)

        if not candidates:
            LIVE_RESULTS = []
            LAST_SCAN_TIME = ist_now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            return

        # ----------------------------------------------------
        # 20 DAY TURNOVER
        # ----------------------------------------------------

        avg_turnovers = get_average_turnovers()

        log(
            f"Average turnover data available: "
            f"{len(avg_turnovers)}"
        )

        # ----------------------------------------------------
        # FINAL SCORE
        # ----------------------------------------------------

        ranked = apply_strength_score(
            candidates,
            avg_turnovers
        )

        LIVE_RESULTS = []

        for item in ranked:

            LIVE_RESULTS.append({
                "symbol": item["symbol"],
                "price": round(item["price"], 2),
                "open": round(item["open"], 2),
                "low": round(item["low"], 2),
                "gap": round(item["gap"], 3),
                "recovery": round(item["recovery"], 2),
                "gain": round(item["gain"], 2),
                "strength": round(item["strength"], 1),
                "avg_turnover_cr": round(
                    item["avg_turnover"] / 10_000_000.0,
                    2
                ),
                "live_turnover_cr": round(
                    item["live_turnover"] / 10_000_000.0,
                    2
                ),
                "volume": int(item["volume"])
            })

        LAST_SCAN_TIME = ist_now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        log(
            f"FINAL RESULTS: {len(LIVE_RESULTS)}"
        )

        if LIVE_RESULTS:
            log(
                "TOP SHARE: "
                + LIVE_RESULTS[0]["symbol"]
                + " | Strength "
                + str(LIVE_RESULTS[0]["strength"])
            )

    except Exception as e:

        LAST_SCAN_ERROR = str(e)

        log(
            "SCAN ERROR: "
            + repr(e)
        )

    finally:

        SCAN_RUNNING = False


# ============================================================
# HTML
# ============================================================

HTML = r"""
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
    padding:10px;
    background:#10151b;
    color:#e8edf3;
    font-family:Arial,Helvetica,sans-serif;
}

.container{
    max-width:1200px;
    margin:auto;
}

.card{
    background:#171d24;
    border:1px solid #2a333d;
    border-radius:12px;
    padding:14px;
    margin-bottom:12px;
}

h1{
    margin:0;
    font-size:24px;
}

.sub{
    margin-top:5px;
    color:#9da8b5;
}

.status{
    padding:10px;
    border-radius:8px;
    background:#202832;
    margin-bottom:10px;
}

button{
    border:0;
    border-radius:8px;
    padding:11px 18px;
    background:#2f80ed;
    color:white;
    font-size:15px;
}

button:disabled{
    opacity:.5;
}

.info{
    color:#aeb8c5;
    margin-bottom:10px;
}

.error{
    color:#ff8f9a;
    margin-top:10px;
}

.filters{
    display:grid;
    grid-template-columns:repeat(5,1fr);
    gap:8px;
}

.filter{
    background:#1d252e;
    border-radius:8px;
    padding:10px;
}

.ft{
    color:#8f9baa;
    font-size:12px;
}

.fv{
    margin-top:4px;
    font-weight:bold;
}

.table-wrap{
    overflow-x:auto;
}

table{
    width:100%;
    border-collapse:collapse;
    min-width:850px;
}

th,td{
    border-bottom:1px solid #29323c;
    padding:9px 7px;
    text-align:center;
    white-space:nowrap;
}

th{
    color:#9da8b5;
    font-size:12px;
}

td{
    font-size:13px;
}

.symbol{
    text-align:left;
    font-weight:bold;
}

.strength{
    font-weight:bold;
    font-size:15px;
}

.top{
    background:#202a23;
}

.empty{
    padding:25px;
    color:#8f9baa;
}

.logic{
    color:#aeb8c5;
    line-height:1.7;
    font-size:13px;
}

@media(max-width:700px){

    body{
        padding:7px;
    }

    h1{
        font-size:21px;
    }

    .filters{
        grid-template-columns:1fr 1fr;
    }

}

</style>
</head>

<body>

<div class="container">

<div class="card">

<h1>Early Strength Rank Scanner</h1>

<div class="sub">
NSE EQ • Live Strength Ranking • Strongest First
</div>

</div>


<div class="card">

<div class="status" id="status">
Scanner तैयार है
</div>

<div class="info" id="info">
Scan शुरू करने के लिए नीचे button दबाएँ।
</div>

<button id="scanButton"
onclick="startScan()">
Scan Now
</button>

<div class="error"
id="error">
</div>

</div>


<div class="filters">

<div class="filter">
<div class="ft">Price</div>
<div class="fv">≥ ₹50</div>
</div>

<div class="filter">
<div class="ft">Gap Weight</div>
<div class="fv">5%</div>
</div>

<div class="filter">
<div class="ft">Recovery</div>
<div class="fv">20%</div>
</div>

<div class="filter">
<div class="ft">Gain</div>
<div class="fv">35%</div>
</div>

<div class="filter">
<div class="ft">Live Turnover</div>
<div class="fv">30%</div>
</div>

</div>


<div class="card">

<h2 id="resultsTitle">
Results: 0
</h2>

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
<th>Gap</th>
<th>Recovery</th>
<th>Gain</th>
<th>Avg 20D Turnover</th>
<th>Live Turnover</th>
</tr>

</thead>

<tbody id="resultsBody">

<tr>
<td colspan="11"
class="empty">
अभी scan शुरू नहीं हुआ है।
</td>
</tr>

</tbody>

</table>

</div>

</div>


<div class="card logic">

<h3>Ranking Logic</h3>

<div>• NSE Equity shares only</div>

<div>• Price ≥ ₹50</div>

<div>• Open-Low Gap Score = 5%</div>

<div>• Recovery Score = 20%</div>

<div>• Gain Score = 35%</div>

<div>• Live Turnover Score = 30%</div>

<div>• Average Turnover Score = 10%</div>

<div>• Previous 20 valid trading days average turnover ≥ ₹10 Crore</div>

<div>• Gain और Recovery केवल scoring में इस्तेमाल होते हैं; इन पर अलग filter नहीं है।</div>

<div>• सबसे मजबूत qualifying share Rank 1 पर आएगा।</div>

</div>

</div>


<script>

let timer=null;

async function startScan(){

    const btn=document.getElementById("scanButton");

    btn.disabled=true;

    document.getElementById("status").innerText=
        "Scan चल रहा है...";

    document.getElementById("info").innerText=
        "Live quotes और 20-day turnover data लिया जा रहा है।";

    document.getElementById("error").innerText="";

    try{

        await fetch("/api/scan",{
            method:"POST"
        });

        if(timer){
            clearInterval(timer);
        }

        timer=setInterval(loadResults,2000);

        loadResults();

    }catch(e){

        btn.disabled=false;

        document.getElementById("error").innerText=
            "Scan start नहीं हो पाया।";

    }

}


async function loadResults(){

    try{

        const r=await fetch(
            "/api/results",
            {cache:"no-store"}
        );

        const d=await r.json();

        const btn=document.getElementById("scanButton");

        if(d.running){

            btn.disabled=true;

            document.getElementById("status").innerText=
                "Scan चल रहा है...";

        }else{

            btn.disabled=false;

            document.getElementById("status").innerText=
                "Scan complete";

            if(timer){

                clearInterval(timer);
                timer=null;

            }

        }

        document.getElementById("info").innerText=
            "Last Scan: "+d.last_scan;

        document.getElementById("error").innerText=
            d.error || "";

        const rows=d.results || [];

        document.getElementById("resultsTitle").innerText=
            "Results: "+rows.length;

        const body=document.getElementById("resultsBody");

        if(!rows.length){

            body.innerHTML=
                '<tr><td colspan="11" class="empty">'+
                (d.error ||
                "कोई qualifying share नहीं मिला।")+
                '</td></tr>';

            return;

        }

        body.innerHTML=rows.map((x,i)=>`

            <tr class="${i===0?'top':''}">

                <td>${i+1}</td>

                <td class="symbol">
                    ${x.symbol}
                </td>

                <td class="strength">
                    ${Number(x.strength).toFixed(1)}
                </td>

                <td>
                    ₹${Number(x.price).toFixed(2)}
                </td>

                <td>
                    ₹${Number(x.open).toFixed(2)}
                </td>

                <td>
                    ₹${Number(x.low).toFixed(2)}
                </td>

                <td>
                    ${Number(x.gap).toFixed(3)}%
                </td>

                <td>
                    ${Number(x.recovery).toFixed(2)}%
                </td>

                <td>
                    ${Number(x.gain).toFixed(2)}%
                </td>

                <td>
                    ₹${Number(x.avg_turnover_cr).toFixed(2)} Cr
                </td>

                <td>
                    ₹${Number(x.live_turnover_cr).toFixed(2)} Cr
                </td>

            </tr>

        `).join("");

    }catch(e){

        document.getElementById("error").innerText=
            "Server response नहीं मिला। Render Logs देखें।";

    }

}

loadResults();

</script>

</body>
</html>
"""


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/api/scan", methods=["POST"])
def api_scan():

    global SCAN_RUNNING

    if SCAN_RUNNING:
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
        "ok": True,
        "message": "Scan started"
    })


@app.route("/api/results")
def api_results():

    return jsonify({
        "running": SCAN_RUNNING,
        "last_scan": LAST_SCAN_TIME,
        "error": LAST_SCAN_ERROR,
        "results": LIVE_RESULTS
    })


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    port = int(os.environ.get("PORT", "10000"))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )
