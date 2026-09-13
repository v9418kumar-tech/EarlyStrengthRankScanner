from flask import Flask, jsonify, send_from_directory
import requests
import os
import json
import gzip
import threading
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

# ============================================================
# SETTINGS
# ============================================================

TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()

MIN_PRICE = 50.0

UPSTOX_QUOTE_URL = (
    "https://api.upstox.com/v3/market-quote/quotes"
)

UPSTOX_INSTRUMENT_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
)

NSE_BHAV_URL = (
    "https://nsearchives.nseindia.com/products/"
    "content/sec_bhavdata_full_{}.csv"
)

# ============================================================
# STATE
# ============================================================

INSTRUMENTS = []
INSTRUMENT_MAP = {}

LIVE_RESULTS = []
SCAN_RUNNING = False
LAST_SCAN_TIME = ""
LAST_ERROR = ""

SCAN_LOCK = threading.Lock()

# ============================================================
# HELPERS
# ============================================================

def headers():

    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}",
        "User-Agent": "EarlyStrengthRankScanner/1.0"
    }


def chunks(items, size):

    for i in range(0, len(items), size):
        yield items[i:i + size]


def clamp(value):

    return max(
        0.0,
        min(100.0, float(value))
    )


# ============================================================
# LOAD UPSTOX NSE EQ INSTRUMENTS
# ============================================================

def load_instruments():

    global INSTRUMENTS
    global INSTRUMENT_MAP

    if INSTRUMENTS:
        return INSTRUMENTS

    r = requests.get(
        UPSTOX_INSTRUMENT_URL,
        timeout=30
    )

    r.raise_for_status()

    raw = gzip.decompress(
        r.content
    )

    data = json.loads(
        raw.decode("utf-8")
    )

    instruments = []

    for x in data:

        if not isinstance(x, dict):
            continue

        if x.get("segment") != "NSE_EQ":
            continue

        if x.get("instrument_type") != "EQ":
            continue

        key = x.get(
            "instrument_key"
        )

        symbol = (
            x.get("trading_symbol")
            or x.get("short_name")
        )

        if not key or not symbol:
            continue

        instruments.append({
            "instrument_key": key,
            "symbol": symbol,
            "name": x.get(
                "name",
                symbol
            )
        })

    INSTRUMENTS = instruments

    INSTRUMENT_MAP = {
        x["instrument_key"]: x
        for x in instruments
    }

    print(
        f"Loaded {len(INSTRUMENTS)} NSE EQ instruments"
    )

    return INSTRUMENTS


# ============================================================
# LIVE UPSTOX QUOTES
# ============================================================

def fetch_quote_batch(batch):

    keys = ",".join(
        x["instrument_key"]
        for x in batch
    )

    try:

        r = requests.get(
            UPSTOX_QUOTE_URL,
            headers=headers(),
            params={
                "instrument_key": keys
            },
            timeout=15
        )

        if r.status_code != 200:

            print(
                "Quote error:",
                r.status_code,
                r.text[:200]
            )

            return []

        data = r.json().get(
            "data",
            {}
        )

        results = []

        for response_key, q in data.items():

            if not isinstance(q, dict):
                continue

            instrument_key = (
                q.get(
                    "instrument_token"
                )
                or response_key
            )

            info = INSTRUMENT_MAP.get(
                instrument_key
            )

            if not info:
                continue

            ohlc = q.get(
                "ohlc",
                {}
            )

            open_price = float(
                ohlc.get(
                    "open",
                    0
                ) or 0
            )

            low_price = float(
                ohlc.get(
                    "low",
                    0
                ) or 0
            )

            current_close = float(
                q.get(
                    "last_price",
                    0
                ) or 0
            )

            volume = float(
                q.get(
                    "volume",
                    ohlc.get(
                        "volume",
                        0
                    )
                )
                or 0
            )

            previous_close = float(
                q.get(
                    "prev_close_price",
                    0
                ) or 0
            )

            if (
                open_price <= 0
                or low_price <= 0
                or current_close <= 0
                or previous_close <= 0
            ):
                continue

            if current_close < MIN_PRICE:
                continue

            results.append({

                "symbol":
                    info["symbol"],

                "name":
                    info["name"],

                "instrument_key":
                    instrument_key,

                "open":
                    open_price,

                "low":
                    low_price,

                "close":
                    current_close,

                "volume":
                    volume,

                "prev_close":
                    previous_close

            })

        return results

    except Exception as e:

        print(
            "Quote batch error:",
            repr(e)
        )

        return []


def fetch_all_live_quotes():

    load_instruments()

    all_results = []

    batches = list(
        chunks(
            INSTRUMENTS,
            500
        )
    )

    workers = min(
        6,
        len(batches)
    )

    with ThreadPoolExecutor(
        max_workers=workers
    ) as executor:

        futures = [
            executor.submit(
                fetch_quote_batch,
                batch
            )
            for batch in batches
        ]

        for future in as_completed(
            futures
        ):

            try:

                rows = future.result()

                all_results.extend(
                    rows
                )

            except Exception as e:

                print(
                    "Worker error:",
                    repr(e)
                )

    print(
        f"Live quotes received: {len(all_results)}"
    )

    return all_results


# ============================================================
# NSE SESSION
# ============================================================

def nse_session():

    s = requests.Session()

    s.headers.update({

        "User-Agent":
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/139.0 Safari/537.36",

        "Accept":
            "text/html,application/json,*/*",

        "Accept-Language":
            "en-US,en;q=0.9",

        "Referer":
            "https://www.nseindia.com/"
    })

    try:

        s.get(
            "https://www.nseindia.com/",
            timeout=8
        )

    except Exception:
        pass

    return s


# ============================================================
# NSE BHAVCOPY
# ============================================================

def get_bhavcopy(date_obj):

    date_str = date_obj.strftime(
        "%d%m%Y"
    )

    url = NSE_BHAV_URL.format(
        date_str
    )

    try:

        s = nse_session()

        r = s.get(
            url,
            timeout=20
        )

        if r.status_code != 200:
            return None

        if not r.text.strip():
            return None

        text = r.text

        lines = text.splitlines()

        if len(lines) < 2:
            return None

        header = [
            x.strip().upper()
            for x in lines[0].split(",")
        ]

        symbol_index = None
        close_index = None
        volume_index = None
        turnover_index = None
        series_index = None

        for i, col in enumerate(header):

            if col == "SYMBOL":
                symbol_index = i

            elif col == "CLOSE_PRICE":
                close_index = i

            elif col in (
                "TTL_TRD_QNTY",
                "TOTTRDQTY"
            ):
                volume_index = i

            elif col == "TOTTRDVAL":
                turnover_index = i

            elif col == "SERIES":
                series_index = i

        if (
            symbol_index is None
            or close_index is None
            or series_index is None
        ):
            return None

        result = {}

        for line in lines[1:]:

            try:

                parts = [
                    x.strip()
                    for x in line.split(",")
                ]

                if len(parts) <= symbol_index:
                    continue

                if (
                    parts[series_index]
                    .upper()
                    != "EQ"
                ):
                    continue

                symbol = (
                    parts[symbol_index]
                    .upper()
                    .strip()
                )

                close = float(
                    parts[close_index]
                    .replace(",", "")
                )

                if (
                    turnover_index
                    is not None
                ):

                    turnover = float(
                        parts[
                            turnover_index
                        ].replace(",", "")
                    )

                elif (
                    volume_index
                    is not None
                ):

                    volume = float(
                        parts[
                            volume_index
                        ].replace(",", "")
                    )

                    turnover = (
                        close * volume
                    )

                else:

                    continue

                if turnover > 0:

                    result[symbol] = {
                        "close": close,
                        "turnover": turnover
                    }

            except Exception:
                continue

        return result

    except Exception as e:

        print(
            "Bhavcopy error:",
            date_obj,
            repr(e)
        )

        return None


# ============================================================
# PREVIOUS 20 VALID TRADING DAYS
# ============================================================

def get_previous_trading_dates():

    dates = []

    d = (
        datetime.now().date()
        - timedelta(days=1)
    )

    while len(dates) < 20:

        if d.weekday() < 5:
            dates.append(d)

        d -= timedelta(days=1)

    return dates


# ============================================================
# BUILD 20-DAY AVERAGE TURNOVER
# ============================================================

def build_average_turnover(symbols):

    dates = (
        get_previous_trading_dates()
    )

    daily_data = []

    with ThreadPoolExecutor(
        max_workers=6
    ) as executor:

        futures = {
            executor.submit(
                get_bhavcopy,
                d
            ): d
            for d in dates
        }

        for future in as_completed(
            futures
        ):

            try:

                result = future.result()

                if result:
                    daily_data.append(
                        result
                    )

            except Exception:
                pass

    if len(daily_data) < 20:

        raise RuntimeError(
            f"Only {len(daily_data)} "
            f"valid trading days available."
        )

    turnover_sum = {
        s: 0.0
        for s in symbols
    }

    turnover_count = {
        s: 0
        for s in symbols
    }

    for day in daily_data:

        for symbol in symbols:

            item = day.get(
                symbol
            )

            if not item:
                continue

            turnover_sum[symbol] += (
                item["turnover"]
            )

            turnover_count[symbol] += 1

    average = {}

    for symbol in symbols:

        if (
            turnover_count[symbol]
            == 20
        ):

            average[symbol] = (
                turnover_sum[symbol]
                / 20.0
            )

    print(
        f"20D average turnover calculated "
        f"for {len(average)} stocks"
    )

    return average


# ============================================================
# EARLY STRENGTH SCORE
# ============================================================

def calculate_score(row, avg_turnover):

    open_price = row["open"]
    low_price = row["low"]
    close_price = row["close"]
    prev_close = row["prev_close"]
    volume = row["volume"]

    average_turnover = (
        avg_turnover
    )

    # --------------------------------------------------------
    # 1. OPEN-LOW GAP SCORE — 30%
    # --------------------------------------------------------

    gap = (
        (
            open_price
            - low_price
        )
        / open_price
    ) * 100

    gap_score = clamp(
        (
            1
            - gap / 0.50
        ) * 100
    )

    # --------------------------------------------------------
    # 2. RECOVERY SCORE — 25%
    # --------------------------------------------------------

    recovery = (
        (
            close_price
            - low_price
        )
        / low_price
    ) * 100

    recovery_score = clamp(
        (
            recovery
            / 1.50
        ) * 100
    )

    # --------------------------------------------------------
    # 3. GAIN SCORE — 15%
    # --------------------------------------------------------

    gain = (
        (
            close_price
            - prev_close
        )
        / prev_close
    ) * 100

    gain_score = clamp(
        (
            gain
            / 3.00
        ) * 100
    )

    # --------------------------------------------------------
    # 4. LIVE TURNOVER SCORE — 20%
    # --------------------------------------------------------

    live_turnover = (
        volume
        * close_price
    )

    if average_turnover > 0:

        live_turnover_score = clamp(
            (
                live_turnover
                / (
                    average_turnover
                    * 2.5
                )
            ) * 100
        )

    else:

        live_turnover_score = 0.0

    # --------------------------------------------------------
    # 5. AVERAGE TURNOVER SCORE — 10%
    # --------------------------------------------------------

    average_turnover_score = clamp(
        (
            average_turnover
            / 100000000
        ) * 100
    )

    # --------------------------------------------------------
    # FINAL EARLY STRENGTH SCORE
    # --------------------------------------------------------

    final_score = (

        gap_score * 0.30

        + recovery_score * 0.25

        + gain_score * 0.15

        + live_turnover_score * 0.20

        + average_turnover_score * 0.10
    )

    return {

        "gap":
            gap,

        "gap_score":
            gap_score,

        "recovery":
            recovery,

        "recovery_score":
            recovery_score,

        "gain":
            gain,

        "gain_score":
            gain_score,

        "live_turnover":
            live_turnover,

        "live_turnover_score":
            live_turnover_score,

        "avg_turnover":
            average_turnover,

        "avg_turnover_score":
            average_turnover_score,

        "score":
            clamp(final_score)
    }


# ============================================================
# MAIN SCAN
# ============================================================

def perform_scan():

    global LAST_SCAN_TIME
    global LAST_ERROR

    LAST_ERROR = ""

    if not TOKEN:

        raise RuntimeError(
            "UPSTOX_ACCESS_TOKEN "
            "Render Environment Variables "
            "में नहीं मिला।"
        )

    print(
        "Starting Early Strength Rank scan..."
    )

    live_quotes = (
        fetch_all_live_quotes()
    )

    if not live_quotes:

        raise RuntimeError(
            "Upstox से live quotes नहीं मिले।"
        )

    symbols = {
        x["symbol"]
        for x in live_quotes
    }

    print(
        f"Eligible price universe: "
        f"{len(live_quotes)} stocks"
    )

    average_turnovers = (
        build_average_turnover(
            symbols
        )
    )

    results = []

    for row in live_quotes:

        symbol = row["symbol"]

        # ----------------------------------------------------
        # ONLY selection condition:
        # PRICE >= ₹50
        # ----------------------------------------------------

        if row["close"] < MIN_PRICE:
            continue

        if symbol not in average_turnovers:
            continue

        scores = calculate_score(
            row,
            average_turnovers[symbol]
        )

        results.append({

            "symbol":
                symbol,

            "name":
                row["name"],

            "score":
                scores["score"],

            "price":
                row["close"],

            "open":
                row["open"],

            "low":
                row["low"],

            "prev_close":
                row["prev_close"],

            "gap":
                scores["gap"],

            "recovery":
                scores["recovery"],

            "gain":
                scores["gain"],

            "live_turnover":
                scores["live_turnover"],

            "avg_turnover":
                scores["avg_turnover"],

            "gap_score":
                scores["gap_score"],

            "recovery_score":
                scores["recovery_score"],

            "gain_score":
                scores["gain_score"],

            "live_turnover_score":
                scores[
                    "live_turnover_score"
                ],

            "avg_turnover_score":
                scores[
                    "avg_turnover_score"
                ]
        })

    results.sort(
        key=lambda x: x["score"],
        reverse=True
    )

    for i, row in enumerate(
        results,
        start=1
    ):

        row["rank"] = i

    LAST_SCAN_TIME = (
        datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    print(
        f"FINAL RESULTS: {len(results)}"
    )

    return results


# ============================================================
# BACKGROUND SCAN
# ============================================================

def scan_worker():

    global SCAN_RUNNING
    global LIVE_RESULTS
    global LAST_ERROR

    try:

        LIVE_RESULTS = perform_scan()

    except Exception as e:

        LAST_ERROR = repr(e)

        print(
            "SCAN ERROR:",
            repr(e)
        )

    finally:

        SCAN_RUNNING = False


def start_scan():

    global SCAN_RUNNING

    with SCAN_LOCK:

        if SCAN_RUNNING:
            return False

        SCAN_RUNNING = True

    threading.Thread(
        target=scan_worker,
        daemon=True
    ).start()

    return True


# ============================================================
# HTML
# ============================================================

HTML = """
<!DOCTYPE html>
<html>
<head>

<meta name="viewport"
      content="width=device-width, initial-scale=1">

<title>Early Strength Rank Scanner</title>

<style>

body{
    margin:0;
    padding:10px;
    background:#11151a;
    color:#e8edf3;
    font-family:Arial,sans-serif;
}

.container{
    max-width:1200px;
    margin:auto;
}

h1{
    font-size:22px;
    margin:5px 0;
}

.subtitle{
    color:#9fa8b3;
    font-size:13px;
    margin-bottom:12px;
}

.card{
    background:#1a2027;
    border:1px solid #303944;
    border-radius:12px;
    padding:12px;
    margin-bottom:12px;
}

button{
    background:#2864e8;
    color:white;
    border:0;
    padding:12px 20px;
    border-radius:8px;
    font-size:16px;
}

.status{
    margin-top:10px;
    color:#b9c2cc;
    font-size:14px;
}

.table-wrap{
    overflow-x:auto;
}

table{
    width:100%;
    border-collapse:collapse;
    min-width:1000px;
    background:#171c22;
    font-size:13px;
}

th{
    background:#252c35;
    padding:9px 6px;
    white-space:nowrap;
}

td{
    padding:8px 6px;
    border-bottom:1px solid #2c333c;
    text-align:center;
    white-space:nowrap;
}

.rank{
    font-weight:bold;
}

.score{
    font-weight:bold;
    font-size:15px;
}

.top{
    background:#1457b8;
}

.note{
    color:#8e98a4;
    font-size:12px;
    line-height:1.5;
}

</style>

</head>

<body>

<div class="container">

<div class="card">

<h1>Early Strength Rank Scanner</h1>

<div class="subtitle">
Upstox NSE EQ • Early Strength Score • Strongest First
</div>

<div>
<b>Price:</b> ₹50 or above<br>
<b>Universe:</b> Upstox NSE EQ<br>
<b>Scoring:</b> Chartink Early Strength formula
</div>

<br>

<button onclick="runScan()">
SCAN NOW
</button>

<div id="status"
     class="status">
Ready
</div>

</div>

<div class="card">

<div class="table-wrap">

<table>

<thead>

<tr>

<th>Rank</th>
<th>Symbol</th>
<th>Strength</th>
<th>Price</th>
<th>Open</th>
<th>Low</th>
<th>Gap</th>
<th>Recovery</th>
<th>Gain</th>
<th>Live Turnover</th>
<th>20D Avg Turnover</th>

</tr>

</thead>

<tbody id="rows">

</tbody>

</table>

</div>

</div>

<div class="card note">

<b>Formula:</b><br>

Open-Low Gap Score = 30%<br>
Recovery Score = 25%<br>
Gain Score = 15%<br>
Live Turnover Score = 20%<br>
Average Turnover Score = 10%<br><br>

Gain और Recovery केवल scoring में इस्तेमाल होते हैं।
इन दोनों पर कोई filter नहीं लगाया गया है।

<br><br>

केवल Price ≥ ₹50 का selection condition है।

</div>

</div>

<script>

async function runScan(){

    const status =
        document.getElementById("status");

    status.innerText =
        "Scanner चल रहा है...";

    try{

        await fetch("/api/start");

        poll();

    }catch(e){

        status.innerText =
            "Scanner start error";

    }
}


async function poll(){

    try{

        const r =
            await fetch("/api/results");

        const data =
            await r.json();

        const status =
            document.getElementById(
                "status"
            );

        if(data.running){

            status.innerText =
                "Live data और 20-day turnover calculation चल रही है...";

            setTimeout(
                poll,
                500
            );

            return;
        }

        if(data.error){

            status.innerText =
                "Error: " + data.error;

            return;
        }

        status.innerText =
            "Scan complete • " +
            data.results.length +
            " stocks found • " +
            data.updated_at;

        const tbody =
            document.getElementById(
                "rows"
            );

        tbody.innerHTML = "";

        data.results.forEach(
            function(x){

                const tr =
                    document.createElement(
                        "tr"
                    );

                if(x.rank <= 5){
                    tr.className = "top";
                }

                tr.innerHTML =

                    "<td class='rank'>" +
                    x.rank +
                    "</td>" +

                    "<td><b>" +
                    x.symbol +
                    "</b></td>" +

                    "<td class='score'>" +
                    x.score.toFixed(1) +
                    "</td>" +

                    "<td>₹" +
                    x.price.toFixed(2) +
                    "</td>" +

                    "<td>₹" +
                    x.open.toFixed(2) +
                    "</td>" +

                    "<td>₹" +
                    x.low.toFixed(2) +
                    "</td>" +

                    "<td>" +
                    x.gap.toFixed(2) +
                    "%</td>" +

                    "<td>" +
                    x.recovery.toFixed(2) +
                    "%</td>" +

                    "<td>" +
                    x.gain.toFixed(2) +
                    "%</td>" +

                    "<td>" +
                    formatCr(
                        x.live_turnover
                    ) +
                    "</td>" +

                    "<td>" +
                    formatCr(
                        x.avg_turnover
                    ) +
                    "</td>";

                tbody.appendChild(tr);

            }
        );

    }catch(e){

        setTimeout(
            poll,
            2000
        );

    }

}


function formatCr(value){

    return (
        (value / 10000000)
        .toFixed(2)
        + " Cr"
    );

}

</script>

</body>
</html>
"""


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    return HTML


@app.route("/api/start")
def api_start():

    started = start_scan()

    return jsonify({
        "started":
            started,
        "running":
            SCAN_RUNNING
    })


@app.route("/api/results")
def api_results():

    return jsonify({

        "running":
            SCAN_RUNNING,

        "error":
            LAST_ERROR,

        "updated_at":
            LAST_SCAN_TIME,

        "results":
            LIVE_RESULTS
    })


@app.route("/api/health")
def health():

    return jsonify({

        "ok":
            True,

        "token_configured":
            bool(TOKEN),

        "nse_eq_stocks":
            len(INSTRUMENTS),

        "scan_running":
            SCAN_RUNNING,

        "last_scan":
            LAST_SCAN_TIME,

        "last_error":
            LAST_ERROR
    })


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False
    )
