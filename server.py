import os
import json
import gzip
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests
from flask import Flask, render_template_string, jsonify

app = Flask(__name__)

TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()

QUOTE_URL = "https://api.upstox.com/v3/market-quote/quotes"
HIST_URL = "https://api.upstox.com/v3/historical-candle"
INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"

PRICE_MIN = 50
HISTORY_DAYS = 20
TOP_HISTORY = 250
WORKERS = 6

AVG_TURNOVER_BASE = 100_000_000
LIVE_TURNOVER_MULTIPLIER = 2.5

cache = {
    "instruments": None,
    "history": {},
    "history_date": None
}

HTML = """
<!doctype html>
<html lang="hi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Early Strength Rank Scanner</title>

<style>
*{box-sizing:border-box}

body{
 margin:0;
 padding:10px;
 background:#0d141b;
 color:#edf2f7;
 font-family:Arial,sans-serif
}

h1{
 font-size:25px;
 margin:8px 0
}

button{
 width:100%;
 padding:14px;
 border:0;
 border-radius:10px;
 background:#198754;
 color:white;
 font-size:18px;
 font-weight:bold;
 margin:8px 0
}

.info{
 background:#151f29;
 border-radius:12px;
 padding:12px;
 margin:8px 0;
 font-size:14px;
 line-height:1.6
}

.status{
 background:#18232e;
 border-radius:10px;
 padding:10px;
 margin:8px 0;
 color:#cbd5df
}

.tablewrap{
 overflow-x:auto
}

table{
 width:100%;
 border-collapse:collapse;
 min-width:900px;
 background:#121b24
}

th,td{
 padding:10px 8px;
 border-bottom:1px solid #26323d;
 text-align:left;
 white-space:nowrap
}

th{
 color:#aeb9c5;
 font-size:14px
}

td{
 font-size:14px
}

.rank{
 font-weight:bold
}

.score{
 font-weight:bold;
 color:#66e3a1
}

.gain{
 color:#66e3a1
}

.neg{
 color:#ff7777
}
</style>
</head>

<body>

<h1>Early Strength Rank Scanner</h1>

<div class="info">
<b>Final Ranking Weights</b><br>
Open-Low Gap: 5% |
Recovery: 20% |
Gain: 35% |
Live Turnover: 30% |
20-Day Average Turnover: 10%
</div>

<button onclick="scan()">SCAN NOW</button>

<div id="status" class="status">Ready</div>

<div id="results"></div>

<script>

async function scan(){

 const s=document.getElementById("status");

 s.innerHTML="Scan शुरू हो रहा है...";

 document.getElementById("results").innerHTML="";

 try{

   const r=await fetch("/scan");

   const d=await r.json();

   if(d.error){
      s.innerHTML="ERROR: "+d.error;
      return;
   }

   s.innerHTML=
      "Results: "+d.results.length+
      " | Updated: "+d.time;

   let h="<div class='tablewrap'><table>";

   h+="<tr>"+
      "<th>Rank</th>"+
      "<th>Share</th>"+
      "<th>Strength</th>"+
      "<th>LTP</th>"+
      "<th>Open</th>"+
      "<th>Low</th>"+
      "<th>Gain</th>"+
      "<th>Live Turnover</th>"+
      "<th>Avg Turnover</th>"+
      "</tr>";

   d.results.forEach((x,i)=>{

      let gc=x.gain>=0?"gain":"neg";

      h+="<tr>"+
        "<td class='rank'>"+(i+1)+"</td>"+
        "<td><b>"+x.symbol+"</b></td>"+
        "<td class='score'>"+x.strength.toFixed(2)+"</td>"+
        "<td>₹"+x.ltp.toFixed(2)+"</td>"+
        "<td>₹"+x.open.toFixed(2)+"</td>"+
        "<td>₹"+x.low.toFixed(2)+"</td>"+
        "<td class='"+gc+"'>"+x.gain.toFixed(2)+"%</td>"+
        "<td>₹"+fmt(x.live_turnover)+"</td>"+
        "<td>₹"+fmt(x.avg_turnover)+"</td>"+
        "</tr>";
   });

   h+="</table></div>";

   document.getElementById("results").innerHTML=h;

 }catch(e){

   s.innerHTML="ERROR: "+e;

 }

}

function fmt(n){

 if(n>=10000000)
   return (n/10000000).toFixed(2)+" Cr";

 if(n>=100000)
   return (n/100000).toFixed(2)+" L";

 if(n>=1000)
   return (n/1000).toFixed(1)+" K";

 return n.toFixed(0);
}

</script>

</body>
</html>
"""


def api_headers():
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }


def load_instruments():

    if cache["instruments"] is not None:
        return cache["instruments"]

    r = requests.get(
        INSTRUMENT_URL,
        timeout=30
    )

    r.raise_for_status()

    raw = gzip.decompress(r.content)

    data = json.loads(
        raw.decode("utf-8")
    )

    instruments = []

    for x in data:

        if x.get("segment") != "NSE_EQ":
            continue

        if x.get("instrument_type") != "EQ":
            continue

        key = x.get("instrument_key")
        symbol = x.get("trading_symbol")

        if not key or not symbol:
            continue

        instruments.append({
            "key": key,
            "symbol": symbol
        })

    cache["instruments"] = instruments

    return instruments


def get_quotes(instruments):

    all_data = []

    for i in range(
        0,
        len(instruments),
        500
    ):

        batch = instruments[
            i:i+500
        ]

        keys = ",".join(
            x["key"] for x in batch
        )

        r = requests.get(
            QUOTE_URL,
            headers=api_headers(),
            params={
                "instrument_key": keys
            },
            timeout=30
        )

        if r.status_code != 200:

            raise RuntimeError(
                f"Upstox Quote HTTP {r.status_code}: "
                f"{r.text[:300]}"
            )

        data = r.json().get(
            "data",
            {}
        )

        for key,q in data.items():

            if not isinstance(q,dict):
                continue

            symbol = q.get("symbol")

            ltp = q.get(
                "last_price"
            )

            prev = q.get(
                "prev_close_price"
            )

            ohlc = q.get("ohlc") or {}

            op = ohlc.get("open")
            low = ohlc.get("low")

            volume = q.get(
                "volume",
                ohlc.get("volume",0)
            )

            try:

                ltp = float(ltp)
                prev = float(prev)
                op = float(op)
                low = float(low)
                volume = float(volume or 0)

            except:

                continue

            if ltp < PRICE_MIN:
                continue

            if op <= 0:
                continue

            if low <= 0:
                continue

            if prev <= 0:
                continue

            all_data.append({

                "key":key,
                "symbol":symbol,
                "ltp":ltp,
                "prev":prev,
                "open":op,
                "low":low,
                "volume":volume

            })

    return all_data


def preliminary_score(x):

    op = x["open"]
    low = x["low"]
    ltp = x["ltp"]
    prev = x["prev"]

    gap = (
        (op-low)/op
    )*100

    recovery = (
        (ltp-low)/low
    )*100

    gain = (
        (ltp-prev)/prev
    )*100

    gap_score = max(
        0,
        (1-gap/0.50)*100
    )

    recovery_score = max(
        0,
        recovery/1.50*100
    )

    gain_score = max(
        0,
        gain/3.00*100
    )

    live_turnover = (
        x["volume"]*ltp
    )

    live_score = max(
        0,
        live_turnover/
        250_000_000*100
    )

    return (

        gap_score*0.05 +

        recovery_score*0.20 +

        gain_score*0.35 +

        live_score*0.30

    )


def get_history(x):

    key = x["key"]

    today = datetime.now().date()

    to_date = (
        today-
        timedelta(days=1)
    )

    from_date = (
        today-
        timedelta(days=40)
    )

    # IMPORTANT:
    # Upstox requires | to be URL encoded as %7C.
    encoded_key = quote(
        key,
        safe=""
    )

    url = (
        f"{HIST_URL}/"
        f"{encoded_key}/"
        f"days/1/"
        f"{to_date.isoformat()}/"
        f"{from_date.isoformat()}"
    )

    try:

        r = requests.get(
            url,
            headers=api_headers(),
            timeout=30
        )

        if r.status_code != 200:
            return key,None

        candles = (
            r.json()
            .get("data",{})
            .get("candles",[])
        )

        turnovers = []

        for c in candles:

            if len(c)<6:
                continue

            try:

                close = float(c[4])
                volume = float(c[5])

                if close<=0:
                    continue

                turnover = (
                    close*volume
                )

                turnovers.append(
                    turnover
                )

            except:

                continue

        if len(turnovers)<HISTORY_DAYS:
            return key,None

        turnovers = turnovers[
            -HISTORY_DAYS:
        ]

        avg_turnover = (
            sum(turnovers)/
            len(turnovers)
        )

        return key,avg_turnover

    except:

        return key,None


def history_for_top(top):

    today_key = (
        datetime.now()
        .date()
        .isoformat()
    )

    if cache["history_date"] != today_key:

        cache["history"] = {}

        cache["history_date"] = today_key

    todo = [

        x for x in top

        if x["key"]
        not in cache["history"]

    ]

    if todo:

        with ThreadPoolExecutor(
            max_workers=WORKERS
        ) as ex:

            futures = {

                ex.submit(
                    get_history,x
                ):x

                for x in todo

            }

            for f in as_completed(
                futures
            ):

                key,avg = f.result()

                if avg is not None:

                    cache["history"][
                        key
                    ] = avg

    return cache["history"]


def final_score(
    x,
    avg_turnover
):

    op = x["open"]
    low = x["low"]
    ltp = x["ltp"]
    prev = x["prev"]

    gap = (
        (op-low)/op
    )*100

    recovery = (
        (ltp-low)/low
    )*100

    gain = (
        (ltp-prev)/prev
    )*100

    live_turnover = (
        x["volume"]*ltp
    )

    # -----------------------------------
    # 1. OPEN-LOW = 5%
    # -----------------------------------

    gap_score = max(
        0,
        (1-gap/0.50)*100
    )

    # -----------------------------------
    # 2. RECOVERY = 20%
    # -----------------------------------

    recovery_score = max(
        0,
        recovery/1.50*100
    )

    # -----------------------------------
    # 3. GAIN = 35%
    #
    # No artificial 100 cap.
    # -----------------------------------

    gain_score = max(
        0,
        gain/3.00*100
    )

    # -----------------------------------
    # 4. LIVE TURNOVER = 30%
    #
    # No artificial 100 cap.
    # -----------------------------------

    if avg_turnover>0:

        live_score = (

            live_turnover/
            (
                avg_turnover*
                LIVE_TURNOVER_MULTIPLIER
            )
        )*100

    else:

        live_score = 0

    live_score = max(
        0,
        live_score
    )

    # -----------------------------------
    # 5. AVERAGE TURNOVER = 10%
    # -----------------------------------

    avg_score = max(
        0,
        avg_turnover/
        AVG_TURNOVER_BASE*100
    )

    # -----------------------------------
    # FINAL RAW SCORE
    # -----------------------------------

    raw_score = (

        gap_score*0.05 +

        recovery_score*0.20 +

        gain_score*0.35 +

        live_score*0.30 +

        avg_score*0.10

    )

    return {

        "raw":raw_score,

        "gap_score":gap_score,

        "recovery_score":recovery_score,

        "gain_score":gain_score,

        "live_score":live_score,

        "avg_score":avg_score,

        "live_turnover":live_turnover,

        "recovery":recovery,

        "gain":gain

    }


@app.route("/")
def home():

    return render_template_string(
        HTML
    )


@app.route("/scan")
def scan():

    if not TOKEN:

        return jsonify({

            "error":
            "UPSTOX_ACCESS_TOKEN Render में उपलब्ध नहीं है."

        }),500

    try:

        instruments = (
            load_instruments()
        )

        quotes = get_quotes(
            instruments
        )

        if not quotes:

            return jsonify({

                "error":
                "Upstox से valid NSE EQ quotes नहीं मिले."

            }),500

        # --------------------------------
        # FAST PRE-SCAN
        # --------------------------------

        for x in quotes:

            x["_pre"] = (
                preliminary_score(x)
            )

        quotes.sort(
            key=lambda x:x["_pre"],
            reverse=True
        )

        top = quotes[
            :TOP_HISTORY
        ]

        # --------------------------------
        # 20 DAY HISTORY
        # --------------------------------

        hist = history_for_top(
            top
        )

        final = []

        for x in top:

            avg = hist.get(
                x["key"]
            )

            if avg is None:
                continue

            fs = final_score(
                x,
                avg
            )

            final.append({

                "symbol":
                x["symbol"],

                "ltp":
                x["ltp"],

                "open":
                x["open"],

                "low":
                x["low"],

                "gain":
                fs["gain"],

                "recovery":
                fs["recovery"],

                "avg_turnover":
                avg,

                "live_turnover":
                fs["live_turnover"],

                "raw":
                fs["raw"]

            })

        if not final:

            return jsonify({

                "error":
                "20-day historical turnover उपलब्ध नहीं हुआ."

            }),500

        # --------------------------------
        # REAL RANKING
        # --------------------------------
        #
        # Primary:
        #   Raw strength
        #
        # Tie-break:
        #   Gain
        #   Live turnover
        #   Recovery
        #   Average turnover
        # --------------------------------

        final.sort(

            key=lambda x:(

                x["raw"],

                x["gain"],

                x["live_turnover"],

                x["recovery"],

                x["avg_turnover"]

            ),

            reverse=True
        )

        # --------------------------------
        # DISPLAY STRENGTH
        # --------------------------------
        #
        # केवल सबसे मजबूत को 100.
        #
        # बाकी उसके अनुपात में.
        #
        # इसलिए KISHT/FSL जैसी
        # 100-100 समस्या नहीं रहेगी.
        # --------------------------------

        max_raw = max(
            x["raw"]
            for x in final
        )

        for x in final:

            if max_raw>0:

                x["strength"] = (

                    x["raw"]/
                    max_raw
                )*100

            else:

                x["strength"] = 0

            del x["raw"]

            del x["recovery"]

        now = datetime.now().strftime(
            "%d-%m-%Y %H:%M:%S"
        )

        return jsonify({

            "time":now,

            "results":final

        })

    except Exception as e:

        return jsonify({

            "error":str(e)

        }),500


if __name__=="__main__":

    app.run(

        host="0.0.0.0",

        port=int(
            os.getenv(
                "PORT",
                "10000"
            )
        )

    )
