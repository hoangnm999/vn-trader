"""
VN Trader Backend - Flask API Server
Dữ liệu thực từ TCBS/SSI qua thư viện vnstock (miễn phí)
Deploy lên Railway để chạy 24/7
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import math
from datetime import datetime, timedelta
import threading
import time

app = Flask(__name__)
CORS(app)  # Cho phép web app gọi API

# ── Cache giá để tránh gọi API quá nhiều ──────────────────────────────────────
price_cache = {}
cache_lock = threading.Lock()
CACHE_TTL = 60  # giây

def get_stock_price(symbol: str):
    """Lấy giá thực từ TCBS qua vnstock. Fallback về cache nếu lỗi."""
    now = time.time()
    with cache_lock:
        if symbol in price_cache:
            cached = price_cache[symbol]
            if now - cached["ts"] < CACHE_TTL:
                return cached["data"]
    try:
        from vnstock import Vnstock
        stock = Vnstock().stock(symbol=symbol, source="TCBS")
        # Lấy giá intraday mới nhất
        df = stock.quote.intraday(symbol=symbol, page_size=1)
        if df is not None and not df.empty:
            price = float(df.iloc[-1]["price"]) * 1000  # TCBS trả về đơn vị nghìn đồng
            change_pct = float(df.iloc[-1].get("change_percent", 0))
            result = {"price": price, "change_pct": change_pct, "source": "TCBS-live"}
        else:
            raise ValueError("Empty dataframe")
    except Exception as e:
        # Fallback: lấy giá đóng cửa hôm qua
        try:
            from vnstock import Vnstock
            stock = Vnstock().stock(symbol=symbol, source="TCBS")
            end = datetime.now().strftime("%Y-%m-%d")
            start = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
            df = stock.quote.history(start=start, end=end, interval="1D")
            if df is not None and not df.empty:
                last = df.iloc[-1]
                price = float(last["close"]) * 1000
                prev  = float(df.iloc[-2]["close"]) * 1000 if len(df) >= 2 else price
                change_pct = (price - prev) / prev * 100 if prev > 0 else 0
                result = {"price": price, "change_pct": round(change_pct, 2), "source": "TCBS-daily"}
            else:
                raise ValueError("No history data")
        except Exception as e2:
            result = {"price": 0, "change_pct": 0, "source": "error", "error": str(e2)}

    with cache_lock:
        price_cache[symbol] = {"data": result, "ts": now}
    return result


def get_stock_history(symbol: str, days: int = 90):
    """Lấy lịch sử giá để vẽ chart."""
    try:
        from vnstock import Vnstock
        stock = Vnstock().stock(symbol=symbol, source="TCBS")
        end   = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=days + 5)).strftime("%Y-%m-%d")
        df    = stock.quote.history(start=start, end=end, interval="1D")
        if df is not None and not df.empty:
            records = []
            for _, row in df.tail(days).iterrows():
                records.append({
                    "date":   str(row.get("time", row.name))[:10],
                    "open":   round(float(row["open"])  * 1000, 0),
                    "high":   round(float(row["high"])  * 1000, 0),
                    "low":    round(float(row["low"])   * 1000, 0),
                    "close":  round(float(row["close"]) * 1000, 0),
                    "volume": int(row.get("volume", 0)),
                })
            return records
    except Exception as e:
        pass
    return []


def calc_indicators(prices: list):
    """Tính RSI, MACD, BB, EMA, SMA từ danh sách giá đóng cửa."""
    if not prices or len(prices) < 2:
        return {}

    def sma(p, n):
        if len(p) < n: return p[-1]
        return sum(p[-n:]) / n

    def ema(p, n):
        if len(p) < n: return p[-1]
        k, e = 2/(n+1), sum(p[:n])/n
        for v in p[n:]: e = v*k + e*(1-k)
        return e

    def rsi(p, n=14):
        if len(p) < n+1: return 50
        gains, losses = 0, 0
        for i in range(len(p)-n, len(p)):
            d = p[i] - p[i-1]
            if d > 0: gains += d
            else:     losses += abs(d)
        ag, al = gains/n, losses/n
        if al == 0: return 100
        return round(100 - 100/(1 + ag/al), 1)

    close = prices
    rsi_val  = rsi(close)
    ema12    = ema(close, 12)
    ema26    = ema(close, 26)
    macd_val = round(ema12 - ema26, 1)
    signal   = round(macd_val * 0.82, 1)
    sma50    = round(sma(close, min(50, len(close))), 0)
    ema20    = round(ema(close, min(20, len(close))), 0)

    n = min(20, len(close))
    bb_mid  = sma(close, n)
    std     = math.sqrt(sum((x - bb_mid)**2 for x in close[-n:]) / n)
    bb_upper = round(bb_mid + 2*std, 0)
    bb_lower = round(bb_mid - 2*std, 0)
    bb_mid   = round(bb_mid, 0)

    cur = close[-1]
    vol_avg = sum(close[-5:]) / 5 if len(close) >= 5 else cur

    # Scoring
    score = 50
    if rsi_val < 35:   score += 15
    elif rsi_val > 70: score -= 15
    if macd_val > signal: score += 10
    else:                 score -= 10
    if cur < bb_lower * 1.02: score += 10
    elif cur > bb_upper * 0.98: score -= 10
    if cur > ema20: score += 8
    else:           score -= 5
    if cur > sma50: score += 7
    else:           score -= 5
    score = max(15, min(90, score))

    signal_txt = "MUA" if score >= 60 else ("BÁN" if score <= 40 else "THEO DÕI")

    sl_pct = 0.05
    tp_pct = 0.12 if score >= 60 else 0.08

    return {
        "rsi":       rsi_val,
        "macd":      macd_val,
        "macd_signal": signal,
        "bb_upper":  bb_upper,
        "bb_lower":  bb_lower,
        "bb_mid":    bb_mid,
        "ema20":     ema20,
        "sma50":     sma50,
        "score":     score,
        "signal":    signal_txt,
        "sl":        round(cur * (1 - sl_pct), 0),
        "tp":        round(cur * (1 + tp_pct), 0),
        "sl_pct":    round(sl_pct * 100, 1),
        "tp_pct":    round(tp_pct * 100, 1),
        "rr_ratio":  round(tp_pct / sl_pct, 1),
    }


# ── API Routes ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return jsonify({"status": "VN Trader API running", "time": datetime.now().isoformat()})


@app.route("/api/price/<symbol>")
def api_price(symbol):
    symbol = symbol.upper().strip()
    data   = get_stock_price(symbol)
    return jsonify({"symbol": symbol, **data})


@app.route("/api/prices")
def api_prices():
    symbols_str = request.args.get("symbols", "VCB,HPG,VHM,FPT,MWG,TCB")
    symbols = [s.strip().upper() for s in symbols_str.split(",")]
    result  = {}
    for sym in symbols[:20]:   # max 20 mã
        result[sym] = get_stock_price(sym)
    return jsonify(result)


@app.route("/api/analyze/<symbol>")
def api_analyze(symbol):
    symbol  = symbol.upper().strip()
    days    = int(request.args.get("days", 90))
    history = get_stock_history(symbol, days)
    if not history:
        return jsonify({"error": f"Không lấy được dữ liệu cho {symbol}"}), 404

    closes     = [r["close"] for r in history]
    indicators = calc_indicators(closes)
    current    = get_stock_price(symbol)

    return jsonify({
        "symbol":     symbol,
        "price":      current.get("price", closes[-1] if closes else 0),
        "change_pct": current.get("change_pct", 0),
        "history":    history,
        "indicators": indicators,
        "source":     current.get("source", "unknown"),
        "updated_at": datetime.now().isoformat(),
    })


@app.route("/api/market")
def api_market():
    """Chỉ số thị trường - VN-Index, HNX, VN30."""
    indices = {
        "VNINDEX": {"name": "VN-INDEX"},
        "HNX":     {"name": "HNX-INDEX"},
        "VN30":    {"name": "VN30"},
        "UPCOM":   {"name": "UPCOM"},
    }
    results = {}
    for sym, meta in indices.items():
        price_data = get_stock_price(sym)
        results[sym] = {**meta, **price_data}
    return jsonify(results)


@app.route("/api/signals")
def api_signals():
    """Quét tín hiệu mua/bán cho danh sách cổ phiếu VN30."""
    watchlist = ["VCB","HPG","VHM","FPT","MWG","TCB","BID","VNM","VIC","CTG",
                 "GAS","VPB","MSN","STB","DGC","ACB","MBB","HDB","VHC","REE"]
    signals = []
    for sym in watchlist:
        try:
            history = get_stock_history(sym, 60)
            if not history: continue
            closes = [r["close"] for r in history]
            ind    = calc_indicators(closes)
            price  = get_stock_price(sym)
            if ind.get("signal") in ("MUA", "BÁN"):
                signals.append({
                    "symbol":     sym,
                    "signal":     ind["signal"],
                    "score":      ind["score"],
                    "rsi":        ind["rsi"],
                    "price":      price.get("price", closes[-1]),
                    "change_pct": price.get("change_pct", 0),
                })
        except:
            continue
    signals.sort(key=lambda x: x["score"], reverse=True)
    return jsonify(signals[:10])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
