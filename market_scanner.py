"""
market_scanner.py v2.0 — Market Scanner toàn sàn HOSE/HNX
===========================================================
Lọc 3 tầng để tìm mã tiềm năng mỗi ngày:

  Tầng 1: Thanh khoản thực tế
    ADTV > 5 tỷ/ngày  (nâng từ 3→5, phù hợp vốn 200 triệu)
    Giá  > 10,000đ    (nâng từ 5k→10k, loại thêm penny stock)

  Tầng 2: Xu hướng + Không overextended
    Giá > MA50 > MA200        (golden zone — uptrend xác nhận)
    Giá < MA50 × 1.15         (chưa quá xa MA50, tránh mua đỉnh)
    RSI < 70                  (chưa overbought)
    RS20 > -2%                (không lag quá nhiều vs VNINDEX)

  Tầng 3: Kỹ thuật hôm nay
    Score A >= 60             (momentum kỹ thuật tốt)

Lệnh Telegram: /scan
"""

import numpy as np
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SCAN_MIN_VOL_BILLION = 5.0    # >= 5 tỷ/ngày = ADTV an toàn cho vốn 200 triệu
                               # Logic: lệnh 20 triệu / 5 tỷ = 0.4% ADTV → không bị slippage
SCAN_MIN_PRICE       = 10_000 # >= 10,000đ → loại penny stock và mã sắp hủy
SCAN_MIN_SCORE_A     = 60     # Score A >= 60 → đang có kỹ thuật tốt (thấp hơn ngưỡng MUA 65)
SCAN_MIN_RS20        = -2.0   # RS 20 ngày >= -2% → không lag quá nhiều vs VNINDEX
SCAN_MAX_EXTEND_MA50 = 1.15   # Giá <= MA50 × 1.15 → chưa overextended (tối đa 15% trên MA50)
SCAN_MAX_RSI         = 70     # RSI <= 70 → chưa overbought, còn room tăng
SCAN_TOP_N           = 10     # Trả về top 10 mã
SCAN_MAX_WORKERS     = 4      # Thread song song (giới hạn tránh rate limit vnstock)
SCAN_DELAY_SEC       = 0.5    # Delay 0.5s giữa các request

# ── Danh sách mã quét (VN30 + mid-cap thanh khoản tốt) ───────────────────────
HOSE_LIQUID = [
    # ── VN30 ──────────────────────────────────────────────────────────────────
    'VCB','BID','CTG','TCB','MBB','VPB','ACB','TPB','HDB','STB',
    'VHM','VIC','VRE','NVL','DXG','PDR','KDH','NLG','DIG','BCM',
    'FPT','CMG','MWG','FRT','PNJ','MSN','SAB','VNM','MCH','QNS',
    'GAS','PLX','BSR','OIL','PVS','PVD','PVT','GEG','POW','REE',
    'HPG','HSG','NKG','SMC','TLH','POM','VIS','DTL','TVN','DNH',
    'SSI','VND','HCM','VCI','BSI','AGR','CTS','MBS','SHS','VIX',
    'DGC','DCM','DPM','CSV','SFG','HAH','VSC','GMD','DVP','PHP',
    'VJC','HVN','ACV','SCS',
    # ── Mid-cap HOSE tiềm năng ────────────────────────────────────────────────
    'PC1','SZC','KBC','IDC','LHG','NTC','TDC','HDG','CII','IJC',
    'VGC','BMP','NT2','GEX','SBA','TBC','TMP','NBB','SCR','HLD',
    'SSB','LPB','BVB','NAB','VBB','BAB','PGB','KLB','SGB','VCF',
    'CTD','FCN','VCG','DGW','PPC','PHR','GVR','HNG','KDC','MCM',
    'BWE','TDM','TIG','HAG','ASM','DBC','BAF','MML','SIP','SCS',
    'VPH','TDH','SC5','HBC',
    # ── HNX thanh khoản ───────────────────────────────────────────────────────
    'PVB','SHB','NVB','VGS','HUT','PLC','VCS','BVS','APS',
    'KSB','THD','NTP','NET','VIT','SDT','HGM','PMG','SGO',
    'CEO','IDJ','TNG','VNS','CLG','MEC','SD5','VNR','HAN',
]

EXCLUDE_SYMBOLS = {'VN30F1M','VN30F2M','VN30F1Q','VNINDEX','HNX30','UPCOM'}


def _load_ohlcv(symbol, days=220):
    """Load OHLCV từ vnstock với fallback VCI → TCBS."""
    end   = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    for source in ['VCI', 'TCBS']:
        try:
            from vnstock import Vnstock
            df = (Vnstock().stock(symbol=symbol, source=source)
                           .quote.history(start=start, end=end, interval='1D'))
            if df is not None and len(df) >= 40:
                return df, source
        except Exception:
            continue
    return None, None


def _arrays(df):
    """Trích xuất numpy arrays từ DataFrame, tự scale nếu cần."""
    def ta(names):
        col = next((c for c in df.columns if c.lower() in names), None)
        if col is None: return None
        arr = pd.to_numeric(df[col], errors='coerce').fillna(0).values.copy()
        if arr.max() < 1000 and arr.max() > 0: arr = arr * 1000
        return arr

    closes  = ta({'close','closeprice','close_price'})
    highs   = ta({'high','highprice','high_price'})
    lows    = ta({'low','lowprice','low_price'})
    volumes = ta({'volume','volume_match','klgd','vol','trading_volume',
                  'match_volume','total_volume','dealvolume','matchingvolume'})
    return closes, highs, lows, volumes


def _calc_rsi_fast(closes, period=14):
    """Tính RSI nhanh từ numpy array closes."""
    if len(closes) < period + 1:
        return 50.0
    d  = np.diff(closes[-(period+2):])
    g  = np.where(d > 0, d, 0.0)
    l  = np.where(d < 0, -d, 0.0)
    ag = np.mean(g[-period:]) if len(g) >= period else np.mean(g)
    al = np.mean(l[-period:]) if len(l) >= period else np.mean(l)
    return round(100 - 100 / (1 + ag / al), 1) if al > 0 else 100.0


def scan_symbol(symbol):
    """
    Quét 1 mã qua 3 tầng lọc.
    Trả về dict kết quả hoặc None nếu không đạt.
    """
    try:
        time.sleep(SCAN_DELAY_SEC)
        df, source = _load_ohlcv(symbol)
        if df is None: return None

        closes, highs, lows, volumes = _arrays(df)
        if closes is None or len(closes) < 50: return None

        price   = float(closes[-1])
        vol_arr = volumes[-20:] if volumes is not None else np.zeros(20)
        vol_pos = vol_arr[vol_arr > 0]
        vol_ma  = float(np.mean(vol_pos)) if len(vol_pos) > 0 else 0
        adtv_b  = vol_ma * price / 1e9   # Tỷ đồng/ngày

        # ══ TẦNG 1: Thanh khoản + Giá tối thiểu ═════════════════════════════
        if price < SCAN_MIN_PRICE:
            return None   # Penny stock / mã yếu
        if adtv_b < SCAN_MIN_VOL_BILLION:
            return None   # Thanh khoản không đủ

        # ══ TẦNG 2: Xu hướng + Không overextended ════════════════════════════
        ma50  = float(np.mean(closes[-50:]))  if len(closes) >= 50  else 0
        ma200 = float(np.mean(closes[-200:])) if len(closes) >= 200 else ma50

        # 2a. Golden zone: Giá > MA50 > MA200
        if price < ma50 * 0.98:   return None   # Dưới MA50
        if ma50  < ma200 * 0.98:  return None   # MA50 dưới MA200

        # 2b. Chưa overextended: giá không quá 15% trên MA50
        #     Tránh mua khi mã đã tăng rất mạnh, dễ điều chỉnh
        if ma50 > 0 and price > ma50 * SCAN_MAX_EXTEND_MA50:
            return None

        # 2c. RSI chưa overbought (< 70)
        rsi = _calc_rsi_fast(closes)
        if rsi > SCAN_MAX_RSI:
            return None

        # 2d. RS vs VNINDEX — không lag quá nhiều
        from relative_strength import calc_rs_signals
        h_arr   = highs if highs is not None else closes
        rs_data = calc_rs_signals(closes, h_arr, symbol)
        rs20    = rs_data.get('rs_20d', 0) or 0
        rs5     = rs_data.get('rs_5d',  0) or 0
        if rs20 < SCAN_MIN_RS20:
            return None

        # ══ TẦNG 3: Score A kỹ thuật ═════════════════════════════════════════
        try:
            from backtest import compute_score_at
            l_arr = lows if lows is not None else closes
            score_a, action = compute_score_at(closes, h_arr, l_arr,
                                               volumes if volumes is not None else np.zeros(len(closes)),
                                               len(closes) - 1)
        except Exception:
            score_a, action = 0, 'THEO_DOI'

        if score_a < SCAN_MIN_SCORE_A:
            return None

        # ── Tính thêm thông tin hiển thị ─────────────────────────────────────
        rs_bonus      = rs_data.get('total_bonus', 0)
        score_total   = score_a + rs_bonus
        b52w          = rs_data.get('breakout_52w', False)
        b60d          = rs_data.get('breakout_60d', False)
        pct_above_ma50 = round((price / ma50 - 1) * 100, 1) if ma50 > 0 else 0

        # Scanner chỉ dùng Score A thực tế để phân loại — không dùng score_total
        # để tránh nhầm lẫn với tín hiệu MUA thực sự (cần Score A >= 65)
        ready_to_buy = (score_a >= 65 and action == 'MUA')

        return {
            'symbol':          symbol,
            'price':           round(price, 0),
            'score_a':         score_a,
            'action':          action,
            'ready_to_buy':    ready_to_buy,
            'rs_5d':           rs5,
            'rs_20d':          rs20,
            'rs_bonus':        rs_bonus,
            'score_total':     score_total,
            'adtv_b':          round(adtv_b, 1),
            'ma50':            round(ma50, 0),
            'ma200':           round(ma200, 0),
            'rsi':             rsi,
            'pct_above_ma50':  pct_above_ma50,
            'breakout_52w':    b52w,
            'breakout_60d':    b60d,
            'rs_emoji':        rs_data.get('rs_emoji', ''),
            'rs_label':        rs_data.get('rs_label', ''),
            'source':          source,
        }

    except Exception as e:
        logger.debug(f'scan_symbol {symbol}: {e}')
        return None


def run_market_scan(symbols=None, top_n=SCAN_TOP_N, progress_cb=None):
    """
    Chạy full market scan song song.
    progress_cb(done, total, symbol) để cập nhật tiến độ.
    """
    if symbols is None:
        symbols = [s for s in HOSE_LIQUID if s not in EXCLUDE_SYMBOLS]

    logger.info(f'Market scan v2: {len(symbols)} symbols...')
    results = []
    total   = len(symbols)
    done    = 0

    with ThreadPoolExecutor(max_workers=SCAN_MAX_WORKERS) as ex:
        futures = {ex.submit(scan_symbol, sym): sym for sym in symbols}
        for future in as_completed(futures):
            done += 1
            sym = futures[future]
            try:
                r = future.result()
                if r: results.append(r)
            except Exception as e:
                logger.debug(f'{sym}: {e}')
            if progress_cb:
                progress_cb(done, total, sym)

    # Sort: breakout 52w ưu tiên cao nhất → score_total → rs_20d
    results.sort(key=lambda x: (
        x.get('breakout_52w', False),
        x.get('score_total', 0),
        x.get('rs_20d', 0),
    ), reverse=True)

    return results[:top_n]


def format_scan_msg(results, scan_time_sec=None):
    """Format kết quả scan thành HTML cho Telegram."""
    NL  = chr(10)
    now = datetime.now().strftime('%d/%m %H:%M')

    if not results:
        return (f'&#x1F4CA; <b>Market Scanner \u2014 {now}</b>{NL}{NL}'
                f'Khong tim thay ma nao du tieu chi hom nay.{NL}'
                f'Co the thi truong dang sideway toan dien hoac bear market.{NL}'
                f'Scanner se tu dong thu lai sau phien tiep theo.')

    t = f' (~{scan_time_sec:.0f}s)' if scan_time_sec else ''
    msg  = f'&#x1F4E1; <b>Market Scanner{t} \u2014 {now}</b>{NL}'
    msg += f'Top {len(results)} ma tiem nang hom nay{NL}'
    msg += 'Loc: Gia&gt;' + str(SCAN_MIN_PRICE//1000) + 'k | ADTV&gt;' + str(SCAN_MIN_VOL_BILLION) + 'ty' + NL
    msg += 'Gia&gt;MA50&gt;MA200 | Gia&lt;MA50x' + str(SCAN_MAX_EXTEND_MA50) + ' | RSI&lt;' + str(SCAN_MAX_RSI) + NL
    msg += '=' * 34 + NL + NL

    for i, r in enumerate(results, 1):
        sym   = r['symbol']
        sc_a  = r['score_a']
        sc_t  = r['score_total']
        rs20  = r['rs_20d']
        rs5   = r['rs_5d']
        em    = r.get('rs_emoji', '')
        pr    = r['price']
        adtv  = r['adtv_b']
        rsi   = r.get('rsi', 0)
        pma50 = r.get('pct_above_ma50', 0)
        b52   = r.get('breakout_52w', False)
        b60   = r.get('breakout_60d', False)
        ready = r.get('ready_to_buy', False)

        # Trạng thái rõ ràng: THEO DOI (chưa đủ 65) hoặc SAN SANG (đủ điều kiện)
        # KHÔNG dùng "MUA" để tránh nhầm với tín hiệu /signals
        if ready:
            status = '&#x2705; <b>SAN SANG</b> (ScoreA=' + str(sc_a) + '>=65)'
        else:
            gap = 65 - sc_a
            status = '&#x1F440; THEO DOI (con thieu ' + str(gap) + 'd de dat nguong MUA)'

        bf  = ' &#x1F3AF;52W-BREAK' if b52 else (' &#x1F4CA;60D' if b60 else '')
        ext = ' &#x26A0;' if pma50 > 10 else ''

        msg += (f'{i}. <b>{sym}</b>{bf}{NL}'
                f'   {status}{NL}'
                f'   Gia: {pr:,.0f}d | ScoreA: {sc_a} | +RS: {rs_t if (rs_t:=sc_t-sc_a) else 0}d{NL}'
                f'   {em} RS20: {rs20:+.1f}% | RSI: {rsi:.0f}{ext}{NL}'
                f'   Gia/MA50: {pma50:+.1f}% | ADTV: {adtv:.1f}ty{NL}{NL}')

    msg += ('<i>Scanner = danh sach THEO DOI, khong phai lenh MUA.</i>' + NL
            + '<i>SAN SANG = ScoreA>=65, van can /analyze xac nhan truoc khi vao lenh.</i>')
    return msg
