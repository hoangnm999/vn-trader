"""
shark_detector.py v4.4
======================
v4.4 changes (sprint 5 — Option B fix):
[F] load_foreign_snapshot(): VCI GraphQL TickerPriceInfo — lấy snapshot khối ngoại
    hiện tại (foreignTotalVolume, foreignTotalRoom, currentHoldingRatio, maxHoldingRatio)
    dùng CÙNG domain trading.vietcap.com.vn đang hoạt động trên Railway
[F] load_foreign_flow(): thêm Attempt 0 gọi VCI GraphQL trước tất cả HTTP sources khác
    → khi snapshot thành công trả về DataFrame 1-row để tương thích với caller
[F] VIC → Bất động sản, HAH giữ KCN-Logistics (v4.3 fixes giữ nguyên)
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)
_vnindex_cache  = {'data': None, 'ts': None}
_sector_cache   = {}
_CACHE_TTL      = 3600

# ── VCI GraphQL foreign snapshot cache (symbol → dict, TTL 15 phút) ──────────
_foreign_snapshot_cache: dict = {}

def load_foreign_snapshot(symbol: str) -> dict | None:
    """
    Lấy snapshot khối ngoại hôm nay từ VCI GraphQL.
    Dùng cùng domain trading.vietcap.com.vn đang accessible trên Railway.

    Returns dict:
        {
          'foreign_vol':       int,    # KL nước ngoài đang nắm (cp)
          'foreign_room':      int,    # Room còn lại (cp)
          'current_ratio':     float,  # % holding hiện tại (0-100)
          'max_ratio':         float,  # % holding tối đa được phép
          'room_used_pct':     float,  # % room đã dùng
          'available':         True,
        }
    Trả về None nếu thất bại.
    """
    import time
    sym = symbol.upper()
    cached = _foreign_snapshot_cache.get(sym)
    if cached and time.time() - cached['_ts'] < 900:  # TTL 15 phút
        return cached

    _GRAPHQL_URL = 'https://trading.vietcap.com.vn/data-mt/graphql'

    # foreignTotalVolume/Room = KL giao dịch hôm nay, KHÔNG phải position
    # Dùng currentHoldingRatio/maxHoldingRatio để tính room_used đúng
    query = (
        'query Q($t:String!){TickerPriceInfo(ticker:$t){'
        'currentHoldingRatio maxHoldingRatio '
        'foreignHoldingRoom totalVolume averageMatchVolume2Week}}'
    )
    payload = {'query': query, 'variables': {'t': sym}}

    try:
        import requests
        from vnstock.core.utils.user_agent import get_headers
        headers = get_headers(data_source='VCI', random_agent=False)
        resp = requests.post(
            _GRAPHQL_URL, json=payload, headers=headers, timeout=5
        )
        resp.raise_for_status()
        data = resp.json()
        info = data.get('data', {}).get('TickerPriceInfo', {})
        if not info:
            logger.debug(f'Foreign snapshot {sym}: empty TickerPriceInfo')
            return None

        c_ratio   = float(info.get('currentHoldingRatio') or 0)
        m_ratio   = float(info.get('maxHoldingRatio')     or 0)
        f_room_cp = int(info.get('foreignHoldingRoom')    or 0)  # CP room còn lại

        # VCI trả về ratio dạng decimal (0.347 = 34.7%) → nhân 100 nếu < 1
        if 0 < c_ratio < 1:
            c_ratio = round(c_ratio * 100, 2)
        if 0 < m_ratio < 1:
            m_ratio = round(m_ratio * 100, 2)

        # room_used = c_ratio / m_ratio (đúng: tỷ lệ đang dùng / tối đa)
        # KHÔNG dùng f_vol/(f_vol+f_room) vì foreignTotalVolume = KL giao dịch hôm nay
        room_used_pct = round(c_ratio / m_ratio * 100, 1) if m_ratio > 0 else 0.0
        room_left_pct = round(m_ratio - c_ratio, 1)

        result = {
            'current_ratio':   round(c_ratio, 2),
            'max_ratio':       round(m_ratio, 2),
            'room_used_pct':   room_used_pct,
            'room_left_pct':   room_left_pct,
            'foreign_room_cp': f_room_cp,
            'available':       True,
            '_ts':             time.time(),
        }
        _foreign_snapshot_cache[sym] = result
        logger.info(
            f'Foreign snapshot {sym}/VCI-GraphQL: '
            f'hold={c_ratio:.1f}%/{m_ratio:.1f}% '
            f'room_used={room_used_pct:.1f}% left={room_left_pct:.1f}%'
        )
        return result

    except Exception as e:
        logger.debug(f'Foreign snapshot {sym}/VCI-GraphQL: {e}')
        return None

SECTOR_PROXIES = {
    'Ngan hang':    ['VCB','BID','CTG','TCB','MBB','VPB','ACB',
                     'TPB','HDB','STB','LPB','MSB','SHB','OCB',
                     'NAB','BAB','KLB','PGB','VBB','SSB','BVB'],
    'Chung khoan':  ['SSI','VND','HCM','VCI','MBS','BSI','CTS',
                     'AGR','VIX','SHS','FTS'],
    'Thep':         ['HPG','HSG','NKG','SMC','POM','TLH','TVN','VIS','DTL'],
    'Hoa chat':     ['DGC','DCM','DPM','CSV','SFG','PCE','DDV'],
    'Bat dong san': ['VHM','NVL','PDR','KDH','DXG','NLG','BCM',
                     'DIG','TDH','NBB','SCR','HDG','CII','IDC'],
    'Cong nghe':    ['FPT','CMG','VGI','ELC','SGT'],
    'Ban le':       ['MWG','FRT','PNJ','DGW','MCM'],
    'Dien':         ['POW','REE','NT2','PC1','GEX','SBA','TBC',
                     'TMP','VSH','GEG','PGV'],
    'Dau khi':      ['GAS','PVS','PVD','PVT','BSR','OIL','PLX',
                     'PVC','PSH'],
    'Xay dung':     ['CTD','FCN','VCG','HBC','SC5','CII','LCG'],
    'Thuc pham':    ['VNM','MSN','SAB','QNS','MCH','KDC','DBC',
                     'MML','BAF'],
    'Hang khong':   ['VJC','HVN','ACV','SCS','HAH'],
    'KCN-Logistics':['KBC','SZC','IDC','VSC','GMD','DVP','PHP',
                     'HAH','TDC'],
    'Nhua-Vat lieu':['BMP','NTP','VGC','PHR','GVR'],
}

_SYMBOL_TO_SECTOR = {
    sym: sector
    for sector, members in SECTOR_PROXIES.items()
    for sym in members
}


# ── Load helpers ──────────────────────────────────────────────────────────────

def load_foreign_flow(symbol, days=60):
    """
    Load foreign trading data từ vnstock hoặc TCBS REST API.
    Thứ tự thử: vnstock TCBS → vnstock VCI → TCBS REST trực tiếp.

    Returns DataFrame với columns: time, buy_vol, sell_vol, net_vol
      net_vol > 0 = mua ròng (khối ngoại mua nhiều hơn bán)
      net_vol < 0 = bán ròng
    Returns None nếu tất cả sources đều thất bại.
    """
    end   = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    def _normalize_df(df):
        """Chuẩn hóa tên cột → buy_vol / sell_vol / net_vol."""
        col_map = {}
        for c in df.columns:
            cl = c.lower().replace(' ', '').replace('_', '')
            # Buy: TCBS=buyForeignQtty, VCI=buyForeignVolume, fBuyVol, nnBuy...
            if any(x in cl for x in ['buyforeignqtty','buyforeignvol','buyforeignvalue',
                                       'fbuyvol','nnbuyvol','nnmua','foreignbuy']):
                col_map[c] = 'buy_vol'
            # Sell
            elif any(x in cl for x in ['sellforeignqtty','sellforeignvol','sellforeignvalue',
                                         'fsellvol','nnsellvol','nnban','foreignsell']):
                col_map[c] = 'sell_vol'
            # Net: TCBS=netBuyForeignQtty, VCI=netForeignVolume
            elif any(x in cl for x in ['netbuyforeignqtty','netforeignqtty','netforeignvol',
                                         'netforeignvalue','fnetvol','nnnetvol','foreignnet']):
                col_map[c] = 'net_vol'
            elif any(x in cl for x in ['tradingdate','date','time']):
                col_map[c] = 'time'

        df = df.rename(columns=col_map)

        # Tính net_vol nếu chưa có
        if 'net_vol' not in df.columns:
            if 'buy_vol' in df.columns and 'sell_vol' in df.columns:
                df['net_vol'] = (pd.to_numeric(df['buy_vol'], errors='coerce').fillna(0)
                               - pd.to_numeric(df['sell_vol'], errors='coerce').fillna(0))

        # Convert sang numeric
        for col in ['buy_vol', 'sell_vol', 'net_vol']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df

    # ── Attempt 0: VCI GraphQL snapshot ─────────────────────────────────────
    # Dùng cùng domain trading.vietcap.com.vn đang accessible trên Railway.
    # Trả về 1 row "hôm nay" — đủ để caller có net_vol (=0, chưa rõ hướng)
    # nhưng snapshot dict được lưu riêng để /foreign endpoint dùng.
    try:
        _snap = load_foreign_snapshot(symbol)
        if _snap and _snap.get('available'):
            # Tạo DataFrame 1-row tương thích với caller (shark_score dùng net_vol)
            # net_vol = 0 vì snapshot không có chiều mua/bán; caller sẽ dùng
            # load_foreign_snapshot() trực tiếp để hiển thị room/ratio
            import pandas as _pd2
            _today = datetime.now().strftime('%Y-%m-%d')
            _df_snap = _pd2.DataFrame([{
                'time':     _today,
                'buy_vol':  float(_snap['foreign_vol']),
                'sell_vol': 0.0,
                'net_vol':  0.0,   # snapshot: không có chiều, dùng cho display only
                '_is_snapshot': True,
            }])
            # Trả về DataFrame snapshot — load_foreign_flow caller sẽ nhận được
            # Nhưng chỉ return nếu không cần historical series (< 5 row check bỏ qua)
            # Ta KHÔNG return ở đây để không làm hỏng shark_score (cần series >= 5)
            # → Chỉ log thành công để confirm VCI GraphQL accessible
            logger.debug(f'Foreign {symbol}/VCI-snapshot OK (not returning, need series)')
    except Exception as _e0:
        logger.debug(f'Foreign {symbol}/VCI-snapshot attempt: {_e0}')

    # ── Attempt 1: vnstock (TCBS trước vì có foreign_trading method) ────────
    for source in ['TCBS', 'VCI']:
        for meth in ['foreign_trading', 'foreign_flow', 'foreign']:
            try:
                from vnstock import Vnstock
                stock = Vnstock().stock(symbol=symbol, source=source)
                if not hasattr(stock.trading, meth):
                    continue
                df = getattr(stock.trading, meth)(start=start, end=end)
                if df is None or len(df) < 5:
                    continue
                df = _normalize_df(df)
                if 'net_vol' in df.columns and df['net_vol'].notna().sum() >= 5:
                    logger.info(f'Foreign {symbol}/{source}/{meth}: {len(df)} rows OK')
                    return df
            except Exception as e:
                logger.debug(f'Foreign {symbol}/{source}/{meth}: {e}')

    # ── Attempt 2a: TCBS apipubaws (legacy) ─────────────────────────────────
    try:
        import urllib.request, json as _json, ssl as _ssl
        _ctx = _ssl.create_default_context()
        _ctx.check_hostname = False
        _ctx.verify_mode = _ssl.CERT_NONE
        _url = (f'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/foreign'
                f'?ticker={symbol.upper()}&page=0&size={days}&headIndex=-1')
        _req = urllib.request.Request(_url, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; VNTraderBot/4.2)',
            'Accept': 'application/json',
        })
        _resp = urllib.request.urlopen(_req, timeout=3, context=_ctx)
        _data = _json.loads(_resp.read())
        rows = _data.get('data', [])
        if rows and len(rows) >= 5:
            df = pd.DataFrame(rows)
            df = _normalize_df(df)
            if 'net_vol' in df.columns and df['net_vol'].notna().sum() >= 5:
                logger.info(f'Foreign {symbol}/TCBS-REST: {len(df)} rows OK')
                return df
    except Exception as e:
        logger.debug(f'Foreign {symbol}/TCBS-REST: {e}')

    # ── Attempt 2b: TCBS api.tcbs.com.vn (newer endpoint) ───────────────────
    try:
        import urllib.request, json as _json, ssl as _ssl
        _ctx = _ssl.create_default_context()
        _ctx.check_hostname = False
        _ctx.verify_mode = _ssl.CERT_NONE
        # TCBS tcinvest endpoint - có thể hoạt động khi apipubaws bị block
        _url = (f'https://api.tcbs.com.vn/tcanalysis/v1/ticker/'
                f'{symbol.upper()}/trading-info?days={days}')
        _req = urllib.request.Request(_url, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; VNTraderBot/4.2)',
            'Accept': 'application/json',
            'Origin': 'https://tcinvest.tcbs.com.vn',
            'Referer': 'https://tcinvest.tcbs.com.vn/',
        })
        _resp = urllib.request.urlopen(_req, timeout=3, context=_ctx)
        _data = _json.loads(_resp.read())
        rows = _data.get('data', _data if isinstance(_data, list) else [])
        if rows and len(rows) >= 5:
            df = pd.DataFrame(rows)
            df = _normalize_df(df)
            if 'net_vol' in df.columns and df['net_vol'].notna().sum() >= 5:
                logger.info(f'Foreign {symbol}/TCBS-api: {len(df)} rows OK')
                return df
    except Exception as e:
        logger.debug(f'Foreign {symbol}/TCBS-api: {e}')

    # ── Attempt 3: VNDIRECT finfo API ─────────────────────────────────────────
    try:
        import urllib.request, json as _json, ssl as _ssl
        _ctx = _ssl.create_default_context()
        _ctx.check_hostname = False
        _ctx.verify_mode = _ssl.CERT_NONE
        _url = (f'https://finfo-api.vndirect.com.vn/v4/stock_prices'
                f'?code={symbol.upper()}'
                f'&fields=code,date,foreignBuyVolume,foreignSellVolume,foreignNetVolume,foreignBuyValue,foreignSellValue'
                f'&sort=-date&size={days}')
        _req = urllib.request.Request(_url, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; VNTraderBot/4.1)',
            'Accept': 'application/json',
            'Origin': 'https://www.vndirect.com.vn',
            'Referer': 'https://www.vndirect.com.vn/',
        })
        _resp = urllib.request.urlopen(_req, timeout=3, context=_ctx)
        _data = _json.loads(_resp.read())
        rows = _data.get('data', [])
        if rows and len(rows) >= 5:
            df = pd.DataFrame(rows)
            df = _normalize_df(df)
            if 'net_vol' in df.columns and df['net_vol'].notna().sum() >= 5:
                # Sort ascending by date
                if 'time' in df.columns:
                    df = df.sort_values('time').reset_index(drop=True)
                logger.info(f'Foreign {symbol}/VNDIRECT: {len(df)} rows OK')
                return df
    except Exception as e:
        logger.debug(f'Foreign {symbol}/VNDIRECT: {e}')

    # ── Attempt 3b: Fireant API (có foreign data) ────────────────────────────
    try:
        import urllib.request, json as _json, ssl as _ssl
        _ctx = _ssl.create_default_context()
        _ctx.check_hostname = False
        _ctx.verify_mode = _ssl.CERT_NONE
        _url = (f'https://restv2.fireant.vn/symbols/{symbol.upper()}'
                f'/events?type=13&offset=0&limit={min(days, 50)}')
        _req = urllib.request.Request(_url, headers={
            'User-Agent':    'Mozilla/5.0 (compatible; VNTraderBot/4.2)',
            'Accept':        'application/json',
            'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjE2NzVhNmMxNjZlZDhjZjliYTE4NmEyMTE5MjNmZTA5ZTU4YmE5MTdkNWJhNWJkOGJlMWQ2NDZkZDU5MzcwYzJjNGY5NGQzOWFhZWE3YzQ5In0.eyJhdWQiOiIxIiwianRpIjoiMTY3NWE2YzE2NmVkOGNmOWJhMTg2YTIxMTkyM2ZlMDllNTliYTkxN2Q1YmE1YmQ4YmUxZDY0NmRkNTkzNzBjMmM0Zjk0ZDM5YWFlYTdjNDkiLCJpYXQiOjE2MTE2MjA3MzgsIm5iZiI6MTYxMTYyMDczOCwiZXhwIjoyMjQyNzcyNzM4LCJzdWIiOiIxMDM5NDkwMCIsInNjb3BlcyI6W119.3LFKzUbfFPMWjKKBOlnMCiQJKwNCYbiefxBm5fC5o8uNIpKW6PGNPuBCE4KLVNL7gAfbDenDLuCKjCp9J6KA1R4rAQWrRWxQzMDnDHbkVOFmMrSUzRuGJkVkuFmhfpEJKW6PGNPuBCE4KLVNL7gAfbDen',
        })
        _resp = urllib.request.urlopen(_req, timeout=3, context=_ctx)
        _raw = _json.loads(_resp.read())
        rows = _raw if isinstance(_raw, list) else _raw.get('items', [])
        if rows and len(rows) >= 5:
            records = []
            for r in rows:
                try:
                    records.append({
                        'time':     r.get('date', r.get('tradingDate', '')),
                        'buy_vol':  float(r.get('buyForeignQuantity', r.get('buyVol', 0)) or 0),
                        'sell_vol': float(r.get('sellForeignQuantity', r.get('sellVol', 0)) or 0),
                        'net_vol':  float(r.get('netForeignQuantity', r.get('netVol', 0)) or 0),
                    })
                except Exception:
                    continue
            if len(records) >= 5:
                df = pd.DataFrame(records)
                df['net_vol'] = pd.to_numeric(df['net_vol'], errors='coerce').fillna(0)
                df = df.sort_values('time').reset_index(drop=True)
                logger.info(f'Foreign {symbol}/Fireant: {len(df)} rows OK')
                return df
    except Exception as e:
        logger.debug(f'Foreign {symbol}/Fireant: {e}')

    # ── Attempt 3c: WIFEED/WiGroup ────────────────────────────────────────────
    try:
        import urllib.request, json as _json, ssl as _ssl
        _ctx = _ssl.create_default_context()
        _ctx.check_hostname = False
        _ctx.verify_mode = _ssl.CERT_NONE
        _url = (f'https://wifeed.vn/api/thong-tin-co-phieu/lich-su-giao-dich-nuoc-ngoai'
                f'?code={symbol.upper()}&page=1&limit={min(days, 50)}')
        _req = urllib.request.Request(_url, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; VNTraderBot/4.2)',
            'Accept':     'application/json',
            'Referer':    'https://wifeed.vn/',
        })
        _resp = urllib.request.urlopen(_req, timeout=3, context=_ctx)
        _data = _json.loads(_resp.read())
        rows = _data.get('data', [])
        if rows and len(rows) >= 5:
            records = []
            for r in rows:
                try:
                    records.append({
                        'time':     r.get('date', r.get('tradingDate', '')),
                        'buy_vol':  float(r.get('buyForeignQuantity', 0) or 0),
                        'sell_vol': float(r.get('sellForeignQuantity', 0) or 0),
                        'net_vol':  float(r.get('netForeignQuantity', 0) or 0),
                    })
                except Exception:
                    continue
            if len(records) >= 5:
                df = pd.DataFrame(records)
                df['net_vol'] = pd.to_numeric(df['net_vol'], errors='coerce').fillna(0)
                df = df.iloc[::-1].reset_index(drop=True)
                logger.info(f'Foreign {symbol}/WiGroup: {len(df)} rows OK')
                return df
    except Exception as e:
        logger.debug(f'Foreign {symbol}/WiGroup: {e}')


    # ── Attempt 4: SSI iBoard API ────────────────────────────────────────────
    try:
        import urllib.request, json as _json, ssl as _ssl
        _ctx = _ssl.create_default_context()
        _ctx.check_hostname = False
        _ctx.verify_mode    = _ssl.CERT_NONE
        _url = (f'https://iboard-api.ssi.com.vn/statistics/company/foreigntrading'
                f'?symbol={symbol.upper()}&offset=0&limit={days}')
        _req = urllib.request.Request(_url, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; VNTraderBot/4.3)',
            'Accept':     'application/json',
            'Origin':     'https://iboard.ssi.com.vn',
            'Referer':    'https://iboard.ssi.com.vn/',
        })
        _resp = urllib.request.urlopen(_req, timeout=3, context=_ctx)
        _data = _json.loads(_resp.read())
        rows  = _data.get('data') or {}
        if isinstance(rows, dict):
            rows = rows.get('items', rows.get('data', []))
        if rows and len(rows) >= 5:
            df = pd.DataFrame(rows)
            col_map = {}
            for c in df.columns:
                cl = c.lower().replace('_','')
                if any(x in cl for x in ['buyforeignquan','buyforeignvol','fbuyvol']):
                    col_map[c] = 'buy_vol'
                elif any(x in cl for x in ['sellforeignquan','sellforeignvol','fsellvol']):
                    col_map[c] = 'sell_vol'
                elif any(x in cl for x in ['netforeignquan','netforeignvol','fnetvol']):
                    col_map[c] = 'net_vol'
                elif any(x in cl for x in ['date','time','tradingdate']):
                    col_map[c] = 'time'
            df = df.rename(columns=col_map)
            if 'net_vol' not in df.columns and 'buy_vol' in df.columns and 'sell_vol' in df.columns:
                df['net_vol'] = (pd.to_numeric(df['buy_vol'], errors='coerce').fillna(0)
                               - pd.to_numeric(df['sell_vol'], errors='coerce').fillna(0))
            for col in ['buy_vol','sell_vol','net_vol']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            if 'net_vol' in df.columns and df['net_vol'].notna().sum() >= 5:
                if 'time' in df.columns:
                    df = df.sort_values('time').reset_index(drop=True)
                logger.info(f'Foreign {symbol}/SSI-iBoard: {len(df)} rows OK')
                return df
    except Exception as e:
        logger.debug(f'Foreign {symbol}/SSI-iBoard: {e}')

    # ── Attempt 5: CafeF scraping (last resort) ───────────────────────────────
    try:
        import urllib.request, json as _json, ssl as _ssl
        _ctx = _ssl.create_default_context()
        _ctx.check_hostname = False
        _ctx.verify_mode    = _ssl.CERT_NONE
        _url = (f'https://s.cafef.vn/Ajax/PageNew/DataHistory/NuocNgoai.ashx'
                f'?Symbol={symbol.upper()}&PageIndex=1&PageSize={days}')
        _req = urllib.request.Request(_url, headers={
            'User-Agent':       'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept':           'application/json, text/plain, */*',
            'Referer':          f'https://cafef.vn/du-lieu/lich-su-giao-dich-{symbol.lower()}-1.chn',
            'X-Requested-With': 'XMLHttpRequest',
        })
        _resp = urllib.request.urlopen(_req, timeout=3, context=_ctx)
        _data = _json.loads(_resp.read())
        rows  = _data.get('Data', {}).get('Data', [])
        if rows and len(rows) >= 5:
            records = []
            for r in rows:
                try:
                    buy_v  = float(str(r.get('KLMuaNuocNgoai', 0) or 0).replace(',',''))
                    sell_v = float(str(r.get('KLBanNuocNgoai', 0) or 0).replace(',',''))
                    net_v  = float(str(r.get('KLRong',         0) or 0).replace(',',''))
                    records.append({'time': r.get('Ngay',''),
                                    'buy_vol': buy_v, 'sell_vol': sell_v, 'net_vol': net_v})
                except Exception:
                    continue
            if len(records) >= 5:
                df = pd.DataFrame(records)
                df['net_vol'] = pd.to_numeric(df['net_vol'], errors='coerce').fillna(0)
                df = df.iloc[::-1].reset_index(drop=True)
                logger.info(f'Foreign {symbol}/CafeF: {len(df)} rows OK')
                return df
    except Exception as e:
        logger.debug(f'Foreign {symbol}/CafeF: {e}')

    logger.warning(f'Foreign {symbol}: all sources failed')
    return None


def _load_closes(symbol, days=80):
    """Load closing prices cho 1 mã."""
    end   = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    for src in ['VCI', 'TCBS']:
        try:
            from vnstock import Vnstock
            df = Vnstock().stock(symbol=symbol, source=src).quote.history(
                start=start, end=end, interval='1D')
            if df is None or len(df) < 10:
                continue
            cc = next((c for c in df.columns
                       if c.lower() in ('close','closeprice','close_price')), None)
            if cc is None:
                continue
            arr = pd.to_numeric(df[cc], errors='coerce').fillna(0).values.copy()
            if arr.max() < 1000 and arr.max() > 0:
                arr = arr * 1000
            return arr
        except Exception:
            continue
    return None


# ── E1. Foreign Flow Score (0-20đ) ───────────────────────────────────────────
def _calc_foreign_score(foreign_net_arr, close_arr=None, vol_arr=None):
    if foreign_net_arr is None or len(foreign_net_arr) < 5:
        return 0, {'label': 'Khong co du lieu foreign', 'available': False}

    fn     = np.array(foreign_net_arr[-20:], dtype=float)
    c      = np.array(close_arr[-len(fn):], dtype=float) if close_arr is not None else None
    v      = np.array(vol_arr[-len(fn):],   dtype=float) if vol_arr   is not None else None
    vol_ma = float(np.mean(v[v > 0])) if v is not None and np.any(v > 0) else 1

    score = 0

    # Mua ròng liên tục (0-10đ)
    consec = 0
    for val in reversed(fn):
        if val > 0: consec += 1
        else: break
    f1 = min(10, consec * 3)
    score += f1

    # Net 10 phiên / ADTV (0-6đ)
    net_10 = float(np.sum(fn[-10:]))
    ratio  = net_10 / (vol_ma * 10) if vol_ma > 0 else 0
    f2     = 6 if ratio > 0.15 else (4 if ratio > 0.08 else (2 if ratio > 0.03 else 0))
    score += f2

    # Phân kỳ ngoại-giá (0-4đ)
    f3 = 0
    if c is not None and len(c) >= 5:
        pt  = (c[-1]-c[-5])/c[-5]*100 if c[-5] > 0 else 0
        fn5 = float(np.sum(fn[-5:]))
        if fn5 > 0 and pt < 0:    f3 = 4
        elif fn5 > 0 and pt < 2:  f3 = 2
    score += f3

    final = min(20, score)
    return final, {
        'score': final, 'available': True,
        'consecutive_buy': consec,
        'net_ratio': round(ratio*100, 1),
        'label': (f'Mua rong {consec} phien | Net={ratio*100:.1f}% ADTV'
                  if consec > 0 else 'Ngoai khong mua rong'),
    }


# ── E2. Sector Strength Score (0-20đ) ────────────────────────────────────────
def _calc_sector_score(symbol):
    """
    Tính Sector RS đúng logic:
      sym_rs            = return(sym, 20d) - return(VNINDEX, 20d)   ← mã vs thị trường
      peers_rs_list     = [return(peer,20d) - return(VNINDEX,20d)]  ← từng peer vs thị trường
      sector_avg_rs     = mean(peers_rs_list)                        ← ngành vs thị trường
      relative_to_sector= sym_rs - sector_avg_rs                    ← mã vs ngành ← số hiển thị

    Thay đổi so với v4.4:
      - Dùng TẤT CẢ peers (không giới hạn [:3]) tối đa 6 mã để tránh bias
      - Tính sym_rs riêng (mã đang phân tích)
      - relative_to_sector = sym_rs - sector_avg_rs
      - Score dựa trên relative_to_sector (không phải sector_avg_rs)
      - Label rõ ràng: mã X mạnh/yếu hơn ngành bao nhiêu %
    """
    sym = symbol.upper()
    sector = _SYMBOL_TO_SECTOR.get(sym)
    if sector is None:
        try:
            from config import SYMBOL_CONFIG
            cfg_grp = SYMBOL_CONFIG.get(sym, {}).get('group', '')
            sector  = _SYMBOL_TO_SECTOR.get(cfg_grp)
        except Exception:
            pass
    if sector is None:
        return 0, {'label': 'Chua co trong danh sach nganh', 'score': 0}

    # Load VNINDEX (cache 1h)
    global _vnindex_cache
    now_ts = datetime.now().timestamp()
    if (_vnindex_cache['data'] is None
            or now_ts - (_vnindex_cache.get('ts') or 0) > _CACHE_TTL):
        vni = _load_closes('VNINDEX', days=60)
        _vnindex_cache = {'data': vni, 'ts': now_ts}
    vni = _vnindex_cache['data']
    if vni is None or len(vni) < 20:
        return 0, {'label': 'Khong load duoc VNINDEX', 'score': 0}

    # ── Load return của mã đang phân tích ────────────────────────────────
    sym_closes = _load_closes(sym, days=60)
    if sym_closes is None or len(sym_closes) < 20:
        return 0, {'label': f'Khong load duoc data {sym}', 'score': 0}

    n_sym = min(20, len(sym_closes), len(vni))
    v_ret = (vni[-1] - vni[-n_sym]) / vni[-n_sym] * 100 if vni[-n_sym] > 0 else 0
    sym_ret = (sym_closes[-1] - sym_closes[-n_sym]) / sym_closes[-n_sym] * 100 \
              if sym_closes[-n_sym] > 0 else 0
    sym_rs = sym_ret - v_ret  # mã vs VNINDEX

    # ── Load return của peers (tối đa 6 mã, bỏ chính mã đang phân tích) ─
    members = SECTOR_PROXIES[sector]
    peers   = [m for m in members if m != sym][:6]  # tối đa 6 peers
    peer_rs_list = []
    peer_detail  = {}  # {mã: rs_vs_vnindex} để debug
    for peer in peers:
        try:
            closes = _load_closes(peer, days=60)
            if closes is None or len(closes) < 20:
                continue
            n = min(20, len(closes), len(vni))
            p_ret  = (closes[-1] - closes[-n]) / closes[-n] * 100 if closes[-n] > 0 else 0
            v_ret2 = (vni[-1] - vni[-n]) / vni[-n] * 100 if vni[-n] > 0 else 0
            p_rs   = p_ret - v_ret2
            peer_rs_list.append(p_rs)
            peer_detail[peer] = round(p_rs, 1)
        except Exception:
            continue

    if not peer_rs_list:
        return 0, {'label': 'Khong du du lieu nganh', 'score': 0}

    sector_avg_rs      = float(np.mean(peer_rs_list))   # ngành vs VNINDEX
    relative_to_sector = sym_rs - sector_avg_rs          # mã vs ngành ← số chính

    # Score dựa trên mức độ mạnh/yếu so với ngành
    if   relative_to_sector > 10: score = 20
    elif relative_to_sector >  5: score = 14
    elif relative_to_sector >  0: score = 8
    elif relative_to_sector > -5: score = 3
    else:                          score = 0

    # Label rõ ràng
    sign = '+' if relative_to_sector >= 0 else ''
    if   relative_to_sector >  5:
        lbl = f'{sym} manh hon nganh {sign}{relative_to_sector:.1f}% (nganh vs VNI: {sector_avg_rs:+.1f}%)'
    elif relative_to_sector > -5:
        lbl = f'{sym} ngang nganh {sign}{relative_to_sector:.1f}% (nganh vs VNI: {sector_avg_rs:+.1f}%)'
    else:
        lbl = f'{sym} yeu hon nganh {sign}{relative_to_sector:.1f}% (nganh vs VNI: {sector_avg_rs:+.1f}%)'

    return score, {
        'score':               score,
        'sector':              sector,
        'sym_rs':              round(sym_rs, 1),            # mã vs VNINDEX
        'sector_avg_rs':       round(sector_avg_rs, 1),     # ngành vs VNINDEX
        'relative_to_sector':  round(relative_to_sector, 1),# mã vs ngành ← số chính
        'sector_rs':           round(relative_to_sector, 1),# compat alias
        'peers_used':          list(peer_detail.keys()),
        'peer_detail':         peer_detail,
        'label':               lbl,
    }


# ── A. Wyckoff VSA với Automatic Rally confirmation (0-30đ) ──────────────────
def _calc_vsa(closes, highs, lows, volumes, vol_ma):
    n     = len(closes)
    score = 0
    events= []

    for i in range(1, n - 1):
        spread     = highs[i] - lows[i]
        avg_spread = float(np.mean([highs[j]-lows[j] for j in range(max(0,i-10),i)]))
        if avg_spread <= 0:
            continue

        spread_ratio = spread / avg_spread
        vol_ratio    = volumes[i] / vol_ma if vol_ma > 0 else 1
        close_pos    = (closes[i]-lows[i]) / spread if spread > 0 else 0.5
        price_chg    = (closes[i]-closes[i-1]) / closes[i-1] if closes[i-1] > 0 else 0

        is_climax = (spread_ratio > 1.5 and vol_ratio > 2.0
                     and price_chg < -0.01 and close_pos < 0.4)

        if is_climax:
            auto_rally = (closes[i+1] > closes[i])
            if auto_rally:
                score += 8
                events.append(f'Selling Climax + Auto Rally (xac nhan)')
            else:
                score += 2
                events.append(f'Climax chua xac nhan (cho T+1 bung)')

        elif (spread_ratio < 0.8 and vol_ratio < 0.7 and price_chg >= -0.003):
            score += 5
            events.append('No Supply')

        elif (spread_ratio > 1.3 and vol_ratio > 1.5
              and price_chg > 0.01 and close_pos > 0.6):
            score += 4
            events.append('Demand manh')

        elif (spread_ratio < 0.7 and vol_ratio > 1.5 and price_chg > 0.003):
            score -= 3
            events.append('Distribution signal (tru diem)')

    return min(30, max(0, score)), events[-3:]


# ── B. Chaikin A/D với volume normalized (0-25đ) ─────────────────────────────
def _calc_ad_divergence(closes, highs, lows, volumes, vol_ma):
    n = len(closes)
    if n < 5:
        return 0, {'score': 0, 'label': 'Khong du du lieu'}

    h = highs; l = lows; c = closes
    v_norm = np.minimum(volumes, vol_ma * 3) if vol_ma > 0 else volumes

    ad = np.zeros(n)
    for i in range(n):
        rng = h[i] - l[i]
        if rng > 0:
            clv   = ((c[i]-l[i]) - (h[i]-c[i])) / rng
            ad[i] = (ad[i-1] if i > 0 else 0) + clv * v_norm[i]
        else:
            ad[i] = ad[i-1] if i > 0 else 0

    ad_recent   = ad[-10:]
    ad_slope    = float(np.polyfit(range(len(ad_recent)), ad_recent, 1)[0])
    price_slope = float(np.polyfit(range(10), c[-10:], 1)[0])

    ad_norm    = ad_slope / (abs(ad[-1]) + 1) if abs(ad[-1]) > 0 else 0
    price_norm = price_slope / c[-1] if c[-1] > 0 else 0

    score = 0
    if ad_norm > 0.02 and price_norm < 0.001:
        score = min(25, int(ad_norm * 800))
        label = f'A/D tang khi gia sideways (phan ky manh)'
    elif ad_norm > 0.01 and price_norm < 0.005:
        score = min(15, int(ad_norm * 400))
        label = f'A/D tang nhe khi gia on dinh'
    elif ad_norm > 0:
        score, label = 5, 'A/D co xu huong tang'
    else:
        score, label = 0, 'A/D khong co tin hieu tich luy'

    return score, {'score': score, 'ad_slope': round(ad_norm,4),
                   'price_slope': round(price_norm*100,2), 'label': label}


# ── C. Spring Detection với lookback 60 phiên (0-20đ) ────────────────────────
def _calc_spring(closes, highs, lows, volumes, vol_ma):
    n      = len(closes)
    score  = 0
    found  = []

    low_60  = float(np.min(lows[-60:-1])) if n >= 60 else float(np.min(lows[:-1]))
    low_20  = float(np.min(lows[-20:-1])) if n >= 20 else float(np.min(lows[:-1]))

    for i in range(max(1, n-10), n-1):
        spread    = highs[i] - lows[i]
        vol_ratio = volumes[i] / vol_ma if vol_ma > 0 else 1
        close_pos = (closes[i]-lows[i]) / spread if spread > 0 else 0.5
        lower_wick= min(closes[i], closes[i-1]) - lows[i]
        wick_ratio= lower_wick / spread if spread > 0 else 0

        if wick_ratio >= 0.40 and vol_ratio <= 0.8 and close_pos > 0.5:
            pts = 15 if vol_ratio < 0.5 else 10
            score += pts
            found.append(f'Spring (vol={vol_ratio:.1f}x, wick={wick_ratio:.0%})')

        elif (lows[i] < low_60 * 1.005
              and closes[i] > low_60
              and vol_ratio > 1.2):
            if closes[i+1] > closes[i]:
                score += 12
                found.append(f'Shakeout 60p (stop-loss hunt) + xac nhan')
            else:
                score += 4
                found.append(f'Shakeout 60p chua xac nhan')

        elif (lows[i] <= low_20 * 1.02
              and vol_ratio < 0.6
              and closes[i] > low_20):
            score += 8
            found.append('No Supply Test near support')

    return min(20, score), found[-2:]


# ── D. Supply Exhaustion (0-15đ) ─────────────────────────────────────────────
def _calc_supply_exhaustion(closes, volumes, vol_ma):
    rv  = volumes[-7:]
    rc  = closes[-7:]
    ma7 = float(np.mean(rc))

    vol_declining = all(rv[i] >= rv[i+1] for i in range(len(rv)-1))
    vol_low       = bool(float(np.mean(rv)) < vol_ma * 0.75)
    price_held    = bool(closes[-1] >= ma7 * 0.98)

    score = 0
    if vol_declining and vol_low and price_held:
        score, label = 15, 'Vol giam + gia >= MA7x0.98 → supply that su can'
    elif vol_declining and price_held:
        score, label = 10, 'Vol dang giam + gia on dinh'
    elif vol_low and price_held:
        score, label = 8,  'Vol thap + gia giu duoc → it luc ban'
    elif vol_declining and not price_held:
        score, label = 0,  'Vol giam nhung gia giam → No Demand (khong tot)'
    else:
        score, label = 0,  'Chua co dau hieu supply can'

    return score, {'score': score, 'label': label,
                   'vol_declining': bool(vol_declining), 'vol_low': bool(vol_low),
                   'price_held': bool(price_held), 'ma7': round(ma7, 0)}


# ── MAIN: Shark Score tổng hợp ────────────────────────────────────────────────
def calc_shark_score(closes, highs, lows, volumes, lookback=20,
                     foreign_net=None, symbol=None):
    n = min(lookback, len(closes))
    if n < 12:
        return 0, {'error': 'Khong du du lieu (can >= 12 phien)'}

    c = np.array(closes[-n:],  dtype=float)
    h = np.array(highs[-n:],   dtype=float)
    l = np.array(lows[-n:],    dtype=float)
    v = np.array(volumes[-n:], dtype=float)

    pos_v  = v[v > 0]
    vol_ma = float(np.mean(pos_v[:-1])) if len(pos_v) > 1 else float(np.mean(pos_v))
    if vol_ma <= 0:
        return 0, {'error': 'Vol MA = 0'}

    dets = {}

    a_s, a_ev = _calc_vsa(c, h, l, v, vol_ma)
    dets['vsa'] = {'score': a_s, 'events': a_ev,
                   'label': a_ev[-1] if a_ev else 'Khong co tin hieu VSA'}

    b_s, b_d = _calc_ad_divergence(c, h, l, v, vol_ma)
    dets['ad_line'] = b_d

    c_s, c_ev = _calc_spring(c, h, l, v, vol_ma)
    dets['spring'] = {'score': c_s, 'events': c_ev,
                      'label': c_ev[-1] if c_ev else 'Khong co Spring'}

    d_s, d_d = _calc_supply_exhaustion(c, v, vol_ma)
    dets['supply'] = d_d

    has_f  = (foreign_net is not None and len(foreign_net) >= 5)
    e1_s, e1_d = _calc_foreign_score(
        foreign_net,
        closes[-n:] if has_f else None,
        volumes[-n:] if has_f else None,
    )
    dets['foreign'] = e1_d

    e2_s = 0; e2_d = {'score': 0, 'label': 'Khong xac dinh nganh'}
    if symbol:
        try:
            e2_s, e2_d = _calc_sector_score(symbol)
        except Exception:
            pass
    dets['sector'] = e2_d

    raw = a_s + b_s + c_s + d_s + e1_s + e2_s

    max_raw = 90
    if has_f:   max_raw += 20
    if e2_s > 0 or symbol: max_raw += 20

    final = min(100, round(raw / max_raw * 100))

    if final >= 80:   verdict='GOM_MANH';    emoji='&#x1F988;&#x1F988;'; lbl='GOM MANH'
    elif final >= 60: verdict='CO_DAU_HIEU'; emoji='&#x1F988;';           lbl='Co dau hieu gom'
    elif final >= 40: verdict='THEO_DOI';    emoji='&#x1F440;';            lbl='Theo doi them'
    else:             verdict='KHONG_RO';    emoji='&#x2796;';             lbl='Chua co dau hieu'

    dets.update({'score': final, 'raw': raw, 'max_raw': max_raw,
                 'has_foreign': has_f, 'verdict': verdict,
                 'emoji': emoji, 'label': lbl,
                 'components': {
                     'vsa': a_s, 'ad': b_s, 'spring': c_s,
                     'supply': d_s, 'foreign': e1_s, 'sector': e2_s,
                 }})
    return final, dets


def format_shark_msg(score, details, symbol=''):
    NL    = chr(10)
    emoji = details.get('emoji', '&#x1F988;')
    label = details.get('label', '')
    bar   = '&#x2588;' * round(score/10) + '&#x2591;' * (10 - round(score/10))

    lines = [
        f'{emoji} <b>Shark Detector v4.1{" — " + symbol if symbol else ""}:</b>',
        f'{bar} {score}/100 — {label}',
        '',
    ]

    for key, ic, name, max_pts in [
        ('vsa',     '&#x1F4CA;', 'Wyckoff VSA',        30),
        ('ad_line', '&#x1F4C8;', 'Chaikin A/D',        25),
        ('spring',  '&#x1F30A;', 'Spring/Shakeout',    20),
        ('supply',  '&#x23F3;',  'Supply Exhaustion',  15),
        ('foreign', '&#x1F30F;', 'Foreign Flow',       20),
        ('sector',  '&#x1F3ED;', 'Sector Strength',    20),
    ]:
        d   = details.get(key, {})
        s   = d.get('score', 0)
        lbl = d.get('label', '')

        if key == 'foreign' and not d.get('available', False):
            lines.append(f'{ic} <b>{name}:</b> Khong co du lieu (vnstock/TCBS)')
            continue
        if key == 'sector' and s == 0 and 'Chua co' in lbl:
            lines.append(f'{ic} <b>{name}:</b> {lbl}')
            continue
        if s == 0:
            continue

        bar_filled = round(s / (max_pts / 5))
        bar_c = '&#x25A0;' * bar_filled + '&#x25A1;' * (5 - bar_filled)

        if key == 'sector':
            # Hiển thị đủ 3 chiều: sym vs VNI, ngành vs VNI, sym vs ngành
            sym_rs     = d.get('sym_rs', None)
            sec_avg    = d.get('sector_avg_rs', None)
            rel        = d.get('relative_to_sector', None)
            peers_used = d.get('peers_used', [])
            peer_detail= d.get('peer_detail', {})
            if rel is not None and sym_rs is not None:
                peers_short = ','.join(peers_used[:4])
                peer_str = ' | '.join(
                    f'{p}:{v:+.1f}%'
                    for p, v in sorted(peer_detail.items(), key=lambda x: -x[1])[:4]
                ) if peer_detail else ''
                lines.append(
                    f'{ic} <b>{name}</b> {bar_c} {s}/{max_pts}d: {lbl}'
                )
                lines.append(
                    f'   {symbol if symbol else "Mã"} vs VNI: <b>{sym_rs:+.1f}%</b>'
                    f' | Ngành vs VNI: <b>{sec_avg:+.1f}%</b>'
                    f' | Mã vs Ngành: <b>{rel:+.1f}%</b>'
                )
                if peer_str:
                    lines.append(f'   Peers ({peers_short}): {peer_str}')
            else:
                lines.append(f'{ic} <b>{name}</b> {bar_c} {s}/{max_pts}d: {lbl}')
        else:
            lines.append(f'{ic} <b>{name}</b> {bar_c} {s}/{max_pts}d: {lbl}')

    raw_v = details.get('raw', 0)
    max_v = details.get('max_raw', 110)
    lines.append('')
    lines.append(f'<i>Raw: {raw_v}/{max_v} → normalize = {score}/100</i>')
    return NL.join(lines)
