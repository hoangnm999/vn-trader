"""
intra_sector_rs.py — Tính Intra-Sector Relative Strength

So sánh 1 mã với tất cả mã cùng nhóm ngành trong SYMBOL_CONFIG.
Metrics: RS_20d vs từng peer → rank + percentile + label.

Functions:
  calc_intra_sector_rs(symbol, closes) → dict
"""
import numpy as np
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from config import SYMBOL_CONFIG


def _get_peers(symbol):
    """Lấy danh sách mã cùng ngành, loại trừ chính nó."""
    sym = symbol.upper()
    cfg = SYMBOL_CONFIG.get(sym, {})
    grp = cfg.get('group', '')
    if not grp:
        return [], grp
    peers = [s for s, c in SYMBOL_CONFIG.items()
             if c.get('group') == grp and s != sym]
    return peers, grp


def _calc_return(closes, days):
    """% return trong N phiên gần nhất."""
    if closes is None or len(closes) < days + 1:
        return None
    return round((closes[-1] / closes[-days - 1] - 1) * 100, 2)


def _load_peer_closes(symbol, days=60):
    """Load closing prices của 1 mã peer. Returns numpy array or None."""
    try:
        from relative_strength import _load_ohlcv
        df = _load_ohlcv(symbol, days=days + 10)
        if df is None or len(df) < 10:
            return None
        cc = next((c for c in df.columns
                   if c.lower() in ('close', 'closeprice', 'close_price')), None)
        if cc is None:
            return None
        arr = df[cc].values.astype(float)
        if arr.max() < 1000:
            arr *= 1000
        return arr
    except Exception:
        return None


def calc_intra_sector_rs(symbol, closes, days=20):
    """
    So sánh symbol với peers cùng ngành.

    Returns dict:
      group        : tên ngành
      peers        : danh sách peer
      symbol_ret   : % return của symbol trong N phiên
      peer_rets    : dict {peer: return}
      rank         : hạng (1 = tốt nhất)
      total        : tổng số mã so sánh (kể cả symbol)
      percentile   : percentile (100 = tốt nhất)
      beat_count   : số peers bị đánh bại
      label        : mô tả ('Manh nhat nganh', 'Tren trung binh'...)
      bonus        : điểm thêm vào Score A (-5 đến +8)
      available    : True nếu có đủ data
    """
    result = {
        'group': '', 'peers': [], 'symbol_ret': None,
        'peer_rets': {}, 'rank': None, 'total': 0,
        'percentile': None, 'beat_count': 0,
        'label': 'Khong du du lieu nganh', 'bonus': 0,
        'available': False,
    }

    peers, grp = _get_peers(symbol)
    result['group'] = grp
    result['peers'] = peers

    if not grp or not peers:
        result['label'] = 'Khong co nganh peer'
        return result

    # Return của symbol
    sym_ret = _calc_return(closes, days)
    if sym_ret is None:
        return result
    result['symbol_ret'] = sym_ret

    # Load và tính return của peers
    peer_rets = {}
    for peer in peers:
        arr = _load_peer_closes(peer, days=days + 15)
        if arr is not None:
            r = _calc_return(arr, days)
            if r is not None:
                peer_rets[peer] = r

    if not peer_rets:
        result['label'] = 'Khong load duoc du lieu peer'
        return result

    result['peer_rets'] = peer_rets

    # Rank: tất cả returns (bao gồm symbol)
    all_rets = list(peer_rets.values()) + [sym_ret]
    all_rets_sorted = sorted(all_rets, reverse=True)  # cao nhất trước
    rank = all_rets_sorted.index(sym_ret) + 1
    total = len(all_rets)
    beat_count = sum(1 for r in peer_rets.values() if sym_ret > r)
    percentile = round(beat_count / len(peer_rets) * 100) if peer_rets else 0

    result['rank']       = rank
    result['total']      = total
    result['beat_count'] = beat_count
    result['percentile'] = percentile
    result['available']  = True

    # Label theo percentile
    avg_ret = sum(all_rets) / len(all_rets)
    diff_vs_avg = round(sym_ret - avg_ret, 1)

    if percentile >= 80:
        label = f'Manh nhat nganh ({rank}/{total}) {sym_ret:+.1f}%'
        bonus = 8
    elif percentile >= 60:
        label = f'Tren TB nganh ({rank}/{total}) {sym_ret:+.1f}%'
        bonus = 4
    elif percentile >= 40:
        label = f'Ngang TB nganh ({rank}/{total}) {sym_ret:+.1f}%'
        bonus = 0
    elif percentile >= 20:
        label = f'Duoi TB nganh ({rank}/{total}) {sym_ret:+.1f}%'
        bonus = -3
    else:
        label = f'Yeu nhat nganh ({rank}/{total}) {sym_ret:+.1f}%'
        bonus = -5

    result['label'] = label
    result['bonus'] = bonus
    return result
