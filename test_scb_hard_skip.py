"""
test_scb_hard_skip.py — Unit tests cho _scb_check_hard_skip + _scb_get_zones
=============================================================================
Chạy độc lập, không cần import telegram_bot (copy inline để tránh dependency).

Usage:
    python test_scb_hard_skip.py
    python test_scb_hard_skip.py -v       # verbose: in chi tiết từng test
    python test_scb_hard_skip.py --fast   # chỉ chạy TRIGGER tests (bỏ qua NO-SKIP)

Coverage:
    - Tất cả 14 mã có rule trong SCB_HARD_SKIP
    - Tất cả 6 condition_key hiện tại: vni_down, ma20_zone, score_bucket,
      vol_high, vol_low, vol_med
    - Boundary values cho _scb_get_zones classifiers
    - NO-SKIP cases: điều kiện gần boundary nhưng không trigger
    - Multi-rule: mã có nhiều rules (HMC 5 rules, DCM 3 rules, GAS 2 rules...)
    - Mã không có rule → không skip
"""

import sys

# ── Copy inline để test độc lập ────────────────────────────────────────────────

SCB_HARD_SKIP = {
    'BSR': [('vni_down', True,        'BSR VNI DOWN Exp -5.17% (12L)')],
    'BSI': [('vni_down', True,        'BSI VNI DOWN Exp -4.11% (7L)')],
    'GAS': [('vni_down', True,        'GAS VNI DOWN Exp -2.90% (12L)'),
            ('score_bucket', '75-84', 'GAS score 75-84 Exp -1.80% (14L)')],
    'CTG': [('vni_down', True,        'CTG VNI DOWN Exp -3.28% (10L)')],
    'FPT': [('ma20_zone', 'EXT',      'FPT EXT Exp -2.69% — overbought')],
    'CNG': [('vni_down', True,        'CNG VNI DOWN Exp -4.15% (7L)')],
    'DPM': [('ma20_zone', 'EXT',      'DPM EXT Exp -1.10% (10L)')],
    'HMC': [('vni_down', True,        'HMC VNI DOWN WR=23% Exp=-4.33% (13L)'),
            ('vol_med',  True,        'HMC Vol MED WR=33% Exp=-0.17% (21L)'),
            ('score_bucket', '75-84', 'HMC Score 75-84 WR=23% Exp=-0.535% (13L)'),
            ('ma20_zone', 'EXT',      'HMC MA20 EXT Exp=-0.025% (8L) — flat')],  # S18b: xóa OPT
    'HBC': [('vni_down', True,        'HBC VNI DOWN Exp -5.05% (12L)'),
            ('ma20_zone', 'FAR',      'HBC FAR Exp -7.50% (4L)')],
    'VIC': [('score_bucket', '65-74', 'VIC score 65-74 Exp -3.40% (23L)')],
    'VCB': [('vni_down', True,        'VCB VNI DOWN Exp -2.16% (7L)')],
    'FRT': [('ma20_zone', 'NEAR',     'FRT MA20 NEAR WR=28% Exp=-1.845% (18L) — S18b')],  # S18b: xóa 95+, thêm NEAR
    'VTP': [('vni_down', True,        'VTP VNI DOWN Exp -3.80% (12L)')],
    'DCM': [('vni_flat', True,        'DCM VNI FLAT WR=30% Exp=-0.488% (23L) — S18b'),
            ('vol_low', True,         'DCM Vol LOW Exp -2.05% (19L)'),
            ('ma20_zone', 'NEAR',     'DCM MA20 NEAR Exp -0.40% (20L) — S16'),
            ('ma20_zone', 'EXT',      'DCM MA20 EXT Exp -1.50% (14L)')],
    'PHP': [('vni_down', True,        'PHP VNI DOWN Exp -2.03% (14L)')],  # S18c: xóa cả OPT (redundant khi MinScore=95),
    'DXS': [('vol_high', True,        'DXS Vol HIGH Exp -0.50% (6L)'),
            ('ma20_zone', 'EXT',      'DXS MA20 EXT Exp -1.31% (9L)')],
}


def _scb_get_zones(vni_chg, ma20_dist, vol_ratio, score_a):
    if vni_chg >= 1.0:    vni_regime = 'UP'
    elif vni_chg <= -2.0: vni_regime = 'DOWN'
    else:                 vni_regime = 'FLAT'

    if ma20_dist < 0:    ma20_zone = 'BELOW'
    elif ma20_dist < 2:  ma20_zone = 'NEAR'
    elif ma20_dist < 10: ma20_zone = 'OPT'
    elif ma20_dist < 20: ma20_zone = 'EXT'
    else:                ma20_zone = 'FAR'

    if score_a < 75:   score_bucket = '65-74'
    elif score_a < 85: score_bucket = '75-84'
    elif score_a < 95: score_bucket = '85-94'
    else:              score_bucket = '95+'

    if vol_ratio >= 2.0:   vol_pat = 'HIGH'
    elif vol_ratio >= 1.2: vol_pat = 'MED'
    elif vol_ratio >= 0.8: vol_pat = 'NORMAL'
    else:                  vol_pat = 'LOW'

    return vni_regime, ma20_zone, score_bucket, vol_pat


def _scb_check_hard_skip(sym, vni_regime, ma20_zone, score_bucket, vol_pat):
    rules = SCB_HARD_SKIP.get(sym, [])
    for cond_key, cond_val, reason in rules:
        if cond_key == 'vni_down'     and vni_regime == 'DOWN' and cond_val:  return True, reason
        if cond_key == 'vni_flat'     and vni_regime == 'FLAT' and cond_val:  return True, reason  # S18b
        if cond_key == 'ma20_zone'    and ma20_zone  == cond_val:             return True, reason
        if cond_key == 'score_bucket' and score_bucket == cond_val:          return True, reason
        if cond_key == 'vol_high'     and vol_pat == 'HIGH' and cond_val:    return True, reason
        if cond_key == 'vol_low'      and vol_pat == 'LOW'  and cond_val:    return True, reason
        if cond_key == 'vol_med'      and vol_pat == 'MED'  and cond_val:    return True, reason
    return False, ''


# ── Test runner ────────────────────────────────────────────────────────────────

VERBOSE = '-v' in sys.argv
FAST    = '--fast' in sys.argv

passed = failed = 0
failures = []


def check(name, sym, vni_chg, ma20_dist, vol_ratio, score_a,
          expect_skip, expect_reason_contains=None):
    """
    Helper: tính zones rồi gọi _scb_check_hard_skip, assert kết quả.
    expect_reason_contains: substring cần có trong reason khi expect_skip=True.
    """
    global passed, failed
    vni_regime, ma20_zone, score_bucket, vol_pat = _scb_get_zones(
        vni_chg, ma20_dist, vol_ratio, score_a
    )
    skip, reason = _scb_check_hard_skip(sym, vni_regime, ma20_zone, score_bucket, vol_pat)

    ok = (skip == expect_skip)
    if ok and expect_reason_contains and skip:
        ok = expect_reason_contains.lower() in reason.lower()

    if ok:
        passed += 1
        if VERBOSE:
            tag = '✅ SKIP' if skip else '✅ PASS'
            print(f'  {tag}  {name}')
    else:
        failed += 1
        tag = f'❌ FAIL'
        msg = (f'{tag}  {name}\n'
               f'       sym={sym} vni_chg={vni_chg} ma20_dist={ma20_dist} '
               f'vol_ratio={vol_ratio} score_a={score_a}\n'
               f'       zones: vni={vni_regime} ma20={ma20_zone} '
               f'bucket={score_bucket} vol={vol_pat}\n'
               f'       expect_skip={expect_skip}, got skip={skip}, reason="{reason}"')
        failures.append(msg)
        print(msg)


def section(title):
    if VERBOSE:
        print(f'\n{"─"*60}')
        print(f'  {title}')
        print(f'{"─"*60}')
    else:
        print(f'  {title}... ', end='', flush=True)


# ══════════════════════════════════════════════════════════════════
# SECTION 1 — _scb_get_zones boundary values
# ══════════════════════════════════════════════════════════════════
section('ZONES — VNI boundaries')
check('VNI UP  boundary (vni=1.0)',  'BSR', 1.0,  5.0, 1.0, 80, expect_skip=False)
check('VNI FLAT boundary (vni=0.9)', 'BSR', 0.9,  5.0, 1.0, 80, expect_skip=False)
check('VNI DOWN boundary (vni=-2.0)','BSR', -2.0, 5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='BSR VNI DOWN')
check('VNI DOWN deep (vni=-5.0)',     'BSR', -5.0, 5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='BSR VNI DOWN')
check('VNI FLAT near DOWN (vni=-1.9)','BSR', -1.9, 5.0, 1.0, 80, expect_skip=False)
if not VERBOSE: print('OK' if failed == 0 else 'FAIL')

_prev_failed = failed
section('ZONES — MA20 boundaries')
check('MA20 BELOW (<0)',    'FPT', 0.5, -0.1, 1.0, 80, expect_skip=False)  # BELOW, not EXT
check('MA20 NEAR (1.9)',    'FPT', 0.5,  1.9, 1.0, 80, expect_skip=False)  # NEAR, not EXT
check('MA20 OPT (2.0)',     'FPT', 0.5,  2.0, 1.0, 80, expect_skip=False)  # OPT, not EXT
check('MA20 OPT (9.9)',     'FPT', 0.5,  9.9, 1.0, 80, expect_skip=False)  # OPT, not EXT
check('MA20 EXT (10.0)',    'FPT', 0.5, 10.0, 1.0, 80, expect_skip=True,   expect_reason_contains='FPT EXT')
check('MA20 EXT (15.0)',    'FPT', 0.5, 15.0, 1.0, 80, expect_skip=True,   expect_reason_contains='FPT EXT')
check('MA20 FAR (20.0)',    'HBC', 0.5, 20.0, 1.0, 80, expect_skip=True,   expect_reason_contains='HBC FAR')
check('MA20 FAR (25.0)',    'HBC', 0.5, 25.0, 1.0, 80, expect_skip=True,   expect_reason_contains='HBC FAR')
check('MA20 EXT not FAR(19.9)','HBC', 0.5, 19.9, 1.0, 80, expect_skip=False)  # EXT, not FAR — HBC chỉ skip FAR
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('ZONES — Score bucket boundaries')
check('Bucket 65-74 (score=74)',  'VIC', 0.5, 5.0, 1.0,  74, expect_skip=True,  expect_reason_contains='VIC score 65-74')
check('Bucket 75-84 (score=75)',  'VIC', 0.5, 5.0, 1.0,  75, expect_skip=False)
check('Bucket 75-84 (score=80)',  'GAS', 0.5, 5.0, 1.0,  80, expect_skip=True,  expect_reason_contains='GAS score 75-84')
check('Bucket 85-94 (score=85)',  'GAS', 0.5, 5.0, 1.0,  85, expect_skip=False)
check('Bucket 95+ (score=95) FRT → NO SKIP (S18b)', 'FRT', 0.5, 5.0, 1.0, 95, expect_skip=False)
check('Bucket 85-94 (score=94)',  'FRT', 0.5, 5.0, 1.0,  94, expect_skip=False)
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('ZONES — Vol pattern boundaries')
check('Vol HIGH (ratio=2.0)',   'DXS', 0.5, 5.0, 2.0,  80, expect_skip=True,  expect_reason_contains='DXS Vol HIGH')
check('Vol MED  (ratio=1.9)',   'DXS', 0.5, 5.0, 1.9,  80, expect_skip=False)
check('Vol MED  (ratio=1.2)',   'HMC', 0.5, 5.0, 1.2,  80, expect_skip=True,  expect_reason_contains='HMC Vol MED')
check('Vol NORMAL (ratio=1.19)','HMC', 0.5, 25.0, 1.19, 85, expect_skip=False)  # score=85, ma20=FAR (không có HMC rule cho FAR)
check('Vol LOW (ratio=0.79)',   'DCM', 1.5, 5.0, 0.79, 80, expect_skip=True,  expect_reason_contains='DCM Vol LOW')  # VNI UP
check('Vol NORMAL (ratio=0.8)', 'DCM', 1.5, 5.0, 0.80, 80, expect_skip=False)  # VNI UP
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')


# ══════════════════════════════════════════════════════════════════
# SECTION 2 — TRIGGER tests: mỗi rule phải SKIP đúng
# ══════════════════════════════════════════════════════════════════
section('TRIGGER — vni_down rules (BSR, BSI, GAS, CTG, CNG, HMC, HBC, VCB, VTP, PHP)')
_prev_failed = failed
check('BSR VNI DOWN',  'BSR', -3.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='BSR VNI DOWN')
check('BSI VNI DOWN',  'BSI', -3.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='BSI VNI DOWN')
check('GAS VNI DOWN',  'GAS', -3.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='GAS VNI DOWN')
check('CTG VNI DOWN',  'CTG', -3.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='CTG VNI DOWN')
check('CNG VNI DOWN',  'CNG', -3.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='CNG VNI DOWN')
check('HMC VNI DOWN',  'HMC', -3.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='HMC VNI DOWN')
check('HBC VNI DOWN',  'HBC', -3.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='HBC VNI DOWN')
check('VCB VNI DOWN',  'VCB', -3.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='VCB VNI DOWN')
check('VTP VNI DOWN',  'VTP', -3.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='VTP VNI DOWN')
check('PHP VNI DOWN',  'PHP', -3.0, 15.0, 1.0, 95, expect_skip=True,  expect_reason_contains='PHP VNI DOWN')  # ma20=EXT → không trigger NEAR/OPT; score=95+ bucket
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('TRIGGER — ma20_zone rules (FPT, DPM, HBC, DCM×2, HMC×2, PHP×2, DXS)')
check('FPT MA20 EXT',  'FPT', 0.5, 15.0, 1.0, 80, expect_skip=True,  expect_reason_contains='FPT EXT')
check('DPM MA20 EXT',  'DPM', 0.5, 15.0, 1.0, 80, expect_skip=True,  expect_reason_contains='DPM EXT')
check('HBC MA20 FAR',  'HBC', 0.5, 25.0, 1.0, 80, expect_skip=True,  expect_reason_contains='HBC FAR')
check('DCM MA20 NEAR', 'DCM', 1.5,  1.0, 1.0, 80, expect_skip=True,  expect_reason_contains='DCM MA20 NEAR')  # VNI UP
check('DCM MA20 EXT',  'DCM', 1.5, 15.0, 1.0, 80, expect_skip=True,  expect_reason_contains='DCM MA20 EXT')  # VNI UP
check('HMC MA20 OPT',  'HMC', 0.5,  5.0, 1.0, 85, expect_skip=False)  # S18b: OPT rule removed | score=85 tránh bucket 75-84
check('HMC MA20 EXT',  'HMC', 0.5, 15.0, 1.0, 85, expect_skip=True,  expect_reason_contains='HMC MA20 EXT')  # score=85 tránh bucket 75-84
check('PHP MA20 NEAR → NO SKIP (S18b)', 'PHP', 0.5, 1.0, 1.0, 95, expect_skip=False)
check('PHP MA20 OPT  → NO SKIP (S18c: OPT rule removed)', 'PHP', 0.5, 5.0, 1.0, 95, expect_skip=False)
check('DXS MA20 EXT',  'DXS', 0.5, 15.0, 1.0, 80, expect_skip=True,  expect_reason_contains='DXS MA20 EXT')
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('TRIGGER — score_bucket rules (GAS, VIC, FRT, HMC)')
check('GAS score 75-84', 'GAS', 0.5, 5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='GAS score 75-84')
check('VIC score 65-74', 'VIC', 0.5, 5.0, 1.0, 70, expect_skip=True,  expect_reason_contains='VIC score 65-74')
check('FRT score 95+',   'FRT', 0.5, 5.0, 1.0, 97, expect_skip=False)  # S18b: 95+ rule removed
check('HMC score 75-84', 'HMC', 0.5, 5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='HMC Score 75-84')
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('TRIGGER — vol_high (DXS) [S18 fix: handler đã có trước]')
check('DXS Vol HIGH',  'DXS', 0.5, 5.0, 2.5, 80, expect_skip=True,  expect_reason_contains='DXS Vol HIGH')
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('TRIGGER — vol_low (DCM) [FIX S18 — was broken]')
check('DCM Vol LOW (0.5x)', 'DCM', 1.5, 5.0, 0.5,  80, expect_skip=True,  expect_reason_contains='DCM Vol LOW')  # VNI UP
check('DCM Vol LOW (0.0x)', 'DCM', 1.5, 5.0, 0.0,  80, expect_skip=True,  expect_reason_contains='DCM Vol LOW')  # VNI UP
check('DCM Vol LOW (0.79x)','DCM', 1.5, 5.0, 0.79, 80, expect_skip=True,  expect_reason_contains='DCM Vol LOW')  # VNI UP
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('TRIGGER — vol_med (HMC) [FIX S18 — was broken]')
check('HMC Vol MED (1.2x)', 'HMC', 0.5, 5.0, 1.2,  80, expect_skip=True,  expect_reason_contains='HMC Vol MED')
check('HMC Vol MED (1.5x)', 'HMC', 0.5, 5.0, 1.5,  80, expect_skip=True,  expect_reason_contains='HMC Vol MED')
check('HMC Vol MED (1.99x)','HMC', 0.5, 5.0, 1.99, 80, expect_skip=True,  expect_reason_contains='HMC Vol MED')
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')


# ══════════════════════════════════════════════════════════════════
# SECTION 3 — NO-SKIP tests: điều kiện không trigger (khi rule không áp dụng)
# ══════════════════════════════════════════════════════════════════
if not FAST:
    _prev_failed = failed
    section('NO-SKIP — vni UP/FLAT không trigger vni_down rules')
    check('BSR VNI UP   → no skip', 'BSR',  1.5, 5.0, 1.0, 80, expect_skip=False)
    check('BSR VNI FLAT → no skip', 'BSR',  0.0, 5.0, 1.0, 80, expect_skip=False)
    check('CTG VNI UP   → no skip', 'CTG',  2.0, 5.0, 1.0, 80, expect_skip=False)
    check('VTP VNI FLAT → no skip', 'VTP', -1.0, 5.0, 1.0, 80, expect_skip=False)
    if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

    _prev_failed = failed
    section('NO-SKIP — ma20 không đúng zone thì không trigger')
    check('FPT MA20 OPT  → no skip', 'FPT', 0.5,  5.0, 1.0, 80, expect_skip=False)
    check('FPT MA20 NEAR → no skip', 'FPT', 0.5,  1.0, 1.0, 80, expect_skip=False)
    check('DPM MA20 OPT  → no skip', 'DPM', 0.5,  5.0, 1.0, 80, expect_skip=False)
    check('HBC MA20 EXT  → no skip (chỉ skip FAR)', 'HBC', 0.5, 15.0, 1.0, 80, expect_skip=False)
    check('DCM MA20 OPT  → no skip', 'DCM', 1.5,  5.0, 1.0, 80, expect_skip=False)  # VNI UP
    check('PHP MA20 EXT  → PREMIUM, no skip', 'PHP', 0.5, 15.0, 1.0, 95, expect_skip=False)
    if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

    _prev_failed = failed
    section('NO-SKIP — score bucket không trigger khi đúng bucket')
    check('GAS score 85-94 → no skip', 'GAS', 0.5, 5.0, 1.0, 90, expect_skip=False)
    check('GAS score 95+   → no skip', 'GAS', 0.5, 5.0, 1.0, 97, expect_skip=False)
    check('GAS score 65-74 → no skip', 'GAS', 0.5, 5.0, 1.0, 70, expect_skip=False)
    check('VIC score 75-84 → no skip', 'VIC', 0.5, 5.0, 1.0, 80, expect_skip=False)
    check('VIC score 95+   → no skip', 'VIC', 0.5, 5.0, 1.0, 97, expect_skip=False)
    check('FRT score 85-94 → no skip (sweet spot)', 'FRT', 0.5, 5.0, 1.0, 90, expect_skip=False)
    check('FRT score 75-84 → no skip (sweet spot)', 'FRT', 0.5, 5.0, 1.0, 80, expect_skip=False)
    if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

    _prev_failed = failed
    section('NO-SKIP — vol NORMAL không trigger vol_low/vol_med/vol_high')
    check('DCM Vol NORMAL → no skip', 'DCM', 1.5, 5.0, 1.0,  80, expect_skip=False)  # VNI UP
    check('DCM Vol MED    → no skip (DCM chỉ skip LOW)', 'DCM', 1.5, 5.0, 1.5, 80, expect_skip=False)  # VNI UP tránh vni_flat
    check('DCM Vol HIGH   → no skip (DCM chỉ skip LOW)', 'DCM', 1.5, 5.0, 2.5, 80, expect_skip=False)  # VNI UP
    check('HMC Vol LOW    → no skip (HMC chỉ skip MED)', 'HMC', 0.5, 25.0, 0.5, 85, expect_skip=False)  # score=85 FAR ma20 tránh tất cả HMC rules
    check('HMC Vol HIGH   → no skip (HMC chỉ skip MED)', 'HMC', 0.5, 25.0, 2.5, 85, expect_skip=False)  # score=85 FAR ma20 tránh tất cả HMC rules
    check('DXS Vol MED    → no skip (DXS chỉ skip HIGH)', 'DXS', 0.5, 5.0, 1.5, 80, expect_skip=False)
    check('DXS Vol LOW    → no skip (DXS chỉ skip HIGH)', 'DXS', 0.5, 5.0, 0.5, 80, expect_skip=False)
    if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

    _prev_failed = failed
    section('NO-SKIP — mã không có rule trong SCB_HARD_SKIP')
    check('BSI  no-rule conditions', 'BSI',  0.5,  5.0, 1.0, 80, expect_skip=False)
    check('ORS  not in SCB_HARD_SKIP', 'ORS', -3.0, 15.0, 0.5, 70, expect_skip=False)
    check('PDR  not in SCB_HARD_SKIP', 'PDR', -3.0, 15.0, 0.5, 70, expect_skip=False)
    check('CTS  not in SCB_HARD_SKIP', 'CTS', -3.0, 15.0, 0.5, 70, expect_skip=False)
    check('DVP  not in SCB_HARD_SKIP', 'DVP', -3.0, 15.0, 0.5, 70, expect_skip=False)
    check('AGG  not in SCB_HARD_SKIP', 'AGG', -3.0, 15.0, 0.5, 70, expect_skip=False)
    check('BSR  not in wrong rule',    'BSR',  0.5,  5.0, 2.5, 80, expect_skip=False)
    if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')


# ══════════════════════════════════════════════════════════════════
# SECTION 4 — MULTI-RULE: mã có nhiều rules, test từng rule độc lập
# ══════════════════════════════════════════════════════════════════
_prev_failed = failed
section('MULTI-RULE — HMC (5 rules): mỗi rule trigger độc lập')
# Setup: context "clean" (không trigger rule khác) rồi kích 1 rule
check('HMC rule1: vni_down',     'HMC', -3.0,  5.0, 1.0,  80, expect_skip=True,  expect_reason_contains='HMC VNI DOWN')
check('HMC rule2: vol_med',      'HMC',  0.5,  5.0, 1.5,  80, expect_skip=True,  expect_reason_contains='HMC Vol MED')
check('HMC rule3: score 75-84',  'HMC',  0.5,  5.0, 1.0,  80, expect_skip=True,  expect_reason_contains='HMC Score 75-84')
check('HMC rule4: ma20 OPT → NO SKIP (xóa S18b)', 'HMC', 0.5, 5.0, 1.0, 85, expect_skip=False)
check('HMC rule5: ma20 EXT',     'HMC',  0.5, 15.0, 1.0,  85, expect_skip=True,  expect_reason_contains='HMC MA20 EXT')
# Clean context: Vol HIGH (not MED), score 85-94, ma20 FAR — chỉ không skip nếu không phải VNI DOWN
check('HMC clean context: HIGH vol, score 85, ma20 FAR → no skip',
      'HMC',  0.5, 25.0, 2.5,  85, expect_skip=False)
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('MULTI-RULE — DCM (3 rules): vol_low, NEAR, EXT')
check('DCM rule1: vol_low',  'DCM', 1.5,  5.0, 0.5, 80, expect_skip=True,  expect_reason_contains='DCM Vol LOW')  # VNI UP
check('DCM rule2: NEAR',     'DCM', 1.5,  1.0, 1.0, 80, expect_skip=True,  expect_reason_contains='DCM MA20 NEAR')  # vni=UP tránh vni_flat
check('DCM rule3: EXT',      'DCM', 1.5, 15.0, 1.0, 80, expect_skip=True,  expect_reason_contains='DCM MA20 EXT')  # vni=UP tránh vni_flat
check('DCM clean: NORMAL vol, OPT ma20, VNI UP → no skip',
      'DCM',  1.5,  5.0, 1.0,  80, expect_skip=False)  # VNI UP tránh vni_flat
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('MULTI-RULE — GAS (2 rules): vni_down + score 75-84')
check('GAS rule1: vni_down',       'GAS', -3.0, 5.0, 1.0, 90, expect_skip=True,  expect_reason_contains='GAS VNI DOWN')
check('GAS rule2: score 75-84',    'GAS',  0.5, 5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='GAS score 75-84')
check('GAS clean: UP, score 90 → no skip', 'GAS', 2.0, 5.0, 1.0, 90, expect_skip=False)
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('MULTI-RULE — PHP (3 rules): NEAR, OPT, vni_down + PHP EXT là PREMIUM')
check('PHP rule1: NEAR → NO SKIP (đã xóa S18b)', 'PHP', 0.5, 1.0, 1.0, 95, expect_skip=False)
check('PHP rule2: OPT → NO SKIP (S18c)', 'PHP', 0.5,  5.0, 1.0, 95, expect_skip=False)
check('PHP rule3: vni_down', 'PHP', -3.0, 15.0, 1.0, 95, expect_skip=True,  expect_reason_contains='PHP VNI DOWN')  # ma20=EXT (PREMIUM, không skip) → chỉ vni_down trigger
check('PHP EXT = PREMIUM → no skip', 'PHP', 0.5, 15.0, 1.0, 95, expect_skip=False)
check('PHP BELOW → no skip (chỉ NEAR và OPT bị skip)', 'PHP', 0.5, -1.0, 1.0, 95, expect_skip=False)
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('MULTI-RULE — DXS (2 rules): vol_high + EXT')
check('DXS rule1: vol_high', 'DXS', 0.5,  5.0, 2.5, 80, expect_skip=True,  expect_reason_contains='DXS Vol HIGH')
check('DXS rule2: EXT',      'DXS', 0.5, 15.0, 1.0, 80, expect_skip=True,  expect_reason_contains='DXS MA20 EXT')
check('DXS clean: NORMAL vol, OPT → no skip', 'DXS', 0.5, 5.0, 1.0, 80, expect_skip=False)
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')


# ══════════════════════════════════════════════════════════════════
# SECTION 5 — REGRESSION: đảm bảo vol_low/vol_med fix S18 không break gì
# ══════════════════════════════════════════════════════════════════
_prev_failed = failed
section('REGRESSION S18 — vol_low fix không ảnh hưởng mã khác')
# Mã có vol_high rule không bị trigger bởi vol_low
check('DXS Vol LOW không skip (chỉ HIGH trigger)', 'DXS', 0.5, 5.0, 0.5, 80, expect_skip=False)
# Mã không có vol rule không bị ảnh hưởng
check('BSR Vol LOW không skip (không có vol rule)',  'BSR', 0.5, 5.0, 0.5, 80, expect_skip=False)
check('CTG Vol LOW không skip (không có vol rule)',  'CTG', 0.5, 5.0, 0.5, 80, expect_skip=False)
check('FPT Vol LOW không skip (không có vol rule)',  'FPT', 0.5, 5.0, 0.5, 80, expect_skip=False)

section('REGRESSION S18 — vol_med fix không ảnh hưởng mã khác')
check('DCM Vol MED không skip (DCM chỉ skip LOW)',  'DCM', 1.5, 5.0, 1.5, 80, expect_skip=False)  # VNI UP
check('DXS Vol MED không skip (DXS chỉ skip HIGH)', 'DXS', 0.5, 5.0, 1.5, 80, expect_skip=False)
check('BSR Vol MED không skip (không có vol rule)',  'BSR', 0.5, 5.0, 1.5, 80, expect_skip=False)
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')


# ══════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════
total = passed + failed
print(f'\n{"═"*60}')
print(f'  KẾT QUẢ: {passed}/{total} tests passed', end='')
if failed == 0:
    print('  ✅ ALL PASSED')
else:
    print(f'  ❌ {failed} FAILED')
    print(f'\nChi tiết failures ({failed}):')
    for f in failures:
        print(f)
print(f'{"═"*60}')


# SECTION 6 — S18b: rule changes FRT/HMC/DCM/PHP
# ══════════════════════════════════════════════════════════════
_prev_failed = failed
section('S18b — HMC: OPT không còn skip, EXT vẫn skip')
check('HMC MA20 OPT → NO SKIP', 'HMC', 0.5,  5.0, 1.0, 85, expect_skip=False)
check('HMC MA20 EXT → vẫn skip','HMC', 0.5, 15.0, 1.0, 85, expect_skip=True, expect_reason_contains='HMC MA20 EXT')
check('HMC clean: NORMAL vol, score 85, ma20 FAR', 'HMC', 0.5, 25.0, 1.0, 85, expect_skip=False)
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('S18b — FRT: 95+ không còn skip, NEAR mới skip')
check('FRT score 95+ → NO SKIP', 'FRT', 0.5,  5.0, 1.0, 97, expect_skip=False)
check('FRT MA20 NEAR → skip mới','FRT', 0.5,  1.0, 1.0, 80, expect_skip=True, expect_reason_contains='FRT MA20 NEAR')
check('FRT MA20 OPT  → no skip', 'FRT', 0.5,  5.0, 1.0, 80, expect_skip=False)
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('S18b — DCM: VNI FLAT mới skip, UP vẫn pass')
check('DCM VNI FLAT → skip mới', 'DCM',  0.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='DCM VNI FLAT')
check('DCM VNI UP   → no skip',  'DCM',  1.5,  5.0, 1.0, 80, expect_skip=False)
check('DCM VNI DOWN → no skip (không có vni_down rule)', 'DCM', -3.0, 5.0, 1.0, 80, expect_skip=False)
check('DCM Vol LOW  → vẫn skip', 'DCM',  1.5,  5.0, 0.5, 80, expect_skip=True,  expect_reason_contains='DCM Vol LOW')
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('S18b — PHP: NEAR không còn skip, OPT vẫn skip')
check('PHP MA20 NEAR → NO SKIP','PHP', 0.5,  1.0, 1.0, 95, expect_skip=False)
check('PHP MA20 OPT  → NO SKIP (S18c: redundant)', 'PHP', 0.5, 5.0, 1.0, 95, expect_skip=False)
check('PHP MA20 EXT  → PREMIUM, no skip','PHP', 0.5, 15.0, 1.0, 95, expect_skip=False)
check('PHP VNI DOWN  → vẫn skip','PHP', -3.0, 15.0, 1.0, 95, expect_skip=True, expect_reason_contains='PHP VNI DOWN')
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')


# ══════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════════════════════
total = passed + failed
print(f'\n{"═"*60}')
print(f'  KẾT QUẢ: {passed}/{total} tests passed', end='')
if failed == 0:
    print('  ✅ ALL PASSED')
else:
    print(f'  ❌ {failed} FAILED')
    for f in failures:
        print(f)
print(f'{"═"*60}')

# ══════════════════════════════════════════════════════════════════
# SECTION 7 — S18d patches
# ══════════════════════════════════════════════════════════════════

# Update inline dicts
SCB_HARD_SKIP.update({
    'BSR': [('vni_down', True, 'BSR VNI DOWN'),
            ('vni_flat', True, 'BSR VNI FLAT WR=41% Exp=-1.868% (22L)'),
            ('ma20_zone', 'OPT', 'BSR MA20 OPT WR=43% Exp=-1.034% (35L)')],
    'BSI': [('vni_down', True, 'BSI VNI DOWN'),
            ('vni_flat', True, 'BSI VNI FLAT WR=33% Exp=-1.986% (21L)'),
            ('vol_low',  True, 'BSI Vol LOW WR=40% Exp=-1.529% (20L)')],
    'DPM': [('vol_low',   True,   'DPM Vol LOW WR=26% Exp=-2.333% (23L)'),
            ('ma20_zone', 'NEAR', 'DPM MA20 NEAR WR=29% Exp=-2.615% (17L)'),
            ('ma20_zone', 'EXT',  'DPM MA20 EXT Exp=-0.955% (11L)')],
    'ORS': [('vni_flat', True, 'ORS VNI FLAT WR=35% Exp=-2.581% (17L)')],
})

_prev_failed = failed
section('S18d — BSR: FLAT và OPT mới skip, DOWN vẫn skip')
check('BSR VNI FLAT → skip mới',  'BSR',  0.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='BSR VNI FLAT')
check('BSR MA20 OPT → skip mới',  'BSR',  1.5,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='BSR MA20 OPT')
check('BSR VNI DOWN → vẫn skip',  'BSR', -3.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='BSR VNI DOWN')
check('BSR MA20 EXT → NO skip',   'BSR',  1.5, 15.0, 1.0, 80, expect_skip=False)
check('BSR MA20 NEAR → NO skip',  'BSR',  1.5,  1.0, 1.0, 80, expect_skip=False)
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('S18d — BSI: FLAT và Vol LOW mới skip')
check('BSI VNI FLAT → skip mới',  'BSI',  0.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='BSI VNI FLAT')
check('BSI Vol LOW  → skip mới',  'BSI',  1.5,  5.0, 0.5, 80, expect_skip=True,  expect_reason_contains='BSI Vol LOW')
check('BSI UP, NORMAL vol → no skip', 'BSI', 1.5, 5.0, 1.0, 80, expect_skip=False)
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('S18d — DPM: Vol LOW, NEAR mới skip; EXT vẫn skip')
check('DPM Vol LOW  → skip mới',  'DPM',  1.5,  5.0, 0.5, 80, expect_skip=True,  expect_reason_contains='DPM Vol LOW')
check('DPM MA20 NEAR → skip mới', 'DPM',  1.5,  1.0, 1.0, 80, expect_skip=True,  expect_reason_contains='DPM MA20 NEAR')
check('DPM MA20 EXT → vẫn skip',  'DPM',  1.5, 15.0, 1.0, 80, expect_skip=True,  expect_reason_contains='DPM MA20 EXT')
check('DPM MA20 OPT → NO skip',   'DPM',  1.5,  5.0, 1.0, 80, expect_skip=False)
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')

_prev_failed = failed
section('S18d — ORS: FLAT mới skip, UP/DOWN không skip')
check('ORS VNI FLAT → skip mới',  'ORS',  0.0,  5.0, 1.0, 80, expect_skip=True,  expect_reason_contains='ORS VNI FLAT')
check('ORS VNI UP   → no skip',   'ORS',  1.5,  5.0, 1.0, 80, expect_skip=False)
check('ORS VNI DOWN → no skip (ORS không có vni_down rule)', 'ORS', -3.0, 5.0, 1.0, 80, expect_skip=False)
if not VERBOSE: print('OK' if failed == _prev_failed else 'FAIL')


# ══════════════════════════════════════════════════════════════════
# FINAL SUMMARY (S18d)
# ══════════════════════════════════════════════════════════════════
total = passed + failed
print(f'\n{"═"*60}')
print(f'  KẾT QUẢ TỔNG S18d: {passed}/{total} tests passed', end='')
if failed == 0:
    print('  ✅ ALL PASSED')
else:
    print(f'  ❌ {failed} FAILED')
    for f in failures:
        print(f)
print(f'{"═"*60}')
import sys; sys.exit(0 if failed == 0 else 1)
