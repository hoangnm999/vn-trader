# ============================================================
# VN TRADER BOT V5 — optimize.py
# Grid search: SL multiplier × TP ratio × max_hold_days
# Metric: Profit Factor (primary) + Win Rate (tiebreak)
# ============================================================

import logging
from copy import deepcopy
from dataclasses import dataclass
from itertools import product
from typing import Optional
import pandas as pd
import numpy as np

from config import BACKTEST_CONFIG, get_symbol_config
from data_fetcher import fetch_ohlcv, fetch_vni
from backtest import _run_backtest_on_df, BacktestResult

logger = logging.getLogger(__name__)

# ── Search grid ───────────────────────────────────────────────
GRID = {
    "stop_loss_atr_mult": [1.5, 2.0, 2.5, 3.0],
    "take_profit_rr":     [1.5, 2.0, 2.5, 3.0],
    "max_hold_days":      [10,  15,  20,  30  ],
}
# Total combinations: 4×4×4 = 64 — fast enough per symbol


@dataclass
class OptimizeResult:
    symbol:          str
    best_sl_mult:    float
    best_tp_rr:      float
    best_hold_days:  int
    best_pf:         float
    best_wr:         float
    best_trades:     int
    baseline_pf:     float   # PF with default params
    improvement_pct: float   # % improvement over baseline
    all_results:     list    # sorted list of (sl, tp, hold, pf, wr, trades)

    def summary(self) -> str:
        lines = [
            f"🔧 Optimize {self.symbol}",
            f"",
            f"Baseline (default): PF={self.baseline_pf:.2f}",
            f"Best params:        PF={self.best_pf:.2f} (+{self.improvement_pct:.1f}%)",
            f"",
            f"  SL mult  = {self.best_sl_mult}x ATR",
            f"  TP ratio = {self.best_tp_rr}:1  (RR)",
            f"  Max hold = {self.best_hold_days} days",
            f"  Trades   = {self.best_trades}  WR={self.best_wr*100:.1f}%",
            f"",
            f"Top 5 param combos:",
        ]
        for i, r in enumerate(self.all_results[:5], 1):
            sl, tp, hold, pf, wr, t = r
            lines.append(
                f"  {i}. SL={sl}x TP={tp}:1 Hold={hold}d "
                f"-> PF={pf:.2f} WR={wr*100:.1f}% T={t}"
            )
        return "\n".join(lines)


def run_optimize(
    symbol: str,
    df: Optional[pd.DataFrame] = None,
    df_vni: Optional[pd.DataFrame] = None,
    rsi_mode: str = "rsi50",
    min_trades: int = 10,
) -> Optional[OptimizeResult]:
    """
    Grid search over SL×TP×hold_days for a single symbol.
    Uses the same signal logic as backtest (vol_required mode).

    Args:
        symbol:     Stock ticker
        df:         Pre-loaded OHLCV (fetched if None)
        df_vni:     Pre-loaded VNINDEX (fetched if None)
        rsi_mode:   "rsi50" or "rsi55"
        min_trades: Skip param combos with fewer trades (avoid lucky small samples)

    Returns:
        OptimizeResult with best params and full grid
    """
    cfg    = get_symbol_config(symbol)
    bt_cfg = deepcopy(BACKTEST_CONFIG)

    if df is None:
        df = fetch_ohlcv(symbol, count=bt_cfg["bt_lookback_bars"])
    if df is None or df.empty:
        logger.warning(f"[{symbol}] No data for optimize")
        return None

    if df_vni is None:
        df_vni = fetch_vni()

    # Baseline with current defaults
    baseline = _run_backtest_on_df(symbol, df, cfg, bt_cfg, df_vni, rsi_mode)

    # Grid search
    results = []
    sl_vals   = GRID["stop_loss_atr_mult"]
    tp_vals   = GRID["take_profit_rr"]
    hold_vals = GRID["max_hold_days"]

    total = len(sl_vals) * len(tp_vals) * len(hold_vals)
    logger.info(f"[{symbol}] Optimizing {total} combinations...")

    for sl, tp, hold in product(sl_vals, tp_vals, hold_vals):
        trial_cfg           = deepcopy(bt_cfg)
        trial_cfg["stop_loss_atr_mult"] = sl
        trial_cfg["take_profit_rr"]     = tp
        trial_cfg["max_hold_days"]      = hold

        r = _run_backtest_on_df(symbol, df, cfg, trial_cfg, df_vni, rsi_mode)

        if r.total_trades < min_trades:
            continue  # not enough trades to trust PF

        results.append((sl, tp, hold, r.profit_factor, r.win_rate, r.total_trades))

    if not results:
        logger.warning(f"[{symbol}] No valid combinations (all < {min_trades} trades)")
        return None

    # Sort by PF desc, then WR desc
    results.sort(key=lambda x: (x[3], x[4]), reverse=True)

    best = results[0]
    sl_b, tp_b, hold_b, pf_b, wr_b, t_b = best

    improvement = ((pf_b - baseline.profit_factor) / max(baseline.profit_factor, 0.01)) * 100

    return OptimizeResult(
        symbol=symbol,
        best_sl_mult=sl_b,
        best_tp_rr=tp_b,
        best_hold_days=hold_b,
        best_pf=pf_b,
        best_wr=wr_b,
        best_trades=t_b,
        baseline_pf=round(baseline.profit_factor, 3),
        improvement_pct=round(improvement, 1),
        all_results=results,
    )


def apply_optimized_params(symbol: str, opt: OptimizeResult) -> dict:
    """
    Return a per-symbol bt_cfg override with optimized params.
    Can be saved to SYMBOL_CONFIG or used directly.
    """
    return {
        "stop_loss_atr_mult": opt.best_sl_mult,
        "take_profit_rr":     opt.best_tp_rr,
        "max_hold_days":      opt.best_hold_days,
    }


# ── Telegram formatter ────────────────────────────────────────

def format_optimize_telegram(symbol: str, opt: Optional[OptimizeResult]) -> str:
    """Plain text output for /optimize command."""
    if opt is None:
        return f"❌ Khong du du lieu de optimize {symbol}"

    imp_str = f"+{opt.improvement_pct:.1f}%" if opt.improvement_pct >= 0 else f"{opt.improvement_pct:.1f}%"

    lines = [
        f"🔧 Optimize {symbol}",
        f"",
        f"Baseline (SL=2x TP=2:1 Hold=20d): PF={opt.baseline_pf:.2f}",
        f"Best combo:                        PF={opt.best_pf:.2f} ({imp_str})",
        f"",
        f"Best params:",
        f"  SL  = {opt.best_sl_mult}x ATR",
        f"  TP  = {opt.best_tp_rr}:1  (RR = {opt.best_tp_rr})",
        f"  SL$ = entry - SL_mult x ATR",
        f"  TP$ = entry + TP_rr x risk",
        f"  Hold= {opt.best_hold_days} ngay toi da",
        f"  WR  = {opt.best_wr*100:.1f}%  |  T={opt.best_trades}",
        f"",
        f"Top 5 combos (PF desc):",
    ]

    for i, r in enumerate(opt.all_results[:5], 1):
        sl, tp, hold, pf, wr, t = r
        marker = " <-- best" if i == 1 else ""
        lines.append(f"  {i}. SL={sl}x TP={tp}:1 Hold={hold}d -> PF={pf:.2f} WR={wr*100:.1f}% T={t}{marker}")

    if opt.improvement_pct > 10:
        lines += [
            f"",
            f"💡 Goi y: cap nhat SYMBOL_CONFIG['{symbol}'] voi:",
            f"   stop_loss_atr_mult={opt.best_sl_mult}, take_profit_rr={opt.best_tp_rr}, max_hold_days={opt.best_hold_days}",
        ]

    return "\n".join(lines)


# ── CLI ───────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    syms = sys.argv[1:] if len(sys.argv) > 1 else ["MBB", "VCB", "FPT"]
    df_vni = fetch_vni()

    for sym in syms:
        print(f"\nOptimizing {sym}...")
        opt = run_optimize(sym, df_vni=df_vni)
        if opt:
            print(opt.summary())
