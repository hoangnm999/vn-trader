# ============================================================
# VN TRADER BOT V5 — backtest.py
# Primary metric : Profit Factor
# Features       : single-symbol BT, walk-forward (always),
#                  RSI-50 vs RSI-55 comparison table,
#                  28-symbol batch runner + summary report
# ============================================================

import logging
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd
import numpy as np

from config import WATCHLIST, BACKTEST_CONFIG, get_symbol_config
from data_fetcher import fetch_ohlcv, fetch_vni
from indicators import compute_signal, compute_atr

logger = logging.getLogger(__name__)


# ── Result dataclasses ────────────────────────────────────────

@dataclass
class Trade:
    symbol:      str
    entry_date:  pd.Timestamp
    exit_date:   pd.Timestamp
    entry_price: float
    exit_price:  float
    direction:   int        # +1 long / -1 short
    pnl_pct:     float      # net % after costs
    pnl_vnd:     float
    exit_reason: str        # SL | TP | MAX_HOLD | SIGNAL_FLIP


@dataclass
class BacktestResult:
    symbol:           str
    rsi_mode:         str     # "rsi50" | "rsi55"
    total_trades:     int
    win_rate:         float
    profit_factor:    float
    avg_win_pct:      float
    avg_loss_pct:     float
    max_drawdown_pct: float
    total_return_pct: float
    trades:           list[Trade] = field(default_factory=list)
    verdict:          str = ""  # ROBUST | MARGINAL | WEAK | THIN_DATA

    def summary_line(self) -> str:
        return (
            f"{self.symbol:5s} [{self.rsi_mode}] | "
            f"PF={self.profit_factor:5.2f} | "
            f"WR={self.win_rate*100:4.1f}% | "
            f"T={self.total_trades:3d} | "
            f"DD={self.max_drawdown_pct:4.1f}% | "
            f"{self.verdict}"
        )


@dataclass
class WalkForwardResult:
    symbol:  str
    rsi_mode: str
    folds:   list[BacktestResult]
    avg_pf:  float
    std_pf:  float
    min_pf:  float
    verdict: str    # ROBUST | INCONSISTENT | WEAK | THIN_DATA


@dataclass
class SymbolResult:
    """Full result for one symbol: BT + WF for both RSI modes."""
    symbol:     str
    rsi50_bt:   BacktestResult
    rsi50_wf:   WalkForwardResult
    rsi55_bt:   BacktestResult
    rsi55_wf:   WalkForwardResult
    date_from:  str = ""    # first bar date
    date_to:    str = ""    # last bar date
    total_bars: int = 0     # total bars used

    def winner(self) -> str:
        if self.rsi50_bt.profit_factor >= self.rsi55_bt.profit_factor:
            return "rsi50"
        return "rsi55"


# ── Core backtest engine ──────────────────────────────────────

def _run_backtest_on_df(
    symbol: str,
    df: pd.DataFrame,
    cfg: dict,
    bt_cfg: dict,
    df_vni: pd.DataFrame = None,
    rsi_mode: str = "rsi50",
) -> BacktestResult:
    """
    Single-pass vectorised backtest.
    Entry on next open after signal. Exit via SL/TP/MAX_HOLD/SIGNAL_FLIP.
    No look-ahead: signal computed on bars 0..i, entry at bar i+1.
    """
    # Apply RSI mode
    cfg = deepcopy(cfg)
    cfg["rsi_min"] = 50 if rsi_mode == "rsi50" else 55

    capital  = bt_cfg["initial_capital"]
    pos_pct  = bt_cfg["position_size_pct"]
    comm     = bt_cfg["commission_pct"]
    slip     = bt_cfg["slippage_pct"]
    sl_mult  = bt_cfg["stop_loss_atr_mult"]
    rr       = bt_cfg["take_profit_rr"]
    max_hold = bt_cfg["max_hold_days"]

    trades: list[Trade] = []
    equity_curve = [capital]

    in_trade   = False
    direction  = 0
    entry_price = sl_price = tp_price = 0.0
    entry_date  = None
    entry_idx   = 0
    min_bars    = cfg["ema_slow"] + 10

    for i in range(min_bars, len(df) - 1):
        row       = df.iloc[i]
        cur_close = float(row["close"])
        cur_date  = row["date"]
        next_open = float(df.iloc[i + 1]["open"])

        if in_trade:
            cur_high = float(row["high"])
            cur_low  = float(row["low"])
            hold     = i - entry_idx
            ep, er   = None, None

            if direction == 1:
                if cur_low  <= sl_price: ep, er = sl_price, "SL"
                elif cur_high >= tp_price: ep, er = tp_price, "TP"
            else:
                if cur_high >= sl_price: ep, er = sl_price, "SL"
                elif cur_low  <= tp_price: ep, er = tp_price, "TP"

            if er is None and hold >= max_hold:
                ep, er = cur_close, "MAX_HOLD"

            if er is None:
                sig = compute_signal(symbol, df.iloc[:i+1], cfg,
                                     df_vni=df_vni, mode="vol_required")
                if sig and sig.final_signal == -direction:
                    ep, er = next_open, "SIGNAL_FLIP"

            if ep is not None:
                raw_pnl = direction * (ep - entry_price) / entry_price
                net_pnl = raw_pnl - (comm + slip) * 2
                trade_cap = capital * pos_pct
                pnl_vnd   = trade_cap * net_pnl
                capital  += pnl_vnd
                trades.append(Trade(
                    symbol=symbol, entry_date=entry_date, exit_date=cur_date,
                    entry_price=entry_price, exit_price=ep,
                    direction=direction,
                    pnl_pct=round(net_pnl * 100, 4),
                    pnl_vnd=round(pnl_vnd, 0),
                    exit_reason=er,
                ))
                equity_curve.append(capital)
                in_trade = False

        if not in_trade:
            sig = compute_signal(symbol, df.iloc[:i+1], cfg,
                                 df_vni=df_vni, mode="vol_required")
            if sig and sig.final_signal != 0:
                atr       = compute_atr(df.iloc[:i+1])
                direction  = sig.final_signal
                entry_price = next_open
                entry_date  = cur_date
                entry_idx   = i
                risk = atr * sl_mult
                if direction == 1:
                    sl_price = entry_price - risk
                    tp_price = entry_price + risk * rr
                else:
                    sl_price = entry_price + risk
                    tp_price = entry_price - risk * rr
                in_trade = True

    return _metrics(symbol, rsi_mode, trades, equity_curve)


def _metrics(
    symbol: str, rsi_mode: str,
    trades: list[Trade], equity_curve: list[float],
) -> BacktestResult:
    if not trades:
        return BacktestResult(
            symbol=symbol, rsi_mode=rsi_mode,
            total_trades=0, win_rate=0, profit_factor=0,
            avg_win_pct=0, avg_loss_pct=0,
            max_drawdown_pct=0, total_return_pct=0,
            verdict="THIN_DATA",
        )
    pnls   = [t.pnl_pct for t in trades]
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    gp  = sum(wins)          if wins   else 0.0
    gl  = abs(sum(losses))   if losses else 0.0
    pf  = (gp / gl)          if gl > 0 else float("inf")
    wr  = len(wins) / len(pnls)
    aw  = float(np.mean(wins))   if wins   else 0.0
    al  = float(np.mean(losses)) if losses else 0.0

    eq   = np.array(equity_curve)
    peak = np.maximum.accumulate(eq)
    mdd  = float(abs(((eq - peak) / peak * 100).min()))
    ret  = (eq[-1] - eq[0]) / eq[0] * 100

    if   len(trades) < 10:    verdict = "THIN_DATA"
    elif pf >= 1.5:            verdict = "ROBUST"
    elif pf >= 1.0:            verdict = "MARGINAL"
    else:                      verdict = "WEAK"

    return BacktestResult(
        symbol=symbol, rsi_mode=rsi_mode,
        total_trades=len(trades),
        win_rate=round(wr, 4), profit_factor=round(pf, 3),
        avg_win_pct=round(aw, 4), avg_loss_pct=round(al, 4),
        max_drawdown_pct=round(mdd, 2), total_return_pct=round(ret, 2),
        trades=trades, verdict=verdict,
    )


# ── Walk-forward validation ───────────────────────────────────

def _walk_forward(
    symbol: str, df: pd.DataFrame, cfg: dict, bt_cfg: dict,
    df_vni: pd.DataFrame, rsi_mode: str, n_folds: int = 3,
) -> WalkForwardResult:
    """
    Walk-forward validation: split df into n_folds sequential slices.
    Each fold is backtested independently.
    Min bars per fold = ema_slow + 60 (enough for at least a few signals).
    """
    fold_size  = len(df) // n_folds
    min_fold   = cfg["ema_slow"] + 60
    wf_min_t   = bt_cfg.get("wf_min_trades", 5)
    results    = []

    for i in range(n_folds):
        start   = i * fold_size
        end     = start + fold_size if i < n_folds - 1 else len(df)
        fold_df = df.iloc[start:end].reset_index(drop=True)

        if len(fold_df) < min_fold:
            logger.debug(f"[{symbol}] WF fold {i+1}: too short ({len(fold_df)} bars), skip")
            continue

        r = _run_backtest_on_df(symbol, fold_df, cfg, bt_cfg, df_vni, rsi_mode)
        results.append(r)   # include ALL folds, even THIN_DATA

    if not results:
        return WalkForwardResult(
            symbol=symbol, rsi_mode=rsi_mode, folds=[],
            avg_pf=0, std_pf=0, min_pf=0, verdict="THIN_DATA",
        )

    # Use folds with enough trades for PF stats
    valid = [r for r in results if r.total_trades >= wf_min_t
             and r.profit_factor != float("inf")]
    pfs   = [r.profit_factor for r in valid]

    if not pfs:
        return WalkForwardResult(
            symbol=symbol, rsi_mode=rsi_mode, folds=results,
            avg_pf=0, std_pf=0, min_pf=0, verdict="THIN_DATA",
        )

    avg_pf = round(float(np.mean(pfs)), 3)
    std_pf = round(float(np.std(pfs)), 3)
    min_pf = round(float(np.min(pfs)), 3)
    cv     = std_pf / avg_pf if avg_pf > 0 else 999

    if   avg_pf >= 1.5 and cv < 0.5 and min_pf >= 1.0: verdict = "ROBUST"
    elif avg_pf >= 1.2:                                  verdict = "INCONSISTENT"
    else:                                                 verdict = "WEAK"

    return WalkForwardResult(
        symbol=symbol, rsi_mode=rsi_mode, folds=results,
        avg_pf=avg_pf, std_pf=std_pf, min_pf=min_pf, verdict=verdict,
    )


# ── Single symbol runner ──────────────────────────────────────

def run_symbol(
    symbol: str,
    df: Optional[pd.DataFrame] = None,
    df_vni: Optional[pd.DataFrame] = None,
    use_walk_forward: bool = True,
) -> Optional[SymbolResult]:
    """
    Run backtest for BOTH rsi50 and rsi55 modes.
    Fetches 600 bars (~2.5 năm) for statistically meaningful results.
    Always includes walk-forward (3 folds).
    """
    cfg    = get_symbol_config(symbol)
    bt_cfg = BACKTEST_CONFIG
    n_folds = bt_cfg["walk_forward_folds"]

    if df is None:
        # Fetch đủ data cho backtest có nghĩa thống kê
        df = fetch_ohlcv(symbol, count=bt_cfg["bt_lookback_bars"])
    if df is None or df.empty:
        logger.warning(f"[{symbol}] No data — skipping")
        return None

    date_from = df["date"].iloc[0].strftime("%d/%m/%Y")
    date_to   = df["date"].iloc[-1].strftime("%d/%m/%Y")
    logger.info(f"[{symbol}] Backtest period: {date_from} → {date_to} ({len(df)} bars)")

    if df_vni is None:
        df_vni = fetch_vni()

    bt50 = _run_backtest_on_df(symbol, df, cfg, bt_cfg, df_vni, "rsi50")
    wf50 = _walk_forward(symbol, df, cfg, bt_cfg, df_vni, "rsi50", n_folds) \
           if use_walk_forward else None

    bt55 = _run_backtest_on_df(symbol, df, cfg, bt_cfg, df_vni, "rsi55")
    wf55 = _walk_forward(symbol, df, cfg, bt_cfg, df_vni, "rsi55", n_folds) \
           if use_walk_forward else None

    logger.info(
        f"[{symbol}] RSI50: T={bt50.total_trades} PF={bt50.profit_factor:.2f} | "
        f"RSI55: T={bt55.total_trades} PF={bt55.profit_factor:.2f}"
    )

    return SymbolResult(
        symbol=symbol,
        rsi50_bt=bt50, rsi50_wf=wf50,
        rsi55_bt=bt55, rsi55_wf=wf55,
        date_from=date_from, date_to=date_to,
        total_bars=len(df),
    )


# ── Batch runner ──────────────────────────────────────────────

def run_all(
    symbols: list[str] = None,
    verbose: bool = True,
) -> dict[str, SymbolResult]:
    if symbols is None:
        symbols = WATCHLIST

    df_vni = fetch_vni()
    results = {}

    for sym in symbols:
        try:
            r = run_symbol(sym, df_vni=df_vni)
            if r:
                results[sym] = r
                if verbose:
                    print(r.rsi50_bt.summary_line())
                    print(r.rsi55_bt.summary_line())
        except Exception as e:
            logger.error(f"[{sym}] error: {e}", exc_info=True)

    return results


# ── Comparison table ──────────────────────────────────────────

def comparison_table(results: dict[str, SymbolResult]) -> pd.DataFrame:
    """
    Build side-by-side RSI-50 vs RSI-55 comparison DataFrame.
    Sorted by RSI-50 Profit Factor descending.
    """
    rows = []
    for sym, r in results.items():
        wf50_verdict = r.rsi50_wf.verdict if r.rsi50_wf else "—"
        wf55_verdict = r.rsi55_wf.verdict if r.rsi55_wf else "—"
        wf50_pf      = r.rsi50_wf.avg_pf  if r.rsi50_wf else 0
        wf55_pf      = r.rsi55_wf.avg_pf  if r.rsi55_wf else 0

        winner = "50" if r.rsi50_bt.profit_factor >= r.rsi55_bt.profit_factor else "55"

        rows.append({
            "symbol":      sym,
            # RSI-50
            "PF_50":       r.rsi50_bt.profit_factor,
            "WR%_50":      round(r.rsi50_bt.win_rate * 100, 1),
            "T_50":        r.rsi50_bt.total_trades,
            "WF_50":       f"{wf50_verdict}({wf50_pf:.2f})",
            "V_50":        r.rsi50_bt.verdict,
            # RSI-55
            "PF_55":       r.rsi55_bt.profit_factor,
            "WR%_55":      round(r.rsi55_bt.win_rate * 100, 1),
            "T_55":        r.rsi55_bt.total_trades,
            "WF_55":       f"{wf55_verdict}({wf55_pf:.2f})",
            "V_55":        r.rsi55_bt.verdict,
            # Winner
            "best_RSI":    winner,
        })

    return pd.DataFrame(rows).sort_values("PF_50", ascending=False)


def print_summary(results: dict[str, SymbolResult]) -> None:
    df = comparison_table(results)

    print("\n" + "=" * 95)
    print("VN TRADER BOT V5 — RSI-50 vs RSI-55 COMPARISON  (sorted by PF_50)")
    print("=" * 95)
    print(df.to_string(index=False))
    print("=" * 95)

    robust50 = df[df["V_50"] == "ROBUST"]["symbol"].tolist()
    robust55 = df[df["V_55"] == "ROBUST"]["symbol"].tolist()
    both     = [s for s in robust50 if s in robust55]
    only50   = [s for s in robust50 if s not in robust55]
    only55   = [s for s in robust55 if s not in robust50]

    print(f"\n✅ ROBUST both modes  ({len(both)}):  {', '.join(both) or '—'}")
    print(f"🔵 ROBUST RSI-50 only ({len(only50)}): {', '.join(only50) or '—'}")
    print(f"🟣 ROBUST RSI-55 only ({len(only55)}): {', '.join(only55) or '—'}")
    print(f"\n→ Recommended: set rsi_min=55 for: {', '.join(robust55) or '—'}")
    print(f"→ Keep rsi_min=50 for:             {', '.join(only50)  or '—'}")


# ── Telegram-friendly BT message ─────────────────────────────

def format_bt_telegram(symbol: str, result: SymbolResult) -> str:
    """
    Format SymbolResult as plain text — no Markdown, no parse errors.
    """
    if result is None:
        return f"❌ Không có dữ liệu backtest cho {symbol}"

    def _ve(v):
        return {"ROBUST": "✅", "MARGINAL": "🟡", "WEAK": "❌",
                "THIN_DATA": "⚠️", "INCONSISTENT": "🔄"}.get(v, "")

    def _wf_block(wf: WalkForwardResult, label: str) -> list[str]:
        if not wf:
            return [f"  WF: —"]
        if not wf.folds:
            return [f"  WF: {_ve(wf.verdict)} {wf.verdict} (không đủ data)"]
        folds_str = "  ".join(
            f"F{i+1}:{f.profit_factor:.2f}({f.total_trades}t)"
            for i, f in enumerate(wf.folds)
        )
        return [
            f"  WF: {_ve(wf.verdict)} {wf.verdict} avg={wf.avg_pf:.2f} std={wf.std_pf:.2f} min={wf.min_pf:.2f}",
            f"  {folds_str}",
        ]

    b50, w50 = result.rsi50_bt, result.rsi50_wf
    b55, w55 = result.rsi55_bt, result.rsi55_wf
    winner   = "RSI-50" if b50.profit_factor >= b55.profit_factor else "RSI-55"

    # Show current SL/TP params used
    sl_m  = BACKTEST_CONFIG["stop_loss_atr_mult"]
    tp_rr = BACKTEST_CONFIG["take_profit_rr"]
    hold  = BACKTEST_CONFIG["max_hold_days"]

    lines = [f"📋 Backtest {symbol}  |  Winner: {winner} ✓"]

    if result.date_from and result.date_to:
        yrs = result.total_bars / 250
        lines.append(f"📅 {result.date_from} -> {result.date_to} ({result.total_bars} bars ~ {yrs:.1f} nam)")

    lines.append(f"⚙️  SL={sl_m}xATR  TP={tp_rr}:1  Hold<={hold}d  (dung /optimize {symbol} de toi uu)")

    lines += [
        "",
        f"[ RSI-50 ]  {_ve(b50.verdict)} {b50.verdict}",
        f"  PF={b50.profit_factor:.2f}  WR={b50.win_rate*100:.1f}%  T={b50.total_trades}",
        f"  AvgW=+{b50.avg_win_pct:.2f}%  AvgL={b50.avg_loss_pct:.2f}%  DD={b50.max_drawdown_pct:.1f}%",
    ]
    lines += _wf_block(w50, "RSI-50")

    lines += [
        "",
        f"[ RSI-55 ]  {_ve(b55.verdict)} {b55.verdict}",
        f"  PF={b55.profit_factor:.2f}  WR={b55.win_rate*100:.1f}%  T={b55.total_trades}",
        f"  AvgW=+{b55.avg_win_pct:.2f}%  AvgL={b55.avg_loss_pct:.2f}%  DD={b55.max_drawdown_pct:.1f}%",
    ]
    lines += _wf_block(w55, "RSI-55")

    return "\n".join(lines)


# ── CLI ───────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    syms = sys.argv[1:] if len(sys.argv) > 1 else WATCHLIST
    print(f"Backtesting {len(syms)} symbols (RSI-50 vs RSI-55)...")
    results = run_all(symbols=syms)
    print_summary(results)
