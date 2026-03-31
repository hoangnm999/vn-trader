# ============================================================
# VN TRADER BOT V5 — telegram_bot.py
# Compatible: python-telegram-bot==21.9, Python 3.11/3.13
# Scheduler: PTB built-in JobQueue (không dùng APScheduler)
# Commands: /scan /bt [SYM] /status /help
# ============================================================

import sys
import logging
from datetime import datetime
import pytz

from telegram import Update
from telegram.ext import (
    Application, ApplicationBuilder,
    CommandHandler, ContextTypes,
)

from config import (
    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, WATCHLIST,
    SCAN_HOUR_HCM, SCAN_MINUTE_HCM, TIMEZONE,
    get_symbol_config,
)
from data_fetcher import fetch_all_symbols, fetch_vni, fetch_ohlcv
from indicators import compute_signal
from backtest import run_symbol, format_bt_telegram, SymbolResult
from optimize import run_optimize, format_optimize_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ── Validate env vars sớm — fail fast & clean ────────────────

def _check_env() -> None:
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        logger.critical(
            "\n══════════════════════════════════════════\n"
            "  ❌ MISSING RAILWAY ENVIRONMENT VARIABLES\n"
            "══════════════════════════════════════════\n"
            f"  Thiếu: {', '.join(missing)}\n"
            "  Fix: Railway Dashboard → Variables → Add\n"
            "══════════════════════════════════════════"
        )
        sys.exit(1)


# ── Formatting ────────────────────────────────────────────────

def _fmt_signal(sig) -> str:
    e = "🟢" if sig.final_signal == 1 else "🔴"
    direction = "MUA" if sig.final_signal == 1 else "BÁN"
    rs_str = f"RS 3M={sig.rs_3m:+.1f}% 1M={sig.rs_1m:+.1f}%" if hasattr(sig, "rs_3m") else ""
    return "\n".join([
        f"{e} *{sig.symbol}* — {direction}  {rs_str}",
        f"Giá: `{sig.close:,.0f}` | {sig.date.strftime('%d/%m')}",
        f"Confidence: `{sig.confidence:.1%}` | Layers: `{sig.layers_agree}/3`",
        "",
        f"📈 Trend: {sig.trend.reason}",
        f"⚡ Momentum: {sig.momentum.reason}",
        f"📊 Volume: {sig.volume.reason}",
    ])


def _fmt_bt(symbol: str, result: dict) -> str:
    bt = result.get("backtest")
    wf = result.get("walk_forward")
    if not bt:
        return f"❌ Không có dữ liệu backtest cho {symbol}"
    ve = {"ROBUST": "✅", "MARGINAL": "🟡", "WEAK": "❌", "THIN_DATA": "⚠️"}.get(bt.verdict, "")
    wf_line = f"\n🔄 Walk-Forward: {wf.verdict} (avg PF={wf.avg_pf:.2f})" if wf else ""
    return "\n".join([
        f"📋 *Backtest {symbol}*",
        f"{ve} Verdict: `{bt.verdict}`",
        "",
        f"Profit Factor: `{bt.profit_factor:.2f}`",
        f"Win Rate:      `{bt.win_rate*100:.1f}%`",
        f"Số lệnh:       `{bt.total_trades}`",
        f"Avg Win:       `+{bt.avg_win_pct:.2f}%`",
        f"Avg Loss:      `{bt.avg_loss_pct:.2f}%`",
        f"Max Drawdown:  `{bt.max_drawdown_pct:.1f}%`",
        f"Total Return:  `{bt.total_return_pct:.1f}%`",
        wf_line,
    ])


# ── Scan logic ────────────────────────────────────────────────

async def _do_scan(symbols: list = None) -> list:
    if symbols is None:
        symbols = WATCHLIST
    df_vni = fetch_vni()
    data   = fetch_all_symbols(symbols)
    msgs = []
    for sym, df in data.items():
        cfg = get_symbol_config(sym)
        sig = compute_signal(sym, df, cfg, df_vni=df_vni, mode="vol_required")
        if sig and sig.final_signal != 0:
            msgs.append(_fmt_signal(sig))
    return msgs


# ── Command handlers ──────────────────────────────────────────

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🔍 Đang quét 28 symbols...")
    try:
        msgs = await _do_scan()
        if msgs:
            for m in msgs:
                await update.message.reply_text(m, parse_mode="Markdown")
            await update.message.reply_text(f"✅ {len(msgs)} tín hiệu tìm thấy.")
        else:
            await update.message.reply_text("⚪ Không có tín hiệu rõ ràng hôm nay.")
    except Exception as e:
        logger.error(f"/scan error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Lỗi: {e}")


async def cmd_bt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Dung: /bt VCB")
        return
    symbol = context.args[0].upper()
    await update.message.reply_text(f"⏳ Dang backtest {symbol} (RSI-50 vs RSI-55, ~5 nam)...")
    try:
        # Dung bt_lookback_bars tu config (1250 bars ~ 5 nam)
        from config import BACKTEST_CONFIG
        df = fetch_ohlcv(symbol, count=BACKTEST_CONFIG["bt_lookback_bars"])
        if df is None or df.empty:
            await update.message.reply_text(
                f"❌ Khong lay duoc du lieu {symbol}\n"
                f"Kiem tra lai ma CK hoac thu lai sau."
            )
            return
        result = run_symbol(symbol, df=df)
        msg = format_bt_telegram(symbol, result)
        # Plain text — khong parse_mode, khong loi entity
        if len(msg) > 4000:
            mid = msg.find("[ RSI-55 ]")
            await update.message.reply_text(msg[:mid].strip())
            await update.message.reply_text(msg[mid:].strip())
        else:
            await update.message.reply_text(msg)
    except Exception as e:
        logger.error(f"/bt {symbol} error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Loi backtest {symbol}: {e}")


async def cmd_optimize(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Dung: /optimize VCB")
        return
    symbol = context.args[0].upper()
    await update.message.reply_text(
        f"⚙️ Dang optimize {symbol} (64 combinations SL x TP x Hold)...\n"
        f"Qua trinh nay mat ~30-60 giay."
    )
    try:
        from config import BACKTEST_CONFIG
        df = fetch_ohlcv(symbol, count=BACKTEST_CONFIG["bt_lookback_bars"])
        if df is None or df.empty:
            await update.message.reply_text(f"❌ Khong lay duoc du lieu {symbol}")
            return
        opt = run_optimize(symbol, df=df)
        msg = format_optimize_telegram(symbol, opt)
        if len(msg) > 4000:
            await update.message.reply_text(msg[:4000])
            await update.message.reply_text(msg[4000:])
        else:
            await update.message.reply_text(msg)
    except Exception as e:
        logger.error(f"/optimize {symbol} error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Loi optimize {symbol}: {e}")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tz  = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    await update.message.reply_text(
        f"✅ *VN Trader Bot V5*\n"
        f"Thời gian: `{now.strftime('%H:%M %d/%m/%Y')} ICT`\n"
        f"Symbols: `{len(WATCHLIST)}`\n"
        f"Scan: `{SCAN_HOUR_HCM:02d}:{SCAN_MINUTE_HCM:02d} ICT` mỗi ngày\n"
        f"Architecture: `Trend · Momentum · Volume + RS`\n"
        f"Metric: `Profit Factor`",
        parse_mode="Markdown",
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "*VN Trader Bot V5*\n\n"
        "/scan — Quét toàn bộ watchlist\n"
        "/bt VCB — Backtest một symbol\n"
        "/status — Trạng thai bot\n"
        "/help — Lệnh này\n\n"
        "/optimize VCB — Tim SL/TP/Hold toi uu\n"
        "Signal khi ca 3 layers dong thuan + RS vuot VNI:\n"
        "Trend: EMA20/50 + ADX\n"
        "Momentum: RSI slope + MACD\n"
        "Volume: surge + VWAP",
        parse_mode="Markdown",
    )


# ── Scheduled scan (PTB JobQueue — không cần APScheduler) ────

async def _scheduled_scan_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("Scheduled scan triggered")
    try:
        msgs = await _do_scan()
        tz  = pytz.timezone(TIMEZONE)
        now = datetime.now(tz).strftime("%d/%m/%Y")
        if msgs:
            await context.bot.send_message(
                TELEGRAM_CHAT_ID,
                f"📡 *Tín hiệu ngày {now}* ({len(msgs)} symbols)",
                parse_mode="Markdown",
            )
            for m in msgs:
                await context.bot.send_message(TELEGRAM_CHAT_ID, m, parse_mode="Markdown")
        else:
            await context.bot.send_message(
                TELEGRAM_CHAT_ID,
                f"⚪ *{now}* — Không có tín hiệu.",
                parse_mode="Markdown",
            )
        logger.info(f"Scheduled scan done: {len(msgs)} signals")
    except Exception as e:
        logger.error(f"Scheduled scan error: {e}", exc_info=True)
        try:
            await context.bot.send_message(TELEGRAM_CHAT_ID, f"❌ Lỗi scan định kỳ: {e}")
        except Exception:
            pass


# ── Main ──────────────────────────────────────────────────────

def main() -> None:
    _check_env()
    logger.info("Env vars OK — building app...")

    app: Application = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("scan",   cmd_scan))
    app.add_handler(CommandHandler("bt",     cmd_bt))
    app.add_handler(CommandHandler("optimize", cmd_optimize))
    app.add_handler(CommandHandler("status",   cmd_status))
    app.add_handler(CommandHandler("help",   cmd_help))

    # PTB JobQueue — chạy mỗi ngày lúc SCAN_HOUR_HCM:SCAN_MINUTE_HCM ICT
    tz = pytz.timezone(TIMEZONE)
    scan_time = datetime.now(tz).replace(
        hour=SCAN_HOUR_HCM,
        minute=SCAN_MINUTE_HCM,
        second=0,
        microsecond=0,
    ).timetz()

    app.job_queue.run_daily(
        _scheduled_scan_job,
        time=scan_time,
        name="daily_scan",
    )
    logger.info(f"Scheduled scan: {SCAN_HOUR_HCM:02d}:{SCAN_MINUTE_HCM:02d} ICT daily")
    logger.info("VN Trader Bot V5 — polling started")

    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
