"""
scorecard_integration_patch.py — Auto-patcher cho telegram_bot.py
Chạy: python scorecard_integration_patch.py --apply telegram_bot.py
"""
import re, shutil, sys, os

def apply_patches(bot_file: str):
    if not os.path.exists(bot_file):
        print(f'ERROR: {bot_file} không tồn tại')
        return False

    bak = bot_file + '.bak'
    shutil.copy2(bot_file, bak)
    print(f'Backup → {bak}')

    with open(bot_file, 'r', encoding='utf-8') as f:
        src = f.read()

    applied = 0

    # ── PATCH 1: Import ──────────────────────────────────────────────────────
    anchor1 = "    logger.warning('market_context module not found — B-filter disabled')"
    insert1 = (
        "\n\n# ── SCORECARD: Context Filter Layer 2 ──────────────────────────────────\n"
        "try:\n"
        "    import context_scorecard as _sc\n"
        "    logger.info('context_scorecard loaded OK')\n"
        "except ImportError:\n"
        "    _sc = None\n"
        "    logger.warning('context_scorecard module not found — Layer 2 disabled')"
    )
    if anchor1 in src and 'import context_scorecard' not in src:
        src = src.replace(anchor1, anchor1 + insert1, 1)
        applied += 1
        print('PATCH-1 OK: import context_scorecard')
    elif 'import context_scorecard' in src:
        print('PATCH-1 SKIP: already applied')
    else:
        print('PATCH-1 WARN: anchor not found')

    # ── PATCH 2: handle_signals compact scorecard ────────────────────────────
    # Tìm đoạn msg += (ae + ... + entry_warn + '\n\n')
    p2_find = "                + div_txt + tio_txt + entry_warn + '\\n\\n'\n            )"
    p2_replace = (
        "                + div_txt + tio_txt + entry_warn\n"
        "                + _sc_line + '\\n\\n'\n"
        "            )"
    )
    sc_block = (
        "\n            # ── Compact Scorecard (Layer 2) ─────────────────────────\n"
        "            _sc_line = ''\n"
        "            if action == 'MUA' and _sc is not None:\n"
        "                try:\n"
        "                    _sc_result, _sc_err = _sc.compute_realtime_context(sym, score_adj)\n"
        "                    if _sc_result:\n"
        "                        _sc_line = '\\n' + _sc.format_scorecard_msg(_sc_result, compact=True)\n"
        "                    else:\n"
        "                        logger.warning(f'scorecard {sym}: {_sc_err}')\n"
        "                except Exception as _sc_ex:\n"
        "                    logger.warning(f'scorecard {sym} exception: {_sc_ex}')\n"
    )
    # Chèn sc_block trước msg += (
    msg_anchor = "            msg += (\n                ae + ' <b>' + sym"
    if msg_anchor in src and '_sc_line' not in src:
        src = src.replace(msg_anchor, sc_block + "            msg += (\n                ae + ' <b>' + sym", 1)
        # Thay + entry_warn + '\n\n' thành + _sc_line
        src = src.replace(p2_find, p2_replace, 1)
        applied += 1
        print('PATCH-2 OK: compact scorecard in handle_signals')
    elif '_sc_line' in src:
        print('PATCH-2 SKIP: already applied')
    else:
        print('PATCH-2 WARN: anchor not found')

    # ── PATCH 3: handle_analyze full scorecard ───────────────────────────────
    p3_anchor = (
        "        except Exception as e:\n"
        "            logger.error('handle_analyze ' + symbol + ': ' + str(e))\n"
        "            logger.error(traceback.format_exc())\n"
        "            # Fallback: gửi chỉ A\n"
        "            send(build_analysis_msg(d), chat_id)\n"
        "            send('⚠ Loi B-filter: ' + str(e)[:100], chat_id)"
    )
    p3_insert = (
        "            # ── Tin 3: Full Context Scorecard (Layer 2) ───────────────\n"
        "            if _sc is not None:\n"
        "                try:\n"
        "                    _sc_result, _sc_err = _sc.compute_realtime_context(symbol, score_adj)\n"
        "                    if _sc_result:\n"
        "                        send(\n"
        "                            '&#x1F4CA; <b>Context Scorecard (Layer 2): ' + symbol + '</b>\\n'\n"
        "                            + '=' * 28 + '\\n'\n"
        "                            + _sc.format_scorecard_msg(_sc_result, compact=False),\n"
        "                            chat_id\n"
        "                        )\n"
        "                    else:\n"
        "                        send('&#x2139; Scorecard ' + symbol + ': ' + str(_sc_err or 'N/A'), chat_id)\n"
        "                except Exception as _sc_ex:\n"
        "                    logger.warning('scorecard analyze ' + symbol + ': ' + str(_sc_ex))\n"
        "\n"
    )
    if p3_anchor in src and 'Tin 3: Full Context Scorecard' not in src:
        src = src.replace(p3_anchor, p3_insert + p3_anchor, 1)
        applied += 1
        print('PATCH-3 OK: full scorecard in handle_analyze')
    elif 'Tin 3: Full Context Scorecard' in src:
        print('PATCH-3 SKIP: already applied')
    else:
        print('PATCH-3 WARN: anchor not found')

    if applied > 0:
        out = bot_file.replace('.py', '_patched.py')
        with open(out, 'w', encoding='utf-8') as f:
            f.write(src)
        print(f'\n✅ {applied}/3 patches applied → {out}')
        print('Review diff, rename thành telegram_bot.py rồi deploy Railway.')
    else:
        print('\nKhông có patch nào mới.')
    return applied > 0

if __name__ == '__main__':
    if '--apply' in sys.argv:
        idx = sys.argv.index('--apply')
        apply_patches(sys.argv[idx + 1] if idx + 1 < len(sys.argv) else 'telegram_bot.py')
    else:
        print('Chạy: python scorecard_integration_patch.py --apply telegram_bot.py')
