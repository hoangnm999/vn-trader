# ============================================================
# HƯỚNG DẪN TRIỂN KHAI ĐẦY ĐỦ — VN TRADER BOT V6
# CF Walk-Forward + Ablation Study
# ============================================================
# Tài liệu này mô tả CHI TIẾT từng bước:
#   - File nào cần upload lên GitHub
#   - Lệnh Telegram nào để chạy
#   - Output trông như thế nào
#   - Quyết định cần đưa ra sau mỗi bước
# ============================================================


# ════════════════════════════════════════════════════════════
# BƯỚC 0 — VERIFY SYMBOL_CONFIG
# ════════════════════════════════════════════════════════════
#
# MỤC ĐÍCH: Đảm bảo SL/TP/hold_days/min_score đúng cho 10 mã
# trước khi chạy WF. Nếu config sai → toàn bộ WF chạy trên
# params sai → kết quả vô nghĩa.
#
# CÁCH LÀM:
#   Mở file config.py trên GitHub, tìm SYMBOL_CONFIG,
#   verify 10 mã Score A có đúng params sau:
#
# ┌─────────┬──────┬──────┬───────────┬───────────┐
# │ Mã      │ SL % │ TP % │ hold_days │ min_score │
# ├─────────┼──────┼──────┼───────────┼───────────┤
# │ MCH     │  7   │  14  │    15     │    65     │
# │ DGC     │  7   │  14  │    15     │    65     │
# │ SSI     │  5   │   9  │    15     │    65     │
# │ HCM     │  7   │  10  │    15     │    65     │
# │ NKG     │  7   │  10  │    15     │    65     │
# │ FRT     │  7   │  10  │    15     │    65     │
# │ HAH     │  7   │  14  │    15     │    65     │
# │ PC1     │  7   │  14  │    15     │    65     │
# │ STB     │  7   │  14  │    15     │    65     │
# │ CTS     │  7   │  14  │    15     │    65     │
# └─────────┴──────┴──────┴───────────┴───────────┘
#
# PASS: Params đúng hết → tiếp tục Bước 1
# FAIL: Có mã sai → sửa config.py, commit lên GitHub trước
#
# THÊM VÀO BOT: Không cần lệnh riêng — chỉ cần mắt đọc config.py


# ════════════════════════════════════════════════════════════
# BƯỚC 1 — DEPLOY 5 FILES LÊN GITHUB/RAILWAY
# ════════════════════════════════════════════════════════════
#
# FILES CẦN UPLOAD (từ các file Claude đã gửi):
#
# 1. context_scorecard.py      ← đã fix CLIMAX_ACCUM/BLOWOFF
# 2. cf_walk_forward.py        ← WF + Ablation study (MỚI)
# 3. cf_validation_framework.py← đồng bộ với cf_walk_forward
# 4. telegram_bot_patched.py   ← rename thành telegram_bot.py
#    (đã có scorecard compact trong /signals, full trong /analyze)
#
# SAU ĐÓ: File này (cfwf_bot_commands.py) cũng upload lên
# và import vào telegram_bot.py (xem hướng dẫn tích hợp bên dưới)
#
# CÁC FILE KHÔNG THAY ĐỔI: backtest.py, config.py, app.py, market_context.py
#
# KIỂM TRA DEPLOY THÀNH CÔNG:
#   Gửi /start hoặc /signals trong Telegram
#   Nếu bot trả lời bình thường → deploy OK
#   Nếu bot không trả lời → xem Railway logs để tìm lỗi import


# ════════════════════════════════════════════════════════════
# BOT COMMANDS MỚI — THÊM VÀO telegram_bot.py
# ════════════════════════════════════════════════════════════
#
# Đây là code cần PASTE vào telegram_bot.py ở 2 chỗ:
#   PHẦN 1: Định nghĩa hàm handle_cfwf() — paste ở cuối file
#            trước dòng "if __name__ == '__main__':"
#   PHẦN 2: Dispatch command — paste vào block elif trong polling loop
#

# ─────────────────────────────────────────────────────────────
# PHẦN 1: PASTE VÀO CUỐI telegram_bot.py (trước __main__)
# ─────────────────────────────────────────────────────────────

import threading as _threading_cfwf

def handle_cfwf(args, chat_id, send_fn=None):
    """
    /cfwf — CF Walk-Forward Validation + Ablation Study
    
    Lệnh:
      /cfwf           — Full WF cho 10 mã Score A (không ablation)
      /cfwf ablation  — Full WF + Ablation study (RECOMMENDED)
      /cfwf quick     — Chỉ ablation, bỏ qua full WF (nhanh hơn)
      /cfwf status    — Xem kết quả lần chạy gần nhất
    
    Thời gian ước tính:
      /cfwf           → ~15-20 phút
      /cfwf ablation  → ~60-90 phút (26 combos × 10 mã)
      /cfwf quick     → ~45-60 phút

    send_fn: hàm gửi Telegram message, nhận (text, chat_id).
             Nếu không truyền vào, dùng fallback import từ telegram_bot.
    """
    # Resolve send function — ưu tiên tham số, fallback import
    if send_fn is None:
        try:
            from telegram_bot import send as _send
            send_fn = _send
        except ImportError:
            raise RuntimeError(
                "handle_cfwf: không tìm thấy send_fn. "
                "Truyền send_fn khi gọi handle_cfwf, hoặc đảm bảo telegram_bot.py có thể import."
            )
    send = send_fn
    import os, sys, json
    NL = chr(10)

    bot_dir = os.path.dirname(os.path.abspath(__file__))
    if bot_dir not in sys.path:
        sys.path.insert(0, bot_dir)

    arg = args[0].lower().strip() if args else ''

    # /cfwf status — xem kết quả cũ
    if arg == 'status':
        cache_path = os.path.join(bot_dir, 'cfwf_results.json')
        try:
            with open(cache_path) as f:
                cached = json.load(f)
            ts       = cached.get('timestamp', 'unknown')
            verdict  = cached.get('verdict', '?')
            avg_dexp = cached.get('avg_oos_dexp', 0)
            optimal  = cached.get('optimal_combo', {})
            weights  = cached.get('suggested_weights', {})
            neg_rules= cached.get('neg_rules', [])

            icon = {'V': '✅', '~': '🟡', '-': '🟡', '!': '❌'}.get(verdict, '⚪')
            msg = (
                f'📊 <b>CF Walk-Forward — Kết quả gần nhất</b>' + NL
                + f'Thời gian: {ts}' + NL
                + f'{'─'*30}' + NL
                + f'{icon} Verdict: [{verdict}]' + NL
                + f'Avg OOS dExp: {avg_dexp:+.3f}%' + NL + NL
            )
            if optimal:
                msg += (
                    f'🏆 <b>Optimal Combo:</b> {optimal.get("combo_id","")}' + NL
                    + f'   dExp: {optimal.get("avg_dexp",0):+.3f}%' + NL
                    + f'   Pass rate: {optimal.get("avg_pass",0):.0f}%' + NL + NL
                )
            if weights:
                msg += '<b>Suggested Weights:</b>' + NL
                for rule, w in sorted(weights.items(),
                                       key=lambda x: x[1], reverse=True):
                    bar = '█' * w
                    msg += f'   {rule}: {w}pt {bar}' + NL
            if neg_rules:
                msg += NL + f'❌ Rules nên bỏ: {", ".join(neg_rules)}' + NL
        except FileNotFoundError:
            msg = '⚠ Chưa có kết quả. Chạy /cfwf ablation trước.'
        except Exception as e:
            msg = f'❌ Lỗi đọc cache: {str(e)[:100]}'
        send(msg, chat_id)
        return

    # Thông báo bắt đầu + ước tính thời gian
    if arg == 'ablation':
        mode_label = 'Full WF + Ablation Study (26 combos)'
        est_time   = '60-90 phút'
    elif arg == 'quick':
        mode_label = 'Ablation Only (bỏ qua full WF)'
        est_time   = '45-60 phút'
    else:
        mode_label = 'Full WF (không ablation)'
        est_time   = '15-20 phút'

    send(
        f'⏳ <b>CF Walk-Forward đang chạy...</b>' + NL
        + f'Mode: {mode_label}' + NL
        + f'Ước tính: {est_time}' + NL
        + f'Symbols: MCH DGC SSI HCM NKG FRT HAH PC1 STB CTS' + NL + NL
        + f'Bot sẽ gửi kết quả khi xong. Không cần làm gì thêm.',
        chat_id
    )

    def _run_cfwf(mode=arg, cid=chat_id, _send=send_fn):
        import os, sys, json, traceback
        send = _send  # noqa: F841 — rebind cho dễ đọc bên dưới
        NL = chr(10)
        bot_dir = os.path.dirname(os.path.abspath(__file__))
        if bot_dir not in sys.path:
            sys.path.insert(0, bot_dir)

        try:
            from cf_walk_forward import (
                run_group_wf, run_ablation_study,
                SCORE_A_WATCHLIST, DEFAULT_CF
            )
            symbols = SCORE_A_WATCHLIST

            group_result  = None
            ablation_result = None

            # Bước 1: Full WF
            if mode != 'quick':
                send('🔄 Đang chạy Full WF (10 mã)...', cid)
                group_result = run_group_wf(
                    symbols,
                    group_name='SCORE_A',
                    detail=True,
                    verbose=False,
                )

            # Bước 2: Ablation
            if mode in ('ablation', 'quick'):
                send('🔄 Đang chạy Ablation Study (26 combos × 10 mã)...', cid)
                ablation_result = run_ablation_study(symbols, verbose=False)

            # ── Format và gửi kết quả ─────────────────────────────────────

            # Tin 1: Full WF summary
            if group_result:
                v    = group_result.get('verdict', '?')
                icon = {'V': '✅', '~': '🟡', '-': '🟡', '!': '❌'}.get(v, '⚪')
                g    = group_result
                msg1 = (
                    f'📊 <b>CF Walk-Forward — Kết quả</b>' + NL
                    + f'{'─'*30}' + NL
                    + f'{icon} <b>[{v}] {g.get("verdict_vi","")}</b>' + NL + NL
                    + f'Avg OOS dExp:  {g.get("avg_oos_dexp",0):+.3f}%' + NL
                    + f'Avg OOS dWR:   {g.get("avg_oos_dwr",0):+.1f}%' + NL
                    + f'Pass rate:     {g.get("avg_pass_rate",0):.0f}%' + NL
                    + f'IS↔OOS gap:    {g.get("avg_is_oos_gap",0):+.3f}%' + NL
                    + f'Overfit flag:  {g.get("n_overfit",0)} mã' + NL + NL
                    + f'Verdict breakdown: '
                    + f'V={g.get("n_V",0)} ~={g.get("n_ok",0)} '
                    + f'-={g.get("n_nt",0)} !={g.get("n_bad",0)}' + NL + NL
                )

                # Per-symbol table
                results = g.get('results', [])
                if results:
                    msg1 += '<b>Per-symbol:</b>' + NL
                    for r in sorted(results,
                                    key=lambda x: x.get('avg_oos_dexp',0),
                                    reverse=True):
                        sym = r['symbol']
                        dex = r.get('avg_oos_dexp', 0)
                        vrd = r.get('verdict', '?')
                        ov  = '⚠' if r.get('overfit_flag') else ''
                        vi  = {'V':'✅','~':'🟡','-':'🟡','!':'❌'}.get(vrd,'⚪')
                        msg1 += (f'  {vi} <b>{sym}</b>: '
                                 f'dExp={dex:+.3f}% [{vrd}] {ov}' + NL)

                # Action guidance
                msg1 += NL + '<b>ACTION:</b>' + NL
                if v == 'V':
                    msg1 += '✅ CF rules có edge OOS thật. Scorecard justified.' + NL
                    msg1 += 'Tiếp theo: chạy /cfwf ablation để tìm optimal combo.'
                elif v == '~':
                    msg1 += '🟡 CF có ích nhẹ. Chạy ablation để xác định rules nào đáng giữ.'
                elif v == '-':
                    msg1 += '🟡 CF trung tính. Cần ablation để tìm sub-combo có edge.'
                else:
                    msg1 += '❌ CF không hiệu quả OOS. Xem ablation để hiểu rule nào gây vấn đề.'

                send(msg1, cid)

            # Tin 2: Ablation summary
            if ablation_result:
                optimal = ablation_result.get('optimal_combo', {})
                marginal= ablation_result.get('marginal', {})
                weights = ablation_result.get('suggested_weights', {})
                combos  = ablation_result.get('combo_summary', [])

                msg2 = (
                    f'🔬 <b>Ablation Study — Kết quả</b>' + NL
                    + f'{'─'*30}' + NL
                )

                # Top 5 combos
                msg2 += '<b>Top 5 combos (OOS dExp):</b>' + NL
                for i, c in enumerate(combos[:5], 1):
                    is_opt = ' ← OPTIMAL' if c['combo_id'] == optimal.get('combo_id') else ''
                    vd = '✅' if c['avg_dexp'] > 0.15 else ('🟡' if c['avg_dexp'] > 0 else '❌')
                    msg2 += (f'  {i}. {vd} <b>{c["combo_id"]}</b>: '
                             f'{c["avg_dexp"]:+.3f}% '
                             f'(pass={c["avg_pass"]:.0f}%){is_opt}' + NL)

                msg2 += NL + '<b>Marginal contribution per rule:</b>' + NL
                for rule, m in sorted(marginal.items(),
                                       key=lambda x: x[1], reverse=True):
                    w    = weights.get(rule, 0)
                    icon = '✅' if m > 0.05 else ('🟡' if m > -0.05 else '❌')
                    keep = 'GIỮ' if m > 0.05 else ('OPTIONAL' if m > -0.05 else 'BỎ')
                    msg2 += (f'  {icon} {rule}: {m:+.3f}% → '
                             f'weight={w}pt [{keep}]' + NL)

                if optimal:
                    msg2 += (
                        NL + f'🏆 <b>Optimal combo: {optimal.get("combo_id","")} </b>' + NL
                        + f'   Rules: {optimal.get("rules",[])}' + NL
                        + f'   dExp: {optimal.get("avg_dexp",0):+.3f}% | '
                        + f'Pass: {optimal.get("avg_pass",0):.0f}%' + NL
                    )

                msg2 += NL + '<b>Suggested scorecard weights:</b>' + NL
                for rule, w in sorted(weights.items(),
                                       key=lambda x: x[1], reverse=True):
                    bar  = '█' * w
                    msg2 += f'  {rule}: {w}pt {bar}' + NL

                # Identify negative rules
                neg_rules = [r for r,m in marginal.items() if m <= -0.05]
                if neg_rules:
                    msg2 += NL + f'❌ <b>Nên xem xét bỏ:</b> {", ".join(neg_rules)}' + NL

                msg2 += NL + '<i>Sau bước này: rebuild context_scorecard.py với weights trên</i>'
                send(msg2, cid)

                # Lưu cache để /cfwf status đọc
                cache = {
                    'timestamp':       str(__import__('datetime').datetime.now()),
                    'verdict':         group_result.get('verdict','?') if group_result else '?',
                    'avg_oos_dexp':    group_result.get('avg_oos_dexp',0) if group_result else 0,
                    'optimal_combo':   optimal,
                    'suggested_weights': weights,
                    'neg_rules':       neg_rules,
                    'marginal':        marginal,
                }
                cache_path = os.path.join(bot_dir, 'cfwf_results.json')
                try:
                    with open(cache_path, 'w') as f:
                        json.dump(cache, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass

        except Exception as e:
            err = traceback.format_exc()
            send(f'❌ Lỗi CF WF: {str(e)[:200]}' + NL
                 + f'<i>{err[-300:]}</i>', cid)

    _threading_cfwf.Thread(target=_run_cfwf, daemon=True).start()


# ─────────────────────────────────────────────────────────────
# PHẦN 2: PASTE VÀO DISPATCH BLOCK trong polling loop
# Tìm dòng: elif cmd == '/sascreen':
# Paste TRƯỚC dòng đó:
# ─────────────────────────────────────────────────────────────
#
#                elif cmd == '/cfwf':
#                    threading.Thread(
#                        target=handle_cfwf,
#                        args=(list(parts[1:]), cid),
#                        daemon=True).start()
#


# ════════════════════════════════════════════════════════════
# BƯỚC 2 — CHẠY CF WALK-FORWARD + ABLATION
# ════════════════════════════════════════════════════════════
#
# SAU KHI DEPLOY, GỬI LỆNH:
#
#   /cfwf ablation
#
# Bot sẽ gửi ngay: "CF Walk-Forward đang chạy... Ước tính 60-90 phút"
# Sau đó gửi TỰ ĐỘNG 2 tin nhắn:
#   Tin 1: Full WF results (verdict, per-symbol table)
#   Tin 2: Ablation results (ranking, marginal, weights)
#
# ĐỌC KẾT QUẢ TIN 1 — Full WF:
#
#   [V] = CF rules CÓ EDGE thật → tiếp tục Bước 3
#   [~] = Có ích nhẹ → đọc Tin 2, tìm combo tốt nhất
#   [-] = Trung tính → PHẢI đọc Tin 2, nhiều rule có thể là noise
#   [!] = Không hiệu quả → đọc Tin 2, xác định rule nào gây vấn đề
#
# ĐỌC KẾT QUẢ TIN 2 — Ablation:
#
#   Phần "Top 5 combos": combo nào có dExp cao nhất là optimal
#   Phần "Marginal contribution":
#     ✅ GIỮ   = rule đóng góp dương → đưa vào scorecard
#     🟡 OPT   = neutral → optional, test thêm
#     ❌ BỎ    = contribution âm → KHÔNG đưa vào scorecard
#   Phần "Suggested weights": copy số này vào Bước 4
#
# NẾU MUỐN XEM LẠI SAU:
#   /cfwf status
#
# NẾU BỊ TIMEOUT (Railway free tier):
#   Chia nhỏ: /cfwf (không ablation) trước, sau đó /cfwf quick


# ════════════════════════════════════════════════════════════
# BƯỚC 3 — SENSITIVITY TEST
# ════════════════════════════════════════════════════════════
#
# MỤC ĐÍCH: Xác nhận ngưỡng của các rules "GIỮ" từ Bước 2 là robust.
# Ví dụ: CF1 ngưỡng 3.0% — nếu đổi thành 2.5% hay 3.5% thì kết quả
# thay đổi nhiều không?
# Nếu thay đổi ít (variance < 0.10%) → ngưỡng robust → giữ nguyên
# Nếu thay đổi nhiều → cần điều chỉnh ngưỡng
#
# SAU KHI DEPLOY, GỬI LỆNH:
#   (Sensitivity test sẽ được thêm vào /cfwf)
#   /cfwf sensitivity
#
# Lệnh này cần thêm vào handle_cfwf() ở trên — sẽ build tiếp
# sau khi có kết quả Bước 2.
#
# TẠM THỜI: Bỏ qua Bước 3 nếu Bước 2 cho verdict [V].
# Chỉ cần Bước 3 nếu verdict [~] hoặc [-].


# ════════════════════════════════════════════════════════════
# BƯỚC 4 — REBUILD SCORECARD VỚI WEIGHTS DATA-DRIVEN
# ════════════════════════════════════════════════════════════
#
# SAU KHI CÓ KẾT QUẢ ABLATION, làm như sau:
#
# 1. Đọc "Suggested weights" từ Tin 2 của /cfwf ablation
#    Ví dụ output:
#      CF1: 4pt ████
#      CF3: 3pt ███
#      CF5: 2pt ██
#      CF2: 1pt █
#      CF4: 0pt  (nên bỏ)
#
# 2. Gửi lại cho Claude: "Ablation cho kết quả sau: [paste output]
#    Hãy rebuild context_scorecard.py với weights này"
#
# 3. Claude sẽ:
#    - Bỏ rules có weight = 0 khỏi scorecard
#    - Cập nhật max_points cho mỗi check
#    - Điều chỉnh grading thresholds
#    - Gửi lại file context_scorecard.py mới
#
# 4. Upload file mới lên GitHub, Railway sẽ auto-redeploy


# ════════════════════════════════════════════════════════════
# BƯỚC 4b — ĐỒNG BỘ OPTIMAL COMBO SANG CF_VALIDATION_FRAMEWORK
# ════════════════════════════════════════════════════════════
#
# SAU KHI BIẾT OPTIMAL COMBO từ ablation:
#
# 1. Gửi cho Claude: "Optimal combo là CF1+CF3+CF5.
#    Hãy cập nhật cf_validation_framework.py để dùng combo này"
#
# 2. Claude sẽ update dòng:
#    active_rules = ['CF1','CF2','CF3']  →  ['CF1','CF3','CF5']
#    trong hàm apply_cf_rules()
#
# 3. Upload file mới lên GitHub


# ════════════════════════════════════════════════════════════
# TÍCH HỢP VÀO telegram_bot.py — HƯỚNG DẪN CHI TIẾT
# ════════════════════════════════════════════════════════════
#
# MỞ FILE telegram_bot.py TRÊN GITHUB, CẦN SỬA 3 CHỖ:
#
# ── CHỖ 1: Thêm import ở đầu file (sau các import khác) ──
#
#   try:
#       from cfwf_bot_commands import handle_cfwf
#       logger.info('cfwf_bot_commands loaded OK')
#   except ImportError as e:
#       logger.warning('cfwf_bot_commands not found: ' + str(e))
#       def handle_cfwf(args, chat_id):
#           send('CF WF module chua san sang.', chat_id)
#
# ── CHỖ 2: Thêm command dispatch (tìm dòng /sascreen) ──
#
#   Tìm đoạn này trong polling loop (khoảng line 6784):
#
#       elif cmd == '/sascreen':
#           threading.Thread(
#               target=handle_sascreen, args=(list(parts[1:]), cid),
#               daemon=True).start()
#
#   Thêm VÀO TRƯỚC đoạn đó:
#
#       elif cmd == '/cfwf':
#           threading.Thread(
#               target=handle_cfwf,
#               args=(list(parts[1:]), cid),
#               daemon=True).start()
#
# ── CHỖ 3: Thêm vào /help hoặc command list (tùy chọn) ──
#   Tìm đoạn hiển thị danh sách lệnh, thêm:
#   /cfwf — CF Walk-Forward + Ablation Study
#
#
# ════════════════════════════════════════════════════════════
# CHECKLIST TRIỂN KHAI
# ════════════════════════════════════════════════════════════
#
# □ Bước 0: Mở config.py, verify SYMBOL_CONFIG 10 mã
#           Pass: tiếp tục | Fail: sửa config.py trước
#
# □ Bước 1: Upload lên GitHub (theo thứ tự):
#           □ context_scorecard.py
#           □ cf_walk_forward.py
#           □ cf_validation_framework.py
#           □ cfwf_bot_commands.py     ← file này
#           □ telegram_bot.py          ← sau khi sửa 3 chỗ trên
#           Verify: /signals vẫn hoạt động sau deploy
#
# □ Bước 2: /cfwf ablation
#           Đợi 60-90 phút
#           Đọc Tin 1: verdict?
#           Đọc Tin 2: marginal contribution của từng rule?
#           Ghi lại: suggested_weights + optimal combo + neg_rules
#
# □ Bước 3: (Chỉ nếu verdict [~] hoặc [-])
#           /cfwf sensitivity
#           (sẽ build sau khi có kết quả Bước 2)
#
# □ Bước 4: Gửi output Bước 2 cho Claude
#           → Nhận context_scorecard.py mới với weights data-driven
#           → Upload lên GitHub
#
# □ Bước 4b: Gửi optimal combo cho Claude
#            → Nhận cf_validation_framework.py đã sync
#            → Upload lên GitHub
#
# □ Verify cuối: /cfwf status → xem tóm tắt kết quả
#               /signals → scorecard mới hiển thị đúng không
#               /analyze NKG → full scorecard mới đúng không
