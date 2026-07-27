import os
import time
import json
import threading
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import yfinance as yf
from flask import Flask

app = Flask(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
PORTFOLIO_FILE = "portfolio.json"

# 글로벌 상태 변수
sent_signals_today = {} # {code: {'name': name, 'time': timestamp}}
last_reset_date = None
daily_summary_sent_date = None
morning_briefing_sent_date = None
nxt_open_sent_date = None
reg_open_sent_date = None
reg_close_sent_date = None
nxt_close_sent_date = None
last_update_id = 0

# 전 종목 코드 매핑 딕셔너리
try:
    krx_all = fdr.StockListing('KRX')
    name_to_code = dict(zip(krx_all['Name'], krx_all['Code']))
except Exception as e:
    name_to_code = {}

def load_portfolio():
    if os.path.exists(PORTFOLIO_FILE):
        try:
            with open(PORTFOLIO_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_portfolio(data):
    try:
        with open(PORTFOLIO_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        pass

def send_telegram_msg(message):
    if not TELEGRAM_TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except:
        pass

def get_kst_now():
    return datetime.now(timezone(timedelta(hours=9)))

def calculate_rsi(data, window=14):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_proper_price(row, current_price):
    try:
        bps = float(row['BPS']) if 'BPS' in row and pd.notnull(row['BPS']) else 0
        eps = float(row['EPS']) if 'EPS' in row and pd.notnull(row['EPS']) else 0
        if bps > 0 and eps > 0:
            roe = (eps / bps) * 100.0  
            req_return = 0.08      
            proper_price = bps + (bps * ((roe / 100.0) - req_return) / req_return)
            if proper_price > 0: return int(proper_price)
    except: pass
    return int(current_price * 1.12)

def check_investor_buying(code):
    """주가 하락 시 개인 매도 & 외인/기관 매수 패턴 포착"""
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=5)
        dfs = pd.read_html(res.text, encoding='euc-kr', match='순매매량')
        if not dfs: return True
        df = dfs[0]
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(0)
        df = df.dropna(subset=['날짜']).head(3)
        
        inst_col = [c for c in df.columns if '기관' in c][0]
        fore_col = [c for c in df.columns if '외국인' in c][0]
        retail_col = [c for c in df.columns if '개인' in c][0]
        
        inst_sum = df[inst_col].astype(str).str.replace(r'[^0-9\-]', '', regex=True).replace('', '0').astype(int).sum()
        fore_sum = df[fore_col].astype(str).str.replace(r'[^0-9\-]', '', regex=True).replace('', '0').astype(int).sum()
        retail_sum = df[retail_col].astype(str).str.replace(r'[^0-9\-]', '', regex=True).replace('', '0').astype(int).sum()
        
        return retail_sum < 0 and (inst_sum > 0 or fore_sum > 0)
    except:
        return True 

def check_kosdaq_status():
    """코스닥 20일선 추세 확인"""
    try:
        now_kst = get_kst_now()
        df = fdr.DataReader('KQ11', now_kst - timedelta(days=40), now_kst)
        df['MA20'] = df['Close'].rolling(20).mean()
        curr = df['Close'].iloc[-1]
        ma20 = df['MA20'].iloc[-1]
        return curr >= ma20 
    except: return True

def get_signal_tier(rsi_val, vol_ratio, investor_ok, margin_of_safety, is_market_good):
    """시그널 강도 5단계 세분화 평가 함수"""
    if rsi_val <= 33 and vol_ratio >= 150.0 and investor_ok and margin_of_safety >= 20.0 and is_market_good:
        return "⭐⭐⭐⭐⭐", "강력추천"
    elif rsi_val <= 36 and vol_ratio >= 140.0 and investor_ok:
        return "⭐⭐⭐⭐", "매우좋음"
    elif rsi_val <= 40 and vol_ratio >= 120.0:
        return "⭐⭐⭐", "좋음"
    elif rsi_val <= 43 and vol_ratio >= 110.0:
        return "⭐⭐", "보통"
    elif rsi_val <= 45 and vol_ratio >= 100.0:
        return "⭐", "낮음"
    return None, None

def run_backtest(code, name):
    """과거 1년 백테스팅 엔진"""
    try:
        now = get_kst_now()
        df = fdr.DataReader(code, now - timedelta(days=365), now)
        if len(df) < 50: return "데이터가 부족합니다."
        
        df['RSI'] = calculate_rsi(df)
        df['MA20'] = df['Close'].rolling(20).mean()
        df['Vol_MA20'] = df['Volume'].rolling(20).mean()
        
        trades = []
        buy_price = 0
        max_price = 0
        trailing = False
        
        for i in range(25, len(df)):
            curr_price = df['Close'].iloc[i]
            rsi = df['RSI'].iloc[i-1]
            vol = df['Volume'].iloc[i-1]
            avg_vol = df['Vol_MA20'].iloc[i-2]
            vol_ratio = (vol / avg_vol * 100) if avg_vol > 0 else 0
            
            if buy_price == 0:
                if rsi <= 40 and vol_ratio >= 120:
                    buy_price = curr_price
                    max_price = curr_price
                    trailing = False
            else:
                profit = ((curr_price - buy_price) / buy_price) * 100
                if curr_price > max_price: max_price = curr_price
                
                if not trailing and profit >= 2.0:
                    trailing = True
                
                if trailing:
                    drop = ((max_price - curr_price) / max_price) * 100
                    if drop >= 1.0:
                        trades.append(profit)
                        buy_price = 0
                elif profit <= -3.0:
                    trades.append(profit)
                    buy_price = 0
                    
        if not trades: return f"📊 <b>[{name}] 백테스트 결과</b>\n지난 1년간 해당 로직에 포착된 매수 타점이 없습니다."
        
        win_trades = [t for t in trades if t > 0]
        win_rate = (len(win_trades) / len(trades)) * 100
        avg_return = sum(trades) / len(trades)
        
        roll_max = df['Close'].rolling(min_periods=1, window=252).max()
        daily_drawdown = df['Close']/roll_max - 1.0
        mdd = daily_drawdown.min() * 100
        
        msg = (
            f"📊 <b>[{name}] 1년 백테스트 결과</b>\n"
            f"<i>(로직: 단타/스윙 통합 백테스트)</i>\n\n"
            f"• <b>총 매매 횟수:</b> {len(trades)}회\n"
            f"• <b>승률:</b> {win_rate:.1f}%\n"
            f"• <b>평균 수익률:</b> {avg_return:+.2f}%\n"
            f"• <b>최대 낙폭(MDD):</b> {mdd:.2f}%\n\n"
        )
        if win_rate > 60 and avg_return > 0: msg += "💡 <b>결론:</b> 현재 시장에서 로직이 매우 잘 통하는 종목입니다!"
        else: msg += "⚠️ <b>결론:</b> 이 종목은 현재 로직과 궁합이 맞지 않거나 하락 추세입니다."
        return msg
    except Exception as e:
        return f"백테스트 중 오류 발생: {e}"

def process_telegram_commands():
    global last_update_id
    if not TELEGRAM_TOKEN: return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    try:
        res = requests.get(url, params={"offset": last_update_id, "timeout": 5}, timeout=10).json()
        if not res.get("ok"): return
        
        for item in res["result"]:
            last_update_id = item["update_id"] + 1
            text = item.get("message", {}).get("text", "").strip()
            if not text.startswith("/"): continue
            
            parts = text.split()
            cmd = parts[0]
            portfolio = load_portfolio()
            
            if cmd == "/매수" or cmd == "/수정":
                if len(parts) >= 3:
                    name = parts[1]
                    try:
                        price = int(parts[2].replace(',', ''))
                        trade_type = parts[3] if len(parts) >= 4 and parts[3] in ["단타", "스윙"] else "단타"
                        code = name_to_code.get(name, "000000")
                        
                        portfolio[name] = {
                            "code": code,
                            "price": price, 
                            "type": trade_type,
                            "max_price": price, 
                            "trailing_active": False
                        }
                        save_portfolio(portfolio)
                        send_telegram_msg(
                            f"✅ <b>[{name}] 등록 완료</b>\n"
                            f"• 단가: {price:,}원\n"
                            f"• 성향: <b>{trade_type}</b> 모드\n"
                            f"<i>이 시간부로 매도 감시 및 트레일링 스톱이 작동합니다.</i>"
                        )
                    except: send_telegram_msg("⚠️ 양식 오류: /매수 [종목명] [단가] [단타/스윙]")
            elif cmd == "/매도완료":
                name = parts[1] if len(parts)>1 else ""
                if name in portfolio:
                    del portfolio[name]
                    save_portfolio(portfolio)
                    send_telegram_msg(f"🗑️ <b>[{name}] 감시 종료</b>\n감시 목록에서 성공적으로 제거되었습니다.")
            elif cmd == "/목록":
                if not portfolio: send_telegram_msg("📂 현재 감시 중인 종목이 없습니다.")
                else:
                    msg = "📂 <b>[현재 보유/감시 종목 목록]</b>\n\n"
                    for n, info in portfolio.items():
                        t_type = info.get('type', '단타')
                        status = "🚀트레일링 가동중" if info.get('trailing_active') else "감시대기"
                        msg += f"• <b>{n}</b> ({t_type}): {info.get('price'):,}원 [{status}]\n"
                    send_telegram_msg(msg)
            elif cmd == "/백테스트":
                if len(parts) >= 2:
                    name = parts[1]
                    code = name_to_code.get(name)
                    if code:
                        send_telegram_msg(f"⏳ <b>[{name}]</b> 백테스트 진행 중...")
                        result = run_backtest(code, name)
                        send_telegram_msg(result)
                    else: send_telegram_msg("⚠️ 해당 종목을 찾을 수 없습니다.")
                else: send_telegram_msg("⚠️ 양식: /백테스트 [종목명]")
            elif cmd == "/도움말":
                help_msg = (
                    "🤖 <b>[뽕실로봇 V5 명령어가이드]</b>\n\n"
                    "🔹 <b>/매수 [종목] [단가] [단타/스윙]</b>\n"
                    "  - 예: <code>/매수 삼성전자 80000 단타</code>\n"
                    "  - 예: <code>/매수 SK하이닉스 180000 스윙</code>\n"
                    "  - (단타/스윙 미입력 시 '단타' 기본 적용)\n\n"
                    "🔹 <b>/수정 [종목] [단가] [단타/스윙]</b>\n"
                    "  - 기존 감시 종목의 단가나 투자 성향을 변경합니다.\n\n"
                    "🔹 <b>/매도완료 [종목명]</b>\n"
                    "  - 예: <code>/매도완료 삼성전자</code>\n"
                    "  - 매도가 완료된 종목을 감시 목록에서 삭제합니다.\n\n"
                    "🔹 <b>/목록</b>\n"
                    "  - 현재 감시 중인 종목의 상태를 확인합니다.\n\n"
                    "🔹 <b>/백테스트 [종목명]</b>\n"
                    "  - 과거 1년간 해당 로직의 매매 성과를 검증합니다."
                )
                send_telegram_msg(help_msg)
    except: pass

def monitor_portfolio():
    """(단타/스윙 구분) 자동 매도 모니터링 엔진"""
    portfolio = load_portfolio()
    if not portfolio: return
    now_kst = get_kst_now()
    portfolio_changed = False
    
    for name, info in list(portfolio.items()):
        code = info.get("code")
        buy_price = info.get("price")
        trade_type = info.get("type", "단타")
        max_price = info.get("max_price", buy_price)
        trailing_active = info.get("trailing_active", False)
        
        if not code or code == "000000": continue
        
        try:
            df = fdr.DataReader(code, now_kst - timedelta(days=20), now_kst)
            curr_price = int(df['Close'].iloc[-1])
            profit_rate = ((curr_price - buy_price) / buy_price) * 100.0
            
            if curr_price > max_price:
                info["max_price"] = curr_price
                portfolio_changed = True
            
            # 단타/스윙 기준 세팅
            if trade_type == "단타":
                target_trigger = 2.0   # +2.0% 시 트레일링 가동
                trailing_drop = 1.0    # 최고가 대비 -1.0% 하락 시 익절
                stop_loss = -3.0       # -3.0% 손절
            else: # 스윙
                target_trigger = 5.0   # +5.0% 시 트레일링 가동
                trailing_drop = 2.0    # 최고가 대비 -2.0% 하락 시 익절
                stop_loss = -7.0       # -7.0% 손절
                
            signal_msg = None
            action_guide = f"👉 <i>매도 후 '<code>/매도완료 {name}</code>'을 입력하세요.</i>"
            
            if not trailing_active:
                if profit_rate >= target_trigger: 
                    info["trailing_active"] = True
                    portfolio_changed = True
                    signal_msg = (
                        f"🚀 <b>[{trade_type} 트레일링 스톱 가동]</b>\n"
                        f"목표 수익률(+{target_trigger}%)을 달성했습니다!\n"
                        f"지금부터 최고가 대비 -{trailing_drop}% 밀리면 즉시 매도 알림을 올립니다."
                    )
                elif profit_rate <= stop_loss:
                    signal_msg = (
                        f"🛑 <b>[{trade_type} 손절가 도달 알림]</b>\n"
                        f"손절 기준선({stop_loss}%)에 도달했습니다.\n"
                        f"리스크 관리를 위해 기계적인 손절 매도를 권장합니다!"
                    )
            else:
                drop_rate = ((max_price - curr_price) / max_price) * 100.0
                if drop_rate >= trailing_drop:
                    signal_msg = (
                        f"🎯 <b>[{trade_type} 익절 신호 발생!]</b>\n"
                        f"달성 최고가({max_price:,}원) 대비 -{drop_rate:.1f}% 하락했습니다.\n"
                        f"<b>지금 시장가로 수익을 확정(매도)하세요!</b>"
                    )
                    
            if signal_msg:
                msg = (
                    f"🚨 <b>[{name} 매도 시그널 - {trade_type}]</b>\n\n"
                    f"{signal_msg}\n\n"
                    f"💰 매수단가: {buy_price:,}원\n"
                    f"💲 현재가격: {curr_price:,}원\n"
                    f"📈 현재수익률: <b>{profit_rate:+.2f}%</b>\n\n"
                    f"{action_guide}"
                )
                send_telegram_msg(msg)
                time.sleep(1)
        except: continue
        
    if portfolio_changed: save_portfolio(portfolio)

def send_morning_briefing():
    """장 시작 전 미국 증시 브리핑"""
    now_kst = get_kst_now()
    date_str = now_kst.strftime("%Y-%m-%d")
    
    tickers = {
        '나스닥 종합지수': '^IXIC',
        '필라델피아 반도체': '^SOX',
        '엔비디아 (반도체)': 'NVDA',
        '테슬라 (2차전지)': 'TSLA',
        '애플 (IT/모바일)': 'AAPL'
    }
    
    results = {}
    for name, ticker in tickers.items():
        try:
            hist = yf.Ticker(ticker).history(period="5d")
            if len(hist) >= 2:
                prev = hist['Close'].iloc[-2]
                curr = hist['Close'].iloc[-1]
                change = ((curr - prev) / prev) * 100
                results[name] = change
            else: results[name] = 0.0
        except: results[name] = 0.0
            
    msg = f"🌅 <b>[뽕실로봇] {date_str} 장전 모닝 브리핑</b>\n\n🇺🇸 <b>[밤사이 미국 증시 마감]</b>\n"
    for name, chg in results.items():
        icon = "🔴" if chg < 0 else "🟢"
        sign = "+" if chg > 0 else ""
        msg += f"{icon} {name}: {sign}{chg:.2f}%\n"
        
    msg += "\n💡 <b>[오늘의 장초반 섹터 전망]</b>\n"
    nvda_chg = results.get('엔비디아 (반도체)', 0)
    if nvda_chg >= 1.5: msg += "📈 <b>반도체 (상승 예상)</b>: 삼성전자, SK하이닉스, 한미반도체\n"
    elif nvda_chg <= -1.5: msg += "📉 <b>반도체 (하락 유의)</b>: 삼성전자, SK하이닉스, 한미반도체\n"
    else: msg += "➖ <b>반도체 (보합 예상)</b>: 개별 장세 예상\n"
        
    tsla_chg = results.get('테슬라 (2차전지)', 0)
    if tsla_chg >= 1.5: msg += "📈 <b>2차전지 (상승 예상)</b>: 에코프로, LG에너지솔루션\n"
    elif tsla_chg <= -1.5: msg += "📉 <b>2차전지 (하락 유의)</b>: 에코프로, LG에너지솔루션\n"
    else: msg += "➖ <b>2차전지 (보합 예상)</b>: 개별 장세 예상\n"
        
    send_telegram_msg(msg)

def send_daily_closing_report():
    """장 마감 종합 브리핑"""
    global sent_signals_today
    now_kst = get_kst_now()
    date_str = now_kst.strftime("%Y-%m-%d")
    
    if not sent_signals_today:
        msg = f"📋 <b>[뽕실로봇] {date_str} 장 마감 브리핑</b>\n\n오늘 장 중 포착된 시그널 종목이 없습니다."
    else:
        stocks_list = "\n".join([f"• {info['name']}" for code, info in sent_signals_today.items()])
        msg = f"📋 <b>[뽕실로봇] {date_str} 장 마감 브리핑</b>\n\n오늘 총 <b>{len(sent_signals_today)}개</b>의 시그널이 포착되었습니다!\n\n<b>[포착 종목 리스트]</b>\n{stocks_list}"
    send_telegram_msg(msg)

def scan_stocks():
    global sent_signals_today, last_reset_date
    now_kst = get_kst_now()
    today_str = now_kst.strftime("%Y-%m-%d")
    
    if last_reset_date != today_str:
        sent_signals_today.clear()
        last_reset_date = today_str

    is_market_good = check_kosdaq_status() 
    
    try:
        krx = fdr.StockListing('KRX')
        top_stocks = krx.sort_values(by='Marcap', ascending=False).head(200)
        
        for idx, row in top_stocks.iterrows():
            code = row['Code']
            name = row['Name']
            
            # 1시간(3600초) 쿨타임 체크
            if code in sent_signals_today:
                if (now_kst - sent_signals_today[code]['time']).total_seconds() < 3600:
                    continue
            
            df = fdr.DataReader(code, now_kst - timedelta(days=60), now_kst)
            if len(df) < 20: continue
            
            df['RSI'] = calculate_rsi(df)
            df['Vol_MA20'] = df['Volume'].rolling(20).mean() 
            
            current_price = int(df['Close'].iloc[-1])
            rsi_val = df['RSI'].iloc[-1]
            current_vol = df['Volume'].iloc[-1]
            avg_vol = df['Vol_MA20'].iloc[-2] if not pd.isna(df['Vol_MA20'].iloc[-2]) and df['Vol_MA20'].iloc[-2] > 0 else 1
            vol_ratio = (current_vol / avg_vol) * 100.0
            
            # 1차 완화 조건 (RSI 45 이하 & 거래량 100% 이상)
            if rsi_val <= 45.0 and vol_ratio >= 100.0:
                investor_ok = check_investor_buying(code)
                proper_price = calculate_proper_price(row, current_price)
                margin_of_safety = ((proper_price - current_price) / proper_price) * 100.0
                
                # 5단계 세분화 평가
                stars, tier_label = get_signal_tier(rsi_val, vol_ratio, investor_ok, margin_of_safety, is_market_good)
                if not stars: continue # 등급 외 제외
                
                msg = (
                    f"🚨 <b>[V5 시그널 포착!]</b> {stars}\n"
                    f"<b>[강도: {tier_label}]</b>\n\n"
                    f"📌 <b>종목명:</b> {name} ({code})\n"
                    f"💰 <b>현재가:</b> {current_price:,}원\n"
                    f"💎 <b>적정주가(S-RIM):</b> {proper_price:,}원 (안전마진 {margin_of_safety:+.1f}%)\n\n"
                    f"📊 <b>[포착 근거]</b>\n"
                    f"• <b>RSI:</b> {rsi_val:.1f} (바닥권 감시)\n"
                    f"• <b>거래량:</b> 평소 대비 {vol_ratio:.1f}%\n"
                    f"• <b>수급:</b> {'개미털기(외인기관매수)' if investor_ok else '보통'}\n\n"
                    f"💡 <b>추천 명령어:</b>\n"
                    f"• 단타 매수: <code>/매수 {name} {current_price} 단타</code>\n"
                    f"• 스윙 매수: <code>/매수 {name} {current_price} 스윙</code>"
                )
                send_telegram_msg(msg)
                sent_signals_today[code] = {'name': name, 'time': now_kst}
                time.sleep(1)
    except: pass

def run_scanner():
    global morning_briefing_sent_date, daily_summary_sent_date
    global nxt_open_sent_date, reg_open_sent_date, reg_close_sent_date, nxt_close_sent_date
    tick_count = 0
    
    while True:
        try:
            now = get_kst_now()
            today_str = now.strftime("%Y-%m-%d")
            
            process_telegram_commands()
            
            if now.weekday() < 5: 
                # 미국 증시 브리핑 (오전 07:30 ~ 07:35 사이 1회)
                if now.hour == 7 and 30 <= now.minute < 35 and morning_briefing_sent_date != today_str:
                    send_morning_briefing()
                    morning_briefing_sent_date = today_str

                # NXT 장 시작 (08:00)
                if now.hour == 8 and 0 <= now.minute < 5 and nxt_open_sent_date != today_str:
                    send_telegram_msg("🔔 <b>[NXT 장 시작]</b>\n대체거래소(NXT) 프리마켓 거래가 시작되었습니다.")
                    nxt_open_sent_date = today_str

                # 정규장 시작 (09:00)
                if now.hour == 9 and 0 <= now.minute < 5 and reg_open_sent_date != today_str:
                    send_telegram_msg("🔔 <b>[정규장 시작]</b>\n정규장 거래가 시작되었습니다. 시그널 스캔을 가동합니다.")
                    reg_open_sent_date = today_str

                # 스캔 타임 (08:00 ~ 20:00)
                if 8 <= now.hour < 20:
                    scan_stocks()
                    if tick_count % 15 == 0: monitor_portfolio()
                    
                # 정규장 마감 (15:30)
                if now.hour == 15 and 30 <= now.minute < 35 and reg_close_sent_date != today_str:
                    send_telegram_msg("🔔 <b>[정규장 마감]</b>\n정규장 거래가 종료되었습니다.")
                    reg_close_sent_date = today_str

                # 마감 브리핑 (15:35)
                if now.hour == 15 and 35 <= now.minute < 40 and daily_summary_sent_date != today_str:
                    send_daily_closing_report()
                    daily_summary_sent_date = today_str
                    
                # NXT 마감 (20:00)
                if now.hour == 20 and 0 <= now.minute < 5 and nxt_close_sent_date != today_str:
                    send_telegram_msg("🔔 <b>[NXT 장 마감]</b>\n대체거래소 애프터마켓 거래가 모두 종료되었습니다.")
                    nxt_close_sent_date = today_str
                    
            tick_count += 1
        except: pass
        time.sleep(15) 

@app.route('/')
def health_check():
    return "뽕실로봇 V5 (5단계 별점 & 단타/스윙 매도 시스템) 정상 작동 중입니다.", 200

if __name__ == "__main__":
    t = threading.Thread(target=run_scanner)
    t.daemon = True
    t.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
