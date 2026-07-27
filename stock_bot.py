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
sent_signals_today = {} # (변경) 중복 알림 쿨타임 계산을 위해 딕셔너리로 변경
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
                if rsi <= 35 and vol_ratio >= 150:
                    buy_price = curr_price
                    max_price = curr_price
                    trailing = False
            else:
                profit = ((curr_price - buy_price) / buy_price) * 100
                if curr_price > max_price: max_price = curr_price
                
                if not trailing and profit >= 3.0:
                    trailing = True
                
                if trailing:
                    drop = ((max_price - curr_price) / max_price) * 100
                    if drop >= 1.5:
                        trades.append(profit)
                        buy_price = 0
                elif profit <= -5.0:
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
            f"<i>(로직: RSI바닥 + 거래량폭발 + 3%익절/동적트레일링)</i>\n\n"
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
                        price = int(parts[-1].replace(',', ''))
                        code = name_to_code.get(name, "000000")
                        portfolio[name] = {
                            "code": code, "price": price, 
                            "max_price": price, "trailing_active": False
                        }
                        save_portfolio(portfolio)
                        send_telegram_msg(f"✅ <b>[{name}] 등록 완료</b>\n단가: {price:,}원\n감시 및 트레일링 스톱을 시작합니다.")
                    except: send_telegram_msg("⚠️ 단가는 숫자로 입력해주세요.")
            elif cmd == "/매도완료":
                name = parts[1] if len(parts)>1 else ""
                if name in portfolio:
                    del portfolio[name]
                    save_portfolio(portfolio)
                    send_telegram_msg(f"🗑️ <b>[{name}] 삭제 완료</b>")
            elif cmd == "/목록":
                if not portfolio: send_telegram_msg("📂 감시 중인 종목이 없습니다.")
                else:
                    msg = "📂 <b>[현재 감시 종목]</b>\n"
                    for n, info in portfolio.items():
                        status = "🚀트레일링 중" if info.get('trailing_active') else "대기중"
                        msg += f"• <b>{n}</b> : {info.get('price'):,}원 ({status})\n"
                    send_telegram_msg(msg)
            elif cmd == "/백테스트":
                if len(parts) >= 2:
                    name = parts[1]
                    code = name_to_code.get(name)
                    if code:
                        send_telegram_msg(f"⏳ <b>[{name}]</b> 백테스트 엔진 가동 중... (약 5초 소요)")
                        result = run_backtest(code, name)
                        send_telegram_msg(result)
                    else: send_telegram_msg("⚠️ 종목을 찾을 수 없습니다.")
                else: send_telegram_msg("⚠️ 양식: /백테스트 [종목명]")
            elif cmd == "/도움말":
                help_msg = (
                    "🤖 <b>[뽕실로봇 V5 명령어 가이드]</b>\n\n"
                    "🔹 <b>/매수 [종목명] [매수가]</b>\n"
                    "  - 예: /매수 삼성전자 80000\n"
                    "  - 로봇의 감시 목록에 추가하고 즉시 트레일링 스톱을 가동합니다.\n\n"
                    "🔹 <b>/수정 [종목명] [매수가]</b>\n"
                    "  - 예: /수정 삼성전자 79000\n"
                    "  - 기존에 감시 중인 종목의 매수 평단가를 수정합니다.\n\n"
                    "🔹 <b>/매도완료 [종목명]</b>\n"
                    "  - 예: /매도완료 삼성전자\n"
                    "  - 익절/손절이 완료된 종목을 감시 목록에서 삭제합니다.\n\n"
                    "🔹 <b>/목록</b>\n"
                    "  - 현재 로봇이 감시 중인 종목 리스트와 진행 상태(트레일링)를 확인합니다.\n\n"
                    "🔹 <b>/백테스트 [종목명]</b>\n"
                    "  - 예: /백테스트 SK하이닉스\n"
                    "  - 해당 종목의 과거 1년간 뽕실로봇 매매 시그널 결과를 시뮬레이션하여 승률과 수익률을 알려줍니다."
                )
                send_telegram_msg(help_msg)
    except: pass

def monitor_portfolio():
    """동적 트레일링 스톱 로직"""
    portfolio = load_portfolio()
    if not portfolio: return
    now_kst = get_kst_now()
    portfolio_changed = False
    
    for name, info in list(portfolio.items()):
        code = info.get("code")
        buy_price = info.get("price")
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
            
            signal = None
            if not trailing_active:
                if profit_rate >= 3.0: 
                    info["trailing_active"] = True
                    portfolio_changed = True
                    signal = f"🚀 <b>[트레일링 스톱 가동]</b>\n수익 3%를 돌파했습니다!\n이제 최고가 대비 -1.5% 하락 시 기계적으로 익절합니다."
                elif profit_rate <= -5.0:
                    signal = f"🛑 <b>[손절가 도달]</b>\n수익률 -5% 이탈. 손절을 권장합니다."
            else:
                drop_rate = ((max_price - curr_price) / max_price) * 100.0
                if drop_rate >= 1.5:
                    signal = f"🎯 <b>[트레일링 스톱 익절 발생!]</b>\n달성 최고가({max_price:,}원) 대비 -1.5% 밀렸습니다.\n수익을 확정지으세요!"
                    
            if signal:
                msg = f"🚨 <b>[{name} 알림]</b>\n\n{signal}\n\n💰 매수가: {buy_price:,}원\n💲 현재가: {curr_price:,}원\n📈 현재 수익률: <b>{profit_rate:+.2f}%</b>"
                send_telegram_msg(msg)
                time.sleep(1)
        except: continue
        
    if portfolio_changed: save_portfolio(portfolio)

def send_morning_briefing():
    """장 시작 전 미국 증시 요약 및 섹터 전망 브리핑"""
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
            else:
                results[name] = 0.0
        except:
            results[name] = 0.0
            
    msg = f"🌅 <b>[뽕실로봇] {date_str} 장전 모닝 브리핑</b>\n\n🇺🇸 <b>[밤사이 미국 증시 마감]</b>\n"
    
    for name, chg in results.items():
        icon = "🔴" if chg < 0 else "🟢"
        sign = "+" if chg > 0 else ""
        msg += f"{icon} {name}: {sign}{chg:.2f}%\n"
        
    msg += "\n💡 <b>[오늘의 장초반 국내 섹터 전망]</b>\n"
    
    nvda_chg = results.get('엔비디아 (반도체)', 0)
    if nvda_chg >= 1.5: msg += "📈 <b>반도체 (상승 예상)</b>: 삼성전자, SK하이닉스, 한미반도체\n"
    elif nvda_chg <= -1.5: msg += "📉 <b>반도체 (하락 유의)</b>: 삼성전자, SK하이닉스, 한미반도체\n"
    else: msg += "➖ <b>반도체 (보합 예상)</b>: 미국 반도체 변동폭 미미, 개별 장세 예상\n"
        
    tsla_chg = results.get('테슬라 (2차전지)', 0)
    if tsla_chg >= 1.5: msg += "📈 <b>2차전지 (상승 예상)</b>: 에코프로, LG에너지솔루션, 포스코퓨처엠\n"
    elif tsla_chg <= -1.5: msg += "📉 <b>2차전지 (하락 유의)</b>: 에코프로, LG에너지솔루션, 포스코퓨처엠\n"
    else: msg += "➖ <b>2차전지 (보합 예상)</b>: 미국 테슬라 변동폭 미미, 개별 장세 예상\n"
        
    aapl_chg = results.get('애플 (IT/모바일)', 0)
    if aapl_chg >= 1.0: msg += "📈 <b>IT/부품 (상승 예상)</b>: LG이노텍, 비에이치, 삼성전기\n"
    elif aapl_chg <= -1.0: msg += "📉 <b>IT/부품 (하락 유의)</b>: LG이노텍, 비에이치, 삼성전기\n"
        
    msg += "\n⚠️ <i>위 전망은 글로벌 동조화 현상에 기반한 기계적 예측입니다.</i>"
    send_telegram_msg(msg)

def send_daily_closing_report():
    """(변경) 장 마감 종합 브리핑 딕셔너리 호환 적용"""
    global sent_signals_today
    now_kst = get_kst_now()
    date_str = now_kst.strftime("%Y-%m-%d")
    
    if not sent_signals_today:
        msg = f"📋 <b>[뽕실로봇] {date_str} 장 마감 종합 브리핑</b>\n\n오늘 장 운영 시간 동안 포착된 시그널 종목이 없습니다."
    else:
        stocks_list = "\n".join([f"• {info['name']}" for code, info in sent_signals_today.items()])
        msg = f"📋 <b>[뽕실로봇] {date_str} 장 마감 종합 브리핑</b>\n\n오늘 총 <b>{len(sent_signals_today)}개</b>의 종목 시그널이 포착되었습니다!\n\n<b>[포착된 종목 리스트]</b>\n{stocks_list}\n\n💡 <i>내일 장에서 성공적인 투자 되시길 바랍니다!</i>"
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
        # [변경] 검색 대상을 시총 상위 200개로 조정
        top_stocks = krx.sort_values(by='Marcap', ascending=False).head(200)
        
        for idx, row in top_stocks.iterrows():
            code = row['Code']
            name = row['Name']
            
            # [변경] 중복 알림 허용하되, 1시간(3600초) 쿨타임 적용 방어 로직
            if code in sent_signals_today:
                last_time = sent_signals_today[code]['time']
                if (now_kst - last_time).total_seconds() < 3600:
                    continue # 1시간 이내면 패스
            
            df = fdr.DataReader(code, now_kst - timedelta(days=60), now_kst)
            if len(df) < 20: continue
            
            df['RSI'] = calculate_rsi(df)
            df['Vol_MA20'] = df['Volume'].rolling(20).mean() 
            
            current_price = int(df['Close'].iloc[-1])
            rsi_val = df['RSI'].iloc[-1]
            current_vol = df['Volume'].iloc[-1]
            avg_vol = df['Vol_MA20'].iloc[-2] if not pd.isna(df['Vol_MA20'].iloc[-2]) and df['Vol_MA20'].iloc[-2] > 0 else 1
            vol_ratio = (current_vol / avg_vol) * 100.0
            
            if rsi_val <= 35 and vol_ratio >= 150.0:
                if not check_investor_buying(code): continue 
                    
                proper_price = calculate_proper_price(row, current_price)
                margin_of_safety = ((proper_price - current_price) / proper_price) * 100.0
                
                if not is_market_good and margin_of_safety < 30.0:
                    continue 
                
                msg = (
                    f"🚨 <b>[V5 시그널 포착!]</b> ⭐⭐⭐\n\n"
                    f"📌 <b>종목명:</b> {name} ({code})\n"
                    f"💰 <b>현재가:</b> {current_price:,}원\n"
                    f"💎 <b>적정주가(S-RIM):</b> {proper_price:,}원 (안전마진 {margin_of_safety:+.1f}%)\n\n"
                    f"📊 <b>[포착 상세 근거]</b>\n"
                    f"• <b>RSI:</b> {rsi_val:.1f} (바닥권)\n"
                    f"• <b>거래량:</b> 평소 대비 {vol_ratio:.1f}% 폭발\n"
                    f"• <b>수급:</b> 개미털기(개인매도/외인기관매수) 확인\n"
                    f"• <b>시장:</b> {'상승장(적극매수)' if is_market_good else '하락장(초저평가 방어매수)'}\n\n"
                    f"💡 <i>명령어: /매수 {name} {current_price}</i>"
                )
                send_telegram_msg(msg)
                
                # [변경] 딕셔너리에 종목명과 현재 전송된 시간 저장 (쿨타임 리셋 및 장마감 브리핑용)
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
                if now.hour == 7 and 30 <= now.minute < 35 and morning_briefing_sent_date != today_str:
                    send_morning_briefing()
                    morning_briefing_sent_date = today_str

                if now.hour == 8 and 0 <= now.minute < 5 and nxt_open_sent_date != today_str:
                    send_telegram_msg("🔔 <b>[NXT 장 시작]</b>\n오전 8시입니다. 대체거래소(NXT) 프리마켓 거래가 시작되었습니다.")
                    nxt_open_sent_date = today_str

                if now.hour == 9 and 0 <= now.minute < 5 and reg_open_sent_date != today_str:
                    send_telegram_msg("🔔 <b>[정규장 시작]</b>\n오전 9시입니다. 정규장 거래가 시작되었습니다. 종목 감시를 본격적으로 가동합니다.")
                    reg_open_sent_date = today_str

                if 8 <= now.hour < 20:
                    scan_stocks()
                    if tick_count % 15 == 0: monitor_portfolio()
                    
                if now.hour == 15 and 30 <= now.minute < 35 and reg_close_sent_date != today_str:
                    send_telegram_msg("🔔 <b>[정규장 마감]</b>\n오후 3시 30분입니다. 정규장 거래가 종료되었습니다. (NXT 애프터마켓은 계속 진행됩니다)")
                    reg_close_sent_date = today_str

                if now.hour == 15 and 35 <= now.minute < 40 and daily_summary_sent_date != today_str:
                    send_daily_closing_report()
                    daily_summary_sent_date = today_str
                    
                if now.hour == 20 and 0 <= now.minute < 5 and nxt_close_sent_date != today_str:
                    send_telegram_msg("🔔 <b>[NXT 장 마감]</b>\n오후 8시입니다. 대체거래소(NXT) 애프터마켓 거래가 모두 종료되었습니다. 편안한 밤 되세요!")
                    nxt_close_sent_date = today_str
                    
            tick_count += 1
        except: pass
        time.sleep(15) 

@app.route('/')
def health_check():
    return "뽕실로봇 V5 (200개 스캔 / 중복 알림 허용) 정상 작동 중입니다.", 200

if __name__ == "__main__":
    t = threading.Thread(target=run_scanner)
    t.daemon = True
    t.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
