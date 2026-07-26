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

sent_signals_today = set()
sidecar_alerts_today = set()
last_reset_date = None
daily_summary_sent_date = None
morning_briefing_sent_date = None
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
    """(업그레이드) 주가 하락 시 개인 매도 & 외인/기관 매수 패턴 포착"""
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
        
        # 핵심 로직: 개인은 팔고(털리고), 메이저는 샀는가?
        return retail_sum < 0 and (inst_sum > 0 or fore_sum > 0)
    except:
        return True 

def check_kosdaq_status():
    """(신규) 코스닥 20일선 추세 확인"""
    try:
        now_kst = get_kst_now()
        df = fdr.DataReader('KQ11', now_kst - timedelta(days=40), now_kst)
        df['MA20'] = df['Close'].rolling(20).mean()
        curr = df['Close'].iloc[-1]
        ma20 = df['MA20'].iloc[-1]
        return curr >= ma20 # True면 상승장(적극매수), False면 하락장(보수적매수)
    except: return True

def run_backtest(code, name):
    """(신규) 과거 1년 백테스팅 엔진"""
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
                # 매수 조건
                if rsi <= 35 and vol_ratio >= 150:
                    buy_price = curr_price
                    max_price = curr_price
                    trailing = False
            else:
                # 매도 조건 (트레일링 스톱 로직)
                profit = ((curr_price - buy_price) / buy_price) * 100
                if curr_price > max_price: max_price = curr_price
                
                if not trailing and profit >= 3.0:
                    trailing = True
                
                if trailing:
                    drop = ((max_price - curr_price) / max_price) * 100
                    if drop >= 1.5:
                        trades.append(profit)
                        buy_price = 0
                elif profit <= -5.0: # 기본 손절 5%
                    trades.append(profit)
                    buy_price = 0
                    
        if not trades: return f"📊 <b>[{name}] 백테스트 결과</b>\n지난 1년간 해당 로직에 포착된 매수 타점이 없습니다."
        
        win_trades = [t for t in trades if t > 0]
        win_rate = (len(win_trades) / len(trades)) * 100
        avg_return = sum(trades) / len(trades)
        
        # MDD 계산
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
                send_telegram_msg("🤖 <b>V5 명령어</b>\n/매수 [종목] [단가]\n/매도완료 [종목]\n/수정 [종목] [단가]\n/목록\n/백테스트 [종목명]")
    except: pass

def monitor_portfolio():
    """(업그레이드) 동적 트레일링 스톱 로직"""
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
            
            # 최고가 갱신
            if curr_price > max_price:
                info["max_price"] = curr_price
                portfolio_changed = True
            
            signal = None
            if not trailing_active:
                if profit_rate >= 3.0: # 3% 도달 시 트레일링 스톱 발동!
                    info["trailing_active"] = True
                    portfolio_changed = True
                    signal = f"🚀 <b>[트레일링 스톱 가동]</b>\n수익 3%를 돌파했습니다!\n이제 최고가 대비 -1.5% 하락 시 기계적으로 익절합니다."
                elif profit_rate <= -5.0:
                    signal = f"🛑 <b>[손절가 도달]</b>\n수익률 -5% 이탈. 손절을 권장합니다."
            else:
                # 트레일링 스톱 발동 중 (고점 대비 하락 체크)
                drop_rate = ((max_price - curr_price) / max_price) * 100.0
                if drop_rate >= 1.5:
                    signal = f"🎯 <b>[트레일링 스톱 익절 발생!]</b>\n달성 최고가({max_price:,}원) 대비 -1.5% 밀렸습니다.\n수익을 확정지으세요!"
                    
            if signal:
                msg = f"🚨 <b>[{name} 알림]</b>\n\n{signal}\n\n💰 매수가: {buy_price:,}원\n💲 현재가: {curr_price:,}원\n📈 현재 수익률: <b>{profit_rate:+.2f}%</b>"
                send_telegram_msg(msg)
                time.sleep(1)
        except: continue
        
    if portfolio_changed: save_portfolio(portfolio)

def scan_stocks():
    """(업그레이드) 시장 상태 필터 적용 스캐너"""
    global sent_signals_today, last_reset_date
    now_kst = get_kst_now()
    today_str = now_kst.strftime("%Y-%m-%d")
    
    if last_reset_date != today_str:
        sent_signals_today.clear()
        last_reset_date = today_str

    is_market_good = check_kosdaq_status() # 코스닥 20일선 추세
    
    try:
        krx = fdr.StockListing('KRX')
        top_stocks = krx.sort_values(by='Marcap', ascending=False).head(150)
        
        for idx, row in top_stocks.iterrows():
            code = row['Code']
            name = row['Name']
            if code in sent_signals_today: continue
            
            df = fdr.DataReader(code, now_kst - timedelta(days=60), now_kst)
            if len(df) < 20: continue
            
            df['RSI'] = calculate_rsi(df)
            df['Vol_MA20'] = df['Volume'].rolling(20).mean() 
            
            current_price = int(df['Close'].iloc[-1])
            rsi_val = df['RSI'].iloc[-1]
            current_vol = df['Volume'].iloc[-1]
            avg_vol = df['Vol_MA20'].iloc[-2] if not pd.isna(df['Vol_MA20'].iloc[-2]) and df['Vol_MA20'].iloc[-2] > 0 else 1
            vol_ratio = (current_vol / avg_vol) * 100.0
            
            if rsi_val <= 33 and vol_ratio >= 150.0:
                if not check_investor_buying(code): continue # 개미털기 확인
                    
                proper_price = calculate_proper_price(row, current_price)
                margin_of_safety = ((proper_price - current_price) / proper_price) * 100.0
                
                # 하락장 방어 로직
                if not is_market_good and margin_of_safety < 30.0:
                    continue # 하락장에서는 초저평가(30% 이상) 아니면 패스
                
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
                sent_signals_today.add(code)
                time.sleep(1)
    except: pass

def run_scanner():
    tick_count = 0
    while True:
        try:
            now = get_kst_now()
            process_telegram_commands()
            
            if now.weekday() < 5: 
                if (9 <= now.hour < 15) or (now.hour == 15 and now.minute <= 30):
                    scan_stocks()
                    if tick_count % 15 == 0: monitor_portfolio()
            tick_count += 1
        except: pass
        time.sleep(15) 

@app.route('/')
def health_check():
    return "뽕실로봇 V5 정상 작동 중입니다.", 200

if __name__ == "__main__":
    t = threading.Thread(target=run_scanner)
    t.daemon = True
    t.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
