import os
import time
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

sent_signals_today = set()
sidecar_alerts_today = set()
last_reset_date = None
daily_summary_sent_date = None
morning_briefing_sent_date = None  # 모닝 브리핑 중복 방지용 변수 추가

def send_telegram_msg(message):
    """텔레그램 메시지 전송"""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"텔레그램 전송 오류: {e}")

def get_kst_now():
    """한국 표준시(KST) 반환"""
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst)

def calculate_rsi(data, window=14):
    """RSI 계산"""
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_proper_price(row, current_price):
    """S-RIM 기반 적정주가 계산"""
    try:
        bps = float(row['BPS']) if 'BPS' in row and pd.notnull(row['BPS']) else 0
        eps = float(row['EPS']) if 'EPS' in row and pd.notnull(row['EPS']) else 0
        
        if bps > 0 and eps > 0:
            roe = (eps / bps) * 100.0  
            required_return = 0.08      
            roe_decimal = roe / 100.0
            proper_price = bps + (bps * (roe_decimal - required_return) / required_return)
            
            if proper_price > 0:
                return int(proper_price)
                
        pbr = float(row['PBR']) if 'PBR' in row and pd.notnull(row['PBR']) else 0
        if pbr > 0 and bps > 0:
            return int(bps * 1.2)
            
    except Exception as e:
        pass
    return int(current_price * 1.12)

def check_investor_buying(code):
    """조건 2: 최근 3거래일 기관 또는 외국인 순매수(수급) 유입 확인"""
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        res = requests.get(url, headers=headers)
        
        dfs = pd.read_html(res.text, encoding='euc-kr', match='순매매량')
        if not dfs:
            return True
            
        df = dfs[0]
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(0)
            
        df = df.dropna(subset=['날짜']).head(3)
        inst_col = [c for c in df.columns if '기관' in c][0]
        fore_col = [c for c in df.columns if '외국인' in c][0]
        
        inst_sum = df[inst_col].astype(str).str.replace(r'[^0-9\-]', '', regex=True).replace('', '0').astype(int).sum()
        fore_sum = df[fore_col].astype(str).str.replace(r'[^0-9\-]', '', regex=True).replace('', '0').astype(int).sum()
        
        return inst_sum > 0 or fore_sum > 0
    except Exception as e:
        return True 

def check_sidecar():
    """KOSPI / KOSDAQ 선물 등락률 감시"""
    global sidecar_alerts_today
    now_kst = get_kst_now()
    capture_time = now_kst.strftime("%Y-%m-%d %H:%M:%S")

    try:
        df_kospi = fdr.DataReader('KS11', now_kst - timedelta(days=5), now_kst)
        if len(df_kospi) >= 2:
            change_rate = ((df_kospi['Close'].iloc[-1] - df_kospi['Close'].iloc[-2]) / df_kospi['Close'].iloc[-2]) * 100
            if change_rate <= -5.0 and "KOSPI_SELL" not in sidecar_alerts_today:
                send_telegram_msg(f"🚨 <b>[시장 속보] KOSPI 매도 사이드카 발동!</b>\n\n📉 KOSPI 변동률: {change_rate:.2f}%\n⚠️ <i>프로그램 매매 호가가 5분간 정지됩니다.</i>")
                sidecar_alerts_today.add("KOSPI_SELL")
            elif change_rate >= 5.0 and "KOSPI_BUY" not in sidecar_alerts_today:
                send_telegram_msg(f"🚀 <b>[시장 속보] KOSPI 매수 사이드카 발동!</b>\n\n📈 KOSPI 변동률: +{change_rate:.2f}%")
                sidecar_alerts_today.add("KOSPI_BUY")
    except Exception as e:
        pass

def send_morning_briefing():
    """장 시작 전 미국 증시 요약 및 섹터 전망 브리핑 (07:30 전송)"""
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
        except Exception:
            results[name] = 0.0
            
    # 브리핑 메시지 조합
    msg = f"🌅 <b>[뽕실로봇] {date_str} 장전 모닝 브리핑</b>\n\n"
    msg += "🇺🇸 <b>[밤사이 미국 증시 마감]</b>\n"
    
    for name, chg in results.items():
        icon = "🔴" if chg < 0 else "🟢"
        sign = "+" if chg > 0 else ""
        msg += f"{icon} {name}: {sign}{chg:.2f}%\n"
        
    msg += "\n💡 <b>[오늘의 장초반 국내 섹터 전망]</b>\n"
    
    # 1. 반도체 섹터 전망 로직
    nvda_chg = results.get('엔비디아 (반도체)', 0)
    if nvda_chg >= 1.5:
        msg += "📈 <b>반도체 (상승 예상)</b>: 삼성전자, SK하이닉스, 한미반도체\n"
    elif nvda_chg <= -1.5:
        msg += "📉 <b>반도체 (하락 유의)</b>: 삼성전자, SK하이닉스, 한미반도체\n"
    else:
        msg += "➖ <b>반도체 (보합 예상)</b>: 미국 반도체 변동폭 미미, 개별 장세 예상\n"
        
    # 2. 2차전지 섹터 전망 로직
    tsla_chg = results.get('테슬라 (2차전지)', 0)
    if tsla_chg >= 1.5:
        msg += "📈 <b>2차전지 (상승 예상)</b>: 에코프로, LG에너지솔루션, 포스코퓨처엠\n"
    elif tsla_chg <= -1.5:
        msg += "📉 <b>2차전지 (하락 유의)</b>: 에코프로, LG에너지솔루션, 포스코퓨처엠\n"
    else:
        msg += "➖ <b>2차전지 (보합 예상)</b>: 미국 테슬라 변동폭 미미, 개별 장세 예상\n"
        
    # 3. IT/모바일 섹터 전망 로직
    aapl_chg = results.get('애플 (IT/모바일)', 0)
    if aapl_chg >= 1.0:
        msg += "📈 <b>IT/부품 (상승 예상)</b>: LG이노텍, 비에이치, 삼성전기\n"
    elif aapl_chg <= -1.0:
        msg += "📉 <b>IT/부품 (하락 유의)</b>: LG이노텍, 비에이치, 삼성전기\n"
        
    msg += "\n⚠️ <i>위 전망은 글로벌 동조화 현상에 기반한 기계적 예측입니다. 실제 개장 후 외국인/기관 수급에 따라 흐름이 달라질 수 있습니다.</i>"
    
    send_telegram_msg(msg)

def send_daily_closing_report():
    """장 마감 종합 브리핑"""
    global sent_signals_today
    now_kst = get_kst_now()
    date_str = now_kst.strftime("%Y-%m-%d")
    
    if not sent_signals_today:
        msg = f"📋 <b>[뽕실로봇] {date_str} 장 마감 종합 브리핑</b>\n\n오늘 장 운영 시간 동안 포착된 시그널 종목이 없습니다."
    else:
        stocks_list = "\n".join([f"• {item}" for item in sent_signals_today])
        msg = f"📋 <b>[뽕실로봇] {date_str} 장 마감 종합 브리핑</b>\n\n오늘 총 <b>{len(sent_signals_today)}개</b>의 종목 시그널이 포착되었습니다!\n\n<b>[포착된 종목 리스트]</b>\n{stocks_list}\n\n💡 <i>내일 장에서 성공적인 투자 되시길 바랍니다!</i>"
    send_telegram_msg(msg)

def scan_stocks():
    """조건 검색 및 텔레그램 전송"""
    global sent_signals_today, last_reset_date, sidecar_alerts_today
    
    now_kst = get_kst_now()
    today_str = now_kst.strftime("%Y-%m-%d")
    
    if last_reset_date != today_str:
        sent_signals_today.clear()
        sidecar_alerts_today.clear()
        last_reset_date = today_str

    check_sidecar()

    try:
        krx = fdr.StockListing('KRX')
        top_stocks = krx.sort_values(by='Marcap', ascending=False).head(100)
        
        for idx, row in top_stocks.iterrows():
            code = row['Code']
            name = row['Name']
            
            if code in sent_signals_today:
                continue
                
            df = fdr.DataReader(code, now_kst - timedelta(days=60), now_kst)
            if len(df) < 20:
                continue
                
            df['RSI'] = calculate_rsi(df)
            df['MA20'] = df['Close'].rolling(20).mean()
            df['Disparity'] = (df['Close'] / df['MA20']) * 100
            df['Vol_MA20'] = df['Volume'].rolling(20).mean() 
            
            current_price = int(df['Close'].iloc[-1])
            rsi_val = df['RSI'].iloc[-1]
            disparity_val = df['Disparity'].iloc[-1]
            
            current_vol = df['Volume'].iloc[-1]
            avg_vol = df['Vol_MA20'].iloc[-2] if not pd.isna(df['Vol_MA20'].iloc[-2]) and df['Vol_MA20'].iloc[-2] > 0 else 1
            vol_ratio = (current_vol / avg_vol) * 100
            
            signal_type = None
            stars = ""
            
            if rsi_val <= 30 and disparity_val <= 95:
                signal_type = "F+"
                stars = "⭐⭐⭐"
            elif rsi_val <= 33 and disparity_val <= 97:
                signal_type = "Q"
                stars = "⭐⭐"
            elif rsi_val <= 35:
                signal_type = "W+"
                stars = "⭐"
                
            if signal_type and vol_ratio >= 150.0:
                if not check_investor_buying(code):
                    continue
                    
                proper_price = calculate_proper_price(row, current_price)
                margin_of_safety = ((proper_price - current_price) / proper_price) * 100.0
                
                if margin_of_safety >= 15.0:
                    trade_type = "📈 스윙 (1~4주 보유 권장)"
                    target_price = int(current_price * 1.10) 
                    stop_loss = int(current_price * 0.95)    
                else:
                    trade_type = "⚡ 단타 (1~3일 보유 권장)"
                    target_price = int(current_price * 1.05) 
                    stop_loss = int(current_price * 0.97)    
                
                msg = (
                    f"🚨 <b>[뽕실로봇] {signal_type} 시그널 포착!</b> {stars}\n\n"
                    f"📌 <b>종목명:</b> {name} ({code})\n"
                    f"🛠️ <b>추천 포지션:</b> <b>{trade_type}</b>\n\n"
                    f"💰 <b>현재가(추천가):</b> {current_price:,}원\n"
                    f"🎯 <b>목표가:</b> {target_price:,}원\n"
                    f"🛑 <b>손절가:</b> {stop_loss:,}원\n\n"
                    f"📊 <b>포착 근거:</b>\n"
                    f"• 거래량 {vol_ratio:.1f}% 폭발 및 외인/기관 수급 유입\n"
                    f"• RSI {rsi_val:.1f} / 이격도 {disparity_val:.1f}%\n"
                    f"💎 <b>추정 적정주가(S-RIM):</b> {proper_price:,}원 (안전마진 {margin_of_safety:+.1f}%)\n"
                )
                
                send_telegram_msg(msg)
                sent_signals_today.add(f"{name} ({code}) - {signal_type} [{trade_type[:2]}]")
                time.sleep(1)
                
    except Exception as e:
        pass

def run_scanner():
    global daily_summary_sent_date, morning_briefing_sent_date
    while True:
        now = get_kst_now()
        today_str = now.strftime("%Y-%m-%d")
        
        if now.weekday() < 5: 
            # 1. 모닝 브리핑 (오전 7시 30분 발송)
            if now.hour == 7 and now.minute == 30:
                if morning_briefing_sent_date != today_str:
                    send_morning_briefing()
                    morning_briefing_sent_date = today_str

            # 2. 개별 종목 스캔 (장 운영 시간)
            if (8 <= now.hour < 15) or (now.hour == 15 and now.minute < 30):
                scan_stocks()
                
            # 3. 장 마감 브리핑 (오후 3시 30분 발송)
            if now.hour == 15 and now.minute >= 30:
                if daily_summary_sent_date != today_str:
                    send_daily_closing_report()
                    daily_summary_sent_date = today_str
                
        time.sleep(15) 

@app.route('/')
def health_check():
    return "뽕실로봇 V3 (모닝브리핑 추가) 정상 작동 중입니다.", 200

if __name__ == "__main__":
    t = threading.Thread(target=run_scanner)
    t.daemon = True
    t.start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
