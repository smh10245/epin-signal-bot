import os
import time
import threading
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from flask import Flask

app = Flask(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

sent_signals_today = set()
sidecar_alerts_today = set()
last_reset_date = None
daily_summary_sent_date = None

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
        print(f"적정주가 계산 예외: {e}")
        
    return int(current_price * 1.12)

def check_investor_buying(code):
    """조건 2: 최근 3거래일 기관 또는 외국인 순매수(수급) 유입 확인"""
    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        res = requests.get(url, headers=headers)
        
        # 네이버 금융에서 순매매량 표 추출
        dfs = pd.read_html(res.text, encoding='euc-kr', match='순매매량')
        if not dfs:
            return True
            
        df = dfs[0]
        # 표 구조(MultiIndex) 평탄화
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(0)
            
        # 최근 3일치 데이터만 추출
        df = df.dropna(subset=['날짜']).head(3)
        
        inst_col = [c for c in df.columns if '기관' in c][0]
        fore_col = [c for c in df.columns if '외국인' in c][0]
        
        # 문자열(콤마, 부호 등)을 숫자로 변환 후 합산
        inst_sum = df[inst_col].astype(str).str.replace(r'[^0-9\-]', '', regex=True).replace('', '0').astype(int).sum()
        fore_sum = df[fore_col].astype(str).str.replace(r'[^0-9\-]', '', regex=True).replace('', '0').astype(int).sum()
        
        # 기관이나 외국인 중 한 곳이라도 3일 누적 순매수(+)라면 True
        return inst_sum > 0 or fore_sum > 0
        
    except Exception as e:
        print(f"{code} 수급 확인 중 오류: {e}")
        return True # 크롤링 오류 시 봇 멈춤 방지를 위해 일단 통과

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
            df['Vol_MA20'] = df['Volume'].rolling(20).mean() # 20일 평균 거래량
            
            current_price = int(df['Close'].iloc[-1])
            rsi_val = df['RSI'].iloc[-1]
            disparity_val = df['Disparity'].iloc[-1]
            
            # 조건 1: 거래량 150% 이상 급증 여부 체크
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
                
            # 시그널이 발생했고, 거래량도 터졌을 때만 2차 검증(수급) 진행
            if signal_type and vol_ratio >= 150.0:
                # 조건 2: 외인/기관 수급 확인 (수급 없으면 패스)
                if not check_investor_buying(code):
                    continue
                    
                capture_time = now_kst.strftime("%Y-%m-%d %H:%M:%S")
                proper_price = calculate_proper_price(row, current_price)
                margin_of_safety = ((proper_price - current_price) / proper_price) * 100.0
                
                # 투자 포지션 결정 (단타 vs 스윙)
                if margin_of_safety >= 15.0:
                    trade_type = "📈 스윙 (1~4주 보유 권장)"
                    target_price = int(current_price * 1.10) # 스윙은 목표가 +10%
                    stop_loss = int(current_price * 0.95)    # 스윙은 손절가 -5%
                else:
                    trade_type = "⚡ 단타 (1~3일 보유 권장)"
                    target_price = int(current_price * 1.05) # 단타는 목표가 +5%
                    stop_loss = int(current_price * 0.97)    # 단타는 손절가 -3%
                
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
        print(f"스캔 중 오류 발생: {e}")

def run_scanner():
    global daily_summary_sent_date
    while True:
        now = get_kst_now()
        today_str = now.strftime("%Y-%m-%d")
        
        if now.weekday() < 5: 
            if (8 <= now.hour < 15) or (now.hour == 15 and now.minute < 30):
                scan_stocks()
                
            if now.hour == 15 and now.minute >= 30:
                if daily_summary_sent_date != today_str:
                    send_daily_closing_report()
                    daily_summary_sent_date = today_str
                
        time.sleep(15) 

@app.route('/')
def health_check():
    return "뽕실로봇 V2 (수급+거래량 단타/스윙 추가) 정상 작동 중입니다.", 200

if __name__ == "__main__":
    t = threading.Thread(target=run_scanner)
    t.daemon = True
    t.start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
