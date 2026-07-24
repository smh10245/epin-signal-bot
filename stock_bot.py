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

def check_sidecar():
    """KOSPI / KOSDAQ 선물 등락률 감시를 통한 사이드카 발동 알림"""
    global sidecar_alerts_today
    now_kst = get_kst_now()
    capture_time = now_kst.strftime("%Y-%m-%d %H:%M:%S")

    try:
        # KOSPI 200 지수 / 대표 선물 지수 모니터링
        df_kospi = fdr.DataReader('KS11', now_kst - timedelta(days=5), now_kst)
        if len(df_kospi) >= 2:
            prev_close = df_kospi['Close'].iloc[-2]
            curr_price = df_kospi['Close'].iloc[-1]
            change_rate = ((curr_price - prev_close) / prev_close) * 100

            # KOSPI 매도 사이드카 조건 (약 -5% 이상 급락 시)
            if change_rate <= -5.0 and "KOSPI_SELL" not in sidecar_alerts_today:
                msg = (
                    f"🚨 <b>[시장 속보] KOSPI 매도 사이드카 발동!</b>\n\n"
                    f"⏰ <b>발동시점:</b> {capture_time}\n"
                    f"📉 <b>KOSPI 변동률:</b> {change_rate:.2f}%\n"
                    f"⚠️ <i>프로그램 매매 호가가 5분간 정지됩니다. 시장 폭락에 유의하세요!</i>"
                )
                send_telegram_msg(msg)
                sidecar_alerts_today.add("KOSPI_SELL")

            # KOSPI 매수 사이드카 조건 (약 +5% 이상 급등 시)
            elif change_rate >= 5.0 and "KOSPI_BUY" not in sidecar_alerts_today:
                msg = (
                    f"🚀 <b>[시장 속보] KOSPI 매수 사이드카 발동!</b>\n\n"
                    f"⏰ <b>발동시점:</b> {capture_time}\n"
                    f"📈 <b>KOSPI 변동률:</b> +{change_rate:.2f}%\n"
                    f"⚠️ <i>프로그램 매매 호가가 5분간 정지됩니다. 시장 급등에 주의하세요!</i>"
                )
                send_telegram_msg(msg)
                sidecar_alerts_today.add("KOSPI_BUY")

        # KOSDAQ 지수 모니터링
        df_kosdaq = fdr.DataReader('KQ11', now_kst - timedelta(days=5), now_kst)
        if len(df_kosdaq) >= 2:
            prev_close_kq = df_kosdaq['Close'].iloc[-2]
            curr_price_kq = df_kosdaq['Close'].iloc[-1]
            change_rate_kq = ((curr_price_kq - prev_close_kq) / prev_close_kq) * 100

            # KOSDAQ 매도 사이드카 조건 (약 -6% 이상 급락 시)
            if change_rate_kq <= -6.0 and "KOSDAQ_SELL" not in sidecar_alerts_today:
                msg = (
                    f"🚨 <b>[시장 속보] KOSDAQ 매도 사이드카 발동!</b>\n\n"
                    f"⏰ <b>발동시점:</b> {capture_time}\n"
                    f"📉 <b>KOSDAQ 변동률:</b> {change_rate_kq:.2f}%\n"
                    f"⚠️ <i>프로그램 매매 호가가 5분간 정지됩니다.</i>"
                )
                send_telegram_msg(msg)
                sidecar_alerts_today.add("KOSDAQ_SELL")

            # KOSDAQ 매수 사이드카 조건 (약 +6% 이상 급등 시)
            elif change_rate_kq >= 6.0 and "KOSDAQ_BUY" not in sidecar_alerts_today:
                msg = (
                    f"🚀 <b>[시장 속보] KOSDAQ 매수 사이드카 발동!</b>\n\n"
                    f"⏰ <b>발동시점:</b> {capture_time}\n"
                    f"📈 <b>KOSDAQ 변동률:</b> +{change_rate_kq:.2f}%\n"
                    f"⚠️ <i>프로그램 매매 호가가 5분간 정지됩니다.</i>"
                )
                send_telegram_msg(msg)
                sidecar_alerts_today.add("KOSDAQ_BUY")

    except Exception as e:
        print(f"사이드카 체크 중 오류: {e}")

def scan_stocks():
    """개별 종목 시그널 스캔"""
    global sent_signals_today, last_reset_date, sidecar_alerts_today
    
    now_kst = get_kst_now()
    today_str = now_kst.strftime("%Y-%m-%d")
    
    if last_reset_date != today_str:
        sent_signals_today.clear()
        sidecar_alerts_today.clear()
        last_reset_date = today_str

    # 시장 사이드카 발동 여부 우선 점검
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
            
            current_price = int(df['Close'].iloc[-1])
            rsi_val = df['RSI'].iloc[-1]
            disparity_val = df['Disparity'].iloc[-1]
            
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
                
            if signal_type:
                capture_time = now_kst.strftime("%Y-%m-%d %H:%M:%S")
                target_price = int(current_price * 1.05)
                stop_loss = int(current_price * 0.97)
                
                msg = (
                    f"🚨 <b>[뽕실로봇] {signal_type} 시그널 포착!</b> {stars}\n\n"
                    f"⏰ <b>포착시점:</b> {capture_time}\n"
                    f"📌 <b>종목명:</b> {name} ({code})\n"
                    f"💰 <b>현재가/추천가:</b> {current_price:,}원\n"
                    f"🎯 <b>목표가:</b> {target_price:,}원 (+5%)\n"
                    f"🛑 <b>손절가:</b> {stop_loss:,}원 (-3%)\n\n"
                    f"📊 <b>지표:</b> RSI {rsi_val:.1f} / 이격도 {disparity_val:.1f}%\n"
                    f"💡 <i>진입 전 악재 공시/뉴스 유무를 가볍게 확인하세요.</i>"
                )
                
                send_telegram_msg(msg)
                sent_signals_today.add(code)
                time.sleep(1)
                
    except Exception as e:
        print(f"스캔 중 오류 발생: {e}")

def run_scanner():
    while True:
        now = get_kst_now()
        if 8 <= now.hour < 20 and now.weekday() < 5:
            scan_stocks()
        time.sleep(60)

@app.route('/')
def health_check():
    return "뽕실로봇 및 사이드카 감지기 정상 작동 중입니다.", 200

if __name__ == "__main__":
    t = threading.Thread(target=run_scanner)
    t.daemon = True
    t.start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
