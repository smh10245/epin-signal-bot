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

# 환경 변수에서 텔레그램 토큰 및 채팅 ID 로드
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# 오늘 이미 알림을 보낸 종목 중복 방지 저장소
sent_signals_today = set()
last_reset_date = None

def send_telegram_msg(message):
    """텔레그램 메시지 전송 함수"""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("[경고] TELEGRAM_TOKEN 또는 CHAT_ID가 설정되지 않았습니다.")
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
    """한국 표준시(KST) 현재 시간 반환"""
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst)

def calculate_rsi(data, window=14):
    """RSI 지표 계산"""
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def scan_stocks():
    """시가총액 상위 종목 스캔 및 시그널 추출"""
    global sent_signals_today, last_reset_date
    
    now_kst = get_kst_now()
    today_str = now_kst.strftime("%Y-%m-%d")
    
    # 일자가 바뀌면 중복 방지 리스트 초기화
    if last_reset_date != today_str:
        sent_signals_today.clear()
        last_reset_date = today_str

    try:
        # KOSPI / KOSDAQ 상위 종목 스캔 (예시)
        krx = fdr.StockListing('KRX')
        top_stocks = krx.sort_values(by='Marcap', ascending=False).head(100)
        
        for idx, row in top_stocks.iterrows():
            code = row['Code']
            name = row['Name']
            
            # 당일 이미 알림이 나간 종목은 건너뜀
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
            
            # 시그널 조건 판정
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
                # 포착시점 생성 (년-월-일 시:분:초)
                capture_time = now_kst.strftime("%Y-%m-%d %H:%M:%S")
                
                target_price = int(current_price * 1.05)  # 목표가 +5%
                stop_loss = int(current_price * 0.97)     # 손절가 -3%
                
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
                time.sleep(1) # 전송 간격 조절
                
    except Exception as e:
        print(f"스캔 중 오류 발생: {e}")

def run_scanner():
    """ 주기적으로 스캔 루프 실행 """
    while True:
        now = get_kst_now()
        # 장 운영 시간 및 감시 시간대 (08:00 ~ 20:00)
        if 8 <= now.hour < 20 and now.weekday() < 5:
            scan_stocks()
        time.sleep(60) # 1분 마다 스캔

@app.route('/')
def health_check():
    return "뽕실로봇 정상 작동 중입니다.", 200

if __name__ == "__main__":
    t = threading.Thread(target=run_scanner)
    t.daemon = True
    t.start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
