import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime, timezone, timedelta

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"})
    except Exception as e:
        print(f"발송 실패: {e}")

kst = timezone(timedelta(hours=9))
now = datetime.now(kst)
current_time = now.strftime('%H:%M')

print(f"🔍 [{current_time}] 과대낙폭(F+/Q/G) 시그널 스캔 시작...")

df_krx = fdr.StockListing('KRX')
target_stocks = df_krx.head(150)

count = 0
for index, row in target_stocks.iterrows():
    symbol = row['Code'] if 'Code' in row else row['Symbol']
    name = row['Name']
    
    try:
        df = fdr.DataReader(symbol)
        if len(df) < 30: continue
            
        today = df.iloc[-1]
        today_price = int(today['Close'])
        today_vol = int(today['Volume'])
        today_high = int(today['High'])
        today_low = int(today['Low'])
        
        prev_close = df.iloc[-2]['Close']
        change_rate = ((today_price - prev_close) / prev_close) * 100
        
        # 지표 계산
        ma20 = df['Close'].rolling(20).mean().iloc[-1]
        disparity_20 = (today_price / ma20) * 100
        
        std20 = df['Close'].rolling(20).std().iloc[-1]
        lower_band = ma20 - (2 * std20)
        
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = (100 - (100 / (1 + rs))).iloc[-1]
        
        close_5d_ago = df.iloc[-6]['Close']
        drop_5d_rate = ((today_price - close_5d_ago) / close_5d_ago) * 100

        # 시그널 조건 판별
        signal_type, guide = None, ""

        if rsi <= 32 and disparity_20 <= 93.0:
            signal_type = "F+ (단타 과대낙폭 바닥)"
            guide = "⚡ 초과매도 기술적 반등 타점! 목표 수익률 +2% 이상 단기 대응"
        elif rsi <= 38 and (change_rate <= -3.5 or today_price <= lower_band):
            signal_type = "Q (단타 과대낙폭 바닥)"
            guide = "⚡ 분봉상 과대낙폭 구간! 목표 수익률 +1% 이상 빠른 단타 대응"
        elif drop_5d_rate <= -8.0 or (rsi <= 35 and disparity_20 <= 92.0):
            signal_type = "G (스윙 과대낙폭 바닥)"
            guide = "🎯 성과성 높은 저평가 눌림 구간! 3~10일 보유 (+5~11% 목표)"

        if signal_type:
            count += 1
            msg = (
                f"🚨 <b>[이핀로봇 - {signal_type}]</b> 🚨\n\n"
                f"1️⃣ <b>포착시각/종류</b>: {current_time} | {signal_type}\n"
                f"2️⃣ <b>종목정보</b>: {name} ({symbol})\n"
                f"3️⃣ <b>현재가/등락률</b>: {today_price:,}원 ({change_rate:+.2f}%)\n"
                f"4️⃣ <b>과매도지표</b>: RSI ({rsi:.1f}) | 20일 이격도 ({disparity_20:.1f}%)\n"
                f"5️⃣ <b>볼린저밴드</b>: 하단 기준가({int(lower_band):,}원) 근접/이탈\n"
                f"6️⃣ <b>당일 고저가</b>: 고가 {today_high:,}원 / 저가 {today_low:,}원\n"
                f"7️⃣ <b>이핀 대응가이드</b>: {guide}"
            )
            send_telegram(msg)
    except:
        continue

print(f"✨ 완료! 총 {count}개 시그널 발송.")
