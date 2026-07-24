import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
from datetime import datetime, timezone, timedelta

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"})
    except Exception as e:
        print(f"발송 실패: {e}")

def run_scanner():
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    
    # 주말(토/일)은 대기
    if now.weekday() > 4:
        print(f"[{now.strftime('%H:%M')}] 주말입니다. 대기 중...")
        return
    
    current_time_str = now.strftime('%H:%M')
    current_time_num = int(now.strftime('%H%M'))
    
    # 평일 장중 시간(08:30 ~ 15:30) 외에는 대기
    if current_time_num < 830 or current_time_num > 1530:
        print(f"[{current_time_str}] 장외 시간입니다. 대기 중...")
        return

    print(f"🔍 [{current_time_str}] 완화된 시그널 실시간 스캔 시작...")

    try:
        df_krx = fdr.StockListing('KRX')
        target_stocks = df_krx.head(200)

        count = 0
        for index, row in target_stocks.iterrows():
            symbol = row['Code'] if 'Code' in row else row['Symbol']
            name = row['Name']
            
            try:
                df = fdr.DataReader(symbol)
                if len(df) < 30: continue
                    
                today = df.iloc[-1]
                today_price = int(today['Close'])
                
                prev_close = df.iloc[-2]['Close']
                change_rate = ((today_price - prev_close) / prev_close) * 100
                
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

                signal_type, guide = None, ""

                # 1) F+ 과대낙폭
                if rsi <= 35.0 and disparity_20 <= 94.0:
                    signal_type = "F+ (과대낙폭 바닥)"
                    guide = "⚡ 기술적 반등 타점! 목표 +2% 이상 단기 대응"

                # 2) Q 단타 눌림
                elif rsi <= 40.0 and (change_rate <= -2.5 or today_price <= lower_band):
                    signal_type = "Q (단타 눌림목)"
                    guide = "⚡ 분봉상 과매도 구간! 목표 +1% 이상 빠른 단타 대응"

                # 3) G 스윙 눌림
                elif drop_5d_rate <= -5.0 or (rsi <= 38.0 and disparity_20 <= 93.0):
                    signal_type = "G (스윙 눌림 바닥)"
                    guide = "🎯 성과성 높음! 3~10일 보유 (+5% 이상 목표)"

                if signal_type:
                    count += 1
                    msg = (
                        f"🚨 <b>[이핀로봇 - {signal_type}]</b> 🚨\n\n"
                        f"1️⃣ <b>포착시각/종류</b>: {current_time_str} | {signal_type}\n"
                        f"2️⃣ <b>종목정보</b>: {name} ({symbol})\n"
                        f"3️⃣ <b>현재가/등락률</b>: {today_price:,}원 ({change_rate:+.2f}%)\n"
                        f"4️⃣ <b>과매도지표</b>: RSI ({rsi:.1f}) | 20일 이격도 ({disparity_20:.1f}%)\n"
                        f"5️⃣ <b>이핀 대응가이드</b>: {guide}"
                    )
                    send_telegram(msg)
            except:
                continue
        print(f"✨ 완료! 총 {count}개 시그널 발송.")
    except Exception as e:
        print(f"스캔 오류: {e}")

if __name__ == "__main__":
    print("🚀 실시간 이핀 봇가동 시작!")
    while True:
        run_scanner()
        time.sleep(60)  # 60초(1분)마다 무한 반복 스캔
