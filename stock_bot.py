import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import os
import time
import threading
from datetime import datetime, timezone, timedelta
from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    return "Epin High-Winrate Stock Bot is running live!"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"})
    except Exception as e:
        print(f"발송 실패: {e}")

# 당일 이미 발송한 종목 저장 (중복 알림 방지)
sent_signals_today = set()
last_checked_date = ""

def run_scanner():
    global sent_signals_today, last_checked_date
    
    while True:
        kst = timezone(timedelta(hours=9))
        now = datetime.now(kst)
        today_date_str = now.strftime('%Y-%m-%d')
        
        # 날짜가 바뀌면 당일 알림 기록 리셋
        if today_date_str != last_checked_date:
            sent_signals_today.clear()
            last_checked_date = today_date_str
            print(f"🗓️ 날짜 변경 ({today_date_str}): 당일 발송 기록 초기화 완료")

        # 주말(토/일) 대기
        if now.weekday() > 4:
            time.sleep(300)
            continue
        
        current_time_str = now.strftime('%H:%M')
        current_time_num = int(now.strftime('%H%M'))
        
        # 평일 장중 시간(08:30 ~ 15:30) 외에는 대기
        if current_time_num < 830 or current_time_num > 1530:
            time.sleep(120)
            continue

        print(f"🔍 [{current_time_str}] 고확률 엄격 시그널 스캔 중...")

        try:
            df_krx = fdr.StockListing('KRX')
            target_stocks = df_krx.head(150)  # 상위 150개 정예 종목

            count = 0
            for index, row in target_stocks.iterrows():
                symbol = row['Code'] if 'Code' in row else row['Symbol']
                name = row['Name']
                
                # 오늘 이미 알림을 받은 종목은 스킵 (중복 폭탄 방지)
                if symbol in sent_signals_today:
                    continue
                
                try:
                    df = fdr.DataReader(symbol)
                    if len(df) < 30: continue
                        
                    today = df.iloc[-1]
                    today_price = int(today['Close'])
                    
                    prev_close = df.iloc[-2]['Close']
                    change_rate = ((today_price - prev_close) / prev_close) * 100
                    
                    # 이동평균 및 이격도
                    ma20 = df['Close'].rolling(20).mean().iloc[-1]
                    disparity_20 = (today_price / ma20) * 100
                    
                    # RSI (14)
                    delta = df['Close'].diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                    rs = gain / loss
                    rsi = (100 - (100 / (1 + rs))).iloc[-1]
                    
                    # 거래량 비율 (전일 대비 %)
                    today_vol = today['Volume']
                    prev_vol = df.iloc[-2]['Volume']
                    vol_ratio = (today_vol / prev_vol * 100) if prev_vol > 0 else 0

                    signal_type, guide = None, ""

                    # --- 고확률 엄격 조건 ---
                    # 1) F+ (극단적 과대낙폭): RSI 28 이하 & 이격도 91% 이하
                    if rsi <= 28.0 and disparity_20 <= 91.0:
                        signal_type = "F+ (극단 과대낙폭 바닥)"
                        guide = "💎 최우선 과매도 바닥 구간! 기술적 반등 유효 타점"

                    # 2) W (강력 수급 돌파): 전일 대비 거래량 250% 이상 & 당일 +3.0% 이상 급등
                    elif vol_ratio >= 250.0 and change_rate >= 3.0 and rsi <= 65.0:
                        signal_type = "W (거래량 폭발 돌파)"
                        guide = "🔥 강한 수급 유입! 당일 단기 추세 상승 유효"

                    if signal_type:
                        count += 1
                        sent_signals_today.add(symbol)  # 오늘 발송했음을 기록
                        
                        msg = (
                            f"🚨 <b>[이핀로봇 - {signal_type}]</b> 🚨\n\n"
                            f"1️⃣ <b>포착시각/종류</b>: {current_time_str} | {signal_type}\n"
                            f"2️⃣ <b>종목정보</b>: {name} ({symbol})\n"
                            f"3️⃣ <b>현재가/등락률</b>: {today_price:,}원 ({change_rate:+.2f}%)\n"
                            f"4️⃣ <b>핵심지표</b>: RSI ({rsi:.1f}) | 20일 이격도 ({disparity_20:.1f}%) | 거래량비율 ({vol_ratio:.0f}%)\n"
                            f"5️⃣ <b>이핀 대응가이드</b>: {guide}"
                        )
                        send_telegram(msg)
                except:
                    continue
            if count > 0:
                print(f"✨ 신규 고확률 시그널 {count}개 발송 완료.")
        except Exception as e:
            print(f"스캔 오류: {e}")

        # 60초 대기 후 재스캔
        time.sleep(60)

if __name__ == "__main__":
    t = threading.Thread(target=run_scanner)
    t.daemon = True
    t.start()
    
    app.run(host='0.0.0.0', port=10000)
