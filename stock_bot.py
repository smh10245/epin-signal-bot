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
    return "Epin High-Winrate Bot with Market Open/Close Notice is running!"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"})
    except Exception as e:
        print(f"발송 실패: {e}")

# 당일 중복 발송 방지 및 상태 관리 변수
sent_signals_today = set()
sent_open_notice = False
sent_close_notice = False
last_checked_date = ""

def run_scanner():
    global sent_signals_today, sent_open_notice, sent_close_notice, last_checked_date
    
    while True:
        kst = timezone(timedelta(hours=9))
        now = datetime.now(kst)
        today_date_str = now.strftime('%Y-%m-%d')
        
        # 자정 기준 당일 알림 및 상태 기록 초기화
        if today_date_str != last_checked_date:
            sent_signals_today.clear()
            sent_open_notice = False
            sent_close_notice = False
            last_checked_date = today_date_str
            print(f"🗓️ 날짜 변경 ({today_date_str}): 발송 및 안내 상태 리셋")

        # 주말(토/일) 대기
        if now.weekday() > 4:
            time.sleep(300)
            continue
        
        current_time_str = now.strftime('%H:%M')
        current_time_num = int(now.strftime('%H%M'))
        
        # ==========================================
        # 🔔 1) 장 시작 / 장 마감 알림 체크
        # ==========================================
        
        # 장 시작 알림 (08:50 ~ 09:02 사이 1회 발송)
        if 850 <= current_time_num <= 902 and not sent_open_notice:
            open_msg = (
                f"🔔 <b>[이핀로봇 - 장 시작 알림]</b> 🔔\n\n"
                f"📅 <b>일자</b>: {today_date_str}\n"
                f"🚀 국내 증시 장이 곧 시작/개장합니다!\n"
                f"📊 <b>시가총액 상위 100개 정예 종목</b> 실시간 스캔을 시작합니다.\n\n"
                f"💡 오늘 하루도 성투하시길 바랍니다!"
            )
            send_telegram(open_msg)
            sent_open_notice = True
            print(f"[{current_time_str}] 장 시작 알림 발송 완료")

        # 장 마감 알림 (15:30 ~ 15:40 사이 1회 발송)
        if 1530 <= current_time_num <= 1540 and not sent_close_notice:
            close_msg = (
                f"🔔 <b>[이핀로봇 - 장 마감 알림]</b> 🔔\n\n"
                f"📅 <b>일자</b>: {today_date_str}\n"
                f"👏 금일 국내 증시 장이 마감되었습니다.\n"
                f"오늘 하루도 수고 많으셨습니다. 편안한 저녁 되세요!"
            )
            send_telegram(close_msg)
            sent_close_notice = True
            print(f"[{current_time_str}] 장 마감 알림 발송 완료")

        # 평일 장중 시간(08:50 ~ 15:35) 외에는 대기
        if current_time_num < 850 or current_time_num > 1535:
            time.sleep(120)
            continue

        print(f"🔍 [{current_time_str}] 상위 100개 종목 밸런스 시그널 스캔 중...")

        # ==========================================
        # 📊 2) 실시간 종목 시그널 스캔
        # ==========================================
        try:
            df_krx = fdr.StockListing('KRX')
            target_stocks = df_krx.head(100)  # 상위 100개 우량주 감시

            count = 0
            for index, row in target_stocks.iterrows():
                symbol = row['Code'] if 'Code' in row else row['Symbol']
                name = row['Name']
                
                # 당일 이미 알림 보낸 종목 스킵
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
                    
                    # 볼린저밴드 하단 (20일, 2표준편차)
                    std20 = df['Close'].rolling(20).std().iloc[-1]
                    lower_band = ma20 - (2 * std20)
                    
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
                    buy_min, buy_max = 0, 0
                    target_price, stop_loss = 0, 0
                    sell_timing = ""

                    # --- 적정 밸런스 조건 ---
                    # 1) F+ (과대낙폭 바닥 반등)
                    if rsi <= 30.0 and disparity_20 <= 92.0:
                        signal_type = "F+ (과대낙폭 바닥)"
                        guide = "💎 낙폭과대 과매도 구간! 반등 확률 높은 타점"
                        
                        buy_max = today_price
                        buy_min = int(today_price * 0.985)
                        target_price = int(today_price * 1.03)
                        stop_loss = int(today_price * 0.98)
                        sell_timing = "⏳ 매수 후 1~3일 이내 반등 시 (+3% 이상 익절)"

                    # 2) Q (단타 눌림목)
                    elif rsi <= 35.0 and change_rate <= -2.5:
                        signal_type = "Q (단타 눌림목)"
                        guide = "⚡ 당일 과매도 단타 타점! 빠른 반등 대응"
                        
                        buy_max = today_price
                        buy_min = int(today_price * 0.99)
                        target_price = int(today_price * 1.025)
                        stop_loss = int(today_price * 0.982)
                        sell_timing = "⏳ 당일 장마감 전 또는 이튿날 오전 중 빠르게 매도"

                    # 3) W+ (수급 주도주 돌파)
                    elif vol_ratio >= 250.0 and (3.0 <= change_rate <= 10.0) and (50.0 <= rsi <= 68.0):
                        signal_type = "W+ (수급 주도주 돌파)"
                        guide = "🔥 강한 수급 유입! 주도주 눌림/돌파 대응"
                        
                        buy_max = today_price
                        buy_min = int(today_price * 0.99)
                        target_price = int(today_price * 1.04)
                        stop_loss = int(today_price * 0.975)
                        sell_timing = "⏳ 매수 후 1~2일 이내 (+4% 달성 시 분할 익절)"

                    if signal_type:
                        count += 1
                        sent_signals_today.add(symbol)
                        
                        msg = (
                            f"🚨 <b>[이핀로봇 - {signal_type}]</b> 🚨\n\n"
                            f"1️⃣ <b>종목정보</b>: <b>{name}</b> ({symbol})\n"
                            f"2️⃣ <b>현재가/등락률</b>: {today_price:,}원 ({change_rate:+.2f}%)\n"
                            f"3️⃣ <b>🎯 추천 매수단가</b>: <code>{buy_min:,}원 ~ {buy_max:,}원</code>\n"
                            f"4️⃣ <b>🎯 목표 매도단가 (익절)</b>: <code>{target_price:,}원</code>\n"
                            f"5️⃣ <b>🛑 손절 매도단가 (손절)</b>: <code>{stop_loss:,}원</code>\n"
                            f"6️⃣ <b>⏱️ 추천 매도시기</b>: {sell_timing}\n"
                            f"7️⃣ <b>보조지표</b>: RSI({rsi:.1f}) | 20일이격도({disparity_20:.1f}%) | 거래량비율({vol_ratio:.0f}%)\n\n"
                            f"💡 <b>대응 가이드</b>: {guide}"
                        )
                        send_telegram(msg)
                except:
                    continue
            if count > 0:
                print(f"✨ 밸런스 시그널 {count}개 발송 완료.")
        except Exception as e:
            print(f"스캔 오류: {e}")

        # 60초 간격 스캔
        time.sleep(60)

if __name__ == "__main__":
    t = threading.Thread(target=run_scanner)
    t.daemon = True
    t.start()
    
    app.run(host='0.0.0.0', port=10000)
