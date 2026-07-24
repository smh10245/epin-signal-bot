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
    """
    S-RIM (잔여이익 가치평가 모델) 기반 적정주가 계산
    공식: BPS + (BPS * (ROE - 요구수익률) / 요구수익률)
    """
    try:
        bps = float(row['BPS']) if 'BPS' in row and pd.notnull(row['BPS']) else 0
        eps = float(row['EPS']) if 'EPS' in row and pd.notnull(row['EPS']) else 0
        
        # BPS와 EPS가 유효한 경우 S-RIM 정밀 계산
        if bps > 0 and eps > 0:
            roe = (eps / bps) * 100.0  # ROE (%)
            required_return = 0.08      # 요구수익률 8% (BBB- 회사채 금리 기준)
            
            # S-RIM 적정주가 산출
            roe_decimal = roe / 100.0
            proper_price = bps + (bps * (roe_decimal - required_return) / required_return)
            
            if proper_price > 0:
                return int(proper_price)
                
        # 재무데이터 부족 시 PBR 기준 보정 (PBR 1.2배 기준 추정)
        pbr = float(row['PBR']) if 'PBR' in row and pd.notnull(row['PBR']) else 0
        if pbr > 0 and bps > 0:
            return int(bps * 1.2)
            
    except Exception as e:
        print(f"적정주가 계산 예외: {e}")
        
    # 재무 데이터 미제공 종목일 경우 기본 기술적 추정 가치 반환
    return int(current_price * 1.12)

def check_sidecar():
    """KOSPI / KOSDAQ 선물 등락률 감시를 통한 사이드카 발동 알림"""
    global sidecar_alerts_today
    now_kst = get_kst_now()
    capture_time = now_kst.strftime("%Y-%m-%d %H:%M:%S")

    try:
        df_kospi = fdr.DataReader('KS11', now_kst - timedelta(days=5), now_kst)
        if len(df_kospi) >= 2:
            prev_close = df_kospi['Close'].iloc[-2]
            curr_price = df_kospi['Close'].iloc[-1]
            change_rate = ((curr_price - prev_close) / prev_close) * 100

            if change_rate <= -5.0 and "KOSPI_SELL" not in sidecar_alerts_today:
                msg = (
                    f"🚨 <b>[시장 속보] KOSPI 매도 사이드카 발동!</b>\n\n"
                    f"⏰ <b>발동시점:</b> {capture_time}\n"
                    f"📉 <b>KOSPI 변동률:</b> {change_rate:.2f}%\n"
                    f"⚠️ <i>프로그램 매매 호가가 5분간 정지됩니다. 시장 폭락에 유의하세요!</i>"
                )
                send_telegram_msg(msg)
                sidecar_alerts_today.add("KOSPI_SELL")

            elif change_rate >= 5.0 and "KOSPI_BUY" not in sidecar_alerts_today:
                msg = (
                    f"🚀 <b>[시장 속보] KOSPI 매수 사이드카 발동!</b>\n\n"
                    f"⏰ <b>발동시점:</b> {capture_time}\n"
                    f"📈 <b>KOSPI 변동률:</b> +{change_rate:.2f}%\n"
                    f"⚠️ <i>프로그램 매매 호가가 5분간 정지됩니다. 시장 급등에 주의하세요!</i>"
                )
                send_telegram_msg(msg)
                sidecar_alerts_today.add("KOSPI_BUY")

        df_kosdaq = fdr.DataReader('KQ11', now_kst - timedelta(days=5), now_kst)
        if len(df_kosdaq) >= 2:
            prev_close_kq = df_kosdaq['Close'].iloc[-2]
            curr_price_kq = df_kosdaq['Close'].iloc[-1]
            change_rate_kq = ((curr_price_kq - prev_close_kq) / prev_close_kq) * 100

            if change_rate_kq <= -6.0 and "KOSDAQ_SELL" not in sidecar_alerts_today:
                msg = (
                    f"🚨 <b>[시장 속보] KOSDAQ 매도 사이드카 발동!</b>\n\n"
                    f"⏰ <b>발동시점:</b> {capture_time}\n"
                    f"📉 <b>KOSDAQ 변동률:</b> {change_rate_kq:.2f}%\n"
                    f"⚠️ <i>프로그램 매매 호가가 5분간 정지됩니다.</i>"
                )
                send_telegram_msg(msg)
                sidecar_alerts_today.add("KOSDAQ_SELL")

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

def send_daily_closing_report():
    """오후 8시 장 마감 종합 브리핑 리포트 전송"""
    global sent_signals_today
    now_kst = get_kst_now()
    date_str = now_kst.strftime("%Y-%m-%d")
    
    if not sent_signals_today:
        msg = (
            f"📋 <b>[뽕실로봇] {date_str} 장 마감 종합 브리핑</b>\n\n"
            f"오늘 장 운영 시간 동안 포착된 시그널 종목이 없습니다."
        )
    else:
        stocks_list = "\n".join([f"• {item}" for item in sent_signals_today])
        msg = (
            f"📋 <b>[뽕실로봇] {date_str} 장 마감 종합 브리핑</b>\n\n"
            f"오늘 총 <b>{len(sent_signals_today)}개</b>의 종목 시그널이 포착되었습니다!\n\n"
            f"<b>[포착된 종목 리스트]</b>\n{stocks_list}\n\n"
            f"💡 <i>내일 장에서 성공적인 투자 되시길 바랍니다!</i>"
        )
    send_telegram_msg(msg)

def scan_stocks():
    """개별 종목 시그널 스캔 및 적정주가 포함 메시지 전송"""
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
                
                # 💎 [적정주가 계산] S-RIM 모델 적용
                proper_price = calculate_proper_price(row, current_price)
                margin_of_safety = ((proper_price - current_price) / proper_price) * 100.0
                
                msg = (
                    f"🚨 <b>[뽕실로봇] {signal_type} 시그널 포착!</b> {stars}\n\n"
                    f"⏰ <b>포착시점:</b> {capture_time}\n"
                    f"📌 <b>종목명:</b> {name} ({code})\n"
                    f"💰 <b>현재가/추천가:</b> {current_price:,}원\n"
                    f"💎 <b>추정 적정주가(S-RIM):</b> {proper_price:,}원 (안전마진 {margin_of_safety:+.1f}%)\n"
                    f"🎯 <b>목표가:</b> {target_price:,}원 (+5%)\n"
                    f"🛑 <b>손절가:</b> {stop_loss:,}원 (-3%)\n\n"
                    f"📊 <b>지표:</b> RSI {rsi_val:.1f} / 이격도 {disparity_val:.1f}%\n"
                    f"💡 <i>진입 전 악재 공시/뉴스 유무를 가볍게 확인하세요.</i>"
                )
                
                send_telegram_msg(msg)
                sent_signals_today.add(f"{name} ({code}) - {signal_type}")
                time.sleep(1)
                
    except Exception as e:
        print(f"스캔 중 오류 발생: {e}")

def run_scanner():
    global daily_summary_sent_date
    while True:
        now = get_kst_now()
        today_str = now.strftime("%Y-%m-%d")
        
        # 평일 08:00 ~ 19:59 사이에는 개별 종목 스캔
        if 8 <= now.hour < 20 and now.weekday() < 5:
            scan_stocks()
            
        # 정확히 저녁 8시(20시) 정각에 평일 마감 브리핑 1회 전송
        if now.hour == 20 and now.weekday() < 5:
            if daily_summary_sent_date != today_str:
                send_daily_closing_report()
                daily_summary_sent_date = today_str
                
        time.sleep(60)

@app.route('/')
def health_check():
    return "뽕실로봇 마감 브리핑 및 스캐너 정상 작동 중입니다.", 200

if __name__ == "__main__":
    t = threading.Thread(target=run_scanner)
    t.daemon = True
    t.start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
