수입품 FinanceDataReader as fdr
판다를 PD로 가져오기
numpy를 np로 가져오기
가져오기 요청
os 가져오기
datetime에서 가져오기 날짜 시간, 시간대, 시간 델타

텔레그램_TOKEN = os.environment.get("텔레그램_TOKEN")
CHAT_ID = os.environment.get("CHAT_ID")

import send_telegram(텍스트):
 URL = f"https://api.telegram.org/bot {TEELGRAM_TOKEN}/sendMessage"
 시도:
 requests.post (url, data={"chat_id": CHAT_ID, "텍스트": 텍스트, "parse_모드": "HTML"})
 예외: e:
 인쇄(f"발송 실패: {e}")

kst = 시간대(timedelta(시간=9))
지금 = datetime.now(kst)
현재_시간 = now.strftime('%H: %M')

인쇄(f"🔍 [현재_시간}] 과대낙폭(F+/Q/G) 시그널 스캔 시작...")

df_krx = fdr.주식 상장('KRX')
target_stocks = df_krx.head(150)

카운트 = 0
인덱스의 경우 target_stocks.iterrows ():
 기호 = 행 [''코드'] 다른 행에 '코드'가 있는 경우 ['기호']
 이름 = 행 ['이름']
    
 시도:
 df = fdr.데이터 리더(기호)
 len(df) < 30인 경우: 계속
            
 today = df.iloc[-1]
 오늘_가격 = int(오늘['닫기')
 오늘_vol = int(오늘 ['볼륨'])
 오늘_high = int(오늘['높음]
 오늘_낮음 = int(오늘['낮음'])
        
 prev_close = df.iloc[-2]['Close']
 change_rate = (오늘 가격 - prev_close) * 100
        
        # 지표 계산 (이격도, 볼린저밴드, RSI)
 ma20 = df['닫기'].롤링(20).mean().iloc[-1]
 디스패리티_20 = (오늘_가격 / ma20) * 100
        
 std20 = df['Close'].rolling(20).std().iloc[-1]
 lower_band = ma20 - (2 * std20)
        
 델타 = df['닫기'.diff()
 게인 = (delta).여기서 (delta > 0, 0). 롤링(window=14). mean()
 손실 = (-delta).where(delta < 0, 0).rolling(window=14).mean()
 rs = 이득/손실
 rsi = (100 - (100 / (1 + rs))).iloc[-1]
        
 close_5d_ago = df.iloc[-6]['Close']
 drop_5d_rate = (오늘 가격 - 종가_5d_ago) * 100

 # 시그널 조건 판별
 signal_type, 가이드 = 없음, ""

 # F+ : 단타 정밀 과대낙폭 (RSI 32 이하 & 20일 이격도 93% 이하)
 rsi <= 32이고 disparity_20 <= 93.0인 경우:
 signal_type = "F+ (단타 과대낙폭 바닥)"
 guide = "⚡ 초과매도 기술적 반등 타점! 목표 수익률 +2% 이상 단기 대응"

 # Q : 단타 빠른 과대낙폭 (RSI 38 이하 & 급락/밴드하단 터치)
 elif rsi <= 38 및 (change_rate <= -3.5 또는 today_price <= lower_band):
 signal_type = "Q (단타 과대낙폭 바닥)"
 guide = "⚡ 분봉상 과대낙폭 구간! 목표 수익률 +1% 이상 빠른 단타 대응"

 # G : 스윙 과대낙폭 (5일간 -8% 이상 급락 or 저평가 바닥)
 elif drop_5d_rate <= -8.0 또는 (rsi <= 35, disparity_20 <= 92.0):
 signal_type = "G (스윙 과대낙폭 바닥)"
 guide = "🎯 성과성 높은 저평가 눌림 구간! 3~10일 보유 (+5~11% 목표)"

 signal_type인 경우:
 카운트 += 1
 메시지 = (
 f"🚨 <b>[이핀로봇 - {signal_type}]</b> 🚨\n\n"
 f"1️⃣ <b>포착시각/종류</b>: {current_time} | {signal_type}\n"
 f"2️⃣ <b>종목정보</b>: {name}({symbol})\n"
 f"3️⃣ <b>현재가/등락률</b>: {오늘_가격:,}원({change_rate:+.2f}%)\n"
 f"4️⃣ <b>과매도지표</b>: RSI ({rsi:.1f}) | 20일 이격도 ({disparity_20:.1f}%)\n"
 f"5️⃣ <b>볼린저밴드</b>: 하단 기준가({int(lower_band):,}원) 근접/이탈\n"
 f"6️⃣ <b>당일 고저가</b>: 고가 {오늘_높음:,}원 / 저가 {오늘_낮음:,}원\n"
 f"7️⃣ <b>이핀 대응가이드</b>: {guide}"
 )
 send_telegram(msg)
 단:
 계속하다.

print(f"✨ 완료! 총 {count}개 시그널 발송.")
