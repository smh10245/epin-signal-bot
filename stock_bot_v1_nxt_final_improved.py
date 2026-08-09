from __future__ import annotations

# ==========================================
# 🤖 쾌걸스민 V2.0 (Dual Core Ultimate)
# 1. 단타 스나이퍼 & 종가배팅 듀얼 코어 (스윙 모듈 완전 적출)
# 2. 시장 서킷 브레이커 (코스피/코스닥 ETF 추적, -1.5% 시 단타 일시정지)
# 3. 4중 교집합 저격 로직 (VWAP 맥점 + 거래량 가뭄 + 수급 폭발 + 양봉)
# 4. 강철심장 스케줄러 & 듀얼 AI 브리핑 (에러 시 원시데이터 강제 송출)
# ==========================================

import os, json, time, threading, queue, logging
from datetime import datetime, timedelta, timezone
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests, websocket
import pandas as pd
import FinanceDataReader as fdr
import yfinance as yf
from flask import Flask, jsonify
from zoneinfo import ZoneInfo

try:
    import google.generativeai as genai
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

# ===== 1. 설정 및 유틸리티 =====
KST = ZoneInfo('Asia/Seoul')
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(threadName)s | %(message)s')
log = logging.getLogger('seumin.main')

def now(): return datetime.now(KST)
def num(v, d=0.0):
    try: return float(str(v).replace(',', '').replace('원', '').replace('주', '').strip())
    except: return d
def pct(n, o): return (n / o - 1) * 100 if o else 0

@dataclass(frozen=True)
class Settings:
    version: str = '2.0 (Dual Core Ultimate)'
    telegram_token: str = os.getenv('TELEGRAM_TOKEN', '').strip()
    chat_id: str = (os.getenv('CHAT_ID') or os.getenv('TELEGRAM_CHAT_ID') or '').strip()
    gemini_api_key: str = os.getenv('GEMINI_API_KEY', '').strip()
    kis_app_key: str = os.getenv('KIS_APP_KEY', '').strip()
    kis_app_secret: str = os.getenv('KIS_APP_SECRET', '').strip()
    kis_env: str = os.getenv('KIS_ENV', 'real').strip().lower()
    port: int = int(os.getenv('PORT', 10000))
    min_market_cap: float = 100_000_000_000  # 1천억 (잡주 컷)
    min_daily_volume: int = 20_000
    circuit_breaker_pct: float = -1.5        # 지수 -1.5% 하락 시 단타 중지
    nxt_trade_tr: str = os.getenv('KIS_NXT_TRADE_TR_ID', 'H0NXCNT0')

SETTINGS = Settings()

# ===== 2. 데이터 모델 =====
@dataclass
class Bar: minute: datetime; open: float; high: float; low: float; close: float; volume: int; trade_strength: float
@dataclass
class Position: name: str; entry: float; qty: float; highest: float; stop: float

# ===== 3. 비동기 큐 & DB (강철 심장) =====
class AsyncWorker:
    def __init__(self, name):
        self.name = name; self.q = queue.Queue()
    def start(self): threading.Thread(target=self._loop, daemon=True).start()
    def submit(self, func): self.q.put_nowait(func)
    def _loop(self):
        while True:
            func = self.q.get()
            try: func()
            except Exception as e: log.error(f'{self.name} 에러: {e}')
            finally: self.q.task_done()

WORKER = AsyncWorker('worker')
class LocalDB:
    def __init__(self):
        self.pos_file = 'seumin_positions.json'
        self.ai_file = 'seumin_ai_memory.json'
    def load_pos(self):
        try:
            if os.path.exists(self.pos_file):
                with open(self.pos_file, 'r', encoding='utf-8') as f: return json.load(f)
        except: pass
        return {}
    def save_pos(self, data):
        WORKER.submit(lambda: json.dump(data, open(self.pos_file, 'w', encoding='utf-8'), ensure_ascii=False))
    def load_ai(self):
        try:
            if os.path.exists(self.ai_file): return json.load(open(self.ai_file, 'r', encoding='utf-8')).get('picks', '데이터 없음')
        except: pass
        return "데이터 없음"
    def save_ai(self, text):
        WORKER.submit(lambda: json.dump({'picks': text}, open(self.ai_file, 'w', encoding='utf-8'), ensure_ascii=False))

DB = LocalDB()
class Telegram:
    def __init__(self): self.s = requests.Session(); self.offset = 0; self.handler = None
    def send(self, text, chat=None): 
        WORKER.submit(lambda: self.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/sendMessage', json={'chat_id': str(chat or SETTINGS.chat_id).strip(), 'text': text, 'parse_mode': 'HTML'}, timeout=10))
    def poll(self):
        while True:
            try:
                r = self.s.get(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/getUpdates', params={'timeout': 25, 'offset': self.offset}, timeout=35)
                if r.status_code == 409: time.sleep(20); continue
                for u in r.json().get('result', []):
                    self.offset = max(self.offset, int(u['update_id']) + 1)
                    text = u.get('message', {}).get('text', '').strip(); chat = u.get('message', {}).get('chat', {}).get('id')
                    if text and chat and self.handler: threading.Thread(target=self.handler, args=(text, chat), daemon=True).start()
            except: time.sleep(5)
BOT = Telegram()

# ===== 4. 쾌걸스민 상태 및 KIS API =====
class SeuminState:
    def __init__(self):
        self.lock = threading.RLock(); self.names = {}; self.meta = {}; self.candidates = []
        self.bars = {}; self.top_20_list = []; self.circuit_breaker = False
        # 시장 지수 추적용 ETF (KODEX 200, KODEX 코스닥150)
        self.index_etfs = {'069500': 'KOSPI', '226490': 'KOSDAQ'} 
        self.index_opens = {'069500': 0, '226490': 0}

    def load_candidates(self):
        try:
            df = pd.concat([fdr.StockListing('KOSPI'), fdr.StockListing('KOSDAQ')], ignore_index=True)
            with self.lock:
                valid = []
                for _, r in df.iterrows():
                    c = str(r.get('Code') or r.get('Symbol')).zfill(6); n = str(r.get('Name') or c)
                    if c in ['000000', 'nan']: continue
                    self.names[c] = n; self.meta[c] = r
                    cap = num(r.get('Marcap')); vol = num(r.get('Volume'))
                    if cap < SETTINGS.min_market_cap or vol < SETTINGS.min_daily_volume: continue
                    amt = num(r.get('Amount')) or (num(r.get('Close')) * vol)
                    valid.append((c, amt, amt / cap if cap else 0))
                
                valid.sort(key=lambda x: x[1], reverse=True); amt_r = {x[0]: i for i, x in enumerate(valid)}
                valid.sort(key=lambda x: x[2], reverse=True); turn_r = {x[0]: i for i, x in enumerate(valid)}
                scores = [(amt_r[c]*0.5 + turn_r[c]*0.5, c, amt, turn) for c, amt, turn in valid]
                scores.sort()
                
                self.top_20_list = [(c, self.names[c]) for _, c, _, _ in scores[:20]]
                self.candidates = list(self.index_etfs.keys()) + [c for _, c, _, _ in scores[:198]]
        except Exception as e: log.error(f"FDR 로드 실패: {e}")

STATE = SeuminState()

class PositionEngine:
    def __init__(self): self.data = {}
    def load(self): self.data = {c: Position(**v) for c, v in DB.load_pos().items()}
    def save(self): DB.save_pos({c: vars(p) for c, p in self.data.items()})
    def register(self, c, n, p, q):
        self.data[c] = Position(n, p, q, p, p * 0.98) # 기본 -2% 손절
        self.save(); return self.data[c]
    def remove(self, c):
        p = self.data.pop(c, None); self.save(); return p
    def check_trailing(self, c, current):
        p = self.data.get(c); msg = None
        if not p: return None
        p.highest = max(p.highest, current)
        gain = pct(current, p.entry)
        
        # 쾌걸스민 동적 트레일링 스탑
        if gain >= 6: p.stop = max(p.stop, p.highest * 0.97) # 고점 대비 -3% 익절
        elif gain >= 3: p.stop = max(p.stop, p.entry * 1.01) # 본전 확보
        
        if current <= p.stop:
            msg = f"🛡️ <b>[쾌걸스민 자동 청산] {p.name}</b>\n💰 현재가: {current:,.0f}원 ({gain:+.2f}%)\n지정된 방어선을 이탈하여 감시 종료합니다."
            self.remove(c)
        else: self.save()
        return msg

POSITIONS = PositionEngine()

class KIS:
    def __init__(self):
        self.rest = 'https://openapi.koreainvestment.com:9443'
        self.ws = 'ws://ops.koreainvestment.com:21000'
        self.s = requests.Session(); self.token = None; self.token_exp = None; self.approval = None
    
    def auth(self):
        if not self.token or now() > self.token_exp:
            r = self.s.post(f'{self.rest}/oauth2/tokenP', json={'grant_type': 'client_credentials', 'appkey': SETTINGS.kis_app_key, 'appsecret': SETTINGS.kis_app_secret}).json()
            self.token = r['access_token']; self.token_exp = now() + timedelta(seconds=int(r['expires_in'])-300)
        return self.token
    
    def get_bars(self, code):
        try:
            r = self.s.get(f'{self.rest}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice', headers={'authorization': f'Bearer {self.auth()}', 'appkey': SETTINGS.kis_app_key, 'appsecret': SETTINGS.kis_app_secret, 'tr_id': 'FHKST03010200', 'custtype': 'P'}, params={'FID_ETC_CLS_CODE': '', 'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': code, 'FID_INPUT_HOUR_1': '153000', 'FID_PW_DATA_INCU_YN': 'Y'})
            return r.json().get('output2') or []
        except: return []

    def stream(self, on_tick):
        while True:
            try:
                appr = self.s.post(f'{self.rest}/oauth2/Approval', json={'grant_type': 'client_credentials', 'appkey': SETTINGS.kis_app_key, 'secretkey': SETTINGS.kis_app_secret}).json()['approval_key']
                def on_msg(ws, msg):
                    if isinstance(msg, bytes): msg = msg.decode('utf-8')
                    if 'PINGPONG' in msg: ws.send(msg); return
                    p = msg.split('|')
                    if len(p) >= 4 and p[1] == SETTINGS.nxt_trade_tr:
                        f = p[3].split('^'); c = f[0].zfill(6)
                        ts = now().replace(hour=int(f[1][:2]), minute=int(f[1][2:4]), second=0, microsecond=0)
                        on_tick(c, num(f[2]), int(f[12] or 0), num(f[18]), ts)
                        
                ws = websocket.WebSocketApp(self.ws, on_open=lambda w: [w.send(json.dumps({'header': {'approval_key': appr, 'custtype': 'P', 'tr_type': '1', 'content-type': 'utf-8'}, 'body': {'input': {'tr_id': SETTINGS.nxt_trade_tr, 'tr_key': c}}})) for c in STATE.candidates[:40]], on_message=on_msg)
                ws.run_forever(ping_interval=25, ping_timeout=10)
            except: time.sleep(10)

KIS_CLIENT = KIS()

# ===== 5. 쾌걸스민 듀얼 코어 두뇌 (Brain) =====
class Brain:
    def check_sniper(self, c):
        bars = list(STATE.bars.get(c, []))
        if len(bars) < 15: return None
        latest = bars[-1]; today_open = bars[0].open
        
        # 1. 추세: 아침부터 쳐박히는 역배열 잡주 컷 (양전 상태 유지)
        if latest.close <= today_open: return None
        
        # 2. VWAP(당일평균가) 계산 및 맥점 도달 확인 (±0.5%)
        tot_vol = sum(b.volume for b in bars)
        if tot_vol == 0: return None
        vwap = sum((b.high+b.low+b.close)/3 * b.volume for b in bars) / tot_vol
        if not (vwap * 0.995 <= latest.close <= vwap * 1.005): return None
        
        # 3. 거래량 가뭄: 누군가 고점에서 안 팔고 개미만 털렸는가?
        max_5m_vol = max(sum(b.volume for b in bars[i:i+5]) for i in range(len(bars)-5))
        recent_5m_vol = sum(b.volume for b in bars[-5:])
        if max_5m_vol == 0 or recent_5m_vol > (max_5m_vol * 0.20): return None
        
        # 4. 수급 브레이크: VWAP 닿자마자 시장가로 긁어모으는가?
        if latest.trade_strength < 105: return None
        
        return latest.close, vwap * 0.985 # 매수가, 동적 손절가(-1.5% 이탈 시)

    def get_closing_bets(self):
        picks = []
        for c in STATE.candidates[2:102]: # 상위 100개만 스캔
            bars = list(STATE.bars.get(c, []))
            if len(bars) < 300: continue
            day_high = max(b.high for b in bars); day_low = min(b.low for b in bars)
            latest = bars[-1].close
            
            # 조건 1. 당일 고가권 상위 15% 이내 버티기
            if latest < day_low + (day_high - day_low) * 0.85: continue
            
            # 조건 2. 오후 2시 이후 거래량 부활 (12~14시 점심 거래량의 1.5배)
            vol_lunch = sum(b.volume for b in bars if 12 <= b.minute.hour < 14)
            vol_after = sum(b.volume for b in bars if b.minute.hour >= 14)
            if vol_lunch > 0 and vol_after < (vol_lunch * 1.5): continue
            
            # 조건 3. 일봉 정배열 확인 (5일선 > 20일선)
            try:
                df = fdr.DataReader(c, start=now().date() - timedelta(days=40))
                if len(df) < 20: continue
                ma5 = df['Close'].rolling(5).mean().iloc[-1]
                ma20 = df['Close'].rolling(20).mean().iloc[-1]
                if ma5 <= ma20: continue
                picks.append((c, STATE.names.get(c, c), latest))
            except: continue
            if len(picks) >= 3: break
        return picks

BRAIN = Brain()

# ===== 6. 메인 스케줄러 & 앱 =====
class App:
    def __init__(self): self.sent = set(); self.today = ""
    
    def sync_bars(self):
        log.info("과거 차트 강제 동기화 (VWAP 복원)")
        n = now()
        for c in STATE.candidates[:40]:
            raw = KIS_CLIENT.get_bars(c)
            with STATE.lock:
                q = STATE.bars.setdefault(c, deque(maxlen=390))
                for r in reversed(raw):
                    m = n.replace(hour=int(r['stck_cntg_hour'][:2]), minute=int(r['stck_cntg_hour'][2:4]), second=0, microsecond=0)
                    q.append(Bar(m, num(r['stck_oprc']), num(r['stck_hgpr']), num(r['stck_lwpr']), num(r['stck_prpr']), int(r['cntg_vol']), 100))
                    # 지수 ETF 시가 저장
                    if c in STATE.index_etfs and STATE.index_opens[c] == 0: STATE.index_opens[c] = num(r['stck_oprc'])

    def on_tick(self, c, p, v, ts, m):
        with STATE.lock:
            q = STATE.bars.setdefault(c, deque(maxlen=390))
            if not q or q[-1].minute != m: q.append(Bar(m, p, p, p, p, v, ts))
            else:
                b = q[-1]; b.high = max(b.high, p); b.low = min(b.low, p); b.close = p; b.volume += v; b.trade_strength = ts
        
        # 지수 추적 및 서킷 브레이커 판단 (ETF 기준)
        if c in STATE.index_etfs and STATE.index_opens[c] > 0:
            drop_pct = pct(p, STATE.index_opens[c])
            if drop_pct <= SETTINGS.circuit_breaker_pct and not STATE.circuit_breaker:
                STATE.circuit_breaker = True
                BOT.send(f"🚨 <b>[시장 서킷 브레이커 발동]</b>\n{STATE.index_etfs[c]} 지수 폭락 감지 ({drop_pct:.2f}%).\n당일 단타 스나이퍼 모드를 전면 중단하고 관망합니다.")
        
        # 트레일링 스탑 감시
        msg = POSITIONS.check_trailing(c, p)
        if msg: BOT.send(msg)

        # 단타 스나이퍼 (9시~14시30분, 폭락장 아닐 때만)
        if 9 <= m.hour < 14 or (m.hour == 14 and m.minute < 30):
            if not STATE.circuit_breaker and c not in POSITIONS.data and c not in self.sent:
                res = BRAIN.check_sniper(c)
                if res:
                    price, vwap_stop = res
                    self.sent.add(c)
                    BOT.send(f"🔫 <b>[쾌걸스민 스나이퍼 포착] {STATE.names.get(c)}</b> ({c})\n\n💰 <b>현재가(VWAP 맥점):</b> {price:,.0f}원\n🛡️ <b>동적 손절선:</b> {vwap_stop:,.0f}원 (이탈 시 즉각 대피)\n\n⚡ <b>사유:</b> 폭발적 수급 + 거래량 가뭄 완벽 교집합")

    def run_night_ai(self):
        top_txt = ", ".join([n for _, n in STATE.top_20_list]) or "데이터 없음"
        if not HAS_GEMINI or not SETTINGS.gemini_api_key:
            DB.save_ai(top_txt); BOT.send(f"🌙 <b>[야간 브리핑]</b>\n오늘 주도주: {top_txt}\n(AI 생략)")
            return
        try:
            genai.configure(api_key=SETTINGS.gemini_api_key)
            res = genai.GenerativeModel('gemini-2.5-flash').generate_content(f"오늘 한국 주도주야: {top_txt}\n최고의 퀀트 트레이더로서 이 중 내일 튈 대장주 3개만 이유와 함께 분석해.")
            DB.save_ai(res.text); BOT.send(f"🌙 <b>[야간 AI 브리핑]</b>\n\n{res.text}")
        except Exception as e:
            DB.save_ai(top_txt); BOT.send(f"🌙 <b>[야간 오류]</b>\n원시 데이터: {top_txt}\n에러: {e}")

    def run_morning_ai(self):
        try: us = "\n".join([f"{n}: {(yf.Ticker(s).history(period='2d')['Close'].iloc[-1] / yf.Ticker(s).history(period='2d')['Close'].iloc[-2] - 1)*100:+.2f}%" for n, s in {'S&P500':'^GSPC', 'Nasdaq':'^IXIC'}.items()])
        except: us = "미증시 로드 실패"
        saved = DB.load_ai()
        
        if not HAS_GEMINI or not SETTINGS.gemini_api_key:
            BOT.send(f"☀️ <b>[아침 브리핑]</b>\n미증시:\n{us}\n\n어제 픽:\n{saved}")
            return
        try:
            genai.configure(api_key=SETTINGS.gemini_api_key)
            res = genai.GenerativeModel('gemini-2.5-flash').generate_content(f"미증시: {us}\n어제픽: {saved}\n미증시 반영해서 오늘 아침 최종 단타 관심주 3개 압축해.")
            BOT.send(f"☀️ <b>[아침 AI 최종 브리핑]</b>\n\n{res.text}")
        except Exception as e:
            BOT.send(f"☀️ <b>[아침 오류]</b>\n미증시: {us}\n어제픽: {saved}")

    def scheduler(self):
        done = {}
        while True:
            n = now(); d = str(n.date())
            if self.today != d: self.today = d; self.sent.clear(); STATE.circuit_breaker = False # 매일 초기화
            
            if n.weekday() < 5:
                # 07시: 아침 브리핑
                if n.hour == 7 and done.get('m_ai') != d: WORKER.submit(self.run_morning_ai); done['m_ai'] = d
                # 09시: VWAP 동기화
                if n.hour == 9 and done.get('sync') != d: WORKER.submit(self.sync_bars); done['sync'] = d
                # 15시 15분: 종배 픽
                if n.hour == 15 and n.minute >= 15 and done.get('close') != d:
                    picks = BRAIN.get_closing_bets()
                    txt = "\n".join([f"• {n} ({c}): {p:,.0f}원" for c, n, p in picks]) or "조건 만족 종목 없음"
                    BOT.send(f"🌇 <b>[쾌걸스민 종가배팅 3선]</b>\n\n{txt}\n\n💡 동시호가 전 진입 고려"); done['close'] = d
                # 22시: 야간 브리핑
                if n.hour == 22 and done.get('n_ai') != d: WORKER.submit(self.run_night_ai); done['n_ai'] = d
            time.sleep(20)

    def handle(self, txt, chat):
        p = txt.split(); cmd = p[0]
        if cmd == '/상태': BOT.send(f"🤖 쾌걸스민 V2.0 작동중\n- 듀얼코어 활성\n- 서킷브레이커: {'발동됨🚨' if STATE.circuit_breaker else '정상녹색지대🟢'}\n- 금일 스나이퍼 발송: {len(self.sent)}건", chat)
        elif cmd == '/매수' and len(p) >= 4:
            qty = num(p[-1]); price = num(p[-2]); c = "".join(p[1:-2])
            for code, name in STATE.names.items():
                if c in name or c == code:
                    pos = POSITIONS.register(code, name, price, qty)
                    BOT.send(f"✅ <b>[방어선 등록] {name}</b>\n초기 칼손절선: {pos.stop:,.0f}원 설정됨.", chat); return
            BOT.send("❌ 종목을 찾을 수 없습니다.", chat)
        elif cmd == '/매도' and len(p) >= 2:
            c = "".join(p[1:])
            for code, name in STATE.names.items():
                if c in name or c == code:
                    if POSITIONS.remove(code): BOT.send(f"✅ {name} 감시 강제 종료.", chat)
                    return
        elif cmd == '/보유':
            if not POSITIONS.data: BOT.send("📭 관리 중인 종목이 없습니다.", chat)
            else:
                msg = "💼 <b>[내 계좌 방어 현황]</b>\n\n"
                for c, pos in POSITIONS.data.items():
                    curr = STATE.bars[c][-1].close if STATE.bars.get(c) else pos.entry
                    gain = pct(curr, pos.entry)
                    msg += f"• <b>{pos.name}</b> (진입: {pos.entry:,.0f})\n  현재: {curr:,.0f} ({gain:+.2f}%) | 🛡️ 방어: {pos.stop:,.0f}\n\n"
                BOT.send(msg, chat)

    def start(self):
        WORKER.start(); POSITIONS.load(); BOT.handler = self.handle
        threading.Thread(target=BOT.poll, daemon=True).start()
        threading.Thread(target=self.scheduler, daemon=True).start()
        STATE.load_candidates()
        if in_session("08:00", "15:30"): self.sync_bars()
        threading.Thread(target=KIS_CLIENT.stream, args=(self.on_tick,), daemon=True).start()
        BOT.send(f"🚀 <b>쾌걸스민 V{SETTINGS.version} 기동 완료!</b>\n스윙 적출 완료 / 단타·종배 듀얼 엔진 가동 / 시장 폭락 방어막 장착")

APP = App(); web = Flask(__name__)
@web.get('/')
def root(): return 'Kkwaegeol Seumin V2.0 Running', 200

if __name__ == '__main__':
    threading.Thread(target=lambda: (time.sleep(2), APP.start()), daemon=True).start()
    web.run(host='0.0.0.0', port=SETTINGS.port, threaded=True, use_reloader=False)
