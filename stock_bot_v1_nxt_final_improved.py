from __future__ import annotations

# ==========================================
# 🤖 이핀로봇 V1.0.3 (Epin Robot - Holdings Price Patch)
# 패치 내역: /보유 명령어 호출 시 현재가 및 실시간 수익률(%) 표시 기능 추가
# ==========================================

import os, json, time, threading, queue, logging, math
from datetime import datetime, timedelta, timezone
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable

import requests, websocket
import FinanceDataReader as fdr
import yfinance as yf
from flask import Flask, jsonify
from zoneinfo import ZoneInfo

# 구글 Gemini AI 연동
try:
    import google.generativeai as genai
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

# ===== 1. 설정 및 기본 유틸리티 =====
KST = ZoneInfo('Asia/Seoul')
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(threadName)s | %(message)s')
log = logging.getLogger('epin.main')

def now(): return datetime.now(KST)
def b(n, d): return os.getenv(n, str(d)).lower() in {'1', 'true', 'yes', 'on'}
def i(n, d): 
    try: return int(os.getenv(n, str(d)))
    except: return d
def f(n, d): 
    try: return float(os.getenv(n, str(d)))
    except: return d

def num(v, d=0.0):
    try:
        s = str(v).replace(',', '').replace('원', '').replace('주', '').strip()
        return float(s)
    except: return d

def pct(n, o): return (n / o - 1) * 100 if o else 0

@dataclass(frozen=True)
class Settings:
    version: str = '1.0.3 (Holdings Price Patch)'
    telegram_token: str = os.getenv('TELEGRAM_TOKEN', '').strip()
    chat_id: str = (os.getenv('CHAT_ID') or os.getenv('TELEGRAM_CHAT_ID') or '').strip()
    gemini_api_key: str = os.getenv('GEMINI_API_KEY', '').strip()
    kis_app_key: str = os.getenv('KIS_APP_KEY', '').strip()
    kis_app_secret: str = os.getenv('KIS_APP_SECRET', '').strip()
    kis_env: str = os.getenv('KIS_ENV', 'real').strip().lower()
    port: int = i('PORT', 10000)
    max_candidates: int = i('MAX_CANDIDATES', 200)
    ws_trade_limit: int = i('WS_TRADE_LIMIT', 6)
    ws_orderbook_limit: int = i('WS_ORDERBOOK_LIMIT', 1)
    ws_total_limit: int = i('WS_TOTAL_LIMIT', 7)
    render_start_delay: int = i('RENDER_START_DELAY', 120)
    min_market_cap: float = f('MIN_MARKET_CAP', 100_000_000_000)
    min_daily_volume: int = i('MIN_DAILY_VOLUME', 20_000)
    nxt_start: str = '08:00'
    nxt_end: str = '20:00'
    nxt_trade_tr: str = os.getenv('KIS_NXT_TRADE_TR_ID', 'H0NXCNT0')
    nxt_order_tr: str = os.getenv('KIS_NXT_ORDERBOOK_TR_ID', 'H0UNASP0')

SETTINGS = Settings()

# ===== 2. 데이터 모델 =====
@dataclass
class Tick: code: str; name: str; market: str; price: float; volume: int; cumulative_volume: int; trade_strength: float; timestamp: datetime
@dataclass
class Bar: code: str; name: str; market: str; minute: datetime; open: float; high: float; low: float; close: float; volume: int; cumulative_volume: int; trade_strength: float
@dataclass
class Position: code: str; name: str; kind: str; entry: float; qty: float; highest: float; stop: float; state: str = '감시중'

# ===== 3. 비동기 큐 & 초경량 로컬 DB =====
class AsyncWorker:
    def __init__(self, name):
        self.name = name
        self.q = queue.Queue(maxsize=1000)
    def start(self): threading.Thread(target=self._loop, daemon=True, name=self.name).start()
    def submit(self, func, *args, **kwargs):
        try: self.q.put_nowait((func, args, kwargs)); return True
        except queue.Full: return False
    def _loop(self):
        while True:
            func, args, kwargs = self.q.get()
            try: func(*args, **kwargs)
            except Exception as e: log.warning(f'{self.name} 에러: {e}')
            finally: self.q.task_done()

TELEGRAM_WORKER = AsyncWorker('telegram-worker')
DB_WORKER = AsyncWorker('db-worker')

class LocalDB:
    def __init__(self):
        self.filepath = 'epin_positions.json'
        self.last_write_ok = True
    def load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f: return json.load(f)
            except: pass
        return {}
    def _save_sync(self, data):
        try:
            with open(self.filepath, 'w', encoding='utf-8') as f: json.dump(data, f, ensure_ascii=False, indent=2)
            self.last_write_ok = True
        except Exception as e: self.last_write_ok = False
    def save(self, data):
        DB_WORKER.submit(self._save_sync, data)

DATABASE = LocalDB()

class Telegram:
    def __init__(self):
        self.s = requests.Session(); self.offset = 0; self.handler = None
    def _send_sync(self, text, chat=None):
        target = str(chat or SETTINGS.chat_id).strip()
        if not SETTINGS.telegram_token or not target: return
        try: self.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/sendMessage', json={'chat_id': target, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': True}, timeout=10)
        except Exception as e: log.warning(f'TG Send: {e}')
    def send(self, text, chat=None): TELEGRAM_WORKER.submit(self._send_sync, text, chat)
    def poll(self):
        if not SETTINGS.telegram_token: return
        while True:
            try:
                r = self.s.get(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/getUpdates', params={'timeout': 25, 'offset': self.offset}, timeout=35)
                if r.status_code == 409: time.sleep(20); continue
                r.raise_for_status()
                for u in r.json().get('result', []):
                    self.offset = max(self.offset, int(u['update_id']) + 1)
                    m = u.get('message') or {}; text = str(m.get('text') or '').strip(); chat = str((m.get('chat') or {}).get('id') or '')
                    if text and chat and self.handler: threading.Thread(target=self.handler, args=(text, chat), daemon=True).start()
            except: time.sleep(10)

BOT = Telegram()

# ===== 4. KIS 웹소켓 인프라 =====
def in_session(start_hhmm, end_hhmm):
    n = now()
    if n.weekday() >= 5: return False
    try:
        sh, sm = map(int, start_hhmm.split(':')); eh, em = map(int, end_hhmm.split(':'))
        return n.replace(hour=sh, minute=sm, second=0, microsecond=0) <= n <= n.replace(hour=eh, minute=em, second=0, microsecond=0)
    except: return False

class KIS:
    def __init__(self):
        self.rest = 'https://openapivts.koreainvestment.com:29443' if SETTINGS.kis_env == 'virtual' else 'https://openapi.koreainvestment.com:9443'
        self.ws = 'ws://ops.koreainvestment.com:31000' if SETTINGS.kis_env == 'virtual' else 'ws://ops.koreainvestment.com:21000'
        self.s = requests.Session(); self.token = None; self.token_exp = None; self.approval = None; self.approval_exp = None; self.lock = threading.Lock()
    
    def access(self):
        if self.token and self.token_exp and datetime.now(timezone.utc) < self.token_exp: return self.token
        r = self.s.post(f'{self.rest}/oauth2/tokenP', json={'grant_type': 'client_credentials', 'appkey': SETTINGS.kis_app_key, 'appsecret': SETTINGS.kis_app_secret}, timeout=20)
        r.raise_for_status(); d = r.json(); self.token = d['access_token']; self.token_exp = datetime.now(timezone.utc) + timedelta(seconds=max(60, int(d.get('expires_in', 86400)) - 300)); return self.token
    
    def approval_key(self):
        with self.lock:
            if self.approval and self.approval_exp and datetime.now(timezone.utc) < self.approval_exp: return self.approval
            r = self.s.post(f'{self.rest}/oauth2/Approval', json={'grant_type': 'client_credentials', 'appkey': SETTINGS.kis_app_key, 'secretkey': SETTINGS.kis_app_secret}, timeout=20)
            r.raise_for_status(); self.approval = r.json()['approval_key']; self.approval_exp = datetime.now(timezone.utc) + timedelta(hours=12); return self.approval

    def get_minute_bars(self, code):
        try:
            t = self.access()
            r = self.s.get(f'{self.rest}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice', headers={'authorization': f'Bearer {t}', 'appkey': SETTINGS.kis_app_key, 'appsecret': SETTINGS.kis_app_secret, 'tr_id': 'FHKST03010200', 'custtype': 'P'}, params={'FID_ETC_CLS_CODE': '', 'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': code, 'FID_INPUT_HOUR_1': '153000', 'FID_PW_DATA_INCU_YN': 'Y'}, timeout=10)
            if r.status_code == 200: return r.json().get('output2') or []
        except: pass
        return []

    def stream(self, codes_fn, names, on_tick, state):
        retry = 5
        while True:
            if not in_session(SETTINGS.nxt_start, SETTINGS.nxt_end):
                state['ws_nxt'] = 'waiting_market_session'
                time.sleep(30); continue

            refresh_stop = threading.Event(); send_lock = threading.Lock()
            requested = set()
            try:
                key = self.approval_key()
                state['ws_nxt'] = 'connecting'

                def send_sub(ws, tr, code, tr_type='1'):
                    payload = {'header': {'approval_key': key, 'custtype': 'P', 'tr_type': tr_type, 'content-type': 'utf-8'}, 'body': {'input': {'tr_id': tr, 'tr_key': code}}}
                    with send_lock: ws.send(json.dumps(payload, ensure_ascii=False))

                def sync_nxt(ws):
                    if not in_session(SETTINGS.nxt_start, SETTINGS.nxt_end):
                        refresh_stop.set(); ws.close(); return
                    trade_codes = codes_fn()[:SETTINGS.ws_trade_limit]
                    wanted = set(trade_codes)
                    for c in list(requested - wanted): send_sub(ws, SETTINGS.nxt_trade_tr, c, '2'); requested.discard(c); time.sleep(0.3)
                    for c in trade_codes:
                        if c not in requested: send_sub(ws, SETTINGS.nxt_trade_tr, c, '1'); requested.add(c); time.sleep(0.3)
                    state['ws_nxt'] = 'connected'

                def refresh_loop(ws):
                    while not refresh_stop.wait(30):
                        try: sync_nxt(ws)
                        except: break

                def message(ws, msg):
                    if isinstance(msg, (bytes, bytearray)): msg = msg.decode('utf-8', errors='ignore')
                    if msg.startswith('{'):
                        if 'PINGPONG' in msg:
                            with send_lock: ws.send(msg)
                            return
                        try:
                            d = json.loads(msg)
                            body = d.get('body') or {}
                            rt_cd = str(body.get('rt_cd', '0'))
                            msg1 = str(body.get('msg1') or '')
                            if rt_cd == '9' or 'MAX SUBSCRIBE OVER' in msg1.upper() or 'ALREADY IN USE' in msg1.upper():
                                log.error(f"NXT WS 에러 감지: {msg1}")
                                refresh_stop.set()
                                ws.close()
                        except: pass
                        return

                    parts = msg.split('|', 3)
                    if len(parts) >= 2 and parts[1] == SETTINGS.nxt_trade_tr:
                        count = max(1, int(parts[2]) if parts[2].isdigit() else 1)
                        f = parts[3].split('^'); width = len(f) // count if count else len(f)
                        if width < 19: return
                        for i in range(count):
                            r = f[i*width:(i+1)*width]
                            code = str(r[0]).zfill(6)
                            ts = now().replace(hour=int(r[1][:2]), minute=int(r[1][2:4]), second=int(r[1][4:6]), microsecond=0)
                            tick = Tick(code, names.get(code, code), 'NXT', num(r[2]), int(r[12] or 0), int(r[13] or 0), num(r[18]), ts)
                            on_tick(tick)

                ws_app = websocket.WebSocketApp(self.ws, on_open=lambda w: threading.Thread(target=lambda: (time.sleep(1), sync_nxt(w), refresh_loop(w)), daemon=True).start(), on_message=message, on_error=lambda w,e: refresh_stop.set(), on_close=lambda w,s,m: refresh_stop.set())
                ws_app.run_forever(ping_interval=25, ping_timeout=10, skip_utf8_validation=True)
                refresh_stop.set()
                time.sleep(retry)
                retry = min(60, retry * 2)
            except Exception as e:
                refresh_stop.set(); time.sleep(retry); retry = min(60, retry * 2)

KIS_CLIENT = KIS()

# ===== 5. 이핀로봇 전용 메모리 & 매매 두뇌 =====
class EpinState:
    def __init__(self):
        self.lock = threading.RLock(); self.names = {}; self.meta = {}; self.candidates = {}; self.bars = {}
        self.runtime = {'ws_nxt': 'stopped'}
        
    def load(self):
        import pandas as pd
        df = None
        try: df = fdr.StockListing('KRX')
        except: pass
        
        if df is None or df.empty:
            try:
                df_kospi = fdr.StockListing('KOSPI')
                df_kosdaq = fdr.StockListing('KOSDAQ')
                df = pd.concat([df_kospi, df_kosdaq], ignore_index=True)
            except Exception as e:
                log.error(f"종목 리스트 우회 로드 실패: {e}")
                return

        with self.lock:
            for _, r in df.iterrows():
                c = str(r.get('Code') or r.get('Symbol') or '').strip().zfill(6)
                n = str(r.get('Name') or c).strip()
                if c and c != '000000' and c.lower() != 'nan':
                    self.names[c] = n
                    self.meta[c] = {str(k): r[k] for k in df.columns}

    def refresh_candidates(self):
        rows = []
        with self.lock:
            for c, m in self.meta.items():
                if num(m.get('Marcap')) < SETTINGS.min_market_cap or num(m.get('Volume')) < SETTINGS.min_daily_volume: continue
                rows.append((num(m.get('Amount')) or num(m.get('Close')) * num(m.get('Volume')), c))
            rows.sort(reverse=True)
            # 보유 종목은 감시 후보 최우선 순위에 자동 편입하여 NXT 실시간 수신 보장
            priority = list(POSITIONS.data.keys())
            self.candidates = priority + [c for _, c in rows if c not in priority][:SETTINGS.max_candidates]

    def get_hot_codes(self):
        with self.lock:
            # 보유 종목은 무조건 NXT 감시 목록에 포함되도록 최우선 배치
            priority = list(POSITIONS.data.keys())
            hot = priority + [c for c in self.candidates if c not in priority]
            return hot[:SETTINGS.ws_trade_limit]

STATE = EpinState()

def resolve_code(q):
    if not q: return None
    q_clean = str(q).replace(' ', '').strip().lower()
    if not q_clean: return None
    if q_clean.isdigit(): return q_clean.zfill(6)
    with STATE.lock:
        for code, name in STATE.names.items():
            if str(name).replace(' ', '').strip().lower() == q_clean:
                return code
        for code, name in STATE.names.items():
            if q_clean in str(name).replace(' ', '').strip().lower():
                return code
    return None

class PositionEngine:
    def __init__(self): self.data = {}
    def load(self):
        saved = DATABASE.load()
        for c, v in saved.items(): self.data[c] = Position(**v)
    def save(self):
        dump = {c: vars(p) for c, p in self.data.items()}
        DATABASE.save(dump)
    def register(self, c, n, price, qty, kind):
        stop = price * (0.97 if kind == '단타' else 0.95)
        p = Position(c, n, kind, price, qty, price, stop)
        self.data[c] = p; self.save()
        STATE.refresh_candidates() # 보유 등록 즉시 감시 순위 갱신
        return p
    def remove(self, c):
        p = self.data.pop(c, None); self.save()
        STATE.refresh_candidates()
        return p
    def evaluate_trailing_stop(self, c, current_price):
        p = self.data.get(c); msg = None
        if not p: return None
        p.highest = max(p.highest, current_price)
        gain = pct(current_price, p.entry)
        
        if gain >= 10: p.stop = max(p.stop, p.highest * 0.96)
        elif gain >= 5: p.stop = max(p.stop, p.highest * 0.97)
        elif gain >= 3: p.stop = max(p.stop, p.entry * 1.01)

        if current_price <= p.stop:
            msg = f"🛡️ <b>[이핀 방어선 이탈 익절/손절] {p.name}</b>\n💰 현재가: {current_price:,.0f}원 ({gain:+.2f}%)\n감시를 종료합니다."
            self.remove(c)
        else: self.save()
        return msg

POSITIONS = PositionEngine()

class EpinBrain:
    def check_day_trade(self, c):
        bars = list(STATE.bars.get(c, []))
        if len(bars) < 15: return None
        latest = bars[-1]

        prev_10 = bars[-11:-1]
        avg_vol = sum(b.volume for b in prev_10) / 10 if prev_10 else 1
        if avg_vol == 0: avg_vol = 1
        if latest.volume < (avg_vol * 3.0): return None

        prev_15 = bars[-16:-1]
        high_15 = max(b.high for b in prev_15) if prev_15 else latest.high
        if latest.close <= high_15: return None
        if latest.trade_strength < 100.0: return None

        predicted = latest.close * 1.05
        stop = min(b.low for b in bars[-5:])
        return c, STATE.names.get(c, c), '단타', latest.close, predicted, stop, '거래량 300% 폭발 & 15분 고점 돌파'

    def check_swing_trade(self, c):
        try:
            end = now().date() + timedelta(days=1); start = end - timedelta(days=60)
            df = fdr.DataReader(c, start, end)
            if df.empty or len(df) < 20: return None
            close = df['Close'].astype(float); open_p = df['Open'].astype(float); low = df['Low'].astype(float); vol = df['Volume'].astype(float)
            
            latest_c = close.iloc[-1]; latest_o = open_p.iloc[-1]
            latest_v = vol.iloc[-1]; prev_v = vol.iloc[-2]
            
            if latest_c <= latest_o or latest_v < (prev_v * 1.5): return None
            
            down_days = (close.iloc[-6:-1].diff().dropna() < 0).sum()
            near_bottom = latest_c <= (low.tail(20).min() * 1.05)
            
            if down_days >= 3 or near_bottom:
                predicted = latest_c * 1.10
                stop = low.tail(20).min() * 0.98
                return c, STATE.names.get(c, c), '스윙', latest_c, predicted, stop, '바닥권 확인 & 거래량 1.5배 추세 전환 양봉'
        except: pass
        return None

BRAIN = EpinBrain()

# ===== 6. 애플리케이션 및 스케줄러 =====
class App:
    def __init__(self):
        self.sent_signals = set()
    
    def on_tick(self, t):
        with STATE.lock:
            minute = t.timestamp.replace(second=0, microsecond=0)
            q = STATE.bars.setdefault(t.code, deque(maxlen=240))
            if not q or q[-1].minute != minute:
                q.append(Bar(t.code, t.name, t.market, minute, t.price, t.price, t.price, t.price, t.volume, t.cumulative_volume, t.trade_strength))
            else:
                b = q[-1]; b.high = max(b.high, t.price); b.low = min(b.low, t.price); b.close = t.price; b.volume += t.volume; b.trade_strength = t.trade_strength
        
        msg = POSITIONS.evaluate_trailing_stop(t.code, t.price)
        if msg: BOT.send(msg)

        if t.code not in POSITIONS.data and t.code not in self.sent_signals:
            sig = BRAIN.check_day_trade(t.code)
            if sig:
                c, n, k, p, pred, stop, rsn = sig
                self.sent_signals.add(c)
                BOT.send(f"🔥 <b>[이핀 {k} 포착] {n}</b> ({c})\n\n💰 <b>현재가:</b> {p:,.0f}원\n🚀 <b>금일 상승예측가:</b> {pred:,.0f}원 (+{pct(pred, p):.1f}%)\n🛡️ <b>손절가:</b> {stop:,.0f}원\n\n⚡ <b>포착 사유:</b> {rsn}\n💡 <b>전략:</b> 수급 유입 확인. 눌림목 분할 진입 추천")

    def run_swing_scan(self):
        BOT.send("🔎 <b>[이핀로봇] 오후 스윙 종배 스캔을 시작합니다...</b>")
        found = []
        for c in STATE.candidates[:40]:
            sig = BRAIN.check_swing_trade(c)
            if sig: found.append(sig)
            time.sleep(0.2)
            
        if found:
            for c, n, k, p, pred, stop, rsn in found:
                BOT.send(f"🌅 <b>[이핀 {k} 종배 추천] {n}</b> ({c})\n\n💰 <b>현재 종가:</b> {p:,.0f}원\n🚀 <b>단기 상승예측가:</b> {pred:,.0f}원 (+{pct(pred, p):.1f}%)\n🛡️ <b>손절가:</b> {stop:,.0f}원\n\n⚡ <b>포착 사유:</b> {rsn}")
        else: BOT.send("📭 오늘 기준에 맞는 확실한 스윙 종목이 포착되지 않았습니다. 현금 관망을 추천합니다.")

    def run_ai_morning_briefing(self):
        try:
            res = []
            for name, symbol in {'S&P 500': '^GSPC', 'Nasdaq': '^IXIC', 'Dow': '^DJI'}.items():
                d = yf.Ticker(symbol).history(period="2d")
                if len(d) >= 2:
                    pct_val = (d['Close'].iloc[-1] / d['Close'].iloc[-2] - 1) * 100
                    res.append(f"{'🔴' if pct_val > 0 else '🔵'} {name}: {d['Close'].iloc[-1]:,.2f} ({pct_val:+.2f}%)")
            us_market = "\n".join(res)
        except: us_market = "미국 증시 정보 로드 실패"

        if not HAS_GEMINI or not SETTINGS.gemini_api_key:
            BOT.send(f"🌎 <b>[이핀 굿모닝 브리핑]</b>\n\n{us_market}\n\n(Gemini API 키가 설정되지 않아 AI 테마 분석이 생략되었습니다. 오늘도 성공 투자하세요!)")
            return

        try:
            genai.configure(api_key=SETTINGS.gemini_api_key)
            model = genai.GenerativeModel('gemini-2.5-flash')
            prompt = f"""
            오늘의 미국 증시 요약 데이터야: {us_market}
            너는 대한민국 최고의 주식 투자 AI '이핀로봇'이야. 
            위의 데이터를 바탕으로 오늘 한국 증시에 미칠 영향을 2줄로 요약하고, 
            오늘 단타나 스윙으로 접근하기 좋은 한국 주식 섹터나 테마, 그리고 가상의 추천 종목 3가지를 제시해줘.
            
            포맷:
            🤖 **[이핀로봇 AI 시장 전망]**
            (전망 내용)
            
            🎯 **[오늘의 AI 추천 섹터 & 종목 3선]**
            1. (종목명) - (이유)
            2. (종목명) - (이유)
            3. (종목명) - (이유)
            """
            response = model.generate_content(prompt)
            BOT.send(f"🌎 <b>[이핀 AI 굿모닝 브리핑]</b>\n\n{us_market}\n\n{response.text}")
        except Exception as e:
            BOT.send(f"🌎 <b>[이핀 굿모닝 브리핑]</b>\n\n{us_market}\n\n(AI 브리핑 생성 중 오류가 발생했습니다: {e})")

    def scheduler(self):
        sent = {}
        while True:
            n = now(); d = str(n.date())
            if n.weekday() < 5:
                if n.hour == 7 and 30 <= n.minute < 35 and sent.get('ai') != d:
                    threading.Thread(target=self.run_ai_morning_briefing, daemon=True).start(); sent['ai'] = d
                if n.hour == 9 and 0 <= n.minute < 5 and sent.get('bars') != d:
                    for c in STATE.candidates[:20]:
                        raw = KIS_CLIENT.get_minute_bars(c)
                        if raw:
                            q = STATE.bars.setdefault(c, deque(maxlen=240))
                            for r in reversed(raw):
                                minute = n.replace(hour=int(r['stck_cntg_hour'][:2]), minute=int(r['stck_cntg_hour'][2:4]), second=0, microsecond=0)
                                q.append(Bar(c, STATE.names.get(c,c), 'KRX', minute, num(r['stck_oprc']), num(r['stck_hgpr']), num(r['stck_lwpr']), num(r['stck_prpr']), int(r['cntg_vol']), int(r['acml_vol']), 100))
                    sent['bars'] = d; self.sent_signals.clear()
                if n.hour == 15 and 15 <= n.minute < 20 and sent.get('swing') != d:
                    threading.Thread(target=self.run_swing_scan, daemon=True).start(); sent['swing'] = d
            time.sleep(20)

    def handle(self, text, chat):
        p = text.split(); cmd = p[0]
        if cmd == '/상태': 
            BOT.send(f"🤖 <b>이핀로봇 V1 가동중</b>\n- NXT 연결: {STATE.runtime['ws_nxt']}\n- 감시 후보: {len(STATE.candidates)}개\n- 관리중인 보유종목: {len(POSITIONS.data)}개\n- 오늘 단타 포착: {len(self.sent_signals)}회", chat)
        
        elif cmd == '/매수':
            if len(p) < 4:
                BOT.send("사용법: /매수 [종목명] [가격] [수량] [단타|스윙(선택)]\n예: /매수 삼성전자 74000원 10주", chat)
                return
            args = p[1:]
            kind = '단타'
            if args and args[-1] in ('단타', '스윙'):
                kind = args.pop()
            if len(args) < 3:
                BOT.send("사용법: /매수 [종목명] [가격] [수량]\n예: /매수 LS ELECTRIC 74000 10", chat)
                return
            
            qty = num(args[-1])
            price = num(args[-2])
            stock_raw = " ".join(args[:-2])
            c = resolve_code(stock_raw)
            
            if c and price > 0 and qty > 0:
                p_obj = POSITIONS.register(c, STATE.names.get(c, c), price, qty, kind)
                BOT.send(f"✅ <b>[이핀 매수 등록 완료] {p_obj.name}</b> ({c})\n\n💰 매수가: {price:,.0f}원\n📦 수량: {qty:g}주\n📌 유형: {kind}\n🛡️ 봇이 동적 트레일링 스탑 관리를 시작합니다.", chat)
            else:
                BOT.send(f"❌ <b>등록 실패</b>\n종목명('<b>{stock_raw}</b>') 또는 가격({price:,.0f}), 수량({qty})을 확인해 주세요.", chat)
                
        elif cmd == '/매도' and len(p) >= 2:
            stock_raw = " ".join(p[1:])
            c = resolve_code(stock_raw)
            p_obj = POSITIONS.remove(c) if c else None
            if p_obj:
                BOT.send(f"✅ <b>{p_obj.name}</b> 감시를 강제 종료했습니다.", chat)
            else:
                BOT.send(f"❌ '<b>{stock_raw}</b>' 종목을 관리 목록에서 찾을 수 없습니다.", chat)
                
        elif cmd == '/보유':
            if not POSITIONS.data:
                BOT.send("📭 이핀로봇이 관리 중인 종목이 없습니다.", chat)
            else:
                msg = "💼 <b>[이핀 보유 관리 현황]</b>\n\n"
                for p_obj in POSITIONS.data.values():
                    # 실시간 현재가 조회 (메모리에 저장된 가장 최근 분봉 캔들 종가 기준)
                    bars = STATE.bars.get(p_obj.code)
                    curr_price = bars[-1].close if bars else p_obj.entry
                    gain = pct(curr_price, p_obj.entry)
                    emoji = "🟢" if gain >= 0 else "🔴"
                    
                    msg += f"• <b>{p_obj.name}</b> ({p_obj.code}) · {p_obj.kind}\n"
                    msg += f"  └ 매수가: {p_obj.entry:,.0f}원 | 현재가: <b>{curr_price:,.0f}원</b> ({emoji} <b>{gain:+.2f}%</b>)\n"
                    msg += f"  └ 수량: {p_obj.qty:g}주 | 🛡️ 손절선: {p_obj.stop:,.0f}원\n\n"
                BOT.send(msg, chat)
        else: 
            BOT.send("명령어: /상태, /보유, /매수 [종목] [가격] [수량], /매도 [종목]", chat)

    def start(self):
        TELEGRAM_WORKER.start(); DB_WORKER.start(); POSITIONS.load()
        BOT.handler = self.handle; threading.Thread(target=BOT.poll, daemon=True).start()
        threading.Thread(target=self.scheduler, daemon=True).start()
        STATE.load(); STATE.refresh_candidates()
        threading.Thread(target=KIS_CLIENT.stream, args=(STATE.get_hot_codes, STATE.names, self.on_tick, STATE.runtime), daemon=True).start()
        BOT.send(f"🚀 <b>이핀로봇 V{SETTINGS.version} 기동 완료!</b>\n\n- 실시간 현재가 및 수익률 연동 완료\n- 초경량 수급 폭발 감시 활성화\n- AI 모닝 브리핑 탑재 완료")

APP = App()

# ===== 7. 웹 서버 =====
web = Flask(__name__)
@web.get('/')
def root(): return f'Epin Robot V{SETTINGS.version} running', 200
@web.get('/health')
def health(): return jsonify({'status': 'ok'})

def delayed_start():
    time.sleep(max(0, SETTINGS.render_start_delay)); APP.start()

if __name__ == '__main__':
    threading.Thread(target=delayed_start, daemon=True).start()
    web.run(host='0.0.0.0', port=SETTINGS.port, threaded=True, use_reloader=False)
