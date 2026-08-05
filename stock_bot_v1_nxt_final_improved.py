from __future__ import annotations

# ==========================================
# 🤖 쾌걸스민 V1.0.6 (Ultimate Sniper & Dual AI)
# 1. 부팅 즉시 KIS 과거 데이터 강제 동기화 (재부팅 시 VWAP 붕괴 방어 완벽 패치)
# 2. VWAP 기반 거래량 극감 눌림목 저격 로직 (알림 폭탄 방지)
# 3. 50:50 하이브리드 수급 랭킹 (대형주 포함)
# 4. 22:00(야간 DB저장) -> 07:00(아침 최종픽) 듀얼 AI 브리핑
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

try:
    import google.generativeai as genai
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

# ===== 1. 설정 및 유틸리티 =====
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
    try: return float(str(v).replace(',', '').replace('원', '').replace('주', '').strip())
    except: return d
def pct(n, o): return (n / o - 1) * 100 if o else 0

@dataclass(frozen=True)
class Settings:
    version: str = '1.0.6 (쾌걸스민 Ultimate Sniper)'
    telegram_token: str = os.getenv('TELEGRAM_TOKEN', '').strip()
    chat_id: str = (os.getenv('CHAT_ID') or os.getenv('TELEGRAM_CHAT_ID') or '').strip()
    gemini_api_key: str = os.getenv('GEMINI_API_KEY', '').strip()
    kis_app_key: str = os.getenv('KIS_APP_KEY', '').strip()
    kis_app_secret: str = os.getenv('KIS_APP_SECRET', '').strip()
    kis_env: str = os.getenv('KIS_ENV', 'real').strip().lower()
    port: int = i('PORT', 10000)
    max_candidates: int = i('MAX_CANDIDATES', 200)
    ws_trade_limit: int = i('WS_TRADE_LIMIT', 6)
    render_start_delay: int = i('RENDER_START_DELAY', 60)
    min_market_cap: float = f('MIN_MARKET_CAP', 100_000_000_000) # 1천억 하한선 (잡주 컷)
    min_daily_volume: int = i('MIN_DAILY_VOLUME', 20_000)
    nxt_start: str = '08:00'
    nxt_end: str = '20:00'
    nxt_trade_tr: str = os.getenv('KIS_NXT_TRADE_TR_ID', 'H0NXCNT0')

SETTINGS = Settings()

# ===== 2. 데이터 모델 =====
@dataclass
class Tick: code: str; name: str; market: str; price: float; volume: int; cumulative_volume: int; trade_strength: float; timestamp: datetime
@dataclass
class Bar: code: str; name: str; market: str; minute: datetime; open: float; high: float; low: float; close: float; volume: int; cumulative_volume: int; trade_strength: float
@dataclass
class Position: code: str; name: str; kind: str; entry: float; qty: float; highest: float; stop: float; state: str = '감시중'

# ===== 3. 비동기 큐 & DB (AI 메모리) =====
class AsyncWorker:
    def __init__(self, name):
        self.name = name; self.q = queue.Queue(maxsize=1000)
    def start(self): threading.Thread(target=self._loop, daemon=True, name=self.name).start()
    def submit(self, func, *args, **kwargs):
        try: self.q.put_nowait((func, args, kwargs)); return True
        except: return False
    def _loop(self):
        while True:
            func, args, kwargs = self.q.get()
            try: func(*args, **kwargs)
            except Exception as e: log.warning(f'{self.name} 에러: {e}')
            finally: self.q.task_done()

TELEGRAM_WORKER = AsyncWorker('telegram-worker'); DB_WORKER = AsyncWorker('db-worker')

class LocalDB:
    def __init__(self):
        self.pos_file = 'epin_positions.json'
        self.ai_file = 'epin_ai_memory.json'
    def load_pos(self):
        try:
            if os.path.exists(self.pos_file):
                with open(self.pos_file, 'r', encoding='utf-8') as f: return json.load(f)
        except: pass
        return {}
    def save_pos(self, data):
        def _save():
            with open(self.pos_file, 'w', encoding='utf-8') as f: json.dump(data, f, ensure_ascii=False, indent=2)
        DB_WORKER.submit(_save)
    def load_ai(self):
        try:
            if os.path.exists(self.ai_file):
                with open(self.ai_file, 'r', encoding='utf-8') as f: return json.load(f).get('picks', '저장된 픽 없음')
        except: pass
        return "어제 저장된 AI 픽 데이터가 없습니다."
    def save_ai(self, text):
        def _save():
            with open(self.ai_file, 'w', encoding='utf-8') as f: json.dump({'picks': text, 'date': str(now().date())}, f, ensure_ascii=False)
        DB_WORKER.submit(_save)

DATABASE = LocalDB()
class Telegram:
    def __init__(self): self.s = requests.Session(); self.offset = 0; self.handler = None
    def send(self, text, chat=None): 
        def _send():
            try: self.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/sendMessage', json={'chat_id': str(chat or SETTINGS.chat_id).strip(), 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': True}, timeout=10)
            except: pass
        TELEGRAM_WORKER.submit(_send)
    def poll(self):
        if not SETTINGS.telegram_token: return
        while True:
            try:
                r = self.s.get(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/getUpdates', params={'timeout': 25, 'offset': self.offset}, timeout=35)
                if r.status_code == 409: time.sleep(20); continue
                for u in r.json().get('result', []):
                    self.offset = max(self.offset, int(u['update_id']) + 1)
                    m = u.get('message') or {}; text = str(m.get('text') or '').strip(); chat = str((m.get('chat') or {}).get('id') or '')
                    if text and chat and self.handler: threading.Thread(target=self.handler, args=(text, chat), daemon=True).start()
            except: time.sleep(10)

BOT = Telegram()

# ===== 4. KIS API =====
def in_session(sh, eh):
    n = now()
    if n.weekday() >= 5: return False
    try:
        sh_h, sh_m = map(int, sh.split(':')); eh_h, eh_m = map(int, eh.split(':'))
        return n.replace(hour=sh_h, minute=sh_m, second=0, microsecond=0) <= n <= n.replace(hour=eh_h, minute=eh_m, second=0, microsecond=0)
    except: return False

class KIS:
    def __init__(self):
        self.rest = 'https://openapi.koreainvestment.com:9443'
        self.ws = 'ws://ops.koreainvestment.com:21000'
        self.s = requests.Session(); self.token = None; self.token_exp = None; self.approval = None; self.approval_exp = None; self.lock = threading.Lock()
    
    def access(self):
        if self.token and self.token_exp and datetime.now(timezone.utc) < self.token_exp: return self.token
        r = self.s.post(f'{self.rest}/oauth2/tokenP', json={'grant_type': 'client_credentials', 'appkey': SETTINGS.kis_app_key, 'appsecret': SETTINGS.kis_app_secret}, timeout=20)
        d = r.json(); self.token = d['access_token']; self.token_exp = datetime.now(timezone.utc) + timedelta(seconds=max(60, int(d.get('expires_in', 86400)) - 300)); return self.token
    
    def approval_key(self):
        with self.lock:
            if self.approval and self.approval_exp and datetime.now(timezone.utc) < self.approval_exp: return self.approval
            r = self.s.post(f'{self.rest}/oauth2/Approval', json={'grant_type': 'client_credentials', 'appkey': SETTINGS.kis_app_key, 'secretkey': SETTINGS.kis_app_secret}, timeout=20)
            self.approval = r.json()['approval_key']; self.approval_exp = datetime.now(timezone.utc) + timedelta(hours=12); return self.approval

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
                state['ws_nxt'] = 'waiting_market_session'; time.sleep(30); continue
            refresh_stop = threading.Event(); send_lock = threading.Lock(); requested = set()
            try:
                key = self.approval_key(); state['ws_nxt'] = 'connecting'
                def send_sub(ws, tr, code, tr_type='1'):
                    with send_lock: ws.send(json.dumps({'header': {'approval_key': key, 'custtype': 'P', 'tr_type': tr_type, 'content-type': 'utf-8'}, 'body': {'input': {'tr_id': tr, 'tr_key': code}}}, ensure_ascii=False))
                def sync_nxt(ws):
                    if not in_session(SETTINGS.nxt_start, SETTINGS.nxt_end): refresh_stop.set(); ws.close(); return
                    trade_codes = codes_fn()[:SETTINGS.ws_trade_limit]; wanted = set(trade_codes)
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
                        if 'MAX SUBSCRIBE' in msg or 'ALREADY IN USE' in msg: refresh_stop.set(); ws.close()
                        return
                    parts = msg.split('|', 3)
                    if len(parts) >= 2 and parts[1] == SETTINGS.nxt_trade_tr:
                        count = max(1, int(parts[2]) if parts[2].isdigit() else 1)
                        f = parts[3].split('^'); width = len(f) // count if count else len(f)
                        if width < 19: return
                        for i in range(count):
                            r = f[i*width:(i+1)*width]
                            code = str(r[0]).zfill(6); ts = now().replace(hour=int(r[1][:2]), minute=int(r[1][2:4]), second=int(r[1][4:6]), microsecond=0)
                            on_tick(Tick(code, names.get(code, code), 'NXT', num(r[2]), int(r[12] or 0), int(r[13] or 0), num(r[18]), ts))
                ws_app = websocket.WebSocketApp(self.ws, on_open=lambda w: threading.Thread(target=lambda: (time.sleep(1), sync_nxt(w), refresh_loop(w)), daemon=True).start(), on_message=message, on_error=lambda w,e: refresh_stop.set(), on_close=lambda w,s,m: refresh_stop.set())
                ws_app.run_forever(ping_interval=25, ping_timeout=10, skip_utf8_validation=True)
                refresh_stop.set(); time.sleep(retry); retry = min(60, retry * 2)
            except: refresh_stop.set(); time.sleep(retry); retry = min(60, retry * 2)

KIS_CLIENT = KIS()

# ===== 5. 50:50 메모리 & 스나이퍼 매매 두뇌 =====
class EpinState:
    def __init__(self):
        self.lock = threading.RLock(); self.names = {}; self.meta = {}; self.candidates = {}; self.bars = {}
        self.top_hybrid_list = []; self.runtime = {'ws_nxt': 'stopped'}
        
    def load(self):
        import pandas as pd
        try: df = pd.concat([fdr.StockListing('KOSPI'), fdr.StockListing('KOSDAQ')], ignore_index=True)
        except: return
        with self.lock:
            for _, r in df.iterrows():
                c = str(r.get('Code') or r.get('Symbol') or '').strip().zfill(6)
                if c and c != '000000' and c.lower() != 'nan':
                    self.names[c] = str(r.get('Name') or c).strip()
                    self.meta[c] = {str(k): r[k] for k in df.columns}

    def refresh_candidates(self):
        valid_items = []
        with self.lock:
            for c, m in self.meta.items():
                marcap = num(m.get('Marcap')); vol = num(m.get('Volume'))
                if marcap < SETTINGS.min_market_cap or vol < SETTINGS.min_daily_volume: continue
                amt = num(m.get('Amount')) or (num(m.get('Close')) * vol)
                turnover = amt / marcap if marcap > 0 else 0
                valid_items.append((c, amt, turnover))
                
            if not valid_items: return
            valid_items.sort(key=lambda x: x[1], reverse=True); amt_ranks = {x[0]: i for i, x in enumerate(valid_items)}
            valid_items.sort(key=lambda x: x[2], reverse=True); turn_ranks = {x[0]: i for i, x in enumerate(valid_items)}
            
            final_scores = [( (amt_ranks[c]*0.5 + turn_ranks[c]*0.5), c, amt, turn ) for c, amt, turn in valid_items]
            final_scores.sort(key=lambda x: x[0])
            
            self.top_hybrid_list = [(c, self.names.get(c,c), amt, turn) for _, c, amt, turn in final_scores[:20]]
            hybrid_candidates = [c for _, c, _, _ in final_scores]
            priority = list(POSITIONS.data.keys())
            self.candidates = priority + [c for c in hybrid_candidates if c not in priority][:SETTINGS.max_candidates]

    def get_hot_codes(self):
        with self.lock: return (list(POSITIONS.data.keys()) + [c for c in self.candidates if c not in POSITIONS.data])[:SETTINGS.ws_trade_limit]

STATE = EpinState()

def resolve_code(q):
    if not q: return None
    q_clean = str(q).replace(' ', '').strip().lower()
    if q_clean.isdigit(): return q_clean.zfill(6)
    with STATE.lock:
        for c, n in STATE.names.items():
            if str(n).replace(' ', '').strip().lower() == q_clean or q_clean in str(n).replace(' ', '').strip().lower(): return c
    return None

class PositionEngine:
    def __init__(self): self.data = {}
    def load(self):
        for c, v in DATABASE.load_pos().items(): self.data[c] = Position(**v)
    def save(self): DATABASE.save_pos({c: vars(p) for c, p in self.data.items()})
    def register(self, c, n, price, qty, kind):
        p = Position(c, n, kind, price, qty, price, price * 0.98) # 기본 칼손절 -2%
        self.data[c] = p; self.save(); STATE.refresh_candidates(); return p
    def remove(self, c):
        p = self.data.pop(c, None); self.save(); STATE.refresh_candidates(); return p
    def evaluate_trailing_stop(self, c, current_price):
        p = self.data.get(c); msg = None
        if not p: return None
        p.highest = max(p.highest, current_price)
        gain = pct(current_price, p.entry)
        
        if gain >= 10: p.stop = max(p.stop, p.highest * 0.96)
        elif gain >= 3: p.stop = max(p.stop, max(p.entry * 1.01, p.highest * 0.97))

        if current_price <= p.stop:
            msg = f"🛡️ <b>[쾌걸스민 자동 청산] {p.name}</b>\n💰 현재가: {current_price:,.0f}원 ({gain:+.2f}%)\n지정된 방어선을 이탈하여 감시를 종료합니다."
            self.remove(c)
        else: self.save()
        return msg

POSITIONS = PositionEngine()

class EpinBrain:
    def check_sniper_trade(self, c):
        bars = list(STATE.bars.get(c, []))
        if len(bars) < 15: return None
        latest = bars[-1]

        # 1. VWAP (당일 거래량 가중 평균단가)
        total_vol = sum(b.volume for b in bars)
        if total_vol == 0: return None
        vwap = sum((b.high + b.low + b.close)/3 * b.volume for b in bars) / total_vol
        
        # 2. 거래량 마름 (10분 평균 대비 40% 이하 급감)
        prev_10 = bars[-11:-1]
        avg_vol = sum(b.volume for b in prev_10) / 10 if prev_10 else 1
        if latest.volume > (avg_vol * 0.4): return None

        # 3. 고점 형성 후 VWAP 1.5% 근접 (눌림목 맥점)
        high_price = max(b.high for b in bars[:-1])
        if latest.close >= high_price * 0.98: return None
        if not (vwap * 0.985 <= latest.close <= vwap * 1.015): return None

        # 4. 체결강도 수급 브레이크 포착
        if latest.trade_strength < 100: return None

        return c, STATE.names.get(c, c), '눌림목', latest.close, high_price, vwap * 0.975, 'VWAP 맥점 지지 & 거래량 극감 (세력 이탈 없음)'

BRAIN = EpinBrain()

# ===== 6. 앱 및 듀얼 AI 스케줄러 =====
class App:
    def __init__(self):
        self.sent_signals = set()
    
    def sync_historical_bars(self):
        log.info("KIS 과거 1분봉 데이터 강제 동기화 시작 (VWAP 복원)")
        n = now()
        for c in STATE.candidates[:20]:
            raw = KIS_CLIENT.get_minute_bars(c)
            if raw:
                with STATE.lock:
                    q = STATE.bars.setdefault(c, deque(maxlen=390))
                    for r in reversed(raw):
                        minute = n.replace(hour=int(r['stck_cntg_hour'][:2]), minute=int(r['stck_cntg_hour'][2:4]), second=0, microsecond=0)
                        q.append(Bar(c, STATE.names.get(c,c), 'KRX', minute, num(r['stck_oprc']), num(r['stck_hgpr']), num(r['stck_lwpr']), num(r['stck_prpr']), int(r['cntg_vol']), int(r['acml_vol']), 100))
        self.sent_signals.clear() # 블랙리스트 초기화
        log.info("VWAP 복원 및 스팸 락 초기화 완료")

    def on_tick(self, t):
        with STATE.lock:
            minute = t.timestamp.replace(second=0, microsecond=0)
            q = STATE.bars.setdefault(t.code, deque(maxlen=390))
            if not q or q[-1].minute != minute:
                q.append(Bar(t.code, t.name, t.market, minute, t.price, t.price, t.price, t.price, t.volume, t.cumulative_volume, t.trade_strength))
            else:
                b = q[-1]; b.high = max(b.high, t.price); b.low = min(b.low, t.price); b.close = t.price; b.volume += t.volume; b.trade_strength = t.trade_strength
        
        msg = POSITIONS.evaluate_trailing_stop(t.code, t.price)
        if msg: BOT.send(msg)

        if t.code not in POSITIONS.data and t.code not in self.sent_signals:
            sig = BRAIN.check_sniper_trade(t.code)
            if sig:
                c, n, k, p, pred, stop, rsn = sig
                self.sent_signals.add(c)
                BOT.send(f"🔫 <b>[쾌걸스민 스나이퍼 포착] {n}</b> ({c})\n\n💰 <b>현재가(맥점):</b> {p:,.0f}원\n🚀 <b>전고점 목표:</b> {pred:,.0f}원 (+{pct(pred, p):.1f}%)\n🛡️ <b>칼손절선:</b> {stop:,.0f}원\n\n⚡ <b>저격 사유:</b> {rsn}")

    def run_night_ai(self):
        if not HAS_GEMINI or not SETTINGS.gemini_api_key or not STATE.top_hybrid_list: return
        try:
            genai.configure(api_key=SETTINGS.gemini_api_key)
            model = genai.GenerativeModel('gemini-2.5-flash')
            top20_text = ", ".join([f"{n}(회전율 {t:.1f})" for _, n, _, t in STATE.top_hybrid_list])
            prompt = f"오늘 한국장 50:50(대금+회전율) 랭킹 최상위 20개 주도주야: {top20_text}\n너는 최고의 퀀트 트레이더 '쾌걸스민'이야. 이 명단 중 내일 시세를 분출할 대장주 딱 5개만 골라서 이유와 함께 설명해. 포맷: 1. 종목명 - 이유"
            response = model.generate_content(prompt)
            DATABASE.save_ai(response.text)
            BOT.send(f"🌙 <b>[쾌걸스민 야간 1차 필터링 완료]</b>\n\n오늘 장 주도주 20개 중 내일의 타겟 5개를 DB에 저장했습니다.\n\n{response.text}")
        except: pass

    def run_morning_ai(self):
        try:
            res = []
            for name, symbol in {'S&P 500': '^GSPC', 'Nasdaq': '^IXIC', 'Dow': '^DJI'}.items():
                d = yf.Ticker(symbol).history(period="2d")
                if len(d) >= 2: res.append(f"{name}: {d['Close'].iloc[-1]:,.2f} ({(d['Close'].iloc[-1] / d['Close'].iloc[-2] - 1) * 100:+.2f}%)")
            us_market = "\n".join(res)
        except: us_market = "미국 증시 정보 로드 실패"

        if not HAS_GEMINI or not SETTINGS.gemini_api_key: return

        try:
            saved_picks = DATABASE.load_ai()
            genai.configure(api_key=SETTINGS.gemini_api_key)
            model = genai.GenerativeModel('gemini-2.5-flash')
            prompt = f"방금 마감한 미국 증시 결과야: {us_market}\n어젯밤 10시에 네가 뽑아둔 오늘장 예비 타겟 5개야: {saved_picks}\n미증시 결과를 반영해서, 어제 뽑아둔 5개 중 버릴 건 버리고 교체해서 '오늘 아침 최종 5선'을 다시 뽑아. 포맷: 🤖 **[미증시 요약]** \n🎯 **[최종 스나이퍼 5선]** (종목 - 이유)"
            response = model.generate_content(prompt)
            BOT.send(f"☀️ <b>[쾌걸스민 굿모닝 최종 브리핑]</b>\n\n{response.text}")
        except: pass

    def scheduler(self):
        sent = {}
        while True:
            n = now(); d = str(n.date())
            if n.weekday() < 5:
                if n.hour == 7 and 0 <= n.minute < 5 and sent.get('ai_morning') != d:
                    threading.Thread(target=self.run_morning_ai, daemon=True).start(); sent['ai_morning'] = d
                if n.hour == 9 and 0 <= n.minute < 5 and sent.get('bars') != d:
                    threading.Thread(target=self.sync_historical_bars, daemon=True).start(); sent['bars'] = d
                if n.hour == 22 and 0 <= n.minute < 5 and sent.get('ai_night') != d:
                    threading.Thread(target=self.run_night_ai, daemon=True).start(); sent['ai_night'] = d
            time.sleep(20)

    def handle(self, text, chat):
        p = text.split(); cmd = p[0]
        if cmd == '/상태': 
            BOT.send(f"🤖 <b>쾌걸스민 V1.0.6 가동중</b>\n- 50:50 랭킹: 활성 (대형주 포함)\n- 스나이퍼 로직: 활성 (VWAP 복원 완료)\n- 듀얼 AI 메모리: 정상 작동\n- 금일 발송 완료 종목: {len(self.sent_signals)}개", chat)
        elif cmd == '/매수':
            args = p[1:]
            if len(args) < 3: BOT.send("사용법: /매수 [종목명] [가격] [수량]", chat); return
            qty = num(args[-1]); price = num(args[-2]); stock_raw = " ".join(args[:-2])
            c = resolve_code(stock_raw)
            if c and price > 0 and qty > 0:
                p_obj = POSITIONS.register(c, STATE.names.get(c, c), price, qty, '단타')
                BOT.send(f"✅ <b>[쾌걸스민 감시 등록] {p_obj.name}</b>\n💰 기준가: {price:,.0f}원 | 🛡️ 자동 손절선: {p_obj.stop:,.0f}원", chat)
            else: BOT.send(f"❌ 등록 실패. 종목명/가격을 확인하세요.", chat)
        elif cmd == '/매도' and len(p) >= 2:
            c = resolve_code(" ".join(p[1:]))
            p_obj = POSITIONS.remove(c) if c else None
            if p_obj: BOT.send(f"✅ <b>{p_obj.name}</b> 감시 강제 종료.", chat)
        elif cmd == '/보유':
            if not POSITIONS.data: BOT.send("📭 쾌걸스민이 관리 중인 종목이 없습니다.", chat)
            else:
                msg = "💼 <b>[쾌걸스민 보유 현황]</b>\n\n"
                for p_obj in POSITIONS.data.values():
                    bars = STATE.bars.get(p_obj.code)
                    curr_price = bars[-1].close if bars else p_obj.entry
                    gain = pct(curr_price, p_obj.entry)
                    msg += f"• <b>{p_obj.name}</b> (매수가: {p_obj.entry:,.0f}원)\n  └ 현재: <b>{curr_price:,.0f}원</b> ({'🟢' if gain>=0 else '🔴'} <b>{gain:+.2f}%</b>) | 🛡️ 방어선: {p_obj.stop:,.0f}원\n\n"
                BOT.send(msg, chat)
        else: BOT.send("명령어: /상태, /보유, /매수 [종목] [가격] [수량], /매도 [종목]", chat)

    def start(self):
        TELEGRAM_WORKER.start(); DB_WORKER.start(); POSITIONS.load()
        BOT.handler = self.handle; threading.Thread(target=BOT.poll, daemon=True).start()
        threading.Thread(target=self.scheduler, daemon=True).start()
        STATE.load(); STATE.refresh_candidates()
        
        # 부팅 직후 무조건 차트 긁어오기 실행 (재부팅 대비)
        if in_session("08:00", "15:30"): threading.Thread(target=self.sync_historical_bars, daemon=True).start()
        
        threading.Thread(target=KIS_CLIENT.stream, args=(STATE.get_hot_codes, STATE.names, self.on_tick, STATE.runtime), daemon=True).start()
        BOT.send(f"🚀 <b>쾌걸스민 V{SETTINGS.version} 기동 완료!</b>\n\n1. 대형주 포함 하이브리드 수급 랭킹\n2. VWAP 거래량 극감 눌림목 저격 모드 (복원 패치 완료)\n3. 야간-아침 듀얼 교차검증 AI 탑재")

APP = App(); web = Flask(__name__)
@web.get('/')
def root(): return f'Kkwaegeol Seumin V{SETTINGS.version} running', 200
@web.get('/health')
def health(): return jsonify({'status': 'ok'})

def delayed_start():
    time.sleep(max(0, SETTINGS.render_start_delay)); APP.start()

if __name__ == '__main__':
    threading.Thread(target=delayed_start, daemon=True).start()
    web.run(host='0.0.0.0', port=SETTINGS.port, threaded=True, use_reloader=False)
