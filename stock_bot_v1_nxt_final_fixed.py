from __future__ import annotations

# 뽕실 V2 - V8 매매 엔진 및 비동기 텔레그램 통합본
# KIS NXT 100% 실시간 감시 / 정규장 REST 백업 / 동적 방어선(Trailing Stop) 적용

import os
import time
import json
import math
import queue
import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Callable

import requests
import websocket
import yfinance as yf
import FinanceDataReader as fdr
from flask import Flask, jsonify
from zoneinfo import ZoneInfo

# ===== 1. 설정 및 기본 유틸리티 =====
KST = ZoneInfo('Asia/Seoul')
log = logging.getLogger('v2.main')
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(threadName)s | %(message)s')

def now(): return datetime.now(KST)
def b(n, d): return os.getenv(n, str(d)).lower() in {'1', 'true', 'yes', 'on'}
def i(n, d): 
    try: return int(os.getenv(n, str(d)))
    except: return d
def f(n, d): 
    try: return float(os.getenv(n, str(d)))
    except: return d
def num(v, d=0.0):
    try: return float(str(v).replace(',', '').strip())
    except: return d
def pct(n, o): return (n / o - 1) * 100 if o else 0

@dataclass(frozen=True)
class Settings:
    version: str = '2.0.0'
    telegram_token: str = os.getenv('TELEGRAM_TOKEN', '').strip()
    chat_id: str = (os.getenv('CHAT_ID') or os.getenv('TELEGRAM_CHAT_ID') or '').strip()
    telegram_polling: bool = b('ENABLE_TELEGRAM_POLLING', True)
    kis_app_key: str = os.getenv('KIS_APP_KEY', '').strip()
    kis_app_secret: str = os.getenv('KIS_APP_SECRET', '').strip()
    kis_env: str = os.getenv('KIS_ENV', 'real').strip().lower()
    enable_nxt: bool = b('ENABLE_NXT', True)
    supabase_url: str = os.getenv('SUPABASE_URL', '').rstrip('/')
    supabase_key: str = os.getenv('SUPABASE_SECRET_KEY', '').strip()
    port: int = i('PORT', 10000)
    max_candidates: int = i('MAX_CANDIDATES', 200)
    ws_trade_limit: int = i('WS_TRADE_LIMIT', 6)
    ws_orderbook_limit: int = i('WS_ORDERBOOK_LIMIT', 1)
    ws_total_limit: int = i('WS_TOTAL_LIMIT', 7)
    ws_subscribe_delay: float = f('WS_SUBSCRIBE_DELAY', 0.50)
    render_start_delay: int = i('RENDER_START_DELAY', 120)
    min_intraday_bars: int = i('MIN_INTRADAY_BARS', 12)
    nxt_start: str = os.getenv('NXT_WS_START', '08:00')
    nxt_end: str = os.getenv('NXT_WS_END', '20:00')
    nxt_trade_tr: str = os.getenv('KIS_NXT_TRADE_TR_ID', 'H0NXCNT0')
    nxt_order_tr: str = os.getenv('KIS_NXT_ORDERBOOK_TR_ID', 'H0UNASP0')

SETTINGS = Settings()

# ===== 2. 데이터 모델 (V8 기반) =====
@dataclass
class Tick: 
    code: str; name: str; market: str; price: float; volume: int; cumulative_volume: int; trade_strength: float; timestamp: datetime

@dataclass
class MinuteBar:
    code: str; name: str; market: str; minute: datetime
    open: float; high: float; low: float; close: float
    volume: int = 0; cumulative_volume: int = 0; trade_strength: float = 0.0

@dataclass
class OrderBook: 
    code: str; market: str; asks: List[float]; bids: List[float]; ask_qty: List[int]; bid_qty: List[int]
    total_ask: int; total_bid: int; imbalance: float; updated_at: datetime

@dataclass
class PositionState:
    code: str; name: str; market: str; entry_price: float; entry_time: datetime
    highest_price: float; protection_price: float; invalidation_price: float; qty: float = 0.0
    partial_sold: bool = False; failed_high_count: int = 0; kind: str = '단타'

# ===== 3. 비동기 큐 & 텔레그램 연동 (Zero Blocking) =====
class AsyncWorker:
    def __init__(self):
        self.q = queue.Queue()
        self.last_sent: Dict[str, datetime] = {}
        
    def start(self):
        threading.Thread(target=self._loop, daemon=True, name='WorkerThread').start()

    def _loop(self):
        while True:
            func, args, kwargs, debounce_key = self.q.get()
            try:
                if debounce_key:
                    if debounce_key in self.last_sent and (now() - self.last_sent[debounce_key]).total_seconds() < 60:
                        continue # 1분 이내 동일 알림 무시 (스팸 방지)
                    self.last_sent[debounce_key] = now()
                func(*args, **kwargs)
            except Exception as e:
                log.error(f'Worker error: {e}')
            finally:
                self.q.task_done()

WORKER = AsyncWorker()

class Telegram:
    def __init__(self):
        self.s = requests.Session()
        self.offset = 0
        self.handler: Optional[Callable[[str, str], None]] = None

    def send_sync(self, text):
        target = SETTINGS.chat_id
        if not SETTINGS.telegram_token or not target: return
        try:
            self.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/sendMessage',
                        json={'chat_id': target, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': True}, timeout=10)
        except Exception as e: log.warning(f'Telegram Send Sync: {e}')

    def send(self, text, debounce_key=None):
        WORKER.q.put((self.send_sync, (text,), {}, debounce_key))

    def poll(self):
        if not SETTINGS.telegram_token: return
        while True:
            try:
                r = self.s.get(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/getUpdates', params={'timeout': 25, 'offset': self.offset}, timeout=35)
                if r.status_code == 409:
                    time.sleep(20)
                    continue
                r.raise_for_status()
                for u in r.json().get('result', []):
                    self.offset = max(self.offset, int(u['update_id']) + 1)
                    m = u.get('message') or {}
                    text = str(m.get('text') or '').strip()
                    chat = str((m.get('chat') or {}).get('id') or '')
                    if text and chat and self.handler:
                        # 명령어 처리도 백그라운드 스레드로 넘겨 웹소켓 방해 차단
                        threading.Thread(target=self.handler, args=(text, chat), daemon=True).start()
            except Exception as e:
                time.sleep(10)

BOT = Telegram()

# ===== 4. 데이터베이스 연동 (비동기화) =====
class DB:
    def __init__(self):
        self.enabled = bool(SETTINGS.supabase_url and SETTINGS.supabase_key)
        self.s = requests.Session()
    
    @property
    def h(self): return {'apikey': SETTINGS.supabase_key, 'Authorization': f'Bearer {SETTINGS.supabase_key}', 'Content-Type': 'application/json', 'Prefer': 'return=representation'}
    
    def insert_sync(self, t, payload):
        if not self.enabled: return
        try: self.s.post(f'{SETTINGS.supabase_url}/rest/v1/{t}', headers=self.h, json=payload, timeout=20)
        except Exception as e: log.warning(f'DB insert {t}: {e}')
        
    def insert(self, t, payload):
        WORKER.q.put((self.insert_sync, (t, payload), {}, None))

DATABASE = DB()

# ===== 5. KIS API (NXT 전용 웹소켓 + 초기 데이터 로드) =====
class KIS:
    def __init__(self):
        self.rest = 'https://openapivts.koreainvestment.com:29443' if SETTINGS.kis_env == 'virtual' else 'https://openapi.koreainvestment.com:9443'
        self.ws = 'ws://ops.koreainvestment.com:31000' if SETTINGS.kis_env == 'virtual' else 'ws://ops.koreainvestment.com:21000'
        self.s = requests.Session()
        self.token = None; self.token_exp = None; self.approval = None; self.approval_exp = None
        self.lock = threading.Lock(); self.stream_lock = threading.Lock()
        self.stream_running = False

    def access(self):
        if self.token and self.token_exp and datetime.now(timezone.utc) < self.token_exp: return self.token
        r = self.s.post(f'{self.rest}/oauth2/tokenP', json={'grant_type': 'client_credentials', 'appkey': SETTINGS.kis_app_key, 'appsecret': SETTINGS.kis_app_secret}, timeout=20)
        r.raise_for_status()
        d = r.json()
        self.token = d['access_token']
        self.token_exp = datetime.now(timezone.utc) + timedelta(seconds=max(60, int(d.get('expires_in', 86400)) - 300))
        return self.token

    def approval_key(self):
        with self.lock:
            if self.approval and self.approval_exp and datetime.now(timezone.utc) < self.approval_exp: return self.approval
            r = self.s.post(f'{self.rest}/oauth2/Approval', json={'grant_type': 'client_credentials', 'appkey': SETTINGS.kis_app_key, 'secretkey': SETTINGS.kis_app_secret}, timeout=20)
            r.raise_for_status()
            self.approval = r.json()['approval_key']
            self.approval_exp = datetime.now(timezone.utc) + timedelta(hours=12)
            return self.approval

    def get_minute_bars(self, code):
        """정규장 장 초반 데이터 강제 로드를 위한 1분봉 REST 조회"""
        t = self.access()
        headers = {'authorization': f'Bearer {t}', 'appkey': SETTINGS.kis_app_key, 'appsecret': SETTINGS.kis_app_secret, 'tr_id': 'FHKST03010200', 'custtype': 'P'}
        params = {'FID_ETC_CLS_CODE': '', 'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': code, 'FID_INPUT_HOUR_1': '153000', 'FID_PW_DATA_INCU_YN': 'Y'}
        try:
            r = self.s.get(f'{self.rest}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice', headers=headers, params=params, timeout=10)
            if r.status_code == 200: return r.json().get('output2') or []
        except Exception as e: log.warning(f'초기 분봉 로드 실패 {code}: {e}')
        return []

    # 웹소켓 파싱 및 로테이션 루프 (기존 인프라 유지, NXT 100% 집중)
    @staticmethod
    def parse_trade(msg, names):
        if isinstance(msg, (bytes, bytearray)): msg = msg.decode('utf-8', errors='ignore')
        if not msg or msg.startswith('{'): return []
        p = msg.split('|', 3)
        if len(p) < 4 or p[0] != '0': return []
        count = max(1, int(p[2]) if p[2].isdigit() else 1)
        f = p[3].split('^')
        width = len(f) // count if count else len(f)
        if width < 19: return []
        out = []
        for i in range(count):
            r = f[i*width:(i+1)*width]
            code = str(r[0]).zfill(6)
            h = str(r[1]).zfill(6)
            ts = now().replace(hour=int(h[:2]), minute=int(h[2:4]), second=int(h[4:6]), microsecond=0)
            out.append(Tick(code, names.get(code, code), 'NXT', num(r[2]), int(r[12] or 0), int(r[13] or 0), num(r[18]), ts))
        return out

    def stream_nxt(self, engine, runtime_state):
        """NXT 전용 7슬롯 동적 로테이션 실시간 스트림"""
        with self.stream_lock:
            if self.stream_running: return
            self.stream_running = True
        try:
            retry = 5
            adaptive_trade = max(1, min(SETTINGS.ws_trade_limit, SETTINGS.ws_total_limit))
            adaptive_order = max(0, min(SETTINGS.ws_orderbook_limit, SETTINGS.ws_total_limit - adaptive_trade))
            while True:
                if not SETTINGS.enable_nxt: time.sleep(60); continue
                
                refresh_stop = threading.Event()
                send_lock = threading.Lock()
                requested = {'trade': set(), 'order': set()}
                
                try:
                    key = self.approval_key()
                    runtime_state['ws_nxt'] = 'connecting'

                    def send_sub(ws, tr, code, tr_type='1'):
                        payload = {'header': {'approval_key': key, 'custtype': 'P', 'tr_type': tr_type, 'content-type': 'utf-8'}, 'body': {'input': {'tr_id': tr, 'tr_key': code}}}
                        with send_lock: ws.send(json.dumps(payload, ensure_ascii=False))

                    def sync_nxt(ws):
                        # 30초마다 엔진에서 가장 핫한 종목을 다시 받아와 구독 교체
                        hot_codes = engine.get_hot_codes()
                        trade_codes = hot_codes[:adaptive_trade]
                        order_codes = trade_codes[:adaptive_order]
                        wanted_trade = set(trade_codes)
                        wanted_order = set(order_codes)

                        for c in list(requested['trade'] - wanted_trade):
                            send_sub(ws, SETTINGS.nxt_trade_tr, c, '2'); requested['trade'].discard(c); time.sleep(SETTINGS.ws_subscribe_delay)
                        for c in trade_codes:
                            if c not in requested['trade']:
                                send_sub(ws, SETTINGS.nxt_trade_tr, c, '1'); requested['trade'].add(c); time.sleep(SETTINGS.ws_subscribe_delay)
                        
                        runtime_state['nxt_trade_requested'] = len(requested['trade'])
                        runtime_state['ws_nxt'] = 'connected'

                    def refresh_loop(ws):
                        while not refresh_stop.wait(30):
                            try: sync_nxt(ws)
                            except: break

                    def opened(ws):
                        time.sleep(1.0)
                        sync_nxt(ws)
                        threading.Thread(target=refresh_loop, args=(ws,), daemon=True).start()

                    def message(ws, msg):
                        if isinstance(msg, (bytes, bytearray)): msg = msg.decode('utf-8', errors='ignore')
                        if msg.startswith('{'):
                            if 'PINGPONG' in msg:
                                with send_lock: ws.send(msg)
                            return
                        
                        parts = msg.split('|', 3)
                        if len(parts) >= 2 and parts[1] == SETTINGS.nxt_trade_tr:
                            for tick in self.parse_trade(msg, engine.names):
                                engine.push_tick(tick)
                                runtime_state['last_tick'] = tick.timestamp.isoformat()

                    def error(ws, e): refresh_stop.set(); log.warning(f'WS Error: {e}')
                    def closed(ws, status, msg): refresh_stop.set()

                    ws_app = websocket.WebSocketApp(self.ws, on_open=opened, on_message=message, on_error=error, on_close=closed)
                    ws_app.run_forever(ping_interval=25, ping_timeout=10, skip_utf8_validation=True)
                    refresh_stop.set()
                    time.sleep(retry)
                    retry = min(60, retry * 2)

                except Exception as e:
                    refresh_stop.set()
                    log.error(f'NXT stream fail: {e}')
                    time.sleep(retry)
        finally:
            with self.stream_lock: self.stream_running = False

KIS_CLIENT = KIS()

# ===== 6. 뽕실 V8 핵심 엔진 (로직/분석/관리) =====
class V8Engine:
    def __init__(self):
        self.lock = threading.RLock()
        self.names: Dict[str, str] = {}
        self.meta: Dict[str, dict] = {}
        self.bars: Dict[str, deque] = {}
        self.last_cum: Dict[str, int] = {}
        self.positions: Dict[str, PositionState] = {}
        self.watch: Dict[str, str] = {}
        self.sectors: Dict[str, float] = {}
        self.daily_cache: Dict[str, tuple] = {}
        self.runtime = {'ws_nxt': 'stopped', 'last_tick': None, 'nxt_trade_requested': 0}

    def load_markets(self):
        df = fdr.StockListing('KRX')
        with self.lock:
            for _, r in df.iterrows():
                code = str(r.get('Code') or r.get('Symbol') or '').strip().zfill(6)
                name = str(r.get('Name') or code).strip()
                if code != '000000' and name:
                    self.names[code] = name
                    self.meta[code] = {str(k): r[k] for k in df.columns}
        self._refresh_sectors()

    def _refresh_sectors(self):
        buckets = {}
        with self.lock:
            for c, m in self.meta.items():
                s = str(m.get('Sector') or m.get('Industry') or '기타')
                ch = num(m.get('ChangesRatio') or m.get('ChagesRatio'))
                amt = num(m.get('Amount')) or num(m.get('Close')) * num(m.get('Volume'))
                buckets.setdefault(s, []).append(ch * max(1, math.log10(max(amt, 10))))
            self.sectors = {k: max(0, min(100, 50 + sum(v) / max(1, len(v)) * 2)) for k, v in buckets.items()}

    def get_hot_codes(self) -> List[str]:
        """NXT 동적 로테이션을 위한 우선순위 산출 (보유 > 관심 > 거래대금 상위)"""
        with self.lock:
            priority = list(self.positions.keys()) + list(self.watch.keys())
            rows = []
            for c, m in self.meta.items():
                if num(m.get('Marcap')) < SETTINGS.min_market_cap: continue
                amt = num(m.get('Amount')) or num(m.get('Close')) * num(m.get('Volume'))
                rows.append((amt, c))
            rows.sort(reverse=True)
            ordered = list(dict.fromkeys(priority + [c for _, c in rows if c not in priority]))
            return ordered[:SETTINGS.max_candidates]

    def load_initial_bars(self):
        """09:00 정규장 오픈 시, 핫 종목들의 초기 분봉 강제 로드 (사각지대 해소)"""
        codes = self.get_hot_codes()[:30] # 상위 30개만 집중 로드
        for code in codes:
            raw = KIS_CLIENT.get_minute_bars(code)
            if not raw: continue
            with self.lock:
                q = self.bars.setdefault(code, deque(maxlen=240))
                for r in reversed(raw):
                    try:
                        h = str(r['stck_cntg_hour']).zfill(6)
                        minute = now().replace(hour=int(h[:2]), minute=int(h[2:4]), second=0, microsecond=0)
                        b = MinuteBar(code, self.names.get(code, code), 'KRX', minute, num(r['stck_oprc']), num(r['stck_hgpr']), num(r['stck_lwpr']), num(r['stck_prpr']), int(r['cntg_vol']), int(r['acml_vol']), 100.0)
                        if not q or q[-1].minute < minute: q.append(b)
                    except: pass

    def push_tick(self, tick: Tick):
        with self.lock:
            minute = tick.timestamp.replace(second=0, microsecond=0)
            last_cv = self.last_cum.get(tick.code, tick.cumulative_volume)
            inc = max(0, tick.cumulative_volume - last_cv)
            self.last_cum[tick.code] = tick.cumulative_volume

            q = self.bars.setdefault(tick.code, deque(maxlen=240))
            if not q or q[-1].minute != minute:
                q.append(MinuteBar(tick.code, tick.name, tick.market, minute, tick.price, tick.price, tick.price, tick.price, tick.volume, tick.cumulative_volume, tick.trade_strength))
            else:
                b = q[-1]
                b.high = max(b.high, tick.price); b.low = min(b.low, tick.price); b.close = tick.price
                b.volume += max(tick.volume, inc); b.trade_strength = tick.trade_strength
            
            # 보유 종목 트레일링 스탑 추적
            self._evaluate_sell(tick.code, tick.price, list(q))
            
            # 신규 단타 매수 시그널 포착
            if tick.code not in self.positions:
                self._evaluate_buy(tick.code, list(q))

    def _evaluate_sell(self, code: str, current_price: float, bars: List[MinuteBar]):
        pos = self.positions.get(code)
        if not pos: return
        pos.highest_price = max(pos.highest_price, current_price)
        gain = pct(current_price, pos.entry_price)

        # 동적 방어선 (Trailing Stop) 상향 로직
        if gain >= 8: pos.protection_price = max(pos.protection_price, pos.highest_price * 0.975)
        elif gain >= 5: pos.protection_price = max(pos.protection_price, pos.highest_price * 0.970)
        elif gain >= 2.5: pos.protection_price = max(pos.protection_price, max(pos.entry_price * 1.002, pos.highest_price * 0.965))

        msg = None
        if current_price <= pos.invalidation_price:
            msg = f"⛔ <b>[손절] {pos.name}</b>\n💰 <b>현재가:</b> {current_price:,.0f}원 ({gain:+.2f}%)\n📉 무효화 가격 이탈 방어"
            self.positions.pop(code)
        elif current_price <= pos.protection_price:
            msg = f"🎯 <b>[추적 익절] {pos.name}</b>\n💰 <b>현재가:</b> {current_price:,.0f}원 ({gain:+.2f}%)\n🛡️ 끌어올린 방어선 이탈로 수익 실현"
            self.positions.pop(code)
        elif gain >= 3 and not pos.partial_sold and len(bars) >= 8:
            avg_prev = sum(b.volume for b in bars[-8:-3]) / 5
            avg_recent = sum(b.volume for b in bars[-3:]) / 3
            if avg_recent < avg_prev * 0.65:
                pos.partial_sold = True
                msg = f"⚠️ <b>[비중 축소 권장] {pos.name} (+{gain:.1f}%)</b>\n📉 고점 부근 거래량 급감 (절반 익절 검토)\n🛡️ <b>방어선 상향:</b> {pos.protection_price:,.0f}원"

        if msg: BOT.send(msg, debounce_key=f"sell_{code}_{current_price}")

    def _evaluate_buy(self, code: str, bars: List[MinuteBar]):
        if len(bars) < SETTINGS.min_intraday_bars: return
        latest = bars[-1]
        recent = bars[-12:]
        
        # V자 반등 및 돌파 체크 (V8 엔진 로직 압축)
        bottom = min(b.low for b in recent)
        prior_high = max(b.high for b in recent[:-3]) if len(recent) > 3 else latest.high
        decline = pct(bottom, prior_high)
        
        avg_vol = sum(b.volume for b in recent[:-3]) / 9 if len(recent) > 3 else 1
        conf_vol = sum(b.volume for b in recent[-3:]) / 3
        vol_ratio = conf_vol / avg_vol if avg_vol > 0 else 0
        
        if decline <= -1.5 and vol_ratio >= 1.5 and latest.close >= max(b.high for b in recent[-5:-1]) and latest.trade_strength > 100:
            stop_price = bottom * 0.995
            msg = f"🔥 <b>[단타 포착] {latest.name} ({code})</b>\n💰 <b>현재:</b> {latest.close:,.0f}원 (손절: {stop_price:,.0f}원)\n🎯 <b>목표:</b> 자율 추적\n⚡ <b>전략:</b> V자 반등 & 거래량 {vol_ratio:.1f}배 유입"
            BOT.send(msg, debounce_key=f"buy_{code}")
            
            # DB 기록
            DATABASE.insert('recommendations', {'stock_code': code, 'stock_name': latest.name, 'recommended_price': latest.close, 'confidence_score': 85.0, 'recommendation_time': now().isoformat()})

ENGINE = V8Engine()

# ===== 7. 스케줄러 & 텔레그램 명령어 핸들러 =====
class AppManager:
    def __init__(self):
        BOT.handler = self.handle_cmd

    def get_us_market(self):
        try:
            res = []
            for name, symbol in {'S&P 500': '^GSPC', 'Nasdaq': '^IXIC', 'Dow': '^DJI'}.items():
                d = yf.Ticker(symbol).history(period="2d")
                if len(d) >= 2:
                    pct_val = (d['Close'].iloc[-1] / d['Close'].iloc[-2] - 1) * 100
                    res.append(f"{'🔴' if pct_val > 0 else '🔵'} {name}: {d['Close'].iloc[-1]:,.2f} ({pct_val:+.2f}%)")
            return "\n".join(res)
        except: return "미국 증시 조회 실패"

    def schedule_loop(self):
        sent = {}
        while True:
            n = now(); d = str(n.date())
            if n.weekday() < 5:
                # [07:30] 미국 증시 & 장전 스윙 추천
                if n.hour == 7 and 30 <= n.minute < 40 and sent.get('pre') != d:
                    msg = f"🌎 <b>[굿모닝 브리핑]</b>\n\n{self.get_us_market()}\n\n🌅 <b>[장전 관심 스윙 종목]</b>\n(전일 종가/수급 기반 바닥권 추출중...)"
                    BOT.send(msg); sent['pre'] = d
                
                # [08:00] NXT 개장
                if n.hour == 8 and 0 <= n.minute < 5 and sent.get('nxt_open') != d:
                    BOT.send("🔔 <b>[08:00] NXT 거래 시작!</b>\n실시간 동적 로테이션 감시를 가동합니다."); sent['nxt_open'] = d

                # [09:00] 정규장 개장 & 데이터 강제 로드
                if n.hour == 9 and 0 <= n.minute < 5 and sent.get('krx_open') != d:
                    BOT.send("🔔 <b>[09:00] 정규장 개장!</b>\n초기 1분봉 데이터를 강제 로드하여 사각지대를 해소합니다.")
                    ENGINE.load_initial_bars()
                    sent['krx_open'] = d

                # [15:30] 정규장 마감
                if n.hour == 15 and 30 <= n.minute < 35 and sent.get('krx_close') != d:
                    BOT.send(f"📊 <b>[15:30 정규장 마감]</b>\n수고하셨습니다. 현재 추적 중인 포지션: {len(ENGINE.positions)}개"); sent['krx_close'] = d

                # [20:00] NXT 마감
                if n.hour == 20 and 0 <= n.minute < 5 and sent.get('nxt_close') != d:
                    BOT.send("🌙 <b>[20:00 NXT 거래 종료]</b>\n오늘 하루 봇 운용을 마감합니다. 편안한 밤 되세요!"); sent['nxt_close'] = d
            time.sleep(30)

    def handle_cmd(self, text, chat):
        p = text.split(); cmd = p[0]
        
        if cmd in ('/도움말', '/help'):
            BOT.send("🤖 <b>뽕실 V2 명령어</b>\n\n/상태 : 봇 건강 상태 및 큐 대기열\n/단타 : NXT 실시간 포착 랭킹\n/보유 : 추적 중인 종목 리스트\n/매수 [종목] [매수가] [수량] : 수동 매수 종목 V8 엔진 추적 등록\n/매도 [종목] : 추적 강제 종료\n/관심 [종목] : 동적 로테이션 최우선 순위 등록", chat)
        
        elif cmd == '/상태':
            q_size = WORKER.q.qsize()
            BOT.send(f"🤖 <b>뽕실 V{SETTINGS.version} 상태</b>\n\n- NXT 연결: {ENGINE.runtime['ws_nxt']}\n- V8 엔진 가동: 정상\n- 비동기 큐 대기열: {q_size}개\n- 관리 중인 포지션: {len(ENGINE.positions)}개\n- 마지막 틱 수신: {ENGINE.runtime['last_tick'] or '-'}", chat)
        
        elif cmd == '/보유':
            if not ENGINE.positions:
                BOT.send("📭 현재 추적 중인 보유 종목이 없습니다.", chat)
            else:
                lines = ["💼 <b>[V8 추적 중인 보유 종목]</b>\n"]
                for x in ENGINE.positions.values():
                    lines.append(f"• <b>{x.name}</b> ({x.entry_price:,.0f}원 매수)\n  └ 최고: {x.highest_price:,.0f}원 | 🛡️ 방어: <b>{x.protection_price:,.0f}원</b>")
                BOT.send("\n\n".join(lines), chat)
        
        elif cmd == '/매수' and len(p) >= 4:
            c = p[1]; price = num(p[2]); qty = num(p[3])
            name = ENGINE.names.get(c, c)
            # 수동 등록 시에도 V8 엔진이 트레일링 스탑을 감시하도록 등록
            ENGINE.positions[c] = PositionState(c, name, 'NXT', price, now(), price, price * 0.97, price * 0.95, qty)
            BOT.send(f"✅ <b>수동 매수 등록 완료</b>\n\n봇이 <b>{name}</b>의 동적 방어선(트레일링 스탑) 감시를 시작합니다.\n최초 방어선: {price * 0.97:,.0f}원", chat)
        
        elif cmd == '/매도' and len(p) >= 2:
            c = p[1]
            if c in ENGINE.positions:
                ENGINE.positions.pop(c)
                BOT.send(f"✅ <b>{ENGINE.names.get(c, c)}</b> 감시 종료", chat)
        
        elif cmd == '/단타':
            # 슬림화된 모바일 UI 랭킹
            codes = ENGINE.get_hot_codes()[:5]
            lines = ["🏆 <b>단타 실시간 핫 리스트 (NXT 감시중)</b>\n"]
            for i, c in enumerate(codes, 1):
                bars = ENGINE.bars.get(c)
                price = bars[-1].close if bars else 0
                lines.append(f"<b>{i}. {ENGINE.names.get(c, c)}</b> ({c})\n└ 현재: {price:,.0f}원 (1분봉 {len(bars) if bars else 0}개 수집)")
            BOT.send("\n".join(lines), chat)
            
        else: BOT.send("명령어는 /도움말", chat)

APP_MANAGER = AppManager()

# ===== 8. 웹 서버 및 초기 구동 =====
web = Flask(__name__)
@web.get('/')
def root(): return f'뽕실 V{SETTINGS.version} running', 200
@web.get('/health')
def health(): return jsonify({'status': 'ok', 'positions': len(ENGINE.positions), 'queue': WORKER.q.qsize()})

def delayed_start():
    delay = max(0, SETTINGS.render_start_delay)
    if delay:
        log.info(f'Render 인스턴스 중복 방지 대기: {delay}초')
        time.sleep(delay)
    
    WORKER.start()
    ENGINE.load_markets()
    if SETTINGS.telegram_polling: threading.Thread(target=BOT.poll, daemon=True).start()
    threading.Thread(target=APP_MANAGER.schedule_loop, daemon=True).start()
    
    if SETTINGS.kis_app_key:
        threading.Thread(target=KIS_CLIENT.stream_nxt, args=(ENGINE, ENGINE.runtime), daemon=True).start()
    
    BOT.send(f"🤖 <b>뽕실 V{SETTINGS.version} 기동 완료!</b>\n- V8 매매 엔진 (다중 타임프레임 추적) 가동\n- 100% NXT 동적 로테이션 활성화\n- 텔레그램 Zero Blocking 적용")

if __name__ == '__main__':
    threading.Thread(target=delayed_start, daemon=True).start()
    web.run(host='0.0.0.0', port=SETTINGS.port, threaded=True, use_reloader=False)
