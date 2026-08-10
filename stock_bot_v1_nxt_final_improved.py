from __future__ import annotations

# ==========================================
# 🤖 쾌걸스민 V4 (VIP Intensive Sniper Mode)
# 1. 5~10개 소수 정예 집중 감시 (동적 추가/삭제)
# 2. 매수 직후 3단계 집중 관리 (원금 절대방어 -> 트레일링)
# 3. 네이버/KRX 크롤링 완전 폐기 (IP 차단 원천 봉쇄)
# 4. 극한의 텔레그램 자연어 UI/UX 적용
# 5. [BEAST MODE] NXT 12시간(08~20시) 풀타임 감시
# ==========================================

import os, json, time, threading, queue, logging, io, urllib.parse
from datetime import datetime, timedelta, timezone
from collections import deque
from dataclasses import dataclass

import requests, websocket
import yfinance as yf
from flask import Flask, jsonify
from zoneinfo import ZoneInfo

import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt

try:
    import google.generativeai as genai
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

KST = ZoneInfo('Asia/Seoul')
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger('seumin.v4')

# ===== 유틸리티 =====
def now(): return datetime.now(KST)
def num(v, d=0.0):
    try: return float(str(v).replace(',', '').replace('원', '').replace('주', '').strip())
    except: return d
def pct(n, o): return (n / o - 1) * 100 if o else 0

# 네이버 자동완성 API를 활용한 초고속 종목코드 검색 (IP 차단 없음)
def get_stock_code(name):
    try:
        url = f"https://ac.finance.naver.com/ac?q={urllib.parse.quote(name)}&q_enc=euc-kr&st=111&r_format=json"
        res = requests.get(url, timeout=5).json()
        items = res.get('items', [[]])[0]
        if items: return items[0][1], items[0][0] # code, exact_name
    except: pass
    return None, None

@dataclass(frozen=True)
class Settings:
    version: str = '4.0.0 (VIP Sniper)'
    telegram_token: str = os.getenv('TELEGRAM_TOKEN', '').strip()
    chat_id: str = (os.getenv('CHAT_ID') or os.getenv('TELEGRAM_CHAT_ID') or '').strip()
    gemini_api_key: str = os.getenv('GEMINI_API_KEY', '').strip()
    kis_app_key: str = os.getenv('KIS_APP_KEY', '').strip()
    kis_app_secret: str = os.getenv('KIS_APP_SECRET', '').strip()
    kis_env: str = os.getenv('KIS_ENV', 'real').strip().lower()
    port: int = int(os.getenv('PORT', 10000))
    circuit_breaker_pct: float = -1.5        
    nxt_trade_tr: str = os.getenv('KIS_NXT_TRADE_TR_ID', 'H0NXCNT0')

SETTINGS = Settings()

@dataclass
class Bar: minute: datetime; open: float; high: float; low: float; close: float; volume: int; trade_strength: float
@dataclass
class Position: name: str; entry: float; qty: float; highest: float; stop: float; level: int = 0

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
        self.vip_file = 'seumin_vip.json'
    def load_pos(self):
        if os.path.exists(self.pos_file): return json.load(open(self.pos_file, 'r', encoding='utf-8'))
        return {}
    def save_pos(self, data):
        WORKER.submit(lambda: json.dump(data, open(self.pos_file, 'w', encoding='utf-8'), ensure_ascii=False))
    def load_vip(self):
        if os.path.exists(self.vip_file): return json.load(open(self.vip_file, 'r', encoding='utf-8'))
        return {"limit": 5, "targets": {"119850": "지엔씨에너지"}} # 기본 타겟
    def save_vip(self, data):
        WORKER.submit(lambda: json.dump(data, open(self.vip_file, 'w', encoding='utf-8'), ensure_ascii=False))

DB = LocalDB()

class Telegram:
    def __init__(self): 
        self.s = requests.Session(); self.offset = 0
        self.text_handler = None; self.callback_handler = None
        self.dashboard_msg_id = None
    
    @property
    def default_markup(self):
        return {
            "keyboard": [
                [{"text": "📊 타겟 현황 / 대시보드"}, {"text": "📰 VIP 타겟 뉴스/공시"}],
                [{"text": "🛑 모든 방어선 해제"}, {"text": "📖 매뉴얼 보기"}]
            ],
            "resize_keyboard": True
        }

    def send(self, text, chat=None, reply_markup=None): 
        def _send():
            payload = {'chat_id': str(chat or SETTINGS.chat_id).strip(), 'text': text, 'parse_mode': 'HTML', 'reply_markup': reply_markup or self.default_markup}
            try: return self.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/sendMessage', json=payload, timeout=10).json().get('result', {}).get('message_id')
            except: return None
        return _send() 

    def send_photo(self, photo_io, caption, chat=None, reply_markup=None):
        def _send_photo():
            url = f'https://api.telegram.org/bot{SETTINGS.telegram_token}/sendPhoto'
            data = {'chat_id': str(chat or SETTINGS.chat_id).strip(), 'caption': caption, 'parse_mode': 'HTML'}
            if reply_markup: data['reply_markup'] = json.dumps(reply_markup)
            try: self.s.post(url, data=data, files={'photo': ('chart.png', photo_io, 'image/png')}, timeout=15)
            except: pass
        WORKER.submit(_send_photo)

    def edit_message(self, chat_id, message_id, text, reply_markup=None):
        def _edit():
            payload = {'chat_id': str(chat_id), 'message_id': message_id, 'text': text, 'parse_mode': 'HTML'}
            if reply_markup: payload['reply_markup'] = reply_markup
            try: self.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/editMessageText', json=payload, timeout=10)
            except: pass
        WORKER.submit(_edit)
        
    def answer_callback(self, callback_id, text=""):
        WORKER.submit(lambda: self.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/answerCallbackQuery', json={'callback_query_id': callback_id, 'text': text}, timeout=5))

    def poll(self):
        while True:
            try:
                r = self.s.get(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/getUpdates', params={'timeout': 25, 'offset': self.offset}, timeout=35)
                if r.status_code == 409: time.sleep(20); continue
                for u in r.json().get('result', []):
                    self.offset = max(self.offset, int(u['update_id']) + 1)
                    if 'callback_query' in u and self.callback_handler: threading.Thread(target=self.callback_handler, args=(u['callback_query'],), daemon=True).start()
                    elif 'message' in u and self.text_handler:
                        t = u['message'].get('text', '').strip(); c = u['message'].get('chat', {}).get('id')
                        if t and c: threading.Thread(target=self.text_handler, args=(t, c), daemon=True).start()
            except: time.sleep(5)
BOT = Telegram()

class SeuminState:
    def __init__(self):
        self.lock = threading.RLock(); self.bars = {}; self.circuit_breaker = False
        self.index_etfs = {'069500': 'KOSPI', '226490': 'KOSDAQ'}; self.index_opens = {'069500': 0, '226490': 0}
        vip_data = DB.load_vip()
        self.vip_limit = vip_data.get('limit', 5)
        self.vip_targets = vip_data.get('targets', {}) # {code: name}
STATE = SeuminState()

class PositionEngine:
    def __init__(self): self.data = {}
    def load(self): self.data = {c: Position(**v) for c, v in DB.load_pos().items()}
    def save(self): DB.save_pos({c: vars(p) for c, p in self.data.items()})
    def register(self, c, n, p, q):
        self.data[c] = Position(n, p, q, p, p * 0.98, 0) # 초기 손절선 -2%
        self.save(); return self.data[c]
    def remove(self, c):
        p = self.data.pop(c, None); self.save(); return p
    
    # [V4 패치] 3단계 집중 관리 로직
    def check_trailing(self, c, current):
        p = self.data.get(c); msg = None
        if not p: return None
        p.highest = max(p.highest, current)
        gain = pct(current, p.entry)
        
        # 3단계: 고점 대비 -3% 트레일링 스탑 (수익 극대화)
        if gain >= 6 and p.level < 2:
            p.level = 2
            msg = f"🔥 <b>[집중관리 3단계: 끝까지 발라먹기] {p.name}</b>\n수익률 +6% 돌파! 고점 대비 -3% 방어선을 가동합니다."
        # 2단계: 원금 절대 방어 (본전 +1% 컷)
        elif gain >= 3 and p.level < 1:
            p.level = 1
            p.stop = max(p.stop, p.entry * 1.01)
            msg = f"🛡️ <b>[집중관리 2단계: 원금 절대방어] {p.name}</b>\n수익률 +3% 돌파! 방어선을 본전 위({p.stop:,.0f}원)로 올려 절대 손실 안 보는 락을 걸었습니다."

        # 레벨 2일때는 계속 고점 따라가며 방어선 올림
        if p.level == 2: p.stop = max(p.stop, p.highest * 0.97)

        # 방어선 터치 시 청산
        if current <= p.stop:
            msg = f"🛑 <b>[집중관리 종료: 자동 청산] {p.name}</b>\n💰 현재가: {current:,.0f}원 ({gain:+.2f}%)\n지정된 방어선({p.stop:,.0f}원)을 이탈하여 감시를 종료합니다."
            self.remove(c)
        else: self.save()
        return msg
POSITIONS = PositionEngine()

class KIS:
    def __init__(self):
        self.rest = 'https://openapi.koreainvestment.com:9443'; self.ws_url = 'ws://ops.koreainvestment.com:21000'
        self.s = requests.Session(); self.token = None; self.token_exp = None
        self.ws = None; self.appr = None
    
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

    def subscribe(self, code, is_unsub=False):
        if self.ws and self.ws.sock and self.ws.sock.connected and self.appr:
            tr_type = '2' if is_unsub else '1'
            self.ws.send(json.dumps({'header': {'approval_key': self.appr, 'custtype': 'P', 'tr_type': tr_type, 'content-type': 'utf-8'}, 'body': {'input': {'tr_id': SETTINGS.nxt_trade_tr, 'tr_key': code}}}))

    def stream(self, on_tick):
        while True:
            try:
                self.appr = self.s.post(f'{self.rest}/oauth2/Approval', json={'grant_type': 'client_credentials', 'appkey': SETTINGS.kis_app_key, 'secretkey': SETTINGS.kis_app_secret}).json().get('approval_key')
                
                def on_msg(ws, msg):
                    if isinstance(msg, bytes): msg = msg.decode('utf-8')
                    if 'PINGPONG' in msg: ws.send(msg); return
                    p = msg.split('|')
                    if len(p) >= 4 and p[1] == SETTINGS.nxt_trade_tr:
                        f = p[3].split('^'); c = f[0].zfill(6)
                        ts = now().replace(hour=int(f[1][:2]), minute=int(f[1][2:4]), second=0, microsecond=0)
                        on_tick(c, num(f[2]), int(f[12] or 0), num(f[18]), ts)
                
                def on_open(ws):
                    log.info("Websocket connected")
                    def _sub_all():
                        for c in list(STATE.index_etfs.keys()) + list(STATE.vip_targets.keys()):
                            self.subscribe(c); time.sleep(0.25)
                    threading.Thread(target=_sub_all, daemon=True).start()
                    
                self.ws = websocket.WebSocketApp(self.ws_url, on_open=on_open, on_message=on_msg)
                self.ws.run_forever(ping_interval=25, ping_timeout=10)
            except Exception as e: 
                log.error(f"WS 에러: {e}"); time.sleep(10)

KIS_CLIENT = KIS()

class Brain:
    def check_sniper(self, c):
        bars = list(STATE.bars.get(c, []))
        if len(bars) < 15: return None
        latest = bars[-1]; today_open = bars[0].open
        if latest.close <= today_open: return None
        tot_vol = sum(b.volume for b in bars)
        if tot_vol == 0: return None
        vwap = sum((b.high+b.low+b.close)/3 * b.volume for b in bars) / tot_vol
        if not (vwap * 0.995 <= latest.close <= vwap * 1.005): return None
        max_5m_vol = max(sum(b.volume for b in bars[i:i+5]) for i in range(len(bars)-5))
        recent_5m_vol = sum(b.volume for b in bars[-5:])
        if max_5m_vol == 0 or recent_5m_vol > (max_5m_vol * 0.20): return None
        if latest.trade_strength < 105: return None
        return latest.close, vwap * 0.985, vwap
BRAIN = Brain()

class App:
    def __init__(self): self.sent = set(); self.today = ""
    
    def sync_bars_for(self, c):
        raw = KIS_CLIENT.get_bars(c); n = now()
        with STATE.lock:
            q = STATE.bars.setdefault(c, deque(maxlen=390))
            for r in reversed(raw):
                m = n.replace(hour=int(r['stck_cntg_hour'][:2]), minute=int(r['stck_cntg_hour'][2:4]), second=0, microsecond=0)
                q.append(Bar(m, num(r['stck_oprc']), num(r['stck_hgpr']), num(r['stck_lwpr']), num(r['stck_prpr']), int(r['cntg_vol']), 100))
                if c in STATE.index_etfs and STATE.index_opens[c] == 0: STATE.index_opens[c] = num(r['stck_oprc'])

    def generate_chart_io(self, c, vwap):
        try:
            bars = list(STATE.bars.get(c, []))[-60:]
            if not bars: return None
            times = [b.minute.strftime('%H:%M') for b in bars]; prices = [b.close for b in bars]
            plt.figure(figsize=(6, 3))
            plt.plot(times, prices, color='red', linewidth=1.5, label='Price')
            plt.axhline(y=vwap, color='blue', linestyle='--', linewidth=1, label='VWAP (Support)')
            plt.xticks(times[::10], rotation=45); plt.grid(True, alpha=0.3); plt.legend(loc='upper right')
            plt.tight_layout()
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100); buf.seek(0)
            plt.clf(); plt.close('all') 
            return buf
        except: plt.close('all'); return None

    def update_dashboard(self):
        msg = f"📊 <b>[VIP 전광판]</b> {now().strftime('%H:%M:%S')}\n⚙️ 한도: {STATE.vip_limit}개 (사용: {len(STATE.vip_targets)}개)\n\n"
        
        msg += "🎯 <b>[감시 대기 중인 타겟]</b>\n"
        targets_only = {c: n for c, n in STATE.vip_targets.items() if c not in POSITIONS.data}
        if not targets_only: msg += "  └ (없음)\n"
        else:
            for c, n in targets_only.items(): msg += f" • {n}\n"
        
        msg += "\n💼 <b>[집중 관리 중 (보유)]</b>\n"
        if not POSITIONS.data: msg += "  └ 관망 중\n"
        else:
            for c, pos in POSITIONS.data.items():
                curr = STATE.bars[c][-1].close if STATE.bars.get(c) else pos.entry
                gain = pct(curr, pos.entry)
                msg += f" • <b>{pos.name}</b>: {curr:,.0f}원 ({gain:+.2f}%)\n    └ 🛡️ 방어선: {pos.stop:,.0f}원 (Lv.{pos.level})\n"
        
        if BOT.dashboard_msg_id is None: BOT.dashboard_msg_id = BOT.send(msg)
        else: BOT.edit_message(SETTINGS.chat_id, BOT.dashboard_msg_id, msg)

    def parse_nlp_command(self, txt, chat_id):
        if not HAS_GEMINI or not SETTINGS.gemini_api_key: return False
        try:
            genai.configure(api_key=SETTINGS.gemini_api_key)
            prompt = f"""사용자의 지시: "{txt}"
현재 감시 목록: {list(STATE.vip_targets.values())}
다음 포맷의 JSON으로 반환해.
action: "add"(타겟 추가), "remove"(타겟 삭제), "buy"(매수/방어 시작), "limit"(한도 변경), "unknown"
stock: 사용자가 언급한 종목명 (add, remove, buy일 때)
price: 매수가격 (buy일 때 숫자만, 모르면 0)
limit: 숫자 (limit일 때만)
"""
            model = genai.GenerativeModel('gemini-2.5-flash', generation_config={"response_mime_type": "application/json"})
            data = json.loads(model.generate_content(prompt).text)
            action = data.get('action')
            
            if action == 'add':
                s_name = data.get('stock')
                if len(STATE.vip_targets) >= STATE.vip_limit:
                    BOT.send(f"⚠️ 감시 한도({STATE.vip_limit}개) 초과. 타겟을 먼저 삭제하거나 한도를 늘리세요.", chat_id); return True
                code, exact = get_stock_code(s_name)
                if code:
                    STATE.vip_targets[code] = exact; DB.save_vip({"limit": STATE.vip_limit, "targets": STATE.vip_targets})
                    KIS_CLIENT.subscribe(code); WORKER.submit(lambda: self.sync_bars_for(code))
                    BOT.send(f"🎯 <b>[{exact}]</b> VIP 감시망에 추가 완료!\n0.1초 단위 밀착 감시를 시작합니다.", chat_id)
                else: BOT.send(f"🤔 '{s_name}' 종목 코드를 찾지 못했습니다. 정확한 이름을 입력해주세요.", chat_id)
                return True
                
            elif action == 'remove':
                s_name = data.get('stock')
                for c, n in list(STATE.vip_targets.items()):
                    if s_name in n or s_name == n:
                        del STATE.vip_targets[c]; DB.save_vip({"limit": STATE.vip_limit, "targets": STATE.vip_targets})
                        KIS_CLIENT.subscribe(c, is_unsub=True)
                        BOT.send(f"🗑️ <b>[{n}]</b> VIP 감시망에서 삭제했습니다.", chat_id); return True
                return True
                
            elif action == 'limit':
                lim = int(data.get('limit', 5))
                STATE.vip_limit = lim; DB.save_vip({"limit": STATE.vip_limit, "targets": STATE.vip_targets})
                BOT.send(f"⚙️ 최대 감시 한도를 <b>{lim}개</b>로 변경했습니다.", chat_id); return True
                
            elif action == 'buy':
                s_name = data.get('stock'); price = float(data.get('price', 0))
                for c, n in STATE.vip_targets.items():
                    if s_name in n or s_name == n:
                        p = POSITIONS.register(c, n, price, 1)
                        BOT.send(f"🤖 <b>[집중관리 시작]</b> {n} 매수 확인!\n최초 방어선({p.stop:,.0f}원)을 설정하고 보호합니다.", chat_id); return True
                return True
            return False
        except Exception as e: log.error(e); return False

    def on_tick(self, c, p, v, ts, m):
        with STATE.lock:
            q = STATE.bars.setdefault(c, deque(maxlen=390))
            if not q or q[-1].minute != m: q.append(Bar(m, p, p, p, p, v, ts))
            else:
                b = q[-1]; b.high = max(b.high, p); b.low = min(b.low, p); b.close = p; b.volume += v; b.trade_strength = ts
        
        if c in STATE.index_etfs and STATE.index_opens[c] > 0:
            drop_pct = pct(p, STATE.index_opens[c])
            if drop_pct <= SETTINGS.circuit_breaker_pct and not STATE.circuit_breaker:
                STATE.circuit_breaker = True; BOT.send(f"🚨 <b>[서킷 브레이커 발동]</b> 시장 폭락 감지. 감시 중지.")
        
        msg = POSITIONS.check_trailing(c, p)
        if msg: BOT.send(msg)

        if 8 <= m.hour < 20 and c in STATE.vip_targets:
            if not STATE.circuit_breaker and c not in POSITIONS.data and c not in self.sent:
                res = BRAIN.check_sniper(c)
                if res:
                    price, vwap_stop, vwap_val = res
                    self.sent.add(c)
                    name = STATE.vip_targets.get(c, c)
                    
                    deep_link = f"https://m.stock.naver.com/domestic/stock/{c}/total"
                    inline_kb = {"inline_keyboard": [
                        [{"text": f"📈 네이버/MTS 차트 바로가기", "url": deep_link}],
                        [{"text": f"💰 현재가({price:,.0f}원) 집중 관리(방어) 시작", "callback_data": f"buy_{c}_{price}"}],
                        [{"text": "🗑️ 이번 타점 무시 (패스)", "callback_data": f"pass_{c}"}]
                    ]}
                    
                    caption = f"🔫 <b>[VIP 타점 포착] {name}</b>\n\nVIP 종목에 V자 수급 반등이 감지되었습니다.\n💰 <b>VWAP 맥점:</b> {price:,.0f}원\n🛡️ <b>예상 손절선:</b> {vwap_stop:,.0f}원\n\n👇 <i>매수 후 버튼을 누르면 즉시 집중관리가 시작됩니다.</i>"
                    chart_io = self.generate_chart_io(c, vwap_val)
                    if chart_io: BOT.send_photo(chart_io, caption, reply_markup=inline_kb)
                    else: BOT.send(caption, reply_markup=inline_kb) 

    def run_vip_news(self):
        vip_names = ", ".join(STATE.vip_targets.values())
        if not vip_names: BOT.send("분석할 VIP 종목이 없습니다."); return
        BOT.send(f"📰 <b>[{vip_names}]</b>\n최신 호재와 모멘텀을 AI가 심층 분석 중입니다. 잠시만 기다려주세요...")
        try:
            genai.configure(api_key=SETTINGS.gemini_api_key)
            res = genai.GenerativeModel('gemini-2.5-flash').generate_content(f"오늘 한국 증시에서 다음 종목들에 대한 최신 뉴스, 수주 공시, 호재를 심도있게 분석해줘: {vip_names}")
            BOT.send(f"📰 <b>[VIP 전담 브리핑]</b>\n\n{res.text}")
        except: BOT.send("AI 뉴스 검색 중 오류가 발생했습니다.")

    def scheduler(self):
        last_dash_update = 0
        while True:
            n = now(); d = str(n.date())
            if self.today != d: self.today = d; self.sent.clear(); STATE.circuit_breaker = False
            if time.time() - last_dash_update > 10: WORKER.submit(self.update_dashboard); last_dash_update = time.time()
            time.sleep(2) 

    def handle_callback(self, cb):
        cb_id = cb['id']; data = cb.get('data', ''); msg = cb.get('message', {})
        chat_id = msg.get('chat', {}).get('id'); msg_id = msg.get('message_id')
        
        if data.startswith('buy_'):
            _, c, p_str = data.split('_'); price = float(p_str); name = STATE.vip_targets.get(c, c)
            POSITIONS.register(c, name, price, 1) 
            BOT.answer_callback(cb_id, text=f"✅ {name} 집중 관리 시작!")
            orig_text = msg.get('caption', '') or msg.get('text', '')
            new_text = orig_text.replace("👇 매수 후 버튼을 누르면 즉시 집중관리가 시작됩니다.", f"✅ <b>[집중 관리 가동 중]</b> 기준가: {price:,.0f}원")
            try: BOT.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/editMessageCaption', json={'chat_id': str(chat_id), 'message_id': msg_id, 'caption': new_text, 'parse_mode': 'HTML'})
            except: pass
            
        elif data.startswith('pass_'):
            BOT.answer_callback(cb_id, text="🗑️ 관망합니다.")
            orig_text = msg.get('caption', '') or msg.get('text', '')
            new_text = orig_text.replace("👇 매수 후 버튼을 누르면 즉시 집중관리가 시작됩니다.", "🗑️ <b>[패스 처리됨]</b>")
            try: BOT.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/editMessageCaption', json={'chat_id': str(chat_id), 'message_id': msg_id, 'caption': new_text, 'parse_mode': 'HTML'})
            except: pass

    def handle_text(self, txt, chat):
        if txt == "📊 타겟 현황 / 대시보드": WORKER.submit(self.update_dashboard); return
        elif txt == "📰 VIP 타겟 뉴스/공시": WORKER.submit(self.run_vip_news); return
        elif txt == "🛑 모든 방어선 해제":
            POSITIONS.data.clear(); POSITIONS.save(); BOT.send("🛑 모든 종목의 방어선과 집중관리를 해제했습니다.", chat); return
        elif txt in ["📖 매뉴얼 보기", "/도움말", "/help", "도움말", "?"]:
            help_text = """
🤖 <b>[쾌걸스민 V4 VIP 집중 관리 모드]</b>

사용자님만의 '소수 정예 주력 종목'을 집중 감시합니다.

<b>1. 🎯 감시 타겟 추가/삭제 (그냥 말하세요!)</b>
• "지엔씨에너지 감시 추가해"
• "테크윙 감시 빼줘"
• "최대 감시 한도 10개로 변경해"

<b>2. 💼 매수 후 3단계 집중 관리</b>
타점 포착 후 버튼을 누르거나, <b>"지엔씨 15000원 매수했어 방어해"</b>라고 치면 즉시 가동!
• <b>[1단계]</b> 기본 손절 방어 (-2%)
• <b>[2단계]</b> 수익 3% 돌파 시 -> 원금 절대방어 락
• <b>[3단계]</b> 수익 6% 돌파 시 -> 고점 트레일링 스탑

<b>3. 🕹️ 하단 리모컨 100% 활용</b>
• <b>[VIP 타겟 뉴스]</b>: 내 주력 종목 호재만 딥 다이브!
"""
            BOT.send(help_text.strip(), chat); return

        if not txt.startswith('/'):
            if not self.parse_nlp_command(txt, chat):
                BOT.send("🤔 명령을 이해하지 못했습니다. 종목 추가/삭제/매수는 자연스럽게 말씀해주세요!\n(매뉴얼 보기: 하단 버튼)", chat)

    def start(self):
        WORKER.start(); POSITIONS.load(); BOT.text_handler = self.handle_text; BOT.callback_handler = self.handle_callback
        for c in STATE.index_etfs.keys(): WORKER.submit(lambda code=c: self.sync_bars_for(code))
        for c in STATE.vip_targets.keys(): WORKER.submit(lambda code=c: self.sync_bars_for(code))
        threading.Thread(target=BOT.poll, daemon=True).start()
        threading.Thread(target=self.scheduler, daemon=True).start()
        threading.Thread(target=KIS_CLIENT.stream, args=(self.on_tick,), daemon=True).start()
        BOT.send(f"🚀 <b>쾌걸스민 V{SETTINGS.version} 기동 완료!</b>\nVIP 전담 감시망이 가동되었습니다. 하단 버튼으로 매뉴얼을 확인하세요!")

APP = App(); web = Flask(__name__)
@web.get('/')
def root(): return 'Kkwaegeol Seumin V4 VIP Sniper Running', 200

if __name__ == '__main__':
    threading.Thread(target=lambda: (time.sleep(2), APP.start()), daemon=True).start()
    web.run(host='0.0.0.0', port=SETTINGS.port, threaded=True, use_reloader=False)
