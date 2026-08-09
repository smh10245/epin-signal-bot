from __future__ import annotations

# ==========================================
# 🤖 쾌걸스민 V3.1.2 (The Absolute Zenith / 통합 완성본)
# 1. 텔레그램 실시간 전광판 (10초 주기 Live Dashboard)
# 2. 자연어 매매 지시 파싱 (Gemini JSON 구조화)
# 3. MTS 호가창 다이렉트 딥링크 버튼 추가
# 4. 차트 즉석 렌더링 + 찌라시 판독 + 서킷브레이커
# 5. [안정화] in_session 함수 및 KIS 웹소켓 DDoS 방지 패치
# 6. [UX] /도움말 매뉴얼 기능 완벽 통합
# ==========================================

import os, json, time, threading, queue, logging, io
from datetime import datetime, timedelta, timezone
from collections import deque
from dataclasses import dataclass

import requests, websocket
import pandas as pd
import FinanceDataReader as fdr
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
log = logging.getLogger('seumin.v312')

# ===== 1. 유틸리티 함수 =====
def now(): return datetime.now(KST)
def num(v, d=0.0):
    try: return float(str(v).replace(',', '').replace('원', '').replace('주', '').strip())
    except: return d
def pct(n, o): return (n / o - 1) * 100 if o else 0

def in_session(sh, eh):
    n = now()
    if n.weekday() >= 5: return False
    try:
        sh_h, sh_m = map(int, sh.split(':')); eh_h, eh_m = map(int, eh.split(':'))
        return n.replace(hour=sh_h, minute=sh_m, second=0, microsecond=0) <= n <= n.replace(hour=eh_h, minute=eh_m, second=0, microsecond=0)
    except: return False

@dataclass(frozen=True)
class Settings:
    version: str = '3.1.2 (The Zenith Final)'
    telegram_token: str = os.getenv('TELEGRAM_TOKEN', '').strip()
    chat_id: str = (os.getenv('CHAT_ID') or os.getenv('TELEGRAM_CHAT_ID') or '').strip()
    gemini_api_key: str = os.getenv('GEMINI_API_KEY', '').strip()
    kis_app_key: str = os.getenv('KIS_APP_KEY', '').strip()
    kis_app_secret: str = os.getenv('KIS_APP_SECRET', '').strip()
    kis_env: str = os.getenv('KIS_ENV', 'real').strip().lower()
    port: int = int(os.getenv('PORT', 10000))
    min_market_cap: float = 100_000_000_000  
    min_daily_volume: int = 20_000
    circuit_breaker_pct: float = -1.5        
    nxt_trade_tr: str = os.getenv('KIS_NXT_TRADE_TR_ID', 'H0NXCNT0')

SETTINGS = Settings()

@dataclass
class Bar: minute: datetime; open: float; high: float; low: float; close: float; volume: int; trade_strength: float
@dataclass
class Position: name: str; entry: float; qty: float; highest: float; stop: float

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
    def __init__(self): 
        self.s = requests.Session(); self.offset = 0
        self.text_handler = None; self.callback_handler = None
        self.dashboard_msg_id = None
    
    @property
    def default_markup(self):
        return {
            "keyboard": [[{"text": "☀️ 아침 브리핑 호출"}, {"text": "🌙 야간 브리핑 호출"}], [{"text": "🛑 모든 방어선 해제"}]],
            "resize_keyboard": True
        }

    def send(self, text, chat=None, reply_markup=None): 
        def _send():
            payload = {'chat_id': str(chat or SETTINGS.chat_id).strip(), 'text': text, 'parse_mode': 'HTML', 'reply_markup': reply_markup or self.default_markup}
            try: 
                r = self.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/sendMessage', json=payload, timeout=10).json()
                return r.get('result', {}).get('message_id')
            except: return None
        return _send() 

    def send_photo(self, photo_io, caption, chat=None, reply_markup=None):
        def _send_photo():
            url = f'https://api.telegram.org/bot{SETTINGS.telegram_token}/sendPhoto'
            data = {'chat_id': str(chat or SETTINGS.chat_id).strip(), 'caption': caption, 'parse_mode': 'HTML'}
            if reply_markup: data['reply_markup'] = json.dumps(reply_markup)
            try: self.s.post(url, data=data, files={'photo': ('chart.png', photo_io, 'image/png')}, timeout=15)
            except Exception as e: log.error(f"사진 전송 에러: {e}")
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
        self.lock = threading.RLock(); self.names = {}; self.meta = {}; self.candidates = []; self.bars = {}; self.top_20_list = []; self.circuit_breaker = False
        self.index_etfs = {'069500': 'KOSPI', '226490': 'KOSDAQ'}; self.index_opens = {'069500': 0, '226490': 0}

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
        self.data[c] = Position(n, p, q, p, p * 0.98) 
        self.save(); return self.data[c]
    def remove(self, c):
        p = self.data.pop(c, None); self.save(); return p
    def check_trailing(self, c, current):
        p = self.data.get(c); msg = None
        if not p: return None
        p.highest = max(p.highest, current)
        gain = pct(current, p.entry)
        if gain >= 6: p.stop = max(p.stop, p.highest * 0.97) 
        elif gain >= 3: p.stop = max(p.stop, p.entry * 1.01) 
        if current <= p.stop:
            msg = f"🛡️ <b>[쾌걸스민 자동 청산] {p.name}</b>\n💰 현재가: {current:,.0f}원 ({gain:+.2f}%)\n지정된 방어선을 이탈하여 감시 종료합니다."
            self.remove(c)
        else: self.save()
        return msg
POSITIONS = PositionEngine()

class KIS:
    def __init__(self):
        self.rest = 'https://openapi.koreainvestment.com:9443'; self.ws = 'ws://ops.koreainvestment.com:21000'
        self.s = requests.Session(); self.token = None; self.token_exp = None
    
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
                appr_res = self.s.post(f'{self.rest}/oauth2/Approval', json={'grant_type': 'client_credentials', 'appkey': SETTINGS.kis_app_key, 'secretkey': SETTINGS.kis_app_secret}).json()
                appr = appr_res.get('approval_key')
                
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
                    def _subscribe():
                        for c in STATE.candidates[:40]:
                            try:
                                if ws.sock and ws.sock.connected:
                                    ws.send(json.dumps({'header': {'approval_key': appr, 'custtype': 'P', 'tr_type': '1', 'content-type': 'utf-8'}, 'body': {'input': {'tr_id': SETTINGS.nxt_trade_tr, 'tr_key': c}}}))
                                    time.sleep(0.25) 
                                else:
                                    log.warning("서버에 의해 웹소켓이 끊어져 구독 루프를 중단합니다.")
                                    break
                            except Exception as e:
                                log.warning(f"구독 요청 중단: {e}")
                                break
                    threading.Thread(target=_subscribe, daemon=True).start()
                    
                ws = websocket.WebSocketApp(self.ws, on_open=on_open, on_message=on_msg)
                ws.run_forever(ping_interval=25, ping_timeout=10)
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

    def get_closing_bets(self):
        picks = []
        for c in STATE.candidates[2:102]:
            bars = list(STATE.bars.get(c, []))
            if len(bars) < 300: continue
            day_high = max(b.high for b in bars); day_low = min(b.low for b in bars); latest = bars[-1].close
            if latest < day_low + (day_high - day_low) * 0.85: continue
            vol_lunch = sum(b.volume for b in bars if 12 <= b.minute.hour < 14); vol_after = sum(b.volume for b in bars if b.minute.hour >= 14)
            if vol_lunch > 0 and vol_after < (vol_lunch * 1.5): continue
            try:
                df = fdr.DataReader(c, start=now().date() - timedelta(days=40))
                if len(df) < 20: continue
                if df['Close'].rolling(5).mean().iloc[-1] <= df['Close'].rolling(20).mean().iloc[-1]: continue
                picks.append((c, STATE.names.get(c, c), latest))
            except: continue
            if len(picks) >= 3: break
        return picks
BRAIN = Brain()

class App:
    def __init__(self): self.sent = set(); self.today = ""
    
    def sync_bars(self):
        n = now()
        for c in STATE.candidates[:40]:
            raw = KIS_CLIENT.get_bars(c)
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
        cb_status = '발동됨🚨' if STATE.circuit_breaker else '정상🟢'
        msg = f"📊 <b>[쾌걸스민 LIVE 전광판]</b> {now().strftime('%H:%M:%S')}\n"
        msg += f"서킷브레이커: {cb_status} | 금일 포착: {len(self.sent)}건\n\n"
        msg += "💼 <b>[방어 중인 종목]</b>\n"
        if not POSITIONS.data: msg += "  └ 관망 중\n"
        else:
            for c, pos in POSITIONS.data.items():
                curr = STATE.bars[c][-1].close if STATE.bars.get(c) else pos.entry
                gain = pct(curr, pos.entry)
                msg += f"• <b>{pos.name}</b>: {curr:,.0f}원 ({gain:+.2f}%) | 🛡️ {pos.stop:,.0f}원\n"
        if BOT.dashboard_msg_id is None:
            BOT.dashboard_msg_id = BOT.send(msg)
        else:
            BOT.edit_message(SETTINGS.chat_id, BOT.dashboard_msg_id, msg)

    def parse_nlp_command(self, txt, chat_id):
        if not HAS_GEMINI or not SETTINGS.gemini_api_key: return False
        try:
            genai.configure(api_key=SETTINGS.gemini_api_key)
            prompt = f"""
            사용자가 봇에게 지시를 내렸어: "{txt}"
            매수했으니 감시하라는 뜻인지 판단해 종목명과 가격을 추출해. JSON만 대답.
            예시: "에코프로 10만원 방어해" -> {{"action": "buy", "stock": "에코프로", "price": 100000}}
            """
            model = genai.GenerativeModel('gemini-2.5-flash', generation_config={"response_mime_type": "application/json"})
            res = model.generate_content(prompt).text
            data = json.loads(res)
            if data.get('action') == 'buy':
                s_name = data.get('stock'); price = float(data.get('price', 0))
                for code, name in STATE.names.items():
                    if s_name in name or s_name == name:
                        pos = POSITIONS.register(code, name, price, 1)
                        BOT.send(f"🤖 <b>[자연어 접수]</b> {name} 방어선({pos.stop:,.0f}원) 구축 완료!", chat_id)
                        return True
            return False
        except: return False

    def on_tick(self, c, p, v, ts, m):
        with STATE.lock:
            q = STATE.bars.setdefault(c, deque(maxlen=390))
            if not q or q[-1].minute != m: q.append(Bar(m, p, p, p, p, v, ts))
            else:
                b = q[-1]; b.high = max(b.high, p); b.low = min(b.low, p); b.close = p; b.volume += v; b.trade_strength = ts
        
        if c in STATE.index_etfs and STATE.index_opens[c] > 0:
            drop_pct = pct(p, STATE.index_opens[c])
            if drop_pct <= SETTINGS.circuit_breaker_pct and not STATE.circuit_breaker:
                STATE.circuit_breaker = True; BOT.send(f"🚨 <b>[서킷 브레이커 발동]</b> {STATE.index_etfs[c]} 폭락. 단타 중지.")
        
        msg = POSITIONS.check_trailing(c, p)
        if msg: BOT.send(msg)

        if 9 <= m.hour < 14 or (m.hour == 14 and m.minute < 30):
            if not STATE.circuit_breaker and c not in POSITIONS.data and c not in self.sent:
                res = BRAIN.check_sniper(c)
                if res:
                    price, vwap_stop, vwap_val = res
                    self.sent.add(c)
                    name = STATE.names.get(c, c)
                    
                    deep_link = f"https://m.stock.naver.com/domestic/stock/{c}/total"
                    inline_kb = {"inline_keyboard": [
                        [{"text": f"📈 네이버/MTS 차트 바로가기", "url": deep_link}],
                        [{"text": f"💰 현재가({price:,.0f}원) 방어선 구축", "callback_data": f"buy_{c}_{price}"}],
                        [{"text": "🗑️ 차트 구림 (관망)", "callback_data": f"pass_{c}"}]
                    ]}
                    
                    caption = f"🔫 <b>[쾌걸스민 즉석 포착] {name}</b> ({c})\n\n💰 <b>VWAP 맥점:</b> {price:,.0f}원\n🛡️ <b>동적 손절선:</b> {vwap_stop:,.0f}원\n\n👇 <i>차트 확인 후 버튼을 누르세요</i>"
                    
                    chart_io = self.generate_chart_io(c, vwap_val)
                    if chart_io: BOT.send_photo(chart_io, caption, reply_markup=inline_kb)
                    else: BOT.send(caption, reply_markup=inline_kb) 

    def run_night_ai(self):
        top_txt = ", ".join([n for _, n in STATE.top_20_list]) or "데이터 없음"
        if not HAS_GEMINI or not SETTINGS.gemini_api_key:
            DB.save_ai(top_txt); BOT.send(f"🌙 <b>[야간 브리핑]</b>\n오늘 주도주: {top_txt}\n(AI 생략)"); return
        try:
            genai.configure(api_key=SETTINGS.gemini_api_key)
            res = genai.GenerativeModel('gemini-2.5-flash').generate_content(f"오늘 주도주: {top_txt}\n내일 튈 대장주 3개 분석해.")
            DB.save_ai(res.text); BOT.send(f"🌙 <b>[야간 AI 브리핑]</b>\n\n{res.text}")
        except Exception as e: DB.save_ai(top_txt); BOT.send(f"🌙 <b>[야간 오류]</b>\n원시 데이터: {top_txt}")

    def run_morning_ai(self):
        try: us = "\n".join([f"{n}: {(yf.Ticker(s).history(period='2d')['Close'].iloc[-1] / yf.Ticker(s).history(period='2d')['Close'].iloc[-2] - 1)*100:+.2f}%" for n, s in {'S&P500':'^GSPC', 'Nasdaq':'^IXIC'}.items()])
        except: us = "미증시 로드 실패"
        saved = DB.load_ai()
        if not HAS_GEMINI or not SETTINGS.gemini_api_key:
            BOT.send(f"☀️ <b>[아침 브리핑]</b>\n미증시:\n{us}\n\n어제 픽:\n{saved}"); return
        try:
            genai.configure(api_key=SETTINGS.gemini_api_key)
            res = genai.GenerativeModel('gemini-2.5-flash').generate_content(f"미증시: {us}\n어제픽: {saved}\n오늘 아침 단타 관심주 3개 압축해.")
            BOT.send(f"☀️ <b>[아침 AI 브리핑]</b>\n\n{res.text}")
        except: BOT.send(f"☀️ <b>[아침 오류]</b>\n미증시: {us}\n어제픽: {saved}")

    def scheduler(self):
        done = {}
        last_dash_update = 0
        while True:
            n = now(); d = str(n.date())
            if self.today != d: self.today = d; self.sent.clear(); STATE.circuit_breaker = False
            
            if time.time() - last_dash_update > 10:
                WORKER.submit(self.update_dashboard)
                last_dash_update = time.time()

            if n.weekday() < 5:
                if n.hour == 7 and done.get('m_ai') != d: WORKER.submit(self.run_morning_ai); done['m_ai'] = d
                if n.hour == 9 and done.get('sync') != d: WORKER.submit(self.sync_bars); done['sync'] = d
                if n.hour == 15 and n.minute >= 15 and done.get('close') != d:
                    picks = BRAIN.get_closing_bets()
                    txt = "\n".join([f"• {n} ({c}): {p:,.0f}원" for c, n, p in picks]) or "조건 만족 없음"
                    BOT.send(f"🌇 <b>[쾌걸스민 종배 3선]</b>\n\n{txt}"); done['close'] = d
                if n.hour == 22 and done.get('n_ai') != d: WORKER.submit(self.run_night_ai); done['n_ai'] = d
            time.sleep(2) 

    def handle_callback(self, cb):
        cb_id = cb['id']; data = cb.get('data', ''); msg = cb.get('message', {})
        chat_id = msg.get('chat', {}).get('id'); msg_id = msg.get('message_id')
        
        if data.startswith('buy_'):
            _, c, p_str = data.split('_'); price = float(p_str); name = STATE.names.get(c, c)
            POSITIONS.register(c, name, price, 1) 
            BOT.answer_callback(cb_id, text=f"✅ {name} 방어선 구축 완료!")
            orig_text = msg.get('caption', '') or msg.get('text', '')
            new_text = orig_text.replace("👇 차트 확인 후 버튼을 누르세요", f"✅ <b>[방어선 가동 중]</b> 기준가: {price:,.0f}원")
            try: BOT.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/editMessageCaption', json={'chat_id': str(chat_id), 'message_id': msg_id, 'caption': new_text, 'parse_mode': 'HTML'})
            except: pass
            
        elif data.startswith('pass_'):
            BOT.answer_callback(cb_id, text="🗑️ 관망합니다.")
            orig_text = msg.get('caption', '') or msg.get('text', '')
            new_text = orig_text.replace("👇 차트 확인 후 버튼을 누르세요", "🗑️ <b>[스팸 락 / 관망 패스]</b>")
            try: BOT.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/editMessageCaption', json={'chat_id': str(chat_id), 'message_id': msg_id, 'caption': new_text, 'parse_mode': 'HTML'})
            except: pass

    def handle_text(self, txt, chat):
        # 1. 리모컨 매핑
        if txt == "☀️ 아침 브리핑 호출": WORKER.submit(self.run_morning_ai); return
        elif txt == "🌙 야간 브리핑 호출": WORKER.submit(self.run_night_ai); return
        elif txt == "🛑 모든 방어선 해제":
            POSITIONS.data.clear(); POSITIONS.save(); BOT.send("🛑 모든 감시와 방어선이 해제되었습니다.", chat); return
        
        p = txt.split(); cmd = p[0]
        # 2. 수동 명령어
        if cmd == '/상태': pass 
        elif cmd == '/매도' and len(p) >= 2:
            c = "".join(p[1:])
            for code, name in STATE.names.items():
                if c in name or c == code:
                    if POSITIONS.remove(code): BOT.send(f"✅ {name} 감시 종료.", chat)
                    return
        elif cmd in ['/도움말', '/help', '도움말', '?']:
            help_text = """
🤖 <b>[쾌걸스민 V3.1.2 사용 매뉴얼]</b>

<b>1. 🕹️ 하단 전용 리모컨</b>
입력창 밑의 버튼을 누르시면 됩니다.
• [아침/야간 브리핑]: AI 시황 즉시 호출
• [모든 방어선 해제]: 감시 중인 전 종목 락 해제

<b>2. 🗣️ 자연어 매매 지시 (카톡하듯 대화)</b>
명령어 없이 편하게 치세요! (Gemini AI 파싱)
<i>예) "스민아 에코프로 10만원에 샀어 방어해"</i>

<b>3. 🎯 타점 포착 시 (1-Click 감시)</b>
스나이퍼 시그널이 오면 메시지 하단의
<b>[💰 현재가 방어선 구축]</b> 버튼을 터치하세요!
봇이 즉각 트레일링 스탑 감시를 시작합니다.

<b>4. ⌨️ 수동 명령어 (예비용)</b>
• <code>/매도 [종목명]</code> : 특정 종목만 감시 종료
• <code>/도움말</code> : 이 매뉴얼 다시 보기
"""
            BOT.send(help_text.strip(), chat)

        # 3. 자연어 매매 지시 AI 파싱
        elif not cmd.startswith('/'):
            if not self.parse_nlp_command(txt, chat):
                BOT.send("🤔 명령을 이해하지 못했습니다.\n(매뉴얼을 보려면 '/도움말'을 입력하세요.)", chat)

    def start(self):
        WORKER.start(); POSITIONS.load(); BOT.text_handler = self.handle_text; BOT.callback_handler = self.handle_callback
        threading.Thread(target=BOT.poll, daemon=True).start()
        threading.Thread(target=self.scheduler, daemon=True).start()
        STATE.load_candidates()
        if in_session("08:00", "15:30"): self.sync_bars()
        threading.Thread(target=KIS_CLIENT.stream, args=(self.on_tick,), daemon=True).start()
        BOT.send(f"🚀 <b>쾌걸스민 V{SETTINGS.version} 기동 완료!</b>\n이제 채팅창 상단에 실시간 전광판이 켜집니다. (매뉴얼: /도움말)")

APP = App(); web = Flask(__name__)
@web.get('/')
def root(): return 'Kkwaegeol Seumin V3.1.2 Zenith Final', 200

if __name__ == '__main__':
    threading.Thread(target=lambda: (time.sleep(2), APP.start()), daemon=True).start()
    web.run(host='0.0.0.0', port=SETTINGS.port, threaded=True, use_reloader=False)
