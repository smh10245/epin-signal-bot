from __future__ import annotations

# ==========================================
# 🤖 쾌걸스민 V3.0 (The Free-Tier God / 무과금 끝판왕)
# 1. 텔레그램 실시간 1분봉 차트(PNG) 즉석 렌더링 & 전송
# 2. 타점 포착 시 Gemini 즉석 상승 이유(찌라시) 판독 결합
# 3. 1-Click 다중 비중 방어선(인라인 키보드) UI
# 4. 극한의 메모리 최적화 (512MB Render 생존 보장)
# ==========================================

import os, json, time, threading, queue, logging, io
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

# [추가됨] 무과금 차트 렌더링을 위한 라이브러리 (서버 메모리 절약을 위해 Agg 백엔드 사용)
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
log = logging.getLogger('seumin.v3')

def now(): return datetime.now(KST)
def num(v, d=0.0):
    try: return float(str(v).replace(',', '').replace('원', '').replace('주', '').strip())
    except: return d
def pct(n, o): return (n / o - 1) * 100 if o else 0

@dataclass(frozen=True)
class Settings:
    version: str = '3.0 (무과금 끝판왕)'
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
    def __init__(self): self.s = requests.Session(); self.offset = 0; self.text_handler = None; self.callback_handler = None
    
    @property
    def default_markup(self):
        return {
            "keyboard": [[{"text": "📊 봇 상태 확인"}, {"text": "💼 내 계좌 방어선"}], [{"text": "☀️ 아침 브리핑 호출"}, {"text": "🌙 야간 브리핑 호출"}]],
            "resize_keyboard": True
        }

    def send(self, text, chat=None, reply_markup=None): 
        def _send():
            payload = {'chat_id': str(chat or SETTINGS.chat_id).strip(), 'text': text, 'parse_mode': 'HTML', 'reply_markup': reply_markup or self.default_markup}
            try: self.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/sendMessage', json=payload, timeout=10)
            except: pass
        WORKER.submit(_send)

    # [핵심 V3.0] 사진(차트) 전송 기능 추가
    def send_photo(self, photo_io, caption, chat=None, reply_markup=None):
        def _send_photo():
            url = f'https://api.telegram.org/bot{SETTINGS.telegram_token}/sendPhoto'
            data = {'chat_id': str(chat or SETTINGS.chat_id).strip(), 'caption': caption, 'parse_mode': 'HTML'}
            if reply_markup: data['reply_markup'] = json.dumps(reply_markup)
            try: 
                self.s.post(url, data=data, files={'photo': ('chart.png', photo_io, 'image/png')}, timeout=15)
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

    # [핵심 V3.0] 극한의 메모리 최적화 차트 생성기
    def generate_chart_io(self, c, vwap):
        try:
            bars = list(STATE.bars.get(c, []))[-60:] # 최근 1시간만 (메모리 절약)
            if not bars: return None
            times = [b.minute.strftime('%H:%M') for b in bars]; prices = [b.close for b in bars]
            
            plt.figure(figsize=(6, 3))
            plt.plot(times, prices, color='red', linewidth=1.5, label='Price')
            plt.axhline(y=vwap, color='blue', linestyle='--', linewidth=1, label='VWAP (Support)')
            plt.xticks(times[::10], rotation=45); plt.grid(True, alpha=0.3); plt.legend(loc='upper right')
            plt.tight_layout()
            
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100); buf.seek(0)
            
            plt.clf(); plt.close('all') # 메모리 강제 삭제 (매우 중요)
            return buf
        except Exception as e:
            log.error(f"차트 렌더링 실패: {e}")
            plt.close('all'); return None

    # [핵심 V3.0] Gemini 즉석 찌라시 판독기
    def get_instant_ai_reason(self, name):
        if not HAS_GEMINI or not SETTINGS.gemini_api_key: return "AI 키 누락으로 판독 불가"
        try:
            genai.configure(api_key=SETTINGS.gemini_api_key)
            prompt = f"현재 한국 주식시장에서 '{name}' 주식이 거래량이 터지며 급등 중이야. 오늘 이 주식이 속한 테마나 최근 이슈(찌라시)를 기반으로 상승 이유를 딱 1줄로 짧게 추정해봐."
            return genai.GenerativeModel('gemini-2.5-flash').generate_content(prompt).text.strip()
        except: return "일시적 수급 쏠림 추정"

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
                    
                    # 1. 찌라시 즉석 판독 (Thread Blocking 방지를 위해 여기서 호출)
                    ai_reason = self.get_instant_ai_reason(name)
                    
                    # 2. 1-Click 다중 방어선 버튼 UI
                    inline_kb = {"inline_keyboard": [
                        [{"text": f"💰 100만 원 (1차 방어선)", "callback_data": f"buy_{c}_{price}"}],
                        [{"text": f"💰 500만 원 (비중 확대)", "callback_data": f"buy_{c}_{price}"}],
                        [{"text": "🗑️ 차트 구림 (관망)", "callback_data": f"pass_{c}"}]
                    ]}
                    
                    caption = f"🔫 <b>[쾌걸스민 즉석 포착] {name}</b> ({c})\n\n💰 <b>VWAP 맥점:</b> {price:,.0f}원\n🛡️ <b>동적 손절선:</b> {vwap_stop:,.0f}원\n\n🤖 <b>AI 찌라시 판독:</b>\n{ai_reason}\n\n👇 <i>HTS 매수 후 아래 버튼 터치!</i>"
                    
                    # 3. 차트 렌더링 및 텔레그램 발송
                    chart_io = self.generate_chart_io(c, vwap_val)
                    if chart_io: BOT.send_photo(chart_io, caption, reply_markup=inline_kb)
                    else: BOT.send(caption, reply_markup=inline_kb) # 차트 실패 시 텍스트만

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
        while True:
            n = now(); d = str(n.date())
            if self.today != d: self.today = d; self.sent.clear(); STATE.circuit_breaker = False
            if n.weekday() < 5:
                if n.hour == 7 and done.get('m_ai') != d: WORKER.submit(self.run_morning_ai); done['m_ai'] = d
                if n.hour == 9 and done.get('sync') != d: WORKER.submit(self.sync_bars); done['sync'] = d
                if n.hour == 15 and n.minute >= 15 and done.get('close') != d:
                    picks = BRAIN.get_closing_bets()
                    txt = "\n".join([f"• {n} ({c}): {p:,.0f}원" for c, n, p in picks]) or "조건 만족 없음"
                    BOT.send(f"🌇 <b>[쾌걸스민 종배 3선]</b>\n\n{txt}"); done['close'] = d
                if n.hour == 22 and done.get('n_ai') != d: WORKER.submit(self.run_night_ai); done['n_ai'] = d
            time.sleep(20)

    def handle_callback(self, cb):
        cb_id = cb['id']; data = cb.get('data', ''); msg = cb.get('message', {})
        chat_id = msg.get('chat', {}).get('id'); msg_id = msg.get('message_id')
        
        if data.startswith('buy_'):
            _, c, p_str = data.split('_'); price = float(p_str); name = STATE.names.get(c, c)
            POSITIONS.register(c, name, price, 1) 
            BOT.answer_callback(cb_id, text=f"✅ {name} 방어선 구축 완료!")
            orig_text = msg.get('caption', '') or msg.get('text', '')
            new_text = orig_text.replace("👇 HTS 매수 후 아래 버튼 터치!", f"✅ <b>[방어선 가동 중]</b> 기준가: {price:,.0f}원")
            # 사진 메시지 캡션 수정
            try: BOT.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/editMessageCaption', json={'chat_id': str(chat_id), 'message_id': msg_id, 'caption': new_text, 'parse_mode': 'HTML'})
            except: pass
            
        elif data.startswith('pass_'):
            BOT.answer_callback(cb_id, text="🗑️ 관망합니다.")
            orig_text = msg.get('caption', '') or msg.get('text', '')
            new_text = orig_text.replace("👇 HTS 매수 후 아래 버튼 터치!", "🗑️ <b>[스팸 락 / 관망 패스]</b>")
            try: BOT.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/editMessageCaption', json={'chat_id': str(chat_id), 'message_id': msg_id, 'caption': new_text, 'parse_mode': 'HTML'})
            except: pass

    def handle_text(self, txt, chat):
        if txt == "📊 봇 상태 확인": txt = "/상태"
        elif txt == "💼 내 계좌 방어선": txt = "/보유"
        elif txt == "☀️ 아침 브리핑 호출": WORKER.submit(self.run_morning_ai); return
        elif txt == "🌙 야간 브리핑 호출": WORKER.submit(self.run_night_ai); return
        
        p = txt.split(); cmd = p[0]
        if cmd == '/상태': BOT.send(f"🤖 쾌걸스민 V3.0 가동\n- 무과금 차트 엔진 ON\n- AI 찌라시 즉석 판독 ON\n- 서킷브레이커: {'발동🚨' if STATE.circuit_breaker else '정상🟢'}", chat)
        elif cmd == '/보유':
            if not POSITIONS.data: BOT.send("📭 방어 중인 종목이 없습니다.", chat)
            else:
                msg = "💼 <b>[내 계좌 방어 현황]</b>\n\n"
                for c, pos in POSITIONS.data.items():
                    curr = STATE.bars[c][-1].close if STATE.bars.get(c) else pos.entry
                    gain = pct(curr, pos.entry)
                    msg += f"• <b>{pos.name}</b> (진입: {pos.entry:,.0f})\n  현재: {curr:,.0f} ({gain:+.2f}%) | 🛡️ 방어: {pos.stop:,.0f}\n"
                BOT.send(msg, chat)

    def start(self):
        WORKER.start(); POSITIONS.load(); BOT.text_handler = self.handle_text; BOT.callback_handler = self.handle_callback
        threading.Thread(target=BOT.poll, daemon=True).start()
        threading.Thread(target=self.scheduler, daemon=True).start()
        STATE.load_candidates()
        if in_session("08:00", "15:30"): self.sync_bars()
        threading.Thread(target=KIS_CLIENT.stream, args=(self.on_tick,), daemon=True).start()
        BOT.send(f"🚀 <b>쾌걸스민 V{SETTINGS.version} 기동 완료!</b>\n무과금의 한계를 넘었습니다. 하단 리모컨을 눌러보십시오.")

APP = App(); web = Flask(__name__)
@web.get('/')
def root(): return 'Kkwaegeol Seumin V3.0 God-Tier Running', 200

if __name__ == '__main__':
    threading.Thread(target=lambda: (time.sleep(2), APP.start()), daemon=True).start()
    web.run(host='0.0.0.0', port=SETTINGS.port, threaded=True, use_reloader=False)
