from __future__ import annotations

# ============================================================
# 🤖 뽕실 VIP V5 - Repeat Sniper
# 목적:
#   - 사용자가 직접 고른 VIP 주력종목 최대 5개(절대 최대 7개)만 집중 감시
#   - NXT 실시간 체결 중심 + 상위 2종목 동적 호가 집중
#   - 눌림관찰 -> 재진입준비 -> 매수관심 -> 보유관리 -> 쿨다운 -> 재탐색
#   - 같은 종목도 하루 여러 파동을 다시 포착
#   - 자동주문 없음. 실제 주문은 사용자가 직접 수행.
# ============================================================

import os
import io
import re
import json
import time
import html
import queue
import zipfile
import logging
import threading
import urllib.parse
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import websocket
from flask import Flask, jsonify
from zoneinfo import ZoneInfo

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    import yfinance as yf
    HAS_YF = True
except Exception:
    HAS_YF = False


KST = ZoneInfo("Asia/Seoul")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s")
log = logging.getLogger("bongsil.vip.v5")


def now() -> datetime:
    return datetime.now(KST)


def env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)))
    except Exception: return default


def env_float(name: str, default: float) -> float:
    try: return float(os.getenv(name, str(default)))
    except Exception: return default


def num(v, default=0.0) -> float:
    try: return float(str(v).replace(",", "").replace("원", "").replace("주", "").strip())
    except Exception: return default


def integer(v, default=0) -> int:
    try: return int(float(str(v).replace(",", "").strip()))
    except Exception: return default


def pct(new, old) -> float:
    return (new / old - 1.0) * 100 if old else 0.0


def clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


def in_session(start="08:00", end="20:00") -> bool:
    n = now()
    if n.weekday() >= 5:
        return False
    sh, sm = map(int, start.split(":")); eh, em = map(int, end.split(":"))
    s = n.replace(hour=sh, minute=sm, second=0, microsecond=0)
    e = n.replace(hour=eh, minute=em, second=0, microsecond=0)
    return s <= n <= e


def atomic_json_write(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


@dataclass(frozen=True)
class Settings:
    version: str = "5.0.2 VIP Repeat Sniper Stable"
    telegram_token: str = os.getenv("TELEGRAM_TOKEN", "").strip()
    chat_id: str = (os.getenv("CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    kis_app_key: str = os.getenv("KIS_APP_KEY", "").strip()
    kis_app_secret: str = os.getenv("KIS_APP_SECRET", "").strip()
    kis_env: str = os.getenv("KIS_ENV", "real").strip().lower()
    port: int = env_int("PORT", 10000)
    render_start_delay: int = env_int("RENDER_START_DELAY", 30)
    vip_limit: int = env_int("VIP_LIMIT", 5)
    vip_hard_max: int = env_int("VIP_HARD_MAX", 7)
    focus_orderbook_slots: int = env_int("FOCUS_ORDERBOOK_SLOTS", 2)
    enable_orderbook: bool = env_bool("ENABLE_ORDERBOOK", True)
    nxt_start: str = os.getenv("NXT_START", "08:00")
    nxt_end: str = os.getenv("NXT_END", "20:00")
    nxt_trade_tr: str = os.getenv("KIS_NXT_TRADE_TR_ID", "H0NXCNT0")
    nxt_order_tr: str = os.getenv("KIS_NXT_ORDERBOOK_TR_ID", "H0UNASP0")
    default_stop_pct: float = env_float("DEFAULT_STOP_PCT", 2.0)
    level2_gain_pct: float = env_float("LEVEL2_GAIN_PCT", 3.0)
    level3_gain_pct: float = env_float("LEVEL3_GAIN_PCT", 6.0)
    trailing_drawdown_pct: float = env_float("TRAILING_DRAWDOWN_PCT", 3.0)
    cooldown_minutes: int = env_int("COOLDOWN_MINUTES", 20)
    signal_expire_minutes: int = env_int("SIGNAL_EXPIRE_MINUTES", 10)
    dashboard_seconds: int = env_int("DASHBOARD_SECONDS", 60)
    risk_caution_pct: float = env_float("RISK_CAUTION_PCT", -0.7)
    risk_red_pct: float = env_float("RISK_RED_PCT", -1.5)
    data_dir: str = os.getenv("DATA_DIR", "./data").strip()
    morning_brief: str = os.getenv("MORNING_BRIEF", "07:30")
    close_brief: str = os.getenv("CLOSE_BRIEF", "15:35")
    nxt_brief: str = os.getenv("NXT_BRIEF", "20:10")
    weekly_brief: str = os.getenv("WEEKLY_BRIEF", "20:20")

SETTINGS = Settings()
DATA_DIR = Path(SETTINGS.data_dir)

ST_WAIT="WAIT"; ST_PULLBACK="PULLBACK"; ST_READY="READY"; ST_SIGNAL="SIGNAL"; ST_HOLD="HOLD"; ST_COOLDOWN="COOLDOWN"
STATE_LABEL={ST_WAIT:"⚪ 대기",ST_PULLBACK:"🟡 눌림관찰",ST_READY:"🟠 재진입준비",ST_SIGNAL:"🟢 매수관심",ST_HOLD:"💼 보유관리",ST_COOLDOWN:"⏳ 쿨다운"}

@dataclass
class Tick:
    code:str; price:float; volume:int; cumulative_volume:int; trade_strength:float; timestamp:datetime

@dataclass
class Bar:
    minute:datetime; open:float; high:float; low:float; close:float; volume:int; cumulative_volume:int; trade_strength:float

@dataclass
class OrderBook:
    code:str; total_ask:int; total_bid:int; imbalance:float; best_ask:float; best_bid:float; updated_at:datetime

@dataclass
class TargetState:
    code:str; name:str; stage:str=ST_WAIT; support:float=0.0; resistance:float=0.0; vwap:float=0.0; last_price:float=0.0
    trade_strength:float=0.0; volume_ratio:float=0.0; pullback_pct:float=0.0; score:float=0.0
    last_signal_at:Optional[str]=None; cooldown_until:Optional[str]=None; stage_changed_at:Optional[str]=None
    signal_count_today:int=0; pass_count_today:int=0; note:str=""

@dataclass
class Position:
    code:str; name:str; entry:float; qty:float; highest:float; stop:float; level:int=0
    stop_alerted:bool=False; opened_at:str=field(default_factory=lambda: now().isoformat())

class AsyncWorker:
    def __init__(self,name="worker",maxsize=2000): self.name=name; self.q=queue.Queue(maxsize=maxsize); self.started=False
    def start(self):
        if self.started:return
        self.started=True; threading.Thread(target=self._loop,daemon=True,name=self.name).start()
    def submit(self,func,*args,**kwargs):
        try:self.q.put_nowait((func,args,kwargs));return True
        except queue.Full:log.warning("%s queue full",self.name);return False
    def _loop(self):
        while True:
            func,args,kwargs=self.q.get()
            try:func(*args,**kwargs)
            except Exception as e:log.exception("%s task error: %s",self.name,e)
            finally:self.q.task_done()
WORKER=AsyncWorker("vip-worker")

class Store:
    def __init__(self):
        self.vip=DATA_DIR/"vip_targets.json";self.pos=DATA_DIR/"positions.json";self.sig=DATA_DIR/"signal_history.json";self.trade=DATA_DIR/"trade_history.json";self.master=DATA_DIR/"stock_master.json"
    def read(self,p,default):
        try:
            if p.exists():return json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:log.warning("load %s failed: %s",p,e)
        return default
    def write(self,p,v):WORKER.submit(atomic_json_write,p,v)
    def load_vip(self):return self.read(self.vip,{"limit":min(5,SETTINGS.vip_hard_max),"targets":{"119850":"지엔씨에너지"}})
    def save_vip(self,limit,targets):self.write(self.vip,{"limit":limit,"targets":targets})
    def load_positions(self):return self.read(self.pos,{})
    def save_positions(self,v):self.write(self.pos,v)
    def load_signals(self):return self.read(self.sig,[])
    def save_signals(self,v):self.write(self.sig,v[-1000:])
    def load_trades(self):return self.read(self.trade,[])
    def save_trades(self,v):self.write(self.trade,v[-1000:])
    def load_master(self):return self.read(self.master,{})
    def save_master(self,v):self.write(self.master,v)
STORE=Store()

class Telegram:
    def __init__(self):self.s=requests.Session();self.offset=0;self.text_handler=None;self.callback_handler=None
    def _req(self,method,payload=None,timeout=15,files=None):
        if not SETTINGS.telegram_token:return {}
        url=f"https://api.telegram.org/bot{SETTINGS.telegram_token}/{method}"
        try:
            r=self.s.post(url,data=payload,files=files,timeout=timeout) if files else self.s.post(url,json=payload,timeout=timeout)
            if r.status_code>=400:log.warning("Telegram %s %s %s",method,r.status_code,r.text[:300]);return {}
            return r.json()
        except Exception as e:log.warning("Telegram %s failed: %s",method,e);return {}
    def send(self,text,chat=None,buttons=None):
        target=str(chat or SETTINGS.chat_id).strip()
        if not target:return None
        p={"chat_id":target,"text":text,"parse_mode":"HTML","disable_web_page_preview":True}
        if buttons:p["reply_markup"]=buttons
        d=self._req("sendMessage",p);return (d.get("result") or {}).get("message_id")
    def send_async(self,text,chat=None,buttons=None):WORKER.submit(self.send,text,chat,buttons)
    def photo(self,buf,caption,chat=None,buttons=None):
        target=str(chat or SETTINGS.chat_id).strip()
        if not target:return
        data={"chat_id":target,"caption":caption,"parse_mode":"HTML"}
        if buttons:data["reply_markup"]=json.dumps(buttons,ensure_ascii=False)
        self._req("sendPhoto",data,20,{"photo":("chart.png",buf.getvalue(),"image/png")})
    def answer_callback(self,cid,text=""):WORKER.submit(self._req,"answerCallbackQuery",{"callback_query_id":cid,"text":text[:180]},8)
    def poll(self):
        if not SETTINGS.telegram_token:return
        while True:
            try:
                url=f"https://api.telegram.org/bot{SETTINGS.telegram_token}/getUpdates"
                r=self.s.get(url,params={"timeout":25,"offset":self.offset},timeout=35)
                if r.status_code==409:log.warning("Telegram 409 previous session alive");time.sleep(20);continue
                r.raise_for_status()
                for u in r.json().get("result",[]):
                    self.offset=max(self.offset,int(u["update_id"])+1)
                    if u.get("callback_query") and self.callback_handler:
                        threading.Thread(target=self.callback_handler,args=(u["callback_query"],),daemon=True).start()
                    elif u.get("message") and self.text_handler:
                        m=u["message"];txt=str(m.get("text") or "").strip();chat=str((m.get("chat") or {}).get("id") or "")
                        if txt and chat:threading.Thread(target=self.text_handler,args=(txt,chat),daemon=True).start()
            except Exception as e:log.warning("Telegram poll: %s",e);time.sleep(10)
BOT=Telegram()

class MasterIndex:
    SOURCES=(("https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip","kospi_code.mst",228),("https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip","kosdaq_code.mst",222))
    def __init__(self):self.code_to_name={};self.name_to_code={}
    @staticmethod
    def _norm(s):return re.sub(r"\s+","",str(s)).lower()
    def _parse(self,content,tail):
        rows={}
        for line in content.decode("cp949",errors="ignore").splitlines():
            if len(line)<=tail+21:continue
            head=line[:-tail];raw=head[:9].strip();name=head[21:].strip();digits="".join(ch for ch in raw if ch.isdigit());code=digits[-6:] if len(digits)>=6 else ""
            if len(code)==6 and name:rows[code]=name
        return rows
    def load(self):
        rows={}
        for url,fname,tail in self.SOURCES:
            try:
                r=requests.get(url,timeout=30,headers={"User-Agent":"Mozilla/5.0"});r.raise_for_status()
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    target=fname if fname in z.namelist() else next((x for x in z.namelist() if x.endswith(".mst")),None)
                    if not target:raise RuntimeError("mst not found")
                    rows.update(self._parse(z.read(target),tail))
            except Exception as e:log.warning("master source failed: %s",e)
        if not rows:rows=STORE.load_master()
        else:STORE.save_master(rows)
        if not rows:raise RuntimeError("KIS master unavailable")
        self.code_to_name=rows;self.name_to_code={self._norm(n):c for c,n in rows.items()}
    def resolve(self,q):
        s=self._norm(q)
        if s.isdigit():
            c=s.zfill(6);return (c,self.code_to_name.get(c,c))
        if s in self.name_to_code:
            c=self.name_to_code[s];return c,self.code_to_name[c]
        p=[(c,n) for c,n in self.code_to_name.items() if s and s in self._norm(n)]
        return p[0] if len(p)==1 else (None,None)
MASTER=MasterIndex()

class RuntimeState:
    def __init__(self):
        self.lock=threading.RLock();vip=STORE.load_vip();self.vip_limit=min(max(1,integer(vip.get("limit"),SETTINGS.vip_limit)),SETTINGS.vip_hard_max)
        self.vip_targets={str(c).zfill(6):str(n) for c,n in (vip.get("targets") or {}).items()};self.targets={c:TargetState(c,n,stage_changed_at=now().isoformat()) for c,n in self.vip_targets.items()}
        self.bars={};self.current={};self.last_cum={};self.books={};self.last_tick_at={}
        self.market_changes={"KOSPI":0.0,"KOSDAQ":0.0};self.last_market_check=0.0
        self.market_risk="NORMAL";self.focus_codes=[];self.runtime={"boot":"created","boot_stage":"created","boot_completed_at":None,"master":"not_started","telegram":"not_started","scheduler":"not_started","ws_engine":"not_started","ws":"stopped","trade_subscribed":0,"order_subscribed":0,"last_error":None,"last_tick":None};self.pending_input={};self.signals=STORE.load_signals();self.trades=STORE.load_trades()
    def ensure_target(self,c,n):
        if c not in self.targets:self.targets[c]=TargetState(c,n,stage_changed_at=now().isoformat())
    def save_vip(self):STORE.save_vip(self.vip_limit,self.vip_targets)
    def set_stage(self,c,st,note=""):
        t=self.targets.get(c)
        if not t:return False
        changed=t.stage!=st
        if changed:t.stage=st;t.stage_changed_at=now().isoformat()
        if note:t.note=note
        return changed
STATE=RuntimeState()

def cooldown_target(c,reason=""):
    t=STATE.targets.get(c)
    if not t:return
    t.cooldown_until=(now()+timedelta(minutes=SETTINGS.cooldown_minutes)).isoformat();STATE.set_stage(c,ST_COOLDOWN,reason or "쿨다운")

class PositionEngine:
    def __init__(self):self.data={};self.lock=threading.RLock()
    def load(self):
        for c,row in STORE.load_positions().items():
            try:self.data[c]=Position(**row);STATE.set_stage(c,ST_HOLD,"보유복원")
            except Exception as e:log.warning("position restore %s: %s",c,e)
    def save(self):STORE.save_positions({c:asdict(p) for c,p in self.data.items()})
    def register(self,c,n,price,qty=1,support=0):
        stop=price*(1-SETTINGS.default_stop_pct/100)
        if support>0 and support<price:stop=max(stop,support*0.995)
        p=Position(c,n,price,max(qty,0.0001),price,stop);self.data[c]=p;self.save();STATE.set_stage(c,ST_HOLD,"사용자 매수등록");return p
    def close(self,c,exit_price,reason="사용자 매도"):
        p=self.data.pop(c,None)
        if not p:return None
        self.save();r={"code":c,"name":p.name,"entry":p.entry,"exit":exit_price,"qty":p.qty,"return_pct":round(pct(exit_price,p.entry),3),"opened_at":p.opened_at,"closed_at":now().isoformat(),"reason":reason};STATE.trades.append(r);STORE.save_trades(STATE.trades);cooldown_target(c,"매도 후 재진입 대기");return r
    def check(self,c,current):
        p=self.data.get(c);out=[]
        if not p:return out
        p.highest=max(p.highest,current);gain=pct(current,p.entry)
        if gain>=SETTINGS.level3_gain_pct and p.level<2:p.level=2;p.stop=max(p.stop,p.highest*(1-SETTINGS.trailing_drawdown_pct/100));out.append(f"🔥 <b>[집중관리 Lv.3] {html.escape(p.name)}</b>\n수익률 {gain:+.2f}% · 고점추적 방어 {p.stop:,.0f}원")
        elif gain>=SETTINGS.level2_gain_pct and p.level<1:p.level=1;p.stop=max(p.stop,p.entry*1.01);out.append(f"🛡️ <b>[집중관리 Lv.2] {html.escape(p.name)}</b>\n수익률 {gain:+.2f}% · 방어선 {p.stop:,.0f}원")
        if p.level==2:p.stop=max(p.stop,p.highest*(1-SETTINGS.trailing_drawdown_pct/100))
        if current<=p.stop and not p.stop_alerted:p.stop_alerted=True;out.append(f"🛑 <b>[매도/감시종료 권고] {html.escape(p.name)}</b>\n현재 {current:,.0f}원 ({gain:+.2f}%) · 방어선 {p.stop:,.0f}원 이탈\n자동주문 없음")
        self.save();return out
POSITIONS=PositionEngine()

class KIS:
    def __init__(self):self.rest="https://openapi.koreainvestment.com:9443";self.ws_url="ws://ops.koreainvestment.com:21000";self.s=requests.Session();self.token=None;self.token_exp=None;self.approval=None;self.approval_exp=None;self.lock=threading.RLock();self.stream_running=False;self.stream_lock=threading.Lock();self.ws=None
    def auth(self):
        with self.lock:
            if self.token and self.token_exp and datetime.now(timezone.utc)<self.token_exp:return self.token
            r=self.s.post(f"{self.rest}/oauth2/tokenP",json={"grant_type":"client_credentials","appkey":SETTINGS.kis_app_key,"appsecret":SETTINGS.kis_app_secret},timeout=20);r.raise_for_status();d=r.json();self.token=d["access_token"];self.token_exp=datetime.now(timezone.utc)+timedelta(seconds=max(60,integer(d.get("expires_in"),86400)-300));return self.token
    def approval_key(self):
        with self.lock:
            if self.approval and self.approval_exp and datetime.now(timezone.utc)<self.approval_exp:return self.approval
            r=self.s.post(f"{self.rest}/oauth2/Approval",json={"grant_type":"client_credentials","appkey":SETTINGS.kis_app_key,"secretkey":SETTINGS.kis_app_secret},timeout=20);r.raise_for_status();self.approval=r.json()["approval_key"];self.approval_exp=datetime.now(timezone.utc)+timedelta(hours=12);return self.approval
    def minute_bars(self,c):
        try:
            r=self.s.get(f"{self.rest}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",headers={"authorization":f"Bearer {self.auth()}","appkey":SETTINGS.kis_app_key,"appsecret":SETTINGS.kis_app_secret,"tr_id":"FHKST03010200","custtype":"P"},params={"FID_ETC_CLS_CODE":"","FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":c,"FID_INPUT_HOUR_1":"153000","FID_PW_DATA_INCU_YN":"Y"},timeout=15);r.raise_for_status();return r.json().get("output2") or []
        except Exception as e:log.warning("minute bars %s: %s",c,e);return []
    def quote(self,c):
        """KIS 공식 REST로 현재가/시가 조회. 시장 위험모드 계산용."""
        try:
            r=self.s.get(
                f"{self.rest}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers={"authorization":f"Bearer {self.auth()}","appkey":SETTINGS.kis_app_key,"appsecret":SETTINGS.kis_app_secret,"tr_id":"FHKST01010100","custtype":"P"},
                params={"FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":c},
                timeout=10
            )
            r.raise_for_status()
            o=r.json().get("output") or {}
            return {"open":num(o.get("stck_oprc")),"price":num(o.get("stck_prpr"))}
        except Exception as e:
            log.warning("quote %s: %s",c,e)
            return None
    @staticmethod
    def parse_trade(msg):
        if isinstance(msg,(bytes,bytearray,memoryview)):msg=bytes(msg).decode("utf-8",errors="ignore")
        if not isinstance(msg,str) or not msg or msg.startswith("{"):return []
        p=msg.split("|",3)
        if len(p)<4 or p[0]!="0" or p[1]!=SETTINGS.nxt_trade_tr:return []
        count=max(1,integer(p[2],1));f=p[3].split("^");w=len(f)//count if count else len(f);out=[]
        if w<19:return []
        for i in range(count):
            r=f[i*w:(i+1)*w]
            try:
                c=str(r[0]).zfill(6);h=str(r[1]).zfill(6);ts=now().replace(hour=int(h[:2]),minute=int(h[2:4]),second=int(h[4:6]),microsecond=0);out.append(Tick(c,num(r[2]),integer(r[12]),integer(r[13]),num(r[18]),ts))
            except Exception:pass
        return out
    @staticmethod
    def parse_book(msg):
        if isinstance(msg,(bytes,bytearray,memoryview)):msg=bytes(msg).decode("utf-8",errors="ignore")
        p=str(msg).split("|",3)
        if len(p)<4 or p[1]!=SETTINGS.nxt_order_tr:return None
        f=p[3].split("^")
        if len(f)<45:return None
        try:
            c=str(f[0]).zfill(6);asks=[num(x) for x in f[3:13]];bids=[num(x) for x in f[13:23]];aq=[integer(x) for x in f[23:33]];bq=[integer(x) for x in f[33:43]];ta=integer(f[43]) or sum(aq);tb=integer(f[44]) or sum(bq);return OrderBook(c,ta,tb,tb/ta if ta else 0,asks[0] if asks else 0,bids[0] if bids else 0,now())
        except Exception:return None
    def trade_codes(self):return list(STATE.vip_targets)
    def order_codes(self):return list(STATE.focus_codes[:SETTINGS.focus_orderbook_slots]) if SETTINGS.enable_orderbook else []
    def stream(self,on_tick,on_book):
        with self.stream_lock:
            if self.stream_running:
                log.warning("NXT duplicate stream start blocked")
                return
            self.stream_running=True
        STATE.runtime["ws_engine"]="running"
        log.info("NXT stream engine started")
        retry=5;order_slots=SETTINGS.focus_orderbook_slots;last_session_state=None
        try:
            while True:
                if SETTINGS.kis_env!="real":
                    STATE.runtime["ws"]="disabled_virtual"
                    if last_session_state!="virtual":
                        log.warning("KIS_ENV is not real - NXT realtime disabled")
                        last_session_state="virtual"
                    time.sleep(60);continue
                if not in_session(SETTINGS.nxt_start,SETTINGS.nxt_end):
                    STATE.runtime["ws"]="waiting_market_session";STATE.runtime["trade_subscribed"]=0;STATE.runtime["order_subscribed"]=0
                    if last_session_state!="waiting":
                        log.info("NXT outside session (%s-%s) - waiting",SETTINGS.nxt_start,SETTINGS.nxt_end)
                        last_session_state="waiting"
                    time.sleep(30);continue
                if last_session_state!="active":
                    log.info("NXT session active - websocket connection starting")
                    last_session_state="active"
                stop=threading.Event();requested_trade=set();requested_order=set();send_lock=threading.Lock();duplicate=False;over=False;refresh_thread=None
                try:
                    key=self.approval_key();STATE.runtime["ws"]="connecting"
                    def send_sub(ws,tr,c,sub=True):
                        with send_lock:ws.send(json.dumps({"header":{"approval_key":key,"custtype":"P","tr_type":"1" if sub else "2","content-type":"utf-8"},"body":{"input":{"tr_id":tr,"tr_key":c}}},ensure_ascii=False));time.sleep(0.25)
                    def sync(ws):
                        wt=set(self.trade_codes());wo=set(self.order_codes()[:order_slots])
                        for c in list(requested_order-wo):send_sub(ws,SETTINGS.nxt_order_tr,c,False);requested_order.discard(c)
                        for c in list(requested_trade-wt):send_sub(ws,SETTINGS.nxt_trade_tr,c,False);requested_trade.discard(c)
                        for c in wt-requested_trade:send_sub(ws,SETTINGS.nxt_trade_tr,c,True);requested_trade.add(c)
                        for c in wo-requested_order:send_sub(ws,SETTINGS.nxt_order_tr,c,True);requested_order.add(c)
                        STATE.runtime["trade_subscribed"]=len(requested_trade);STATE.runtime["order_subscribed"]=len(requested_order);STATE.runtime["ws"]="connected"
                    def refresh(ws):
                        while not stop.wait(30):
                            try:
                                if not in_session(SETTINGS.nxt_start,SETTINGS.nxt_end):stop.set();ws.close();return
                                sync(ws)
                            except Exception as e:log.warning("refresh: %s",e);stop.set();ws.close();return
                    def opened(ws):
                        nonlocal refresh_thread
                        log.info("NXT Websocket connected");sync(ws);log.info("NXT subscriptions ready: trade %s / orderbook %s",STATE.runtime["trade_subscribed"],STATE.runtime["order_subscribed"]);refresh_thread=threading.Thread(target=refresh,args=(ws,),daemon=True,name="NXT-subscription-refresh");refresh_thread.start()
                    def message(ws,msg):
                        nonlocal duplicate,over
                        if isinstance(msg,(bytes,bytearray,memoryview)):msg=bytes(msg).decode("utf-8",errors="ignore")
                        if isinstance(msg,str) and msg.startswith("{"):
                            try:
                                d=json.loads(msg);h=d.get("header") or {};b=d.get("body") or {}
                                if h.get("tr_id")=="PINGPONG":
                                    with send_lock:ws.send(msg)
                                    return
                                m=str(b.get("msg1") or "").upper()
                                if "ALREADY IN USE APPKEY" in m:duplicate=True;stop.set();ws.close()
                                elif "MAX SUBSCRIBE OVER" in m:over=True;stop.set();ws.close()
                            except Exception:pass
                            return
                        parts=str(msg).split("|",3);tr=parts[1] if len(parts)>1 else ""
                        if tr==SETTINGS.nxt_trade_tr:
                            for t in self.parse_trade(msg):on_tick(t)
                        elif tr==SETTINGS.nxt_order_tr:
                            b=self.parse_book(msg)
                            if b:on_book(b)
                    def err(ws,e):STATE.runtime["last_error"]=f"NXT WS: {e}";log.warning("NXT WS %s",e)
                    self.ws=websocket.WebSocketApp(self.ws_url,on_open=opened,on_message=message,on_error=err,on_close=lambda w,s,m:stop.set());self.ws.run_forever(ping_interval=25,ping_timeout=10,skip_utf8_validation=True);stop.set()
                    if refresh_thread and refresh_thread.is_alive():refresh_thread.join(timeout=3)
                    if duplicate:self.approval=None;self.approval_exp=None;time.sleep(180);retry=5
                    elif over:
                        if order_slots>0:order_slots-=1;log.warning("호가 슬롯 %s로 하향",order_slots)
                        time.sleep(120);retry=5
                    else:time.sleep(retry);retry=min(120,retry*2)
                except Exception as e:STATE.runtime["last_error"]=str(e);log.exception("stream failed");time.sleep(retry);retry=min(120,retry*2)
        finally:
            with self.stream_lock:self.stream_running=False
KIS_CLIENT=KIS()

class Brain:
    @staticmethod
    def metrics(c):
        bars=list(STATE.bars.get(c,[]));cur=STATE.current.get(c)
        if cur:bars.append(cur)
        if len(bars)<15:return None
        r=bars[-40:];latest=r[-1];vols=[max(0,b.volume) for b in r];tot=sum(vols)
        if tot<=0:return None
        vwap=sum(((b.high+b.low+b.close)/3)*max(0,b.volume) for b in r)/tot;prior=vols[-15:-3];recent=vols[-3:];pa=sum(prior)/len(prior) if prior else 0;ra=sum(recent)/len(recent) if recent else 0;vr=ra/pa if pa else 0
        highs=[b.high for b in r];lows=[b.low for b in r];high20=max(highs[-20:-1]);low12=min(lows[-12:]);support=max(vwap*0.995,low12);resistance=high20;pull=pct(latest.close,high20);prev3=max(highs[-4:-1]);br=latest.close>=prev3;ob=STATE.books.get(c);imb=ob.imbalance if ob and (now()-ob.updated_at).total_seconds()<90 else 1.0;score=clamp(50+clamp((latest.trade_strength-95)*0.8,-15,20)+(10 if -0.8<=pct(latest.close,vwap)<=1.2 else -5)+(12 if -4.5<=pull<=-0.6 else -5)+(8 if vr<=0.85 else 0)+(12 if br else 0)+clamp((imb-1)*12,-8,10))
        return {"price":latest.close,"vwap":vwap,"support":support,"resistance":resistance,"pullback_pct":pull,"volume_ratio":vr,"strength":latest.trade_strength,"short_break":br,"imbalance":imb,"score":score,"today_open":r[0].open}
    def evaluate(self,c):
        t=STATE.targets.get(c);m=self.metrics(c)
        if not t or not m:return None,m,"분봉 수집중"
        for k,a in (("last_price","price"),("vwap","vwap"),("support","support"),("resistance","resistance"),("trade_strength","strength"),("volume_ratio","volume_ratio"),("pullback_pct","pullback_pct"),("score","score")):setattr(t,k,m[a])
        if c in POSITIONS.data:return ST_HOLD,m,"보유중"
        if t.stage==ST_COOLDOWN:
            try:
                if t.cooldown_until and now()<datetime.fromisoformat(t.cooldown_until):return ST_COOLDOWN,m,"쿨다운"
            except Exception:pass
            t.cooldown_until=None;return ST_WAIT,m,"쿨다운 종료"
        if t.stage==ST_SIGNAL and t.last_signal_at:
            try:
                if now()-datetime.fromisoformat(t.last_signal_at)>timedelta(minutes=SETTINGS.signal_expire_minutes):t.cooldown_until=(now()+timedelta(minutes=SETTINGS.cooldown_minutes)).isoformat();return ST_COOLDOWN,m,"타점 만료"
            except Exception:pass
        near=abs(pct(m["price"],m["support"]))<=1.1 or abs(pct(m["price"],m["vwap"]))<=1.2;healthy=-4.5<=m["pullback_pct"]<=-0.6;dry=m["volume_ratio"]<=0.85;pullback=near and healthy and dry and m["price"]>=m["today_open"]*0.985;reentry=m["price"]>=m["vwap"]*0.998 and m["strength"]>=108 and m["short_break"] and m["volume_ratio"]>=0.95
        if STATE.market_risk=="RISK":return (ST_PULLBACK if pullback else ST_WAIT),m,"시장 RISK - 신규진입 보류"
        if t.stage in (ST_PULLBACK,ST_READY) and reentry:
            if STATE.market_risk=="CAUTION" and m["score"]<78:return ST_READY,m,"시장 CAUTION - 강한 확인 대기"
            return ST_SIGNAL,m,"체결강도 회복 + 단기고점 돌파 + 거래량 재유입"
        if pullback:return (ST_READY if m["strength"]>=100 else ST_PULLBACK),m,("지지구간 + 매수세 회복" if m["strength"]>=100 else "지지구간 접근 + 거래량 감소")
        return ST_WAIT,m,"조건 대기"
BRAIN=Brain()

def google_news_headlines(q,limit=4):
    """Google News RSS 제목과 RSS 발행시각(KST)을 함께 반환."""
    try:
        url=f"https://news.google.com/rss/search?q={urllib.parse.quote(q)}&hl=ko&gl=KR&ceid=KR:ko"
        r=requests.get(url,timeout=10,headers={"User-Agent":"Mozilla/5.0"})
        r.raise_for_status()
        root=ET.fromstring(r.text)
        rows=[]
        for i in root.findall(".//item"):
            title=(i.findtext("title") or "").strip()
            if not title:
                continue
            published=None
            raw=(i.findtext("pubDate") or "").strip()
            if raw:
                try:
                    dt=parsedate_to_datetime(raw)
                    if dt.tzinfo is None:
                        dt=dt.replace(tzinfo=timezone.utc)
                    published=dt.astimezone(KST)
                except Exception:
                    published=None
            rows.append({"title":title,"published":published})
        rows.sort(key=lambda x:x["published"] or datetime.min.replace(tzinfo=KST),reverse=True)
        return rows[:limit]
    except Exception as e:
        log.warning("google news failed: %s",e)
        return []

def news_time_text(dt):
    if not dt:
        return "발행시간 확인불가"
    sec=max(0,int((now()-dt).total_seconds()))
    if sec<3600:
        ago=f"{max(1,sec//60)}분 전"
    elif sec<86400:
        ago=f"{sec//3600}시간 전"
    else:
        ago=f"{sec//86400}일 전"
    return f"{dt.strftime('%Y-%m-%d %H:%M')} · {ago}"

def us_market_summary():
    if not HAS_YF:return "미국증시: yfinance 미설치"
    try:
        rows=[]
        for label,s in (("S&P500","^GSPC"),("NASDAQ","^IXIC"),("DOW","^DJI")):
            d=yf.Ticker(s).history(period="5d")
            if len(d)>=2:rows.append(f"{label} {pct(float(d['Close'].iloc[-1]),float(d['Close'].iloc[-2])):+.2f}%")
        return " · ".join(rows) if rows else "미국증시 데이터 없음"
    except Exception as e:return f"미국증시 조회 실패: {e}"

class App:
    def __init__(self):self.started=False;self.start_lock=threading.Lock();self.today="";self.stage_alert={};self.brief_sent={}
    def sync_bars_for(self,c):
        raw=KIS_CLIENT.minute_bars(c);q=deque(maxlen=390);n=now()
        for r in reversed(raw):
            try:
                h=str(r.get("stck_cntg_hour") or "").zfill(6);m=n.replace(hour=int(h[:2]),minute=int(h[2:4]),second=0,microsecond=0);q.append(Bar(m,num(r.get("stck_oprc")),num(r.get("stck_hgpr")),num(r.get("stck_lwpr")),num(r.get("stck_prpr")),integer(r.get("cntg_vol")),integer(r.get("acml_vol")),100))
            except Exception:pass
        if q:
            with STATE.lock:STATE.bars[c]=q
            log.info("VIP minute bars ready: %s %s bars",STATE.vip_targets.get(c,c),len(q))
        else:
            log.warning("VIP minute bars empty: %s",STATE.vip_targets.get(c,c))
    def on_book(self,b):STATE.books[b.code]=b
    def on_tick(self,t):
        if t.price<=0 or abs((now()-t.timestamp).total_seconds())>180:return
        with STATE.lock:
            prev=STATE.last_cum.get(t.code);inc=t.cumulative_volume-prev if prev is not None and t.cumulative_volume>=prev else max(0,t.volume);STATE.last_cum[t.code]=t.cumulative_volume;minute=t.timestamp.replace(second=0,microsecond=0);cur=STATE.current.get(t.code)
            if not cur:STATE.current[t.code]=Bar(minute,t.price,t.price,t.price,t.price,inc,t.cumulative_volume,t.trade_strength)
            elif cur.minute==minute:cur.high=max(cur.high,t.price);cur.low=min(cur.low,t.price);cur.close=t.price;cur.volume+=inc;cur.cumulative_volume=max(cur.cumulative_volume,t.cumulative_volume);cur.trade_strength=t.trade_strength
            else:STATE.bars.setdefault(t.code,deque(maxlen=390)).append(cur);STATE.current[t.code]=Bar(minute,t.price,t.price,t.price,t.price,inc,t.cumulative_volume,t.trade_strength)
            STATE.runtime["last_tick"]=t.timestamp.isoformat()
        for m in POSITIONS.check(t.code,t.price):BOT.send_async(m,buttons=self.position_buttons(t.code))
        if t.code in STATE.vip_targets:self.evaluate_target(t.code)
    def update_market_risk(self):
        """KIS 공식 REST로 KOSPI200/KOSDAQ150 ETF의 시가 대비 변화를 확인한다."""
        try:
            rows={}
            for label,code in (("KOSPI","069500"),("KOSDAQ","229200")):
                q=KIS_CLIENT.quote(code)
                if q and q["open"]>0 and q["price"]>0:
                    rows[label]=pct(q["price"],q["open"])
            if not rows:
                return
            STATE.market_changes.update(rows)
            w=min(rows.values());old=STATE.market_risk
            new="RISK" if w<=SETTINGS.risk_red_pct else "CAUTION" if w<=SETTINGS.risk_caution_pct else "NORMAL" if w>-0.5 else old
            if new!=old:
                STATE.market_risk=new
                BOT.send_async(
                    f"{'🟢' if new=='NORMAL' else '🟡' if new=='CAUTION' else '🔴'} <b>시장 위험모드 {new}</b>\n"
                    f"KOSPI200/KOSDAQ150 ETF 시가 대비 최저 {w:+.2f}%"
                )
        except Exception as e:
            log.warning("market risk update failed: %s",e)
    def update_focus(self):
        pri={ST_HOLD:100,ST_SIGNAL:90,ST_READY:75,ST_PULLBACK:60,ST_WAIT:20,ST_COOLDOWN:5};STATE.focus_codes=[c for _,c in sorted(((pri.get(t.stage,0)+t.score/10,c) for c,t in STATE.targets.items()),reverse=True)[:SETTINGS.focus_orderbook_slots]]
    def evaluate_target(self,c):
        t=STATE.targets[c];st,m,reason=BRAIN.evaluate(c)
        if not st:return
        changed=STATE.set_stage(c,st,reason);self.update_focus()
        if st==ST_SIGNAL and (changed or self.allowed(c,"signal",10)):
            t.last_signal_at=now().isoformat();t.signal_count_today+=1;STATE.signals.append({"code":c,"name":t.name,"time":t.last_signal_at,"price":t.last_price,"support":t.support,"resistance":t.resistance,"vwap":t.vwap,"strength":t.trade_strength,"score":t.score});STORE.save_signals(STATE.signals);BOT.send_async(self.signal_text(t),buttons=self.signal_buttons(c));self.stage_alert[(c,"signal")]=now()
        elif changed and st==ST_PULLBACK and self.allowed(c,"pull",30):BOT.send_async(f"🟡 <b>{html.escape(t.name)} 눌림관찰</b>\n현재 {t.last_price:,.0f} · VWAP {t.vwap:,.0f} · 지지 {t.support:,.0f}\n거래량비 {t.volume_ratio:.2f} · 체결강도 {t.trade_strength:.0f}\n아직 매수신호 아님");self.stage_alert[(c,"pull")]=now()
    def allowed(self,c,k,m):
        x=self.stage_alert.get((c,k));return not x or now()-x>=timedelta(minutes=m)
    def signal_text(self,t):
        ob=STATE.books.get(t.code);obs=f"\n호가잔량비 {ob.imbalance:.2f}배" if ob else ""
        return f"🟢 <b>[VIP 재진입 타점] {html.escape(t.name)}</b>\n현재 <b>{t.last_price:,.0f}원</b>\nVWAP {t.vwap:,.0f} · 지지 {t.support:,.0f} · 저항 {t.resistance:,.0f}\n체결강도 {t.trade_strength:.0f} · 거래량비 {t.volume_ratio:.2f} · 점수 {t.score:.0f}{obs}\n\n✅ 눌림 후 매수세 회복\n✅ 단기고점 돌파\n⚠️ 자동주문 없음"
    def main_buttons(self):return {"inline_keyboard":[[{"text":"🎯 VIP 현황","callback_data":"vip"},{"text":"🟢 타점대기","callback_data":"ready"}],[{"text":"💼 보유관리","callback_data":"hold"},{"text":"📊 시장상태","callback_data":"market"}],[{"text":"➕ VIP 추가","callback_data":"add"},{"text":"➖ VIP 삭제","callback_data":"remove"}],[{"text":"📰 VIP 뉴스","callback_data":"newsall"},{"text":"🩺 시스템진단","callback_data":"diag"}],[{"text":"🌅 아침브리핑","callback_data":"bm"},{"text":"🌙 마감브리핑","callback_data":"bn"}]]}
    def signal_buttons(self,c):return {"inline_keyboard":[[{"text":"✅ 매수했음","callback_data":f"buy:{c}"},{"text":"⏳ 더 관찰","callback_data":f"observe:{c}"}],[{"text":"❌ 이번 파동 패스","callback_data":f"pass:{c}"},{"text":"📈 상세차트","callback_data":f"chart:{c}"}],[{"text":"🏠 메인","callback_data":"main"}]]}
    def position_buttons(self,c):return {"inline_keyboard":[[{"text":"🔄 현재상태","callback_data":f"detail:{c}"},{"text":"✅ 현재가로 매도종료","callback_data":f"sell:{c}"}],[{"text":"📈 차트","callback_data":f"chart:{c}"}]]}
    def target_buttons(self,c):return {"inline_keyboard":[[{"text":"🔄 새로고침","callback_data":f"detail:{c}"},{"text":"📈 차트","callback_data":f"chart:{c}"}],[{"text":"💼 현재가 매수등록","callback_data":f"buy:{c}"},{"text":"❌ 파동 패스","callback_data":f"pass:{c}"}],[{"text":"📰 종목뉴스","callback_data":f"news:{c}"}],[{"text":"🏠 메인","callback_data":"main"}]]}
    def dashboard(self):return "\n".join(["🤖 <b>VIP SNIPER CONTROL</b>",f"시장: {STATE.market_risk}",f"VIP: {len(STATE.vip_targets)}/{STATE.vip_limit}",f"보유: {len(POSITIONS.data)}",""]+[f"• <b>{html.escape(n)}</b> · {STATE_LABEL.get(STATE.targets[c].stage,'')} · {STATE.targets[c].last_price:,.0f}원 · {STATE.targets[c].score:.0f}점" for c,n in STATE.vip_targets.items()])
    def detail(self,c):
        t=STATE.targets[c];p=POSITIONS.data.get(c);s=f"🎯 <b>{html.escape(t.name)}</b> ({c})\n상태 {STATE_LABEL.get(t.stage,t.stage)}\n현재 {t.last_price:,.0f} · VWAP {t.vwap:,.0f}\n지지 {t.support:,.0f} · 저항 {t.resistance:,.0f}\n체결강도 {t.trade_strength:.0f} · 거래량비 {t.volume_ratio:.2f}\n점수 {t.score:.0f}\n판단 {html.escape(t.note or '-')}"
        if p:s+=f"\n\n💼 매수가 {p.entry:,.0f} · 수익 {pct(t.last_price or p.entry,p.entry):+.2f}% · 방어 {p.stop:,.0f}"
        return s
    def chart(self,c):
        bars=list(STATE.bars.get(c,[]));cur=STATE.current.get(c)
        if cur:bars.append(cur)
        bars=bars[-60:]
        if not bars:return None
        try:
            x=[b.minute.strftime("%H:%M") for b in bars];y=[b.close for b in bars];fig=plt.figure(figsize=(7,3.5));plt.plot(x,y,label="Price");t=STATE.targets[c];
            if t.vwap:plt.axhline(t.vwap,linestyle="--",label="VWAP")
            if t.support:plt.axhline(t.support,linestyle=":",label="Support")
            plt.xticks(x[::max(1,len(x)//6)],rotation=30);plt.grid(alpha=.25);plt.legend();plt.tight_layout();buf=io.BytesIO();plt.savefig(buf,format="png",dpi=110);plt.close(fig);buf.seek(0);return buf
        except Exception:plt.close("all");return None
    def add_vip(self,q,chat):
        if len(STATE.vip_targets)>=STATE.vip_limit:BOT.send(f"⚠️ VIP 한도 {STATE.vip_limit}개",chat);return
        c,n=MASTER.resolve(q)
        if not c:BOT.send("정확한 종목명 또는 6자리 코드를 입력해주세요.",chat);return
        STATE.vip_targets[c]=n;STATE.ensure_target(c,n);STATE.save_vip();WORKER.submit(self.sync_bars_for,c);BOT.send(f"🎯 {html.escape(n)} VIP 추가 완료",chat,buttons=self.target_buttons(c))
    def remove_vip(self,c,chat):
        if c in POSITIONS.data:BOT.send("보유관리 중이라 삭제 불가",chat);return
        n=STATE.vip_targets.pop(c,None);STATE.targets.pop(c,None);STATE.save_vip();BOT.send(f"🗑️ {html.escape(n or c)} 삭제 완료",chat,buttons=self.main_buttons())
    def news_text(self,c=None):
        codes=[c] if c else list(STATE.vip_targets)
        lines=["📰 <b>VIP 뉴스 헤드라인</b>","기사 발행시각 기준 · 최신순"]
        for x in codes:
            n=STATE.vip_targets.get(x,x)
            hs=google_news_headlines(f"{n} 주식 수주 공시",4)
            lines.append(f"\n<b>{html.escape(n)}</b>")
            if hs:
                for row in hs:
                    lines.append(f"• <b>{html.escape(news_time_text(row.get('published')))}</b>")
                    lines.append(f"  {html.escape(row.get('title') or '')}")
            else:
                lines.append("헤드라인 없음")
        lines.append(f"\n⏱ 뉴스 확인 {now().strftime('%Y-%m-%d %H:%M')}")
        return "\n".join(lines)
    def morning(self):return "🌅 <b>VIP 장전 브리핑</b>\n"+us_market_summary()+"\n\n"+"\n".join(f"• <b>{html.escape(n)}</b> · 지지 {STATE.targets[c].support:,.0f} · 저항 {STATE.targets[c].resistance:,.0f}" for c,n in STATE.vip_targets.items())+"\n\n전략: 급등 추격보다 눌림→반등 확인 우선"
    def close_brief(self,title="📊 정규장 VIP 브리핑"):
        return f"<b>{title}</b>\n시장 {STATE.market_risk}\n\n"+"\n".join(f"• <b>{html.escape(n)}</b> {STATE_LABEL.get(STATE.targets[c].stage,'')} · 현재 {STATE.targets[c].last_price:,.0f} · 신호 {STATE.targets[c].signal_count_today}회" for c,n in STATE.vip_targets.items())
    def nxt_brief(self):
        ranked=sorted(STATE.targets.values(),key=lambda t:t.score,reverse=True);return "🌙 <b>VIP NXT 마감 브리핑</b>\n\n"+"\n".join(f"{i}. <b>{html.escape(t.name)}</b> · {STATE_LABEL.get(t.stage,'')} · {t.score:.0f}점\n   현재 {t.last_price:,.0f} · 지지 {t.support:,.0f} · 저항 {t.resistance:,.0f}" for i,t in enumerate(ranked,1))
    def weekly(self):
        cut=now()-timedelta(days=7);rows=[]
        for r in STATE.trades:
            try:
                if datetime.fromisoformat(r["closed_at"])>=cut:rows.append(r)
            except Exception:pass
        if not rows:return "📅 <b>VIP 주간 성적</b>\n종료 매매 없음"
        by={}
        for r in rows:by.setdefault(r["name"],[]).append(r)
        return "📅 <b>VIP 주간 성적</b>\n"+"\n".join(f"• {html.escape(n)} {len(rs)}회 · 평균 {sum(num(x['return_pct']) for x in rs)/len(rs):+.2f}%" for n,rs in by.items())
    def diag(self):
        return (
            f"🩺 <b>VIP V5 진단</b>\n"
            f"부팅 {STATE.runtime.get('boot','-')} · 단계 {STATE.runtime.get('boot_stage','-')}\n"
            f"마스터 {STATE.runtime.get('master','-')} · Telegram {STATE.runtime.get('telegram','-')}\n"
            f"스케줄러 {STATE.runtime.get('scheduler','-')} · NXT엔진 {STATE.runtime.get('ws_engine','-')}\n"
            f"NXT {STATE.runtime['ws']}\n"
            f"체결 {STATE.runtime['trade_subscribed']} · 호가 {STATE.runtime['order_subscribed']}\n"
            f"VIP {len(STATE.vip_targets)}/{STATE.vip_limit} · 보유 {len(POSITIONS.data)}\n"
            f"마지막체결 {STATE.runtime['last_tick'] or '-'}\n"
            f"부팅완료 {STATE.runtime.get('boot_completed_at') or '-'}\n"
            f"오류 {STATE.runtime['last_error'] or '-'}"
        )
    def handle_cb(self,cb):
        cid=cb.get("id");d=str(cb.get("data") or "");chat=str(((cb.get("message") or {}).get("chat") or {}).get("id") or SETTINGS.chat_id);BOT.answer_callback(cid,"처리중")
        if d in ("main","vip"):BOT.send(self.dashboard(),chat,buttons=self.main_buttons())
        elif d=="ready":BOT.send("🟢 <b>타점대기</b>\n"+"\n".join(f"• {t.name} {STATE_LABEL[t.stage]}" for t in STATE.targets.values() if t.stage in (ST_PULLBACK,ST_READY,ST_SIGNAL)),chat,buttons=self.main_buttons())
        elif d=="hold":BOT.send("💼 <b>보유관리</b>\n"+"\n".join(f"• {p.name} {pct(STATE.targets[c].last_price or p.entry,p.entry):+.2f}%" for c,p in POSITIONS.data.items()) if POSITIONS.data else "💼 보유 없음",chat,buttons=self.main_buttons())
        elif d=="market":BOT.send(f"📊 시장 {STATE.market_risk}\nKOSPI {STATE.market_changes.get('KOSPI',0):+.2f}% · KOSDAQ {STATE.market_changes.get('KOSDAQ',0):+.2f}%",chat,buttons=self.main_buttons())
        elif d=="add":STATE.pending_input[chat]={"action":"add"};BOT.send("추가할 종목명/코드 입력",chat)
        elif d=="remove":BOT.send("삭제할 종목 선택",chat,buttons={"inline_keyboard":[[{"text":"❌ "+n,"callback_data":f"rm:{c}"}] for c,n in STATE.vip_targets.items()]})
        elif d=="newsall":WORKER.submit(lambda:BOT.send(self.news_text(),chat,buttons=self.main_buttons()))
        elif d=="diag":BOT.send(self.diag(),chat,buttons=self.main_buttons())
        elif d=="bm":BOT.send(self.morning(),chat,buttons=self.main_buttons())
        elif d=="bn":BOT.send(self.nxt_brief(),chat,buttons=self.main_buttons())
        elif d.startswith("detail:"):c=d.split(":",1)[1];BOT.send(self.detail(c),chat,buttons=self.target_buttons(c))
        elif d.startswith("chart:"):c=d.split(":",1)[1];b=self.chart(c);BOT.photo(b,f"📈 {STATE.vip_targets.get(c,c)} 최근 60분",chat,self.target_buttons(c)) if b else BOT.send("차트 데이터 부족",chat)
        elif d.startswith("news:"):c=d.split(":",1)[1];WORKER.submit(lambda:BOT.send(self.news_text(c),chat,buttons=self.target_buttons(c)))
        elif d.startswith("buy:"):
            c=d.split(":",1)[1];t=STATE.targets[c]
            if t.last_price>0:p=POSITIONS.register(c,t.name,t.last_price,1,t.support);BOT.send(f"💼 {html.escape(t.name)} 관리시작 · {p.entry:,.0f}원 · 방어 {p.stop:,.0f}원\n실제 주문 아님",chat,buttons=self.position_buttons(c))
        elif d.startswith("sell:"):
            c=d.split(":",1)[1];t=STATE.targets[c];r=POSITIONS.close(c,t.last_price,"현재가 매도종료") if t.last_price>0 else None
            if r:BOT.send(f"✅ {html.escape(r['name'])} 종료 {r['return_pct']:+.2f}% · {SETTINGS.cooldown_minutes}분 후 재탐색",chat,buttons=self.main_buttons())
        elif d.startswith("pass:"):c=d.split(":",1)[1];cooldown_target(c,"사용자 패스");BOT.send(f"⏳ 이번 파동 패스 · {SETTINGS.cooldown_minutes}분 후 재탐색",chat,buttons=self.main_buttons())
        elif d.startswith("observe:"):c=d.split(":",1)[1];STATE.set_stage(c,ST_READY,"사용자 추가관찰");BOT.send("🟠 추가 관찰",chat)
        elif d.startswith("rm:"):self.remove_vip(d.split(":",1)[1],chat)
    def handle_text(self,txt,chat):
        p=STATE.pending_input.pop(str(chat),None)
        if p and p.get("action")=="add":self.add_vip(txt,chat);return
        if txt in ("/start","/메뉴","메뉴"):BOT.send(self.dashboard(),chat,buttons=self.main_buttons());return
        if txt in ("/상태","/진단"):BOT.send(self.diag(),chat,buttons=self.main_buttons());return
        if txt.startswith("/추가 "):self.add_vip(txt[4:].strip(),chat);return
        if txt.startswith("/삭제 "):
            c,n=MASTER.resolve(txt[4:].strip());self.remove_vip(c,chat) if c in STATE.vip_targets else BOT.send("VIP에서 못 찾음",chat);return
        if txt.startswith("/매수 "):
            parts=txt.split();price=num(parts[-2] if len(parts)>=4 else parts[-1]);qty=num(parts[-1]) if len(parts)>=4 else 1;stock=" ".join(parts[1:-2] if len(parts)>=4 else parts[1:-1]);c,n=MASTER.resolve(stock)
            if c in STATE.vip_targets and price>0:t=STATE.targets[c];p=POSITIONS.register(c,n,price,qty,t.support);BOT.send(f"💼 {n} 등록 · {p.entry:,.0f} · 방어 {p.stop:,.0f}",chat,buttons=self.position_buttons(c));return
        if txt.startswith("/매도 "):
            parts=txt.split();price=num(parts[-1]);stock=" ".join(parts[1:-1]);c,n=MASTER.resolve(stock)
            if c in POSITIONS.data and price>0:r=POSITIONS.close(c,price,"직접입력 매도");BOT.send(f"✅ {r['name']} 종료 {r['return_pct']:+.2f}%",chat);return
        BOT.send("명령: /메뉴 /상태 /추가 종목 /삭제 종목 /매수 종목 가격 [수량] /매도 종목 가격",chat,buttons=self.main_buttons())
    @staticmethod
    def within(n,start,end):
        sh,sm=map(int,start.split(":"));eh,em=map(int,end.split(":"))
        cur=n.hour*60+n.minute;return sh*60+sm<=cur<eh*60+em
    def scheduler(self):
        while True:
            n=now();d=str(n.date())
            if self.today!=d:
                self.today=d;self.stage_alert.clear()
                for t in STATE.targets.values():
                    t.signal_count_today=0;t.pass_count_today=0
                    if t.stage!=ST_HOLD:STATE.set_stage(t.code,ST_WAIT,"새 거래일")
            if time.time()-STATE.last_market_check>=60:
                STATE.last_market_check=time.time();WORKER.submit(self.update_market_risk)
            if n.weekday()<5:
                if self.within(n,SETTINGS.morning_brief,"09:00") and self.brief_sent.get("m")!=d:
                    WORKER.submit(lambda:BOT.send(self.morning(),buttons=self.main_buttons()));self.brief_sent["m"]=d
                if self.within(n,SETTINGS.close_brief,"16:00") and self.brief_sent.get("c")!=d:
                    BOT.send_async(self.close_brief(),buttons=self.main_buttons());self.brief_sent["c"]=d
                if self.within(n,SETTINGS.nxt_brief,"21:00") and self.brief_sent.get("n")!=d:
                    BOT.send_async(self.nxt_brief(),buttons=self.main_buttons());self.brief_sent["n"]=d
                if n.weekday()==4 and self.within(n,SETTINGS.weekly_brief,"21:00") and self.brief_sent.get("w")!=d:
                    BOT.send_async(self.weekly(),buttons=self.main_buttons());self.brief_sent["w"]=d
            self.update_focus()
            # 틱이 잠시 없어도 SIGNAL 만료/COOLDOWN 해제를 처리
            for c,t in list(STATE.targets.items()):
                if t.stage in (ST_SIGNAL,ST_COOLDOWN):
                    st,_,reason=BRAIN.evaluate(c)
                    if st and st!=t.stage:STATE.set_stage(c,st,reason)
            time.sleep(5)
    def start(self):
        with self.start_lock:
            if self.started:
                log.warning("APP.start duplicate call blocked")
                return
            self.started=True

        STATE.runtime["boot"]="starting"
        STATE.runtime["boot_stage"]="worker"
        log.info("VIP V5 APP.start entered")
        try:
            WORKER.start()
            log.info("Async worker started")

            STATE.runtime["boot_stage"]="master"
            try:
                MASTER.load()
                STATE.runtime["master"]=f"ready:{len(MASTER.code_to_name)}"
                log.info("KIS stock master ready: %s symbols",len(MASTER.code_to_name))
            except Exception as e:
                STATE.runtime["master"]="failed"
                STATE.runtime["last_error"]=f"master: {e}"
                log.exception("master load failed - continuing with stored VIP codes")

            STATE.runtime["boot_stage"]="vip_restore"
            for c in list(STATE.vip_targets):
                if c in MASTER.code_to_name:
                    STATE.vip_targets[c]=MASTER.code_to_name[c]
                STATE.ensure_target(c,STATE.vip_targets[c])
            STATE.save_vip()
            POSITIONS.load()
            log.info("VIP/position restore complete: VIP %s / positions %s",len(STATE.vip_targets),len(POSITIONS.data))

            STATE.runtime["boot_stage"]="telegram"
            BOT.text_handler=self.handle_text
            BOT.callback_handler=self.handle_cb
            if SETTINGS.telegram_token:
                threading.Thread(target=BOT.poll,daemon=True,name="telegram-poll").start()
                STATE.runtime["telegram"]="polling"
                log.info("Telegram polling thread started")
            else:
                STATE.runtime["telegram"]="disabled_missing_token"
                log.warning("Telegram token missing")

            STATE.runtime["boot_stage"]="bars"
            for c in list(STATE.vip_targets):
                WORKER.submit(self.sync_bars_for,c)
            log.info("Initial minute-bar sync queued for %s VIP symbols",len(STATE.vip_targets))

            STATE.runtime["boot_stage"]="scheduler"
            threading.Thread(target=self.scheduler,daemon=True,name="scheduler").start()
            STATE.runtime["scheduler"]="running"
            log.info("Scheduler started")

            STATE.runtime["boot_stage"]="nxt"
            if SETTINGS.kis_app_key and SETTINGS.kis_app_secret:
                threading.Thread(target=KIS_CLIENT.stream,args=(self.on_tick,self.on_book),daemon=True,name="KIS-NXT-websocket").start()
                STATE.runtime["ws_engine"]="starting"
                log.info("NXT websocket engine thread started")
            else:
                STATE.runtime["ws"]="disabled_missing_credentials"
                STATE.runtime["ws_engine"]="disabled_missing_credentials"
                STATE.runtime["last_error"]="KIS_APP_KEY/KIS_APP_SECRET missing"
                log.error("KIS credentials missing - NXT disabled")

            STATE.runtime["boot"]="ready"
            STATE.runtime["boot_stage"]="complete"
            STATE.runtime["boot_completed_at"]=now().isoformat()
            log.info("VIP V5 boot complete")

            BOT.send_async(
                f"🚀 <b>뽕실 VIP V{SETTINGS.version}</b>\n"
                f"VIP {len(STATE.vip_targets)}/{STATE.vip_limit} · 집중호가 {SETTINGS.focus_orderbook_slots}\n"
                f"반복매매 쿨다운 {SETTINGS.cooldown_minutes}분\n"
                f"NXT 상태 {STATE.runtime['ws']}\n"
                f"자동주문 없음",
                buttons=self.main_buttons()
            )
        except Exception as e:
            STATE.runtime["boot"]="failed"
            STATE.runtime["boot_stage"]="fatal"
            STATE.runtime["last_error"]=f"startup fatal: {e}"
            log.exception("VIP V5 startup fatal error")
APP=App()

web=Flask(__name__)
@web.get("/")
def root():return f"Bongsil VIP V5 running - {SETTINGS.version}",200
@web.get("/health")
def health():return jsonify({"status":"ok" if STATE.runtime.get("boot")!="failed" else "degraded","boot_ready":STATE.runtime.get("boot")=="ready","version":SETTINGS.version,"vip":len(STATE.vip_targets),"positions":len(POSITIONS.data),"market_risk":STATE.market_risk,"runtime":STATE.runtime})
def delayed_start():
    try:
        STATE.runtime["boot"]="waiting"
        STATE.runtime["boot_stage"]="render_delay"
        delay=max(0,min(SETTINGS.render_start_delay,60))
        if SETTINGS.render_start_delay>60:
            log.warning("RENDER_START_DELAY=%ss is too long; capped at 60s",SETTINGS.render_start_delay)
        if delay:
            log.info("Render previous instance wait %ss",delay)
            time.sleep(delay)
        log.info("Render wait complete - calling APP.start")
        APP.start()
    except Exception as e:
        STATE.runtime["boot"]="failed"
        STATE.runtime["boot_stage"]="delayed_start_fatal"
        STATE.runtime["last_error"]=f"delayed_start: {e}"
        log.exception("delayed_start fatal error")
if __name__=="__main__":
    threading.Thread(target=delayed_start,daemon=True,name="app-initializer").start();web.run(host="0.0.0.0",port=SETTINGS.port,threaded=True,use_reloader=False)
