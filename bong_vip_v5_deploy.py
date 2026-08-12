from __future__ import annotations

# ============================================================
# 🤖 명하 VIP V5 - Quant Repeat Sniper
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
import statistics
import hashlib
import hmac
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import websocket
from flask import Flask, jsonify, request, send_file, make_response
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
log = logging.getLogger("myeongha.vip.v5")


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


def estimated_net_pnl(entry, current, qty):
    """Estimate P/L after buy/sell commission and sell-side domestic stock tax."""
    entry=max(0.0,num(entry));current=max(0.0,num(current));qty=max(0.0,num(qty))
    buy_amount=entry*qty
    sell_amount=current*qty
    buy_fee=buy_amount*(SETTINGS.namuh_buy_fee_pct/100.0)
    sell_fee=sell_amount*(SETTINGS.namuh_sell_fee_pct/100.0)
    sell_tax=sell_amount*(SETTINGS.domestic_sell_tax_pct/100.0)
    gross_pnl=sell_amount-buy_amount
    net_pnl=gross_pnl-buy_fee-sell_fee-sell_tax
    invested=buy_amount+buy_fee
    net_return_pct=(net_pnl/invested*100.0) if invested else 0.0
    return {
        "buy_amount":buy_amount,"sell_amount":sell_amount,
        "buy_fee":buy_fee,"sell_fee":sell_fee,"sell_tax":sell_tax,
        "gross_pnl":gross_pnl,"net_pnl":net_pnl,
        "net_return_pct":net_return_pct,
        "total_estimated_cost":buy_fee+sell_fee+sell_tax,
    }


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
    version: str = "5.1.1 Myeongha Quant Repeat Sniper"
    telegram_token: str = os.getenv("TELEGRAM_TOKEN", "").strip()
    chat_id: str = (os.getenv("CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    kis_app_key: str = os.getenv("KIS_APP_KEY", "").strip()
    kis_app_secret: str = os.getenv("KIS_APP_SECRET", "").strip()
    kis_env: str = os.getenv("KIS_ENV", "real").strip().lower()
    port: int = env_int("PORT", 10000)
    render_start_delay: int = env_int("RENDER_START_DELAY", 180)
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
    # Quant patch
    projected_volume_min_seconds: int = env_int("PROJECTED_VOLUME_MIN_SECONDS", 15)
    projected_volume_ratio: float = env_float("PROJECTED_VOLUME_RATIO", 1.35)
    signal_strength_min: float = env_float("SIGNAL_STRENGTH_MIN", 108.0)
    signal_orderbook_min: float = env_float("SIGNAL_ORDERBOOK_MIN", 0.95)
    daily_ma_refresh_minutes: int = env_int("DAILY_MA_REFRESH_MINUTES", 30)
    # NaMu/NH domestic-stock estimated costs.
    # Account/event/channel fees differ, so Render env values can override these defaults.
    namuh_buy_fee_pct: float = env_float("NAMUH_BUY_FEE_PCT", 0.01)
    namuh_sell_fee_pct: float = env_float("NAMUH_SELL_FEE_PCT", 0.01)
    domestic_sell_tax_pct: float = env_float("DOMESTIC_SELL_TAX_PCT", 0.15)
    miniapp_url: str = os.getenv("MINIAPP_URL", "https://epin-signal-bot.onrender.com/miniapp").strip()
    miniapp_require_auth: bool = env_bool("MINIAPP_REQUIRE_AUTH", True)

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
    projected_volume_ratio:float=0.0; ma5d:float=0.0; ma20d:float=0.0; daily_trend:str="UNKNOWN"
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
    def configure_miniapp_menu(self):
        if not SETTINGS.telegram_token or not SETTINGS.miniapp_url:return
        d=self._req("setChatMenuButton",{
            "menu_button":{"type":"web_app","text":"📱 명하 Mini App","web_app":{"url":SETTINGS.miniapp_url}}
        },12)
        if d.get("ok"):log.info("Telegram Mini App menu button configured")
        else:log.warning("Telegram Mini App menu button config failed")
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
        self.bars={};self.current={};self.last_cum={};self.books={};self.last_tick_at={};self.daily_trend={};self.daily_trend_at={}
        self.market_changes={"KOSPI":0.0,"KOSDAQ":0.0};self.last_market_check=0.0
        self.market_risk="NORMAL";self.focus_codes=[];self.runtime={"ws":"stopped","trade_subscribed":0,"order_subscribed":0,"last_error":None,"last_tick":None};self.pending_input={};self.signals=STORE.load_signals();self.trades=STORE.load_trades()
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
    def daily_bars(self,c,days=40):
        """KIS 공식 국내주식 기간별시세 API로 일봉을 조회한다."""
        try:
            end=now().strftime("%Y%m%d")
            start=(now()-timedelta(days=max(60,days*3))).strftime("%Y%m%d")
            r=self.s.get(
                f"{self.rest}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                headers={
                    "authorization":f"Bearer {self.auth()}",
                    "appkey":SETTINGS.kis_app_key,
                    "appsecret":SETTINGS.kis_app_secret,
                    "tr_id":"FHKST03010100",
                    "custtype":"P",
                },
                params={
                    "FID_COND_MRKT_DIV_CODE":"J",
                    "FID_INPUT_ISCD":c,
                    "FID_INPUT_DATE_1":start,
                    "FID_INPUT_DATE_2":end,
                    "FID_PERIOD_DIV_CODE":"D",
                    "FID_ORG_ADJ_PRC":"0",
                },
                timeout=15,
            )
            r.raise_for_status()
            return (r.json().get("output2") or [])[:max(25,days)]
        except Exception as e:
            log.warning("daily bars %s: %s",c,e)
            return []
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
            if self.stream_running:return
            self.stream_running=True
        retry=5;order_slots=SETTINGS.focus_orderbook_slots
        try:
            while True:
                if SETTINGS.kis_env!="real":STATE.runtime["ws"]="disabled_virtual";time.sleep(60);continue
                if not in_session(SETTINGS.nxt_start,SETTINGS.nxt_end):STATE.runtime["ws"]="waiting_market_session";STATE.runtime["trade_subscribed"]=0;STATE.runtime["order_subscribed"]=0;time.sleep(30);continue
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
                        log.info("NXT Websocket connected");sync(ws);refresh_thread=threading.Thread(target=refresh,args=(ws,),daemon=True);refresh_thread.start()
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
    def _median(values):
        vals=[float(v) for v in values if v is not None and float(v)>=0]
        return statistics.median(vals) if vals else 0.0

    @staticmethod
    def metrics(c):
        bars=list(STATE.bars.get(c,[]));cur=STATE.current.get(c)
        if cur:bars.append(cur)
        if len(bars)<20:return None

        r=bars[-60:]
        latest=r[-1]
        completed=r[:-1] if STATE.current.get(c) is not None else r
        vols=[max(0,b.volume) for b in r]
        tot=sum(vols)
        if tot<=0:return None

        vwap=sum(((b.high+b.low+b.close)/3)*max(0,b.volume) for b in r)/tot

        # 기존 눌림 거래량비: 최근 3개 완성/진행 분봉 vs 이전 12개
        prior=vols[-15:-3]
        recent=vols[-3:]
        pa=sum(prior)/len(prior) if prior else 0
        ra=sum(recent)/len(recent) if recent else 0
        vr=ra/pa if pa else 0

        highs=[b.high for b in r];lows=[b.low for b in r]
        high20=max(highs[-20:-1]) if len(highs)>=2 else latest.high
        low12=min(lows[-12:])
        support=max(vwap*0.995,low12)
        resistance=high20
        pull=pct(latest.close,high20)
        prev3=max(highs[-4:-1]) if len(highs)>=4 else latest.high
        br=latest.close>=prev3

        # 1분 미완성 거래량 선제예측:
        # 15초 이전에는 노이즈로 보고 신호 조건에 사용하지 않는다.
        elapsed=60
        projected=latest.volume
        projected_ratio=0.0
        if STATE.current.get(c) is not None and latest.minute==STATE.current[c].minute:
            tick_at=STATE.last_tick_at.get(c) or now()
            elapsed=max(1,int((tick_at-latest.minute).total_seconds())+1)
            baseline=Brain._median([b.volume for b in completed[-5:]])
            if elapsed>=SETTINGS.projected_volume_min_seconds and baseline>0:
                projected=latest.volume*(60.0/min(60,elapsed))
                projected_ratio=projected/baseline
        else:
            baseline=Brain._median([b.volume for b in completed[-6:-1]])
            if baseline>0:projected_ratio=latest.volume/baseline

        ob=STATE.books.get(c)
        imb=ob.imbalance if ob and (now()-ob.updated_at).total_seconds()<90 else 1.0

        # 진짜 일봉 5일/20일선(KIS REST 캐시)
        dt=STATE.daily_trend.get(c) or {}
        ma5d=num(dt.get("ma5"));ma20d=num(dt.get("ma20"))
        prev_ma5=num(dt.get("prev_ma5"));prev_ma20=num(dt.get("prev_ma20"))
        daily_cross=bool(ma5d and ma20d and prev_ma5 and prev_ma20 and prev_ma5<=prev_ma20 and ma5d>ma20d)
        daily_bull=bool(ma5d and ma20d and ma5d>=ma20d)
        daily_pullback=bool(ma20d and ma20d*0.99<=latest.close<=ma20d*1.02)
        daily_known=bool(ma5d and ma20d)
        daily_trend="GOLDEN_CROSS" if daily_cross else "BULL" if daily_bull else "MA20_PULLBACK" if daily_pullback else "BEAR" if daily_known else "UNKNOWN"

        # 선제 거래량은 보너스이면서 최종 방아쇠 중 하나.
        volume_burst=projected_ratio>=SETTINGS.projected_volume_ratio or vr>=1.05

        score=50.0
        score+=clamp((latest.trade_strength-95)*0.8,-15,20)
        score+=(10 if -0.8<=pct(latest.close,vwap)<=1.2 else -5)
        score+=(12 if -4.5<=pull<=-0.6 else -5)
        score+=(8 if vr<=0.85 else 0)
        score+=(12 if br else 0)
        score+=clamp((imb-1)*12,-8,10)
        score+=(10 if projected_ratio>=SETTINGS.projected_volume_ratio else 0)
        score+=(8 if daily_cross else 5 if daily_bull else 4 if daily_pullback else -4 if daily_known else 0)
        score=clamp(score)

        return {
            "price":latest.close,"vwap":vwap,"support":support,"resistance":resistance,
            "pullback_pct":pull,"volume_ratio":vr,"strength":latest.trade_strength,
            "short_break":br,"imbalance":imb,"score":score,"today_open":r[0].open,
            "projected_volume":projected,"projected_volume_ratio":projected_ratio,
            "volume_burst":volume_burst,"elapsed_seconds":elapsed,
            "ma5d":ma5d,"ma20d":ma20d,"daily_cross":daily_cross,
            "daily_bull":daily_bull,"daily_pullback":daily_pullback,
            "daily_known":daily_known,"daily_trend":daily_trend,
        }

    def evaluate(self,c):
        t=STATE.targets.get(c);m=self.metrics(c)
        if not t or not m:return None,m,"분봉 수집중"

        for k,a in (
            ("last_price","price"),("vwap","vwap"),("support","support"),
            ("resistance","resistance"),("trade_strength","strength"),
            ("volume_ratio","volume_ratio"),("pullback_pct","pullback_pct"),
            ("score","score"),("projected_volume_ratio","projected_volume_ratio"),
            ("ma5d","ma5d"),("ma20d","ma20d"),("daily_trend","daily_trend")
        ):setattr(t,k,m[a])

        if c in POSITIONS.data:return ST_HOLD,m,"보유중"

        if t.stage==ST_COOLDOWN:
            try:
                if t.cooldown_until and now()<datetime.fromisoformat(t.cooldown_until):
                    return ST_COOLDOWN,m,"쿨다운"
            except Exception:pass
            t.cooldown_until=None
            return ST_WAIT,m,"쿨다운 종료"

        if t.stage==ST_SIGNAL and t.last_signal_at:
            try:
                if now()-datetime.fromisoformat(t.last_signal_at)>timedelta(minutes=SETTINGS.signal_expire_minutes):
                    t.cooldown_until=(now()+timedelta(minutes=SETTINGS.cooldown_minutes)).isoformat()
                    return ST_COOLDOWN,m,"타점 만료"
            except Exception:pass

        # 1) 눌림 후보: 기존 명하 로직 보존
        near=abs(pct(m["price"],m["support"]))<=1.1 or abs(pct(m["price"],m["vwap"]))<=1.2
        healthy=-4.5<=m["pullback_pct"]<=-0.6
        dry=m["volume_ratio"]<=0.85
        pullback=near and healthy and dry and m["price"]>=m["today_open"]*0.985

        # 2) 일봉 맥락: 데이터가 없으면 장애로 막지 않고 기존 엔진으로 폴백
        daily_context=(not m["daily_known"]) or m["daily_bull"] or m["daily_cross"] or m["daily_pullback"]

        # 3) 최종 방아쇠:
        # VWAP 회복 + 체결강도 + 단기고점 돌파 + 거래량 재유입/선제폭발 + 호가 방어 + 일봉 맥락
        reentry=(
            m["price"]>=m["vwap"]*0.998
            and m["strength"]>=SETTINGS.signal_strength_min
            and m["short_break"]
            and m["volume_burst"]
            and m["imbalance"]>=SETTINGS.signal_orderbook_min
            and daily_context
        )

        if STATE.market_risk=="RISK":
            return (ST_PULLBACK if pullback else ST_WAIT),m,"시장 RISK - 신규진입 보류"

        if t.stage in (ST_PULLBACK,ST_READY) and reentry:
            if STATE.market_risk=="CAUTION" and m["score"]<78:
                return ST_READY,m,"시장 CAUTION - 강한 확인 대기"
            reason="체결강도 + 단기돌파 + 거래량 선제확인"
            if m["daily_cross"]:reason+=" + 일봉 골든크로스"
            elif m["daily_pullback"]:reason+=" + 20일선 눌림"
            elif m["daily_bull"]:reason+=" + 일봉 정배열"
            return ST_SIGNAL,m,reason

        if pullback:
            if m["strength"]>=100:
                return ST_READY,m,"지지구간 + 매수세 회복"
            return ST_PULLBACK,m,"지지구간 접근 + 거래량 감소"

        return ST_WAIT,m,"조건 대기"
BRAIN=Brain()

def google_news_headlines(q,limit=4):
    """Google News RSS headline + publication time (KST)."""
    try:
        url=f"https://news.google.com/rss/search?q={urllib.parse.quote(q)}&hl=ko&gl=KR&ceid=KR:ko"
        r=requests.get(url,timeout=10,headers={"User-Agent":"Mozilla/5.0"})
        r.raise_for_status()
        root=ET.fromstring(r.text)
        out=[]
        for i in root.findall(".//item")[:limit]:
            title=(i.findtext("title") or "").strip()
            if not title:
                continue
            pub_raw=(i.findtext("pubDate") or "").strip()
            pub_text="시간정보 없음"
            if pub_raw:
                try:
                    dt=parsedate_to_datetime(pub_raw)
                    if dt.tzinfo is None:
                        dt=dt.replace(tzinfo=timezone.utc)
                    pub_text=dt.astimezone(KST).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    pub_text=pub_raw
            link=(i.findtext("link") or "").strip()
            out.append({"title":title,"published":pub_text,"link":link})
        return out
    except Exception as e:
        log.warning("Google news fetch failed: %s",e)
        return []

class App:
    def __init__(self):self.started=False;self.start_lock=threading.Lock();self.today="";self.stage_alert={};self.brief_sent={};self.last_daily_refresh={}
    def sync_bars_for(self,c):
        raw=KIS_CLIENT.minute_bars(c);q=deque(maxlen=390);n=now()
        for r in reversed(raw):
            try:
                h=str(r.get("stck_cntg_hour") or "").zfill(6);m=n.replace(hour=int(h[:2]),minute=int(h[2:4]),second=0,microsecond=0);q.append(Bar(m,num(r.get("stck_oprc")),num(r.get("stck_hgpr")),num(r.get("stck_lwpr")),num(r.get("stck_prpr")),integer(r.get("cntg_vol")),integer(r.get("acml_vol")),100))
            except Exception:pass
        if q:
            with STATE.lock:STATE.bars[c]=q
    def sync_daily_for(self,c,force=False):
        last=self.last_daily_refresh.get(c)
        if not force and last and now()-last<timedelta(minutes=SETTINGS.daily_ma_refresh_minutes):
            return
        raw=KIS_CLIENT.daily_bars(c,40)
        closes=[]
        # KIS output2는 최신순인 경우가 일반적이므로 날짜 기준 정렬
        rows=[]
        for r in raw:
            try:
                d=str(r.get("stck_bsop_date") or "")
                cp=num(r.get("stck_clpr"))
                if d and cp>0:rows.append((d,cp))
            except Exception:pass
        rows.sort(key=lambda x:x[0])
        closes=[x[1] for x in rows]
        if len(closes)<20:
            log.info("daily MA pending: %s %s bars",c,len(closes));return
        ma5=sum(closes[-5:])/5
        ma20=sum(closes[-20:])/20
        prev_ma5=sum(closes[-6:-1])/5 if len(closes)>=21 else ma5
        prev_ma20=sum(closes[-21:-1])/20 if len(closes)>=21 else ma20
        with STATE.lock:
            STATE.daily_trend[c]={"ma5":ma5,"ma20":ma20,"prev_ma5":prev_ma5,"prev_ma20":prev_ma20,"asof":rows[-1][0]}
            STATE.daily_trend_at[c]=now()
        self.last_daily_refresh[c]=now()
        log.info("daily MA ready: %s MA5 %.0f MA20 %.0f",STATE.vip_targets.get(c,c),ma5,ma20)
    def on_book(self,b):STATE.books[b.code]=b
    def on_tick(self,t):
        if t.price<=0 or abs((now()-t.timestamp).total_seconds())>180:return
        with STATE.lock:
            prev=STATE.last_cum.get(t.code);inc=t.cumulative_volume-prev if prev is not None and t.cumulative_volume>=prev else max(0,t.volume);STATE.last_cum[t.code]=t.cumulative_volume;minute=t.timestamp.replace(second=0,microsecond=0);cur=STATE.current.get(t.code)
            if not cur:STATE.current[t.code]=Bar(minute,t.price,t.price,t.price,t.price,inc,t.cumulative_volume,t.trade_strength)
            elif cur.minute==minute:cur.high=max(cur.high,t.price);cur.low=min(cur.low,t.price);cur.close=t.price;cur.volume+=inc;cur.cumulative_volume=max(cur.cumulative_volume,t.cumulative_volume);cur.trade_strength=t.trade_strength
            else:STATE.bars.setdefault(t.code,deque(maxlen=390)).append(cur);STATE.current[t.code]=Bar(minute,t.price,t.price,t.price,t.price,inc,t.cumulative_volume,t.trade_strength)
            STATE.runtime["last_tick"]=t.timestamp.isoformat();STATE.last_tick_at[t.code]=t.timestamp
        for m in POSITIONS.check(t.code,t.price):BOT.send_async(m,buttons=self.position_buttons(t.code))
        if t.code in STATE.vip_targets:
            if t.code not in STATE.daily_trend:
                WORKER.submit(self.sync_daily_for,t.code)
            self.evaluate_target(t.code)
    def update_market_risk(self):
        """VIP NXT 구독 슬롯을 아끼기 위해 시장위험은 1분 주기로 지수 프록시를 조회한다."""
        if not HAS_YF:
            return
        try:
            rows={}
            for label,symbol in (("KOSPI","^KS11"),("KOSDAQ","^KQ11")):
                d=yf.Ticker(symbol).history(period="2d",interval="1m")
                if d is None or d.empty:
                    continue
                day=d.index[-1].date()
                today=d[d.index.date==day]
                if today.empty:
                    continue
                op=float(today["Open"].iloc[0]); last=float(today["Close"].iloc[-1])
                if op>0:rows[label]=pct(last,op)
            if not rows:return
            STATE.market_changes.update(rows)
            w=min(rows.values());old=STATE.market_risk
            new="RISK" if w<=SETTINGS.risk_red_pct else "CAUTION" if w<=SETTINGS.risk_caution_pct else "NORMAL" if w>-0.5 else old
            if new!=old:
                STATE.market_risk=new
                BOT.send_async(f"{'🟢' if new=='NORMAL' else '🟡' if new=='CAUTION' else '🔴'} <b>시장 위험모드 {new}</b>\nKOSPI/KOSDAQ 시가 대비 최저 {w:+.2f}%")
        except Exception as e:
            log.debug("market risk update failed: %s",e)
    def update_focus(self):
        pri={ST_HOLD:100,ST_SIGNAL:90,ST_READY:75,ST_PULLBACK:60,ST_WAIT:20,ST_COOLDOWN:5};STATE.focus_codes=[c for _,c in sorted(((pri.get(t.stage,0)+t.score/10,c) for c,t in STATE.targets.items()),reverse=True)[:SETTINGS.focus_orderbook_slots]]
    def evaluate_target(self,c):
        t=STATE.targets[c];st,m,reason=BRAIN.evaluate(c)
        if not st:return
        changed=STATE.set_stage(c,st,reason);self.update_focus()
        if st==ST_SIGNAL and (changed or self.allowed(c,"signal",10)):
            t.last_signal_at=now().isoformat();t.signal_count_today+=1;STATE.signals.append({"code":c,"name":t.name,"time":t.last_signal_at,"price":t.last_price,"support":t.support,"resistance":t.resistance,"vwap":t.vwap,"strength":t.trade_strength,"score":t.score,"projected_volume_ratio":t.projected_volume_ratio,"ma5d":t.ma5d,"ma20d":t.ma20d});STORE.save_signals(STATE.signals);WORKER.submit(self.send_signal_alert,c);self.stage_alert[(c,"signal")]=now()
        elif changed and st==ST_PULLBACK and self.allowed(c,"pull",30):BOT.send_async(f"🟡 <b>{html.escape(t.name)} 눌림관찰</b>\n현재 {t.last_price:,.0f} · VWAP {t.vwap:,.0f} · 지지 {t.support:,.0f}\n거래량비 {t.volume_ratio:.2f} · 체결강도 {t.trade_strength:.0f}\n아직 매수신호 아님");self.stage_alert[(c,"pull")]=now()
    def allowed(self,c,k,m):
        x=self.stage_alert.get((c,k));return not x or now()-x>=timedelta(minutes=m)
    def signal_text(self,t):
        ob=STATE.books.get(t.code);obs=f"\n호가잔량비 {ob.imbalance:.2f}배" if ob else ""
        proj=f" · 예상1분 {t.projected_volume_ratio:.2f}배" if t.projected_volume_ratio>0 else ""
        daily=""
        if t.ma5d and t.ma20d:daily=f"\n일봉 MA5 {t.ma5d:,.0f} · MA20 {t.ma20d:,.0f} · {t.daily_trend}"
        return f"🟢 <b>[명하 VIP 재진입 타점] {html.escape(t.name)}</b>\n현재 <b>{t.last_price:,.0f}원</b>\nVWAP {t.vwap:,.0f} · 지지 {t.support:,.0f} · 저항 {t.resistance:,.0f}\n체결강도 {t.trade_strength:.0f} · 거래량비 {t.volume_ratio:.2f}{proj} · 점수 {t.score:.0f}{obs}{daily}\n\n✅ 눌림/추세 맥락 확인\n✅ 체결강도 + 단기고점 돌파\n✅ 거래량 재유입/선제증가 확인\n⚠️ 실제 주문은 사용자가 직접 수행"
    def send_signal_alert(self,c):
        t=STATE.targets.get(c)
        if not t:return
        caption=self.signal_text(t);buf=self.chart(c)
        if buf:BOT.photo(buf,caption,buttons=self.signal_buttons(c))
        else:BOT.send(caption,buttons=self.signal_buttons(c))
    def main_buttons(self):
        rows=[]
        if SETTINGS.miniapp_url:
            rows.append([{"text":"📱 명하 Mini App 열기","web_app":{"url":SETTINGS.miniapp_url}}])
        rows += [
            [{"text":"🎯 VIP 현황","callback_data":"vip"},{"text":"🟢 타점대기","callback_data":"ready"}],
            [{"text":"💼 보유관리","callback_data":"hold"},{"text":"📊 시장상태","callback_data":"market"}],
            [{"text":"➕ VIP 추가","callback_data":"add"},{"text":"➖ VIP 삭제","callback_data":"remove"}],
            [{"text":"📰 VIP 뉴스","callback_data":"newsall"},{"text":"🩺 시스템진단","callback_data":"diag"}],
            [{"text":"🌅 아침브리핑","callback_data":"bm"},{"text":"🌙 마감브리핑","callback_data":"bn"}],
        ]
        return {"inline_keyboard":rows}
    def signal_buttons(self,c):
        rows=[
            [{"text":"💼 현재가 매수등록 + 집중관리","callback_data":f"buy:{c}"}],
            [{"text":"⏳ 더 관찰","callback_data":f"observe:{c}"},{"text":"❌ 이번 파동 패스","callback_data":f"pass:{c}"}],
            [{"text":"📈 상세차트","callback_data":f"chart:{c}"}],
        ]
        if SETTINGS.miniapp_url:rows.append([{"text":"📱 명하 Mini App 열기","web_app":{"url":SETTINGS.miniapp_url}}])
        rows.append([{"text":"🏠 메인","callback_data":"main"}])
        return {"inline_keyboard":rows}
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
            x=list(range(len(bars)));y=[b.close for b in bars];labels=[b.minute.strftime("%H:%M") for b in bars]
            fig=plt.figure(figsize=(7,3.5));plt.plot(x,y,label="Price");t=STATE.targets[c]
            if t.vwap:plt.axhline(t.vwap,linestyle="--",label="VWAP")
            if t.support:plt.axhline(t.support,linestyle=":",label="Support")
            if t.resistance:plt.axhline(t.resistance,linestyle=":",label="Resistance")
            step=max(1,len(x)//6);ticks=x[::step]
            plt.xticks(ticks,[labels[i] for i in ticks],rotation=30)
            plt.grid(alpha=.25);plt.legend();plt.tight_layout();buf=io.BytesIO();plt.savefig(buf,format="png",dpi=110);plt.close(fig);buf.seek(0);return buf
        except Exception as e:
            log.debug("chart failed %s: %s",c,e);plt.close("all");return None
    def add_vip(self,q,chat):
        if len(STATE.vip_targets)>=STATE.vip_limit:BOT.send(f"⚠️ VIP 한도 {STATE.vip_limit}개",chat);return
        c,n=MASTER.resolve(q)
        if not c:BOT.send("정확한 종목명 또는 6자리 코드를 입력해주세요.",chat);return
        STATE.vip_targets[c]=n;STATE.ensure_target(c,n);STATE.save_vip();WORKER.submit(self.sync_bars_for,c);WORKER.submit(self.sync_daily_for,c,True);BOT.send(f"🎯 {html.escape(n)} VIP 추가 완료",chat,buttons=self.target_buttons(c))
    def remove_vip(self,c,chat):
        if c in POSITIONS.data:BOT.send("보유관리 중이라 삭제 불가",chat);return
        n=STATE.vip_targets.pop(c,None);STATE.targets.pop(c,None);STATE.save_vip();BOT.send(f"🗑️ {html.escape(n or c)} 삭제 완료",chat,buttons=self.main_buttons())
    def news_text(self,c=None):
        codes=[c] if c else list(STATE.vip_targets);lines=["📰 <b>VIP 뉴스 헤드라인</b>"]
        for x in codes:
            n=STATE.vip_targets.get(x,x);hs=google_news_headlines(f"{n} 주식 수주 공시",4)
            if hs:
                rows=[]
                for h in hs:
                    title=html.escape(h.get("title",""))
                    published=html.escape(h.get("published","시간정보 없음"))
                    link=html.escape(h.get("link",""),quote=True)
                    headline=f'<a href="{link}">{title}</a>' if link else title
                    rows.append(f"• {headline}\n  🕒 {published}")
                body="\n".join(rows)
            else:
                body="헤드라인 없음"
            lines.append(f"\n<b>{html.escape(n)}</b>\n"+body)
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
    def diag(self):return f"🩺 <b>VIP V5 진단</b>\nNXT {STATE.runtime['ws']}\n체결 {STATE.runtime['trade_subscribed']} · 호가 {STATE.runtime['order_subscribed']}\nVIP {len(STATE.vip_targets)}/{STATE.vip_limit} · 보유 {len(POSITIONS.data)}\n마지막체결 {STATE.runtime['last_tick'] or '-'}\n오류 {STATE.runtime['last_error'] or '-'}"
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
            if self.started:return
            self.started=True
        WORKER.start()
        try:MASTER.load()
        except Exception as e:STATE.runtime["last_error"]=f"master: {e}";log.exception("master load")
        for c in list(STATE.vip_targets):
            if c in MASTER.code_to_name:STATE.vip_targets[c]=MASTER.code_to_name[c]
            STATE.ensure_target(c,STATE.vip_targets[c])
        STATE.save_vip();POSITIONS.load();BOT.text_handler=self.handle_text;BOT.callback_handler=self.handle_cb
        if SETTINGS.telegram_token:threading.Thread(target=BOT.poll,daemon=True,name="telegram-poll").start()
        if SETTINGS.telegram_token:WORKER.submit(BOT.configure_miniapp_menu)
        for c in list(STATE.vip_targets):WORKER.submit(self.sync_bars_for,c);WORKER.submit(self.sync_daily_for,c,True)
        threading.Thread(target=self.scheduler,daemon=True,name="scheduler").start()
        if SETTINGS.kis_app_key and SETTINGS.kis_app_secret:threading.Thread(target=KIS_CLIENT.stream,args=(self.on_tick,self.on_book),daemon=True,name="KIS-NXT-websocket").start()
        else:STATE.runtime["ws"]="disabled_missing_credentials"
        BOT.send_async(f"🚀 <b>명하 VIP V{SETTINGS.version}</b>\nVIP {len(STATE.vip_targets)}/{STATE.vip_limit} · 집중호가 {SETTINGS.focus_orderbook_slots}\n반복매매 쿨다운 {SETTINGS.cooldown_minutes}분\n자동주문 없음",buttons=self.main_buttons())
APP=App()

web=Flask(__name__)
@web.get("/")
def root():return f"Myeongha VIP V5 running - {SETTINGS.version}",200
@web.get("/health")
def health():return jsonify({"status":"ok","version":SETTINGS.version,"vip":len(STATE.vip_targets),"positions":len(POSITIONS.data),"market_risk":STATE.market_risk,"runtime":STATE.runtime})

def _validate_telegram_init_data(init_data:str):
    if not SETTINGS.miniapp_require_auth:return {"id":str(SETTINGS.chat_id or "preview"),"preview":True}
    if not init_data or not SETTINGS.telegram_token:return None
    try:
        data=dict(urllib.parse.parse_qsl(init_data,keep_blank_values=True));recv=data.pop("hash",None)
        if not recv:return None
        check="\n".join(f"{k}={data[k]}" for k in sorted(data))
        secret=hmac.new(b"WebAppData",SETTINGS.telegram_token.encode(),hashlib.sha256).digest()
        calc=hmac.new(secret,check.encode(),hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc,recv):return None
        auth_date=integer(data.get("auth_date"),0)
        if auth_date and abs(time.time()-auth_date)>86400:return None
        user=json.loads(data.get("user") or "{}");allowed=str(SETTINGS.chat_id).strip()
        if allowed and user and str(user.get("id"))!=allowed:return None
        return user or {"id":allowed}
    except Exception as e:
        log.warning("Mini App auth failed: %s",e);return None

def _mini_auth():return _validate_telegram_init_data(request.headers.get("X-Telegram-Init-Data",""))
def _mini_json():return request.get_json(silent=True) or {}
def _mini_target(code):
    code=str(code or "").zfill(6);return code,STATE.targets.get(code) if code in STATE.vip_targets else None
def _no_store(resp):
    resp.headers["Cache-Control"]="no-store, no-cache, must-revalidate, max-age=0";resp.headers["Pragma"]="no-cache";return resp
def _plain(s):return html.unescape(re.sub(r"<[^>]+>","",str(s or "")))
def _google_news_items(q,limit=6):
    try:
        url=f"https://news.google.com/rss/search?q={urllib.parse.quote(q)}&hl=ko&gl=KR&ceid=KR:ko"
        r=requests.get(url,timeout=10,headers={"User-Agent":"Mozilla/5.0"});r.raise_for_status()
        root=ET.fromstring(r.text);out=[]
        for i in root.findall(".//item")[:limit]:
            out.append({"title":(i.findtext("title") or "").strip(),"published":(i.findtext("pubDate") or "").strip()})
        return out
    except Exception as e:log.warning("Mini App news failed: %s",e);return []

@web.get("/miniapp")
def miniapp():return send_file(str(Path(__file__).with_name("miniapp.html")))

@web.get("/api/miniapp/state")
def miniapp_state():
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    vip=[]
    for c,n in STATE.vip_targets.items():
        t=STATE.targets.get(c)
        if not t:continue
        ob=STATE.books.get(c)
        vip.append({"code":c,"name":n,"stage":t.stage,"stage_label":STATE_LABEL.get(t.stage,t.stage),
            "price":round(t.last_price or 0,2),"vwap":round(t.vwap or 0,2),"support":round(t.support or 0,2),
            "resistance":round(t.resistance or 0,2),"strength":round(t.trade_strength or 0,1),
            "volume_ratio":round(t.volume_ratio or 0,2),"pullback_pct":round(t.pullback_pct or 0,2),
            "score":round(t.score or 0,1),"note":t.note or "","signal_count_today":t.signal_count_today,
            "last_signal_at":t.last_signal_at,"orderbook_imbalance":round(ob.imbalance,2) if ob else None,
            "best_ask":round(ob.best_ask,2) if ob else None,"best_bid":round(ob.best_bid,2) if ob else None,
            "last_tick_at":STATE.last_tick_at.get(c).isoformat() if STATE.last_tick_at.get(c) else None})
    holdings=[]
    for c,p in POSITIONS.data.items():
        t=STATE.targets.get(c);current=(t.last_price if t and t.last_price else p.entry)
        costs=estimated_net_pnl(p.entry,current,p.qty)
        holdings.append({"code":c,"name":p.name,"entry":round(p.entry,2),"qty":p.qty,"current":round(current,2),
            "return_pct":round(pct(current,p.entry),2),"net_return_pct":round(costs["net_return_pct"],2),
            "buy_amount":round(costs["buy_amount"],2),"market_value":round(costs["sell_amount"],2),
            "gross_pnl":round(costs["gross_pnl"],2),"net_pnl":round(costs["net_pnl"],2),
            "buy_fee":round(costs["buy_fee"],2),"sell_fee":round(costs["sell_fee"],2),
            "sell_tax":round(costs["sell_tax"],2),"estimated_cost":round(costs["total_estimated_cost"],2),
            "highest":round(p.highest,2),"stop":round(p.stop,2),"level":p.level,"opened_at":p.opened_at})
    return _no_store(make_response(jsonify({"ok":True,"version":SETTINGS.version,"server_time":now().isoformat(),
        "market":{"risk":STATE.market_risk,"kospi":round(STATE.market_changes.get("KOSPI",0),2),"kosdaq":round(STATE.market_changes.get("KOSDAQ",0),2)},
        "runtime":STATE.runtime,"vip":vip,"holdings":holdings,"signals":list(reversed(STATE.signals[-20:])),
        "trades":list(reversed(STATE.trades[-20:]))})))

@web.get("/api/miniapp/stock/<code>")
def miniapp_stock(code):
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    code,t=_mini_target(code)
    if not t:return jsonify({"ok":False,"error":"not_found"}),404
    bars=list(STATE.bars.get(code,[]));cur=STATE.current.get(code)
    if cur:bars.append(cur)
    bars=bars[-120:];p=POSITIONS.data.get(code);ob=STATE.books.get(code)
    return _no_store(make_response(jsonify({"ok":True,"code":code,"name":STATE.vip_targets.get(code,code),
        "state":{"stage":t.stage,"stage_label":STATE_LABEL.get(t.stage,t.stage),"price":t.last_price,"vwap":t.vwap,
                 "support":t.support,"resistance":t.resistance,"strength":t.trade_strength,"volume_ratio":t.volume_ratio,
                 "pullback_pct":t.pullback_pct,"score":t.score,"note":t.note},
        "orderbook":{"imbalance":ob.imbalance,"best_ask":ob.best_ask,"best_bid":ob.best_bid,"updated_at":ob.updated_at.isoformat()} if ob else None,
        "position":(lambda current,costs:{
                    "entry":p.entry,"qty":p.qty,"current":current,
                    "return_pct":pct(current,p.entry),"net_return_pct":costs["net_return_pct"],
                    "buy_amount":costs["buy_amount"],"market_value":costs["sell_amount"],
                    "gross_pnl":costs["gross_pnl"],"net_pnl":costs["net_pnl"],
                    "buy_fee":costs["buy_fee"],"sell_fee":costs["sell_fee"],
                    "sell_tax":costs["sell_tax"],"estimated_cost":costs["total_estimated_cost"],
                    "highest":p.highest,"stop":p.stop,"level":p.level})(
                        t.last_price if t.last_price else p.entry,
                        estimated_net_pnl(p.entry,t.last_price if t.last_price else p.entry,p.qty)
                    ) if p else None,
        "bars":[{"time":b.minute.isoformat(),"open":b.open,"high":b.high,"low":b.low,"close":b.close,
                 "volume":b.volume,"strength":b.trade_strength} for b in bars]})))

@web.post("/api/miniapp/vip/add")
def miniapp_vip_add():
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    d=_mini_json();q=str(d.get("query") or d.get("stock") or d.get("name") or d.get("code") or "").strip()
    if not q:return jsonify({"ok":False,"error":"stock_required"}),400
    if len(STATE.vip_targets)>=STATE.vip_limit:return jsonify({"ok":False,"error":"vip_limit","limit":STATE.vip_limit}),409
    c,n=MASTER.resolve(q)
    if not c:return jsonify({"ok":False,"error":"stock_not_found"}),404
    if c in STATE.vip_targets:return jsonify({"ok":True,"code":c,"name":STATE.vip_targets[c],"already_exists":True})
    STATE.vip_targets[c]=n;STATE.ensure_target(c,n);STATE.save_vip();WORKER.submit(APP.sync_bars_for,c);APP.update_focus()
    return jsonify({"ok":True,"code":c,"name":n,"message":"VIP 추가 완료"})

@web.post("/api/miniapp/vip/<code>/remove")
def miniapp_vip_remove(code):
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    code,t=_mini_target(code)
    if not t:return jsonify({"ok":False,"error":"not_found"}),404
    if code in POSITIONS.data:return jsonify({"ok":False,"error":"position_active","message":"보유관리 중이라 삭제할 수 없습니다."}),409
    n=STATE.vip_targets.pop(code,None);STATE.targets.pop(code,None);STATE.save_vip();APP.update_focus()
    return jsonify({"ok":True,"code":code,"name":n,"message":"VIP 삭제 완료"})

@web.post("/api/miniapp/stock/<code>/observe")
def miniapp_observe(code):
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    code,t=_mini_target(code)
    if not t:return jsonify({"ok":False,"error":"not_found"}),404
    STATE.set_stage(code,ST_READY,"Mini App 추가관찰");APP.update_focus()
    return jsonify({"ok":True,"message":"추가 관찰로 변경","stage":ST_READY})

@web.post("/api/miniapp/stock/<code>/pass")
def miniapp_pass(code):
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    code,t=_mini_target(code)
    if not t:return jsonify({"ok":False,"error":"not_found"}),404
    t.pass_count_today+=1;cooldown_target(code,"Mini App 사용자 패스");APP.update_focus()
    return jsonify({"ok":True,"message":f"{SETTINGS.cooldown_minutes}분 쿨다운 시작","stage":ST_COOLDOWN})

@web.post("/api/miniapp/stock/<code>/buy")
def miniapp_buy(code):
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    code,t=_mini_target(code)
    if not t:return jsonify({"ok":False,"error":"not_found"}),404
    if code in POSITIONS.data:return jsonify({"ok":False,"error":"already_holding"}),409
    d=_mini_json();price=num(d.get("price"),t.last_price);qty=num(d.get("qty"),1)
    if price<=0:return jsonify({"ok":False,"error":"price_required"}),400
    p=POSITIONS.register(code,t.name,price,qty if qty>0 else 1,t.support);APP.update_focus()
    BOT.send_async(f"💼 <b>{html.escape(t.name)} Mini App 매수등록</b>\n관리 시작 {p.entry:,.0f}원 · 방어 {p.stop:,.0f}원\n⚠️ 실제 주문 아님",buttons=APP.position_buttons(code))
    return jsonify({"ok":True,"message":"보유관리 시작 · 실제 주문 아님","entry":p.entry,"qty":p.qty,"stop":p.stop})

@web.post("/api/miniapp/stock/<code>/sell")
def miniapp_sell(code):
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    code,t=_mini_target(code)
    if not t:return jsonify({"ok":False,"error":"not_found"}),404
    if code not in POSITIONS.data:return jsonify({"ok":False,"error":"not_holding"}),409
    d=_mini_json();price=num(d.get("price"),t.last_price)
    if price<=0:return jsonify({"ok":False,"error":"price_required"}),400
    r=POSITIONS.close(code,price,"Mini App 매도종료");APP.update_focus()
    BOT.send_async(f"✅ <b>{html.escape(r['name'])} Mini App 관리종료</b>\n종료가 {r['exit']:,.0f}원 · 수익률 {r['return_pct']:+.2f}%\n⚠️ 실제 주문 아님",buttons=APP.main_buttons())
    return jsonify({"ok":True,"message":"보유관리 종료 · 실제 주문 아님","trade":r})

@web.get("/api/miniapp/stock/<code>/news")
def miniapp_news(code):
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    code,t=_mini_target(code)
    if not t:return jsonify({"ok":False,"error":"not_found"}),404
    rows=_google_news_items(f"{t.name} 주식 수주 공시",6)
    return _no_store(make_response(jsonify({"ok":True,"code":code,"name":t.name,"checked_at":now().isoformat(),
        "news":[{"title":r.get("title",""),"published":r.get("published"),"time_text":r.get("published") or "시간정보 없음"} for r in rows]})))

@web.get("/api/miniapp/brief/<kind>")
def miniapp_brief(kind):
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    kind=str(kind or "").lower()
    if kind=="morning":title,body="아침브리핑",APP.morning()
    elif kind=="close":title,body="마감브리핑",APP.close_brief()
    elif kind=="nxt":title,body="NXT마감브리핑",APP.nxt_brief()
    elif kind=="weekly":title,body="주간성적",APP.weekly()
    else:return jsonify({"ok":False,"error":"invalid_brief"}),400
    return _no_store(make_response(jsonify({"ok":True,"title":title,"text":_plain(body)})))

@web.get("/api/miniapp/diag")
def miniapp_diag():
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    return _no_store(make_response(jsonify({"ok":True,"version":SETTINGS.version,"server_time":now().isoformat(),
        "runtime":STATE.runtime,"market_risk":STATE.market_risk,"market_changes":STATE.market_changes,
        "vip_count":len(STATE.vip_targets),"vip_limit":STATE.vip_limit,"positions":len(POSITIONS.data),
        "text":_plain(APP.diag())})))

def delayed_start():
    if SETTINGS.render_start_delay:log.info("Render previous instance wait %ss",SETTINGS.render_start_delay);time.sleep(SETTINGS.render_start_delay)
    APP.start()
if __name__=="__main__":
    threading.Thread(target=delayed_start,daemon=True,name="app-initializer").start();web.run(host="0.0.0.0",port=SETTINGS.port,threaded=True,use_reloader=False)
