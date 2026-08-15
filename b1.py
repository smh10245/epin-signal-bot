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
import gc
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from nextday_engine import NextDayAnalyzer, render_text as render_nextday_text
from gemini_advisor import GeminiAdvisor

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
logging.getLogger("yfinance").setLevel(logging.CRITICAL)


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



def kst_dt(dt: datetime) -> datetime:
    """Return a timezone-aware Asia/Seoul datetime."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=KST)
    return dt.astimezone(KST)


def format_kst_korean(dt: datetime) -> str:
    dt=kst_dt(dt)
    weekdays=["월","화","수","목","금","토","일"]
    ampm="오전" if dt.hour<12 else "오후"
    hour12=dt.hour%12 or 12
    return f"{dt.year}년 {dt.month}월 {dt.day}일({weekdays[dt.weekday()]}) {ampm} {hour12}시 {dt.minute:02d}분"


def signal_grade(score: float) -> str:
    s=num(score)
    if s>=88:return "A+"
    if s>=82:return "A"
    if s>=75:return "B+"
    if s>=68:return "B"
    return "C"


def normalize_intraday_bars(rows, trading_date=None):
    """Sort/dedupe minute bars and keep only one KST trading day.

    Duplicate minute bars can happen when REST history overlaps with the live NXT bar.
    We merge OHLC conservatively and use the larger minute volume to avoid double counting.
    """
    trading_date=trading_date or now().date()
    merged={}
    for b in rows or []:
        try:
            m=kst_dt(b.minute).replace(second=0,microsecond=0)
            if m.date()!=trading_date:continue
            # NXT session boundary; protects against malformed/previous-day timestamps.
            minutes=m.hour*60+m.minute
            if minutes < 8*60 or minutes > 20*60:continue
            candidate=Bar(m,num(b.open),num(b.high),num(b.low),num(b.close),max(0,integer(b.volume)),max(0,integer(b.cumulative_volume)),num(b.trade_strength))
            old=merged.get(m)
            if old is None:
                merged[m]=candidate
            else:
                merged[m]=Bar(
                    m,
                    old.open if old.open>0 else candidate.open,
                    max(old.high,candidate.high),
                    min(x for x in (old.low,candidate.low) if x>0) if old.low>0 and candidate.low>0 else max(old.low,candidate.low),
                    candidate.close if candidate.close>0 else old.close,
                    max(old.volume,candidate.volume),
                    max(old.cumulative_volume,candidate.cumulative_volume),
                    candidate.trade_strength if candidate.trade_strength>0 else old.trade_strength,
                )
        except Exception:
            continue
    return [merged[k] for k in sorted(merged)]

@dataclass(frozen=True)
class Settings:
    version: str = "5.4.6 Myeongha + Gemini Advisor"
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
    # 상승 지속형 타점
    momentum_strength_min: float = env_float("MOMENTUM_STRENGTH_MIN", 105.0)
    breakout_strength_min: float = env_float("BREAKOUT_STRENGTH_MIN", 112.0)
    breakout_orderbook_min: float = env_float("BREAKOUT_ORDERBOOK_MIN", 1.00)
    breakout_volume_ratio: float = env_float("BREAKOUT_VOLUME_RATIO", 1.25)
    breakout_vwap_max_pct: float = env_float("BREAKOUT_VWAP_MAX_PCT", 2.2)
    breakout_extension_max_pct: float = env_float("BREAKOUT_EXTENSION_MAX_PCT", 0.8)
    # NaMu/NH domestic-stock estimated costs.
    # Account/event/channel fees differ, so Render env values can override these defaults.
    namuh_buy_fee_pct: float = env_float("NAMUH_BUY_FEE_PCT", 0.01)
    namuh_sell_fee_pct: float = env_float("NAMUH_SELL_FEE_PCT", 0.01)
    domestic_sell_tax_pct: float = env_float("DOMESTIC_SELL_TAX_PCT", 0.15)
    miniapp_url: str = os.getenv("MINIAPP_URL", "https://epin-signal-bot.onrender.com/miniapp").strip()
    miniapp_require_auth: bool = env_bool("MINIAPP_REQUIRE_AUTH", True)
    # Integrated final: signal quality / anti-chase / data-health
    reentry_score_min: float = env_float("REENTRY_SCORE_MIN", 68.0)
    second_wave_score_min: float = env_float("SECOND_WAVE_SCORE_MIN", 78.0)
    second_wave_pullback_min_pct: float = env_float("SECOND_WAVE_PULLBACK_MIN_PCT", 0.6)
    second_wave_pullback_max_pct: float = env_float("SECOND_WAVE_PULLBACK_MAX_PCT", 3.5)
    second_wave_recovery_min_pct: float = env_float("SECOND_WAVE_RECOVERY_MIN_PCT", 0.5)
    chase_block_day_gain_pct: float = env_float("CHASE_BLOCK_DAY_GAIN_PCT", 4.5)
    chase_block_high_gap_pct: float = env_float("CHASE_BLOCK_HIGH_GAP_PCT", 0.5)
    signal_max_price_drift_pct: float = env_float("SIGNAL_MAX_PRICE_DRIFT_PCT", 0.8)
    signal_result_minutes: int = env_int("SIGNAL_RESULT_MINUTES", 120)
    stale_tick_seconds: int = env_int("STALE_TICK_SECONDS", 90)
    stale_orderbook_seconds: int = env_int("STALE_ORDERBOOK_SECONDS", 90)
    # Manual V-pattern finder. Blocked during weekday market hours to protect the live engine.
    finder_min_price: float = env_float("FINDER_MIN_PRICE", 20000.0)
    finder_days: int = env_int("FINDER_DAYS", 90)
    finder_min_turnover: float = env_float("FINDER_MIN_TURNOVER", 2_000_000_000.0)
    finder_min_touches: int = env_int("FINDER_MIN_TOUCHES", 3)
    finder_min_rebound_pct: float = env_float("FINDER_MIN_REBOUND_PCT", 5.0)
    finder_result_limit: int = env_int("FINDER_RESULT_LIMIT", 10)
    finder_block_start: str = os.getenv("FINDER_BLOCK_START", "07:50")
    finder_block_end: str = os.getenv("FINDER_BLOCK_END", "20:10")
    # Finder low-load guardrails
    finder_chunk_size: int = env_int("FINDER_CHUNK_SIZE", 12)
    finder_pause_seconds: float = env_float("FINDER_PAUSE_SECONDS", 1.5)
    finder_max_seconds: int = env_int("FINDER_MAX_SECONDS", 1200)
    finder_max_rss_mb: int = env_int("FINDER_MAX_RSS_MB", 360)
    finder_progress_every: int = env_int("FINDER_PROGRESS_EVERY", 240)
    # Optional durable state. If configured, VIP survives Render instance replacement.
    supabase_url: str = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
    supabase_key: str = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
    supabase_state_table: str = os.getenv("SUPABASE_STATE_TABLE", "bot_state").strip()
    # Next-day scenario engine (manual, post-market only)
    nextday_block_start: str = os.getenv("NEXTDAY_BLOCK_START", "07:50")
    nextday_block_end: str = os.getenv("NEXTDAY_BLOCK_END", "20:10")
    nextday_min_daily_bars: int = env_int("NEXTDAY_MIN_DAILY_BARS", 40)
    nextday_min_30m_bars: int = env_int("NEXTDAY_MIN_30M_BARS", 8)
    # Optional Gemini free-tier advisor. Rule engine remains primary/fallback.
    gemini_api_key: str = os.getenv("GEMINI_API_KEY", "").strip()
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview").strip()
    gemini_timeout_seconds: int = env_int("GEMINI_TIMEOUT_SECONDS", 25)

SETTINGS = Settings()
DATA_DIR = Path(SETTINGS.data_dir)

ST_WAIT="WAIT"; ST_PULLBACK="PULLBACK"; ST_READY="READY"; ST_MOMENTUM="MOMENTUM"; ST_SIGNAL="SIGNAL"; ST_BREAKOUT="BREAKOUT"; ST_HOLD="HOLD"; ST_COOLDOWN="COOLDOWN"
STATE_LABEL={ST_WAIT:"⚪ 대기",ST_PULLBACK:"🟡 눌림관찰",ST_READY:"🟠 재진입준비",ST_MOMENTUM:"🔵 상승초입",ST_SIGNAL:"🟢 재진입타점",ST_BREAKOUT:"🟣 2차상승확인",ST_HOLD:"💼 보유관리",ST_COOLDOWN:"⏳ 쿨다운"}

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
    signal_price:float=0.0; signal_kind:str=""; signal_grade:str="C"
    last_signal_at:Optional[str]=None; cooldown_until:Optional[str]=None; stage_changed_at:Optional[str]=None
    signal_count_today:int=0; pass_count_today:int=0; note:str=""

@dataclass
class Position:
    code:str; name:str; entry:float; qty:float; highest:float; stop:float; level:int=0
    stop_alerted:bool=False; risk_alerted:bool=False; opened_at:str=field(default_factory=lambda: now().isoformat())

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
        self.vip=DATA_DIR/"vip_targets.json";self.vip_bak=DATA_DIR/"vip_targets.backup.json";self.pos=DATA_DIR/"positions.json";self.sig=DATA_DIR/"signal_history.json";self.trade=DATA_DIR/"trade_history.json";self.master=DATA_DIR/"stock_master.json";self.nextday=DATA_DIR/"nextday_predictions.json"
    def read(self,p,default):
        try:
            if p.exists():return json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:log.warning("load %s failed: %s",p,e)
        return default
    def write(self,p,v):WORKER.submit(atomic_json_write,p,v)

    def _sb_enabled(self):
        return bool(SETTINGS.supabase_url and SETTINGS.supabase_key and SETTINGS.supabase_state_table)
    def _sb_headers(self):
        return {"apikey":SETTINGS.supabase_key,"authorization":f"Bearer {SETTINGS.supabase_key}","content-type":"application/json"}
    def _sb_load(self,key):
        if not self._sb_enabled():return None
        try:
            url=f"{SETTINGS.supabase_url}/rest/v1/{SETTINGS.supabase_state_table}"
            r=requests.get(url,headers=self._sb_headers(),params={"select":"value","key":f"eq.{key}","limit":"1"},timeout=8)
            if r.status_code>=400:
                log.warning("Supabase state load %s failed: %s %s",key,r.status_code,r.text[:160]);return None
            rows=r.json() or []
            return rows[0].get("value") if rows and isinstance(rows[0],dict) else None
        except Exception as e:
            log.warning("Supabase state load %s failed: %s",key,e);return None
    def _sb_save(self,key,value):
        if not self._sb_enabled():return False
        try:
            url=f"{SETTINGS.supabase_url}/rest/v1/{SETTINGS.supabase_state_table}"
            headers=self._sb_headers();headers["Prefer"]="resolution=merge-duplicates,return=minimal"
            r=requests.post(url,headers=headers,json={"key":key,"value":value},timeout=8)
            if r.status_code>=400:
                log.warning("Supabase state save %s failed: %s %s",key,r.status_code,r.text[:160]);return False
            return True
        except Exception as e:
            log.warning("Supabase state save %s failed: %s",key,e);return False

    def load_vip(self):
        default={"limit":min(5,SETTINGS.vip_hard_max),"targets":{"119850":"지엔씨에너지"}}
        remote=self._sb_load("vip_targets")
        if isinstance(remote,dict) and isinstance(remote.get("targets"),dict) and remote.get("targets"):
            try:
                atomic_json_write(self.vip,remote);atomic_json_write(self.vip_bak,remote)
            except Exception as e:log.warning("VIP local mirror write failed: %s",e)
            log.info("VIP restored from durable state: %s targets",len(remote.get("targets") or {}))
            return remote
        primary=self.read(self.vip,None)
        if isinstance(primary,dict) and isinstance(primary.get("targets"),dict) and primary.get("targets"):return primary
        backup=self.read(self.vip_bak,None)
        if isinstance(backup,dict) and isinstance(backup.get("targets"),dict) and backup.get("targets"):
            log.warning("VIP primary missing/invalid - restored from local backup")
            try:atomic_json_write(self.vip,backup)
            except Exception as e:log.warning("VIP primary restore failed: %s",e)
            return backup
        return default
    def save_vip(self,limit,targets):
        payload={"limit":limit,"targets":dict(targets)}
        atomic_json_write(self.vip,payload);atomic_json_write(self.vip_bak,payload)
        if self._sb_enabled():self._sb_save("vip_targets",payload)

    def load_positions(self):return self.read(self.pos,{})
    def save_positions(self,v):self.write(self.pos,v)
    def load_signals(self):return self.read(self.sig,[])
    def save_signals(self,v):self.write(self.sig,v[-1000:])
    def load_trades(self):return self.read(self.trade,[])
    def save_trades(self,v):self.write(self.trade,v[-1000:])

    def load_nextday_predictions(self):
        remote=self._sb_load("nextday_predictions")
        if isinstance(remote,list):
            try:atomic_json_write(self.nextday,remote)
            except Exception as e:log.warning("nextday local mirror failed: %s",e)
            return remote
        return self.read(self.nextday,[])

    def save_nextday_predictions(self,v):
        rows=list(v or [])[-1000:]
        atomic_json_write(self.nextday,rows)
        if self._sb_enabled():self._sb_save("nextday_predictions",rows)

    def load_master(self):
        raw=self.read(self.master,{})
        if isinstance(raw,dict) and isinstance(raw.get("rows"),dict):return raw
        if isinstance(raw,dict) and raw:return {"rows":raw,"markets":{}}
        return {"rows":{},"markets":{}}
    def save_master(self,rows,markets):
        self.write(self.master,{"rows":dict(rows),"markets":dict(markets)})

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
    SOURCES=(
        ("https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip","kospi_code.mst",228,"KOSPI"),
        ("https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip","kosdaq_code.mst",222,"KOSDAQ"),
    )
    def __init__(self):self.code_to_name={};self.name_to_code={};self.code_market={}
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
        rows={};markets={}
        for url,fname,tail,market in self.SOURCES:
            try:
                r=requests.get(url,timeout=30,headers={"User-Agent":"Mozilla/5.0"});r.raise_for_status()
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    target=fname if fname in z.namelist() else next((x for x in z.namelist() if x.endswith(".mst")),None)
                    if not target:raise RuntimeError("mst not found")
                    parsed=self._parse(z.read(target),tail);rows.update(parsed)
                    for c in parsed:markets[c]=market
            except Exception as e:log.warning("master source failed: %s",e)
        if rows:
            STORE.save_master(rows,markets)
        else:
            cached=STORE.load_master();rows=dict(cached.get("rows") or {});markets=dict(cached.get("markets") or {})
            if rows and not markets:log.warning("master cache has names but no market mapping; finder waits for master refresh")
        if not rows:raise RuntimeError("KIS master unavailable")
        self.code_to_name=rows;self.code_market=markets;self.name_to_code={self._norm(n):c for c,n in rows.items()}
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
        self.market_risk="NORMAL";self.focus_codes=[];self.runtime={"ws":"stopped","trade_subscribed":0,"order_subscribed":0,"last_error":None,"last_tick":None,"ready":False,"boot_phase":"created","stale_targets":0};self.pending_input={};self.signals=STORE.load_signals();self.trades=STORE.load_trades()
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
        """보유 종목의 매도 타이밍을 고정 수익률이 아니라 실시간 수급/추세 훼손으로 판단한다.
        자동매도는 하지 않고 텔레그램 경고와 동적 방어선만 갱신한다.
        """
        p=self.data.get(c);out=[]
        if not p:return out

        p.highest=max(p.highest,current)
        gain=pct(current,p.entry)
        drawdown=pct(current,p.highest) if p.highest else 0.0

        # Brain이 이미 계산하는 실시간 지표를 그대로 재사용한다.
        # 데이터가 아직 부족하면 기존 방어선만 유지한다.
        try:m=Brain.metrics(c) or {}
        except Exception:m={}
        vwap=num(m.get("vwap"));support=num(m.get("support"));resistance=num(m.get("resistance"))
        strength=num(m.get("strength"));vr=num(m.get("volume_ratio"));imb=num(m.get("imbalance"),1.0)
        score=num(m.get("score"),50.0);ma5d=num(m.get("ma5d"));ma20d=num(m.get("ma20d"))

        # 1) 수익 보호선: 수익률 숫자 하나로 익절하지 않고 고점/지지/VWAP를 따라 올라간다.
        dynamic_stop=p.stop
        if gain>0:
            # 수익권에서는 최소한 매수가 부근까지 방어선을 끌어올린다.
            dynamic_stop=max(dynamic_stop,p.entry*1.001)
            if vwap>0 and vwap<current:dynamic_stop=max(dynamic_stop,vwap*0.995)
            if support>0 and support<current:dynamic_stop=max(dynamic_stop,support*0.995)
            # 수익이 커질수록 고점 추적폭을 자연스럽게 좁힌다.
            trail_pct=3.0 if gain<3 else 2.2 if gain<6 else 1.6
            dynamic_stop=max(dynamic_stop,p.highest*(1-trail_pct/100.0))
        p.stop=max(p.stop,dynamic_stop)

        # 2) 매도 압력 점수: 한 가지 조건이 아니라 추세+수급+호가+고점이탈을 합산한다.
        exit_score=0;reasons=[]
        if vwap>0 and current<vwap:
            exit_score+=2;reasons.append("VWAP 이탈")
        if support>0 and current<support:
            exit_score+=3;reasons.append("단기 지지선 이탈")
        if strength>0 and strength<90:
            exit_score+=2;reasons.append(f"체결강도 약화({strength:.0f})")
        elif strength>0 and strength<100:
            exit_score+=1;reasons.append(f"체결강도 둔화({strength:.0f})")
        if imb>0 and imb<0.80:
            exit_score+=2;reasons.append(f"매도호가 우세({imb:.2f})")
        elif imb>0 and imb<0.95:
            exit_score+=1;reasons.append(f"호가 약화({imb:.2f})")
        if vr>=1.35 and current<p.highest*0.99:
            exit_score+=2;reasons.append("하락 거래량 증가")
        if drawdown<=-2.0:
            exit_score+=2;reasons.append(f"고점대비 {drawdown:.1f}% 하락")
        elif drawdown<=-1.2:
            exit_score+=1;reasons.append(f"고점대비 {drawdown:.1f}% 밀림")
        if score<45:
            exit_score+=2;reasons.append(f"종합점수 약화({score:.0f})")
        if ma5d>0 and ma20d>0 and ma5d<ma20d:
            exit_score+=1;reasons.append("일봉 추세 약세")

        # 3) 단계 알림: +3%, +6% 도달 자체가 아니라 실제 매도압력으로 단계가 바뀐다.
        if gain>0 and exit_score>=5 and p.level<2:
            p.level=2
            out.append(
                f"🔴 <b>[익절 우선 검토] {html.escape(p.name)}</b>\n"
                f"현재 {current:,.0f}원 · 수익률 {gain:+.2f}% · 고점대비 {drawdown:+.2f}%\n"
                f"매도압력 {exit_score}점 · {' / '.join(reasons[:4])}\n"
                f"동적 방어선 {p.stop:,.0f}원\n자동주문 없음 · 분할익절/매도 판단 권고"
            )
        elif gain>0 and exit_score>=3 and p.level<1:
            p.level=1
            out.append(
                f"🟠 <b>[익절 경계] {html.escape(p.name)}</b>\n"
                f"현재 {current:,.0f}원 · 수익률 {gain:+.2f}% · 고점대비 {drawdown:+.2f}%\n"
                f"매도압력 {exit_score}점 · {' / '.join(reasons[:3])}\n"
                f"아직 즉시 매도 신호는 아님 · 방어선 {p.stop:,.0f}원"
            )

        # 손실권은 고정 -몇%만 기다리지 않고 복합 약화를 먼저 경고한다.
        if gain<=0 and exit_score>=4 and not p.risk_alerted:
            p.risk_alerted=True
            out.append(
                f"🟠 <b>[손절 경계] {html.escape(p.name)}</b>\n"
                f"현재 {current:,.0f}원 · 수익률 {gain:+.2f}% · 매도압력 {exit_score}점\n"
                f"{' / '.join(reasons[:4])}\n동적 방어선 {p.stop:,.0f}원 · 반등 실패 시 손절 우선 검토"
            )
        elif gain>0 or exit_score<=2:
            p.risk_alerted=False

        # 4) 최종 매도 권고: 동적 방어선 이탈 또는 강한 복합 약화.
        hard_exit=(current<=p.stop) or (exit_score>=7 and gain>0) or (exit_score>=7 and gain<=0 and current<p.entry*0.995)
        if hard_exit and not p.stop_alerted:
            p.stop_alerted=True
            why=" / ".join(reasons[:5]) if reasons else "동적 방어선 이탈"
            out.append(
                f"🛑 <b>[매도 권고] {html.escape(p.name)}</b>\n"
                f"현재 {current:,.0f}원 ({gain:+.2f}%) · 고점 {p.highest:,.0f}원\n"
                f"동적 방어선 {p.stop:,.0f}원 · 매도압력 {exit_score}점\n"
                f"근거: {why}\n자동주문 없음"
            )

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
            r=self.s.get(f"{self.rest}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",headers={"authorization":f"Bearer {self.auth()}","appkey":SETTINGS.kis_app_key,"appsecret":SETTINGS.kis_app_secret,"tr_id":"FHKST03010200","custtype":"P"},params={"FID_ETC_CLS_CODE":"","FID_COND_MRKT_DIV_CODE":"J","FID_INPUT_ISCD":c,"FID_INPUT_HOUR_1":"153000","FID_PW_DATA_INCU_YN":"N"},timeout=15);r.raise_for_status();return r.json().get("output2") or []
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
        base=list(STATE.bars.get(c,[]));cur=STATE.current.get(c)
        if cur:base.append(cur)
        bars=normalize_intraday_bars(base)
        if len(bars)<20:return None

        r=bars[-60:]  # 타점 계산은 최근 60분 중심. 화면 차트는 당일 전체.
        latest=r[-1]
        completed=r[:-1] if cur is not None and latest.minute==kst_dt(cur.minute).replace(second=0,microsecond=0) else r
        vols=[max(0,b.volume) for b in r]
        tot=sum(vols)
        if tot<=0:return None

        vwap=sum(((b.high+b.low+b.close)/3)*max(0,b.volume) for b in r)/tot
        prior=vols[-15:-3];recent=vols[-3:]
        pa=sum(prior)/len(prior) if prior else 0;ra=sum(recent)/len(recent) if recent else 0
        vr=ra/pa if pa else 0

        highs=[b.high for b in r];lows=[b.low for b in r]
        high20=max(highs[-20:-1]) if len(highs)>=2 else latest.high
        low12=min(lows[-12:]);support=max(vwap*0.995,low12);resistance=high20
        pull=pct(latest.close,high20)
        prev3=max(highs[-4:-1]) if len(highs)>=4 else latest.high
        br=latest.close>=prev3

        closes=[b.close for b in r];hh_hl=False;rising_closes=False
        if len(r)>=7:
            prev_hi=max(b.high for b in r[-7:-4]);curr_hi=max(b.high for b in r[-4:-1])
            prev_lo=min(b.low for b in r[-7:-4]);curr_lo=min(b.low for b in r[-4:-1])
            hh_hl=(curr_hi>prev_hi and curr_lo>=prev_lo)
            rising_closes=(closes[-1]>closes[-2] and closes[-2]>=closes[-3])
        vwap_gap=pct(latest.close,vwap) if vwap else 0.0
        breakout_extension=pct(latest.close,prev3) if prev3 else 0.0

        # 당일 전체 맥락: 고점 추격 여부와 2차 파동 구조 판정에 사용.
        day_high=max(b.high for b in bars);day_low=min(b.low for b in bars)
        day_open=bars[0].open;day_gain=pct(latest.close,day_open) if day_open else 0.0
        day_high_gap=pct(latest.close,day_high) if day_high else 0.0
        recent_peak=max((b.high for b in r[-30:-6]),default=0.0) if len(r)>=12 else 0.0
        recent_dip=min((b.low for b in r[-6:-1]),default=0.0) if len(r)>=7 else 0.0
        wave_pullback=pct(recent_dip,recent_peak) if recent_peak and recent_dip else 0.0
        wave_recovery=pct(latest.close,recent_dip) if recent_dip else 0.0
        chase_risk=bool(day_gain>=SETTINGS.chase_block_day_gain_pct and day_high_gap>=-SETTINGS.chase_block_high_gap_pct)

        elapsed=60;projected=latest.volume;projected_ratio=0.0
        if cur is not None and latest.minute==kst_dt(cur.minute).replace(second=0,microsecond=0):
            tick_at=STATE.last_tick_at.get(c) or now();elapsed=max(1,int((kst_dt(tick_at)-latest.minute).total_seconds())+1)
            baseline=Brain._median([b.volume for b in completed[-5:]])
            if elapsed>=SETTINGS.projected_volume_min_seconds and baseline>0:
                projected=latest.volume*(60.0/min(60,elapsed));projected_ratio=projected/baseline
        else:
            baseline=Brain._median([b.volume for b in completed[-6:-1]])
            if baseline>0:projected_ratio=latest.volume/baseline

        ob=STATE.books.get(c);order_age=(now()-ob.updated_at).total_seconds() if ob else None
        imb=ob.imbalance if ob and order_age is not None and order_age<SETTINGS.stale_orderbook_seconds else 1.0

        dt=STATE.daily_trend.get(c) or {};ma5d=num(dt.get("ma5"));ma20d=num(dt.get("ma20"))
        prev_ma5=num(dt.get("prev_ma5"));prev_ma20=num(dt.get("prev_ma20"))
        daily_cross=bool(ma5d and ma20d and prev_ma5 and prev_ma20 and prev_ma5<=prev_ma20 and ma5d>ma20d)
        daily_bull=bool(ma5d and ma20d and ma5d>=ma20d)
        daily_pullback=bool(ma20d and ma20d*0.99<=latest.close<=ma20d*1.02)
        daily_known=bool(ma5d and ma20d)
        daily_trend="GOLDEN_CROSS" if daily_cross else "BULL" if daily_bull else "MA20_PULLBACK" if daily_pullback else "BEAR" if daily_known else "UNKNOWN"
        volume_burst=projected_ratio>=SETTINGS.projected_volume_ratio or vr>=1.05

        score=50.0
        score+=clamp((latest.trade_strength-95)*0.8,-15,20)
        score+=(10 if -0.8<=pct(latest.close,vwap)<=1.2 else -5)
        score+=(12 if -4.5<=pull<=-0.6 else -5)
        score+=(8 if vr<=0.85 else 0);score+=(12 if br else 0)
        score+=clamp((imb-1)*12,-8,10);score+=(10 if projected_ratio>=SETTINGS.projected_volume_ratio else 0)
        score+=(8 if daily_cross else 5 if daily_bull else 4 if daily_pullback else -4 if daily_known else 0)
        if chase_risk:score-=18
        score=clamp(score)

        return {
            "price":latest.close,"vwap":vwap,"support":support,"resistance":resistance,
            "pullback_pct":pull,"volume_ratio":vr,"strength":latest.trade_strength,
            "short_break":br,"imbalance":imb,"score":score,"grade":signal_grade(score),"today_open":day_open,
            "projected_volume":projected,"projected_volume_ratio":projected_ratio,
            "volume_burst":volume_burst,"elapsed_seconds":elapsed,
            "hh_hl":hh_hl,"rising_closes":rising_closes,"vwap_gap_pct":vwap_gap,
            "breakout_extension_pct":breakout_extension,"day_high":day_high,"day_low":day_low,
            "day_gain_pct":day_gain,"day_high_gap_pct":day_high_gap,"chase_risk":chase_risk,
            "wave_pullback_pct":wave_pullback,"wave_recovery_pct":wave_recovery,"recent_peak":recent_peak,
            "ma5d":ma5d,"ma20d":ma20d,"daily_cross":daily_cross,"daily_bull":daily_bull,
            "daily_pullback":daily_pullback,"daily_known":daily_known,"daily_trend":daily_trend,
            "orderbook_age":order_age,"bar_count_day":len(bars),
        }

    def evaluate(self,c):
        t=STATE.targets.get(c);m=self.metrics(c)
        if not t or not m:return None,m,"분봉 수집중"
        for k,a in (("last_price","price"),("vwap","vwap"),("support","support"),("resistance","resistance"),("trade_strength","strength"),("volume_ratio","volume_ratio"),("pullback_pct","pullback_pct"),("score","score"),("projected_volume_ratio","projected_volume_ratio"),("ma5d","ma5d"),("ma20d","ma20d"),("daily_trend","daily_trend"),("signal_grade","grade")):
            setattr(t,k,m[a])
        if c in POSITIONS.data:return ST_HOLD,m,"보유중"
        if not STATE.runtime.get("ready"):return ST_WAIT,m,"초기 데이터 준비중"

        if t.stage==ST_COOLDOWN:
            try:
                if t.cooldown_until and now()<datetime.fromisoformat(t.cooldown_until):return ST_COOLDOWN,m,"쿨다운"
            except Exception:pass
            t.cooldown_until=None;return ST_WAIT,m,"쿨다운 종료"

        # 타점은 시간뿐 아니라 가격 이탈/지지 훼손 시 즉시 만료한다.
        if t.stage in (ST_SIGNAL,ST_BREAKOUT) and t.last_signal_at:
            try:
                age=now()-datetime.fromisoformat(t.last_signal_at)
                drift=pct(m["price"],t.signal_price) if t.signal_price else 0.0
                broken=(m["support"]>0 and m["price"]<m["support"]*0.995)
                if age>timedelta(minutes=SETTINGS.signal_expire_minutes) or drift>SETTINGS.signal_max_price_drift_pct or broken:
                    why="타점 만료"
                    if drift>SETTINGS.signal_max_price_drift_pct:why="타점 가격 이탈 - 추격 금지"
                    elif broken:why="지지 훼손 - 타점 무효"
                    t.cooldown_until=(now()+timedelta(minutes=SETTINGS.cooldown_minutes)).isoformat()
                    return ST_COOLDOWN,m,why
            except Exception:pass

        near=abs(pct(m["price"],m["support"]))<=1.1 or abs(pct(m["price"],m["vwap"]))<=1.2
        healthy=-4.5<=m["pullback_pct"]<=-0.6;dry=m["volume_ratio"]<=0.85
        pullback=near and healthy and dry and m["price"]>=m["today_open"]*0.985
        daily_context=(not m["daily_known"]) or m["daily_bull"] or m["daily_cross"] or m["daily_pullback"]
        reentry=(m["price"]>=m["vwap"]*0.998 and m["strength"]>=SETTINGS.signal_strength_min and m["short_break"] and m["volume_burst"] and m["imbalance"]>=SETTINGS.signal_orderbook_min and daily_context and m["score"]>=SETTINGS.reentry_score_min and not m["chase_risk"])

        # 상승구간은 직접 추격하지 않는다. 반드시 '상승 -> 눌림 -> 지지 -> 재가속'이 확인된 2차 파동만 보조신호.
        pb_abs=-m["wave_pullback_pct"]
        second_wave=(
            m["recent_peak"]>0 and SETTINGS.second_wave_pullback_min_pct<=pb_abs<=SETTINGS.second_wave_pullback_max_pct
            and m["wave_recovery_pct"]>=SETTINGS.second_wave_recovery_min_pct
            and m["price"]>=m["vwap"]*0.998 and m["short_break"]
            and m["strength"]>=SETTINGS.breakout_strength_min and m["volume_burst"]
            and m["imbalance"]>=SETTINGS.breakout_orderbook_min and daily_context
            and m["score"]>=SETTINGS.second_wave_score_min and not m["chase_risk"]
            and m["vwap_gap_pct"]<=min(1.6,SETTINGS.breakout_vwap_max_pct)
            and m["price"]<=m["recent_peak"]*1.003
        )
        momentum=(m["price"]>m["vwap"] and m["hh_hl"] and m["rising_closes"] and m["strength"]>=SETTINGS.momentum_strength_min and daily_context and not m["chase_risk"])

        if STATE.market_risk=="RISK":return (ST_PULLBACK if pullback else ST_WAIT),m,"시장 RISK - 신규진입 보류"
        if m["chase_risk"]:return ST_WAIT,m,f"고점추격 차단 · 당일 {m['day_gain_pct']:+.1f}% · 고점이격 {m['day_high_gap_pct']:+.1f}%"

        if t.stage in (ST_PULLBACK,ST_READY) and reentry:
            if STATE.market_risk=="CAUTION" and m["score"]<78:return ST_READY,m,"시장 CAUTION - 강한 확인 대기"
            reason=f"체결강도 + 단기돌파 + 거래량 확인 · 타점 {m['score']:.0f}점({m['grade']})"
            if m["daily_cross"]:reason+=" + 일봉 골든크로스"
            elif m["daily_pullback"]:reason+=" + 20일선 눌림"
            elif m["daily_bull"]:reason+=" + 일봉 정배열"
            return ST_SIGNAL,m,reason
        if second_wave:
            return ST_BREAKOUT,m,f"눌림 {m['wave_pullback_pct']:.1f}% 후 재가속 · 타점 {m['score']:.0f}점({m['grade']})"
        if pullback:
            return (ST_READY if m["strength"]>=100 else ST_PULLBACK),m,("지지구간 + 매수세 회복" if m["strength"]>=100 else "지지구간 접근 + 거래량 감소")
        if momentum:return ST_MOMENTUM,m,f"상승추세 관찰 · 아직 진입신호 아님 · 타점 {m['score']:.0f}점"

        # 사용자가 관찰로 올렸어도 추세가 명확히 무너지면 목록 삭제 없이 대기 상태로만 되돌린다.
        if t.stage in (ST_READY,ST_MOMENTUM) and m["price"]<m["vwap"]*0.985 and m["strength"]<90:
            return ST_WAIT,m,"관찰조건 약화 - VIP는 유지"
        return ST_WAIT,m,"조건 대기"

BRAIN=Brain()

def _fetch_google_news(q,limit=6):
    """Latest Google News RSS items: 7 days first, then fill from 30 days; KST + dedupe."""
    try:
        url=f"https://news.google.com/rss/search?q={urllib.parse.quote(q)}&hl=ko&gl=KR&ceid=KR:ko"
        r=requests.get(url,timeout=10,headers={"User-Agent":"Mozilla/5.0"});r.raise_for_status();root=ET.fromstring(r.text)
        items=[];seen=set();cut30=now()-timedelta(days=30);cut7=now()-timedelta(days=7)
        for i in root.findall(".//item")[:40]:
            title=(i.findtext("title") or "").strip();link=(i.findtext("link") or "").strip();raw=(i.findtext("pubDate") or "").strip()
            if not title:continue
            key=re.sub(r"\\s+"," ",title).strip().lower()
            if key in seen:continue
            try:
                dt=parsedate_to_datetime(raw) if raw else None
                if dt is None:continue
                dt=kst_dt(dt)
            except Exception:continue
            if dt<cut30:continue
            seen.add(key);items.append({"title":title,"published":format_kst_korean(dt),"time_text":format_kst_korean(dt),"link":link,"_dt":dt})
        items.sort(key=lambda x:x["_dt"],reverse=True)
        recent=[x for x in items if x["_dt"]>=cut7]
        chosen=recent[:limit]
        if len(chosen)<limit:
            chosen+= [x for x in items if x not in chosen][:limit-len(chosen)]
        for x in chosen:x.pop("_dt",None)
        return chosen
    except Exception as e:log.warning("Google news fetch failed: %s",e);return []

def google_news_headlines(q,limit=4):return _fetch_google_news(q,limit)

def us_market_summary():
    """장전 브리핑용 미국 주요지수 요약. 실패해도 브리핑 전체를 막지 않는다."""
    if not HAS_YF:
        return "🇺🇸 미국시장: 데이터 모듈 사용불가"
    out=[]
    try:
        for name,symbol in (("S&P500","^GSPC"),("나스닥","^IXIC"),("다우","^DJI")):
            try:
                d=yf.Ticker(symbol).history(period="7d",interval="1d",auto_adjust=False)
                if d is None or len(d)<2:continue
                prev=float(d["Close"].iloc[-2]);cur=float(d["Close"].iloc[-1])
                if prev>0:out.append(f"{name} {pct(cur,prev):+.2f}%")
            except Exception:
                continue
    except Exception:
        pass
    return "🇺🇸 미국시장: "+(" · ".join(out) if out else "조회 실패")


class App:
    def __init__(self):
        self.started=False;self.start_lock=threading.Lock();self.today="";self.stage_alert={};self.brief_sent={};self.last_daily_refresh={};self.last_outcome_check=0.0
        self.daily_price_structure={}
        self.finder_lock=threading.Lock();self.finder_running=False;self.finder_progress="대기";self.finder_last_results=[];self.finder_last_at=None
        self.nextday_lock=threading.Lock()
        self.nextday_running=set()
        self.nextday_cache={}
        self.nextday_analyzer=NextDayAnalyzer(
            min_daily_bars=SETTINGS.nextday_min_daily_bars,
            min_30m_bars=SETTINGS.nextday_min_30m_bars,
        )
        self.nextday_predictions=STORE.load_nextday_predictions()
        self.nextday_last_eval=0.0
        self.gemini_advisor=GeminiAdvisor(
            api_key=SETTINGS.gemini_api_key,
            model=SETTINGS.gemini_model,
            timeout=SETTINGS.gemini_timeout_seconds,
        )
    def sync_bars_for(self,c):
        raw=KIS_CLIENT.minute_bars(c);rows=[];n=now();current_minute=n.replace(second=0,microsecond=0)
        for r in raw:
            try:
                h=str(r.get("stck_cntg_hour") or "").zfill(6)
                m=n.replace(hour=int(h[:2]),minute=int(h[2:4]),second=0,microsecond=0)
                # REST의 진행 중 현재분은 NXT current와 겹칠 수 있어 제외한다.
                if m>=current_minute:continue
                rows.append(Bar(m,num(r.get("stck_oprc")),num(r.get("stck_hgpr")),num(r.get("stck_lwpr")),num(r.get("stck_prpr")),integer(r.get("cntg_vol")),integer(r.get("acml_vol")),100))
            except Exception:pass
        rows=normalize_intraday_bars(rows,n.date())
        if rows:
            with STATE.lock:STATE.bars[c]=deque(rows,maxlen=390)
    def sync_daily_for(self,c,force=False):
        last=self.last_daily_refresh.get(c)
        if not force and last and now()-last<timedelta(minutes=SETTINGS.daily_ma_refresh_minutes):
            return
        raw=KIS_CLIENT.daily_bars(c,max(60,SETTINGS.finder_days))
        rows=[]
        for r in raw:
            try:
                d=str(r.get("stck_bsop_date") or "")
                cp=num(r.get("stck_clpr"));op=num(r.get("stck_oprc"));hi=num(r.get("stck_hgpr"));lo=num(r.get("stck_lwpr"));vol=integer(r.get("acml_vol"))
                if d and cp>0 and hi>0 and lo>0:
                    rows.append({"date":d,"open":op or cp,"high":hi,"low":lo,"close":cp,"volume":max(0,vol)})
            except Exception:
                pass
        rows.sort(key=lambda x:x["date"])
        closes=[x["close"] for x in rows]
        if len(closes)<20:
            log.info("daily MA pending: %s %s bars",c,len(closes));return
        ma5=sum(closes[-5:])/5
        ma20=sum(closes[-20:])/20
        prev_ma5=sum(closes[-6:-1])/5 if len(closes)>=21 else ma5
        prev_ma20=sum(closes[-21:-1])/20 if len(closes)>=21 else ma20
        # Store actual daily price structure. Signal target estimates reuse these real highs/lows,
        # not a fixed current-price percentage.
        recent=rows[-60:]
        swing_highs=[]
        for i in range(2,len(recent)-2):
            h=recent[i]["high"]
            if h>=max(x["high"] for x in recent[i-2:i+3]):
                swing_highs.append(h)
        structure={
            "rows":recent,
            "swing_highs":swing_highs[-12:],
            "high5":max(x["high"] for x in recent[-5:]) if len(recent)>=5 else 0,
            "high10":max(x["high"] for x in recent[-10:]) if len(recent)>=10 else 0,
            "high20":max(x["high"] for x in recent[-20:]) if len(recent)>=20 else 0,
        }
        with STATE.lock:
            STATE.daily_trend[c]={"ma5":ma5,"ma20":ma20,"prev_ma5":prev_ma5,"prev_ma20":prev_ma20,"asof":rows[-1]["date"]}
            STATE.daily_trend_at[c]=now()
            self.daily_price_structure[c]=structure
        self.last_daily_refresh[c]=now()
        log.info("daily MA ready: %s MA5 %.0f MA20 %.0f",STATE.vip_targets.get(c,c),ma5,ma20)
    @staticmethod
    def _percentile(values,q):
        vals=sorted(float(v) for v in values if v is not None)
        if not vals:return 0.0
        if len(vals)==1:return vals[0]
        pos=(len(vals)-1)*q;lo=int(pos);hi=min(len(vals)-1,lo+1);w=pos-lo
        return vals[lo]*(1-w)+vals[hi]*w

    @staticmethod
    def _cluster_levels(levels,tolerance_pct=1.0):
        vals=sorted(v for v in levels if v and v>0)
        clusters=[]
        for v in vals:
            placed=False
            for cl in clusters:
                center=sum(cl)/len(cl)
                if abs(pct(v,center))<=tolerance_pct:
                    cl.append(v);placed=True;break
            if not placed:clusters.append([v])
        return [sum(cl)/len(cl) for cl in clusters]

    def estimate_upside(self,c,current=None,m=None):
        """Estimate the next structural resistance band from actual intraday/daily highs.

        This deliberately does NOT use current_price * fixed %.  If there is not enough
        structure above price, it returns unavailable instead of inventing a target.
        """
        t=STATE.targets.get(c);current=num(current or (t.last_price if t else 0))
        if current<=0:return {"ok":False,"reason":"현재가 없음"}
        m=m or BRAIN.metrics(c) or {}
        levels=[]
        for v in (m.get("resistance"),m.get("day_high"),m.get("recent_peak")):
            v=num(v)
            if v>current*1.003:levels.append(v)
        ds=self.daily_price_structure.get(c) or {}
        for v in (ds.get("swing_highs") or []):
            v=num(v)
            if current*1.003<v<=current*1.20:levels.append(v)
        for k in ("high5","high10","high20"):
            v=num(ds.get(k))
            if current*1.003<v<=current*1.20:levels.append(v)
        clustered=self._cluster_levels(levels,1.0)
        clustered=sorted(v for v in clustered if v>current*1.003)
        if not clustered:
            return {"ok":False,"reason":"현재가 위 구조적 저항대 자료 부족"}
        center=clustered[0]
        # If a second independent level sits very close, merge them into one resistance zone.
        nearby=[x for x in clustered[:3] if abs(pct(x,center))<=2.0]
        if nearby:center=sum(nearby)/len(nearby)
        band_low=center*0.9965;band_high=center*1.0035
        support=num(m.get("support")) or num(t.support if t else 0)
        stop=(support*0.995 if support and support<current else current*(1-SETTINGS.default_stop_pct/100))
        reward=max(0,center-current);risk=max(0,current-stop)
        rr=(reward/risk) if risk>0 else 0
        src=[]
        if num(m.get("resistance"))>current*1.003:src.append("최근분봉 저항")
        if num(m.get("day_high"))>current*1.003:src.append("당일고점")
        if num(m.get("recent_peak"))>current*1.003:src.append("직전파동 고점")
        if ds.get("swing_highs"):src.append("일봉 스윙고점")
        return {
            "ok":True,"low":band_low,"high":band_high,"center":center,
            "upside_pct":pct(center,current),"stop":stop,"risk_reward":rr,
            "source":" + ".join(dict.fromkeys(src)) or "가격구조 저항대",
        }

    def finder_allowed(self,n=None):
        n=n or now()
        # Weekends have no live Korean session, so manual scanning is allowed.
        if n.weekday()>=5:return True
        sh,sm=map(int,SETTINGS.finder_block_start.split(":"));eh,em=map(int,SETTINGS.finder_block_end.split(":"))
        cur=n.hour*60+n.minute
        return not (sh*60+sm<=cur<eh*60+em)

    @staticmethod
    def _finder_name_allowed(name):
        s=str(name or "").upper()
        blocked=("ETF","ETN","스팩","SPAC","인버스","레버리지")
        return bool(s) and not any(x in s for x in blocked)

    @staticmethod
    def _rss_mb():
        """Linux/Render 현재 RSS 메모리(MB). 읽지 못하면 0."""
        try:
            for line in Path("/proc/self/status").read_text(encoding="utf-8",errors="ignore").splitlines():
                if line.startswith("VmRSS:"):
                    return float(line.split()[1])/1024.0
        except Exception:
            pass
        return 0.0

    def analyze_v_pattern(self,code,name,rows):
        """Score a repeated V-rebound pattern from actual daily OHLCV history."""
        if len(rows)<45:return None
        closes=[num(x.get("close")) for x in rows];highs=[num(x.get("high")) for x in rows];lows=[num(x.get("low")) for x in rows];vols=[num(x.get("volume")) for x in rows]
        if min(closes)<=0 or min(lows)<=0:return None
        current=closes[-1]
        if current<SETTINGS.finder_min_price:return None
        turnover=[closes[i]*vols[i] for i in range(max(0,len(rows)-20),len(rows))]
        median_turn=statistics.median(turnover) if turnover else 0
        if median_turn<SETTINGS.finder_min_turnover:return None

        # Local lows: must be a 5-day local trough, then cluster similar lows.
        troughs=[]
        for i in range(2,len(rows)-8):
            if lows[i]<=min(lows[i-2:i+3]):
                troughs.append(i)
        if len(troughs)<SETTINGS.finder_min_touches:return None

        clusters=[]
        for idx in troughs:
            lv=lows[idx];placed=False
            for cl in clusters:
                center=statistics.median([lows[j] for j in cl])
                if abs(pct(lv,center))<=3.0:
                    # Prevent one prolonged low from being counted as multiple independent touches.
                    if not cl or idx-cl[-1]>=4:cl.append(idx)
                    placed=True;break
            if not placed:clusters.append([idx])
        clusters=[cl for cl in clusters if len(cl)>=SETTINGS.finder_min_touches]
        if not clusters:return None

        best=None
        for cl in clusters:
            support_vals=[lows[i] for i in cl];support=statistics.median(support_vals)
            # Ignore historical supports that are now clearly broken.
            recent_low=min(lows[-10:])
            if recent_low<support*0.95:continue
            rebounds=[]
            for i in cl:
                future=highs[i+1:min(len(rows),i+9)]
                if not future:continue
                rebounds.append(pct(max(future),lows[i]))
            if len(rebounds)<SETTINGS.finder_min_touches:continue
            success=sum(1 for x in rebounds if x>=SETTINGS.finder_min_rebound_pct)/len(rebounds)
            med_rebound=statistics.median(rebounds)
            if success<0.60 or med_rebound<SETTINGS.finder_min_rebound_pct:continue

            ma20=sum(closes[-20:])/20
            ma60=sum(closes[-60:])/60 if len(closes)>=60 else ma20
            if ma20<ma60*0.92:continue
            day_changes=[abs(pct(closes[i],closes[i-1])) for i in range(max(1,len(closes)-20),len(closes))]
            shock_count=sum(1 for x in day_changes if x>=18)
            current_gap=pct(current,support)
            if current_gap<-3 or current_gap>22:continue

            # Historical exit zone comes from the actual rebound distribution after these support touches.
            r50=self._percentile(rebounds,0.50);r75=self._percentile(rebounds,0.75)
            exit_low=support*(1+r50/100);exit_high=support*(1+r75/100)
            zone_low=min(support_vals)*0.995;zone_high=max(support_vals)*1.005
            stability=max(support_vals)/min(support_vals)-1 if min(support_vals)>0 else 1
            score=0.0
            score+=min(28,len(cl)*7)
            score+=success*22
            score+=clamp((med_rebound-SETTINGS.finder_min_rebound_pct)*2,0,18)
            score+=clamp((1-stability/0.08)*12,0,12)
            score+=12 if 0<=current_gap<=8 else 7 if current_gap<=12 else 2
            score+=8 if ma20>=ma60 else 3
            score-=shock_count*5
            score=clamp(score)
            item={
                "code":code,"name":name,"price":current,"score":score,"touches":len(cl),
                "support_low":zone_low,"support_high":zone_high,"support_center":support,
                "success_rate":success,"median_rebound_pct":med_rebound,
                "exit_low":exit_low,"exit_high":exit_high,"current_gap_pct":current_gap,
                "median_turnover":median_turn,"shock_count":shock_count,
            }
            if best is None or item["score"]>best["score"]:best=item
        return best

    def _yf_daily_chunk(self,pairs):
        """Finder 전용 저부하 일봉 다운로드."""
        if not HAS_YF or not pairs:return {}
        symbols=[];code_by_symbol={}
        for c,market in pairs:
            suffix=".KS" if market=="KOSPI" else ".KQ"
            s=c+suffix;symbols.append(s);code_by_symbol[s]=c
        d=None
        try:
            d=yf.download(symbols,period="6mo",interval="1d",group_by="ticker",auto_adjust=False,threads=False,progress=False)
        except Exception as e:
            log.warning("finder yfinance chunk failed: %s",e);return {}
        out={}
        try:
            for s,c in code_by_symbol.items():
                try:
                    if hasattr(d.columns,"nlevels") and d.columns.nlevels>=2:
                        frame=d[s].dropna(how="all")
                    else:
                        frame=d.dropna(how="all")
                    rows=[]
                    for _,r in frame.tail(SETTINGS.finder_days).iterrows():
                        cp=num(r.get("Close"));hi=num(r.get("High"));lo=num(r.get("Low"));op=num(r.get("Open"));vol=num(r.get("Volume"))
                        if cp>0 and hi>0 and lo>0:rows.append({"open":op or cp,"high":hi,"low":lo,"close":cp,"volume":vol})
                    if rows:out[c]=rows
                except Exception:
                    continue
            return out
        finally:
            try:del d
            except Exception:pass
            gc.collect()

    def run_v_finder(self,chat):
        if not self.finder_allowed():
            BOT.send("🔒 <b>VIP 종목발굴 잠금</b>\n평일 오전 7:50 ~ 오후 8:10(KST)에는 실시간 봇 보호를 위해 실행하지 않습니다.\n오후 8:10 이후 또는 오전 7:50 이전에 다시 실행해주세요.",chat,buttons=self.main_buttons());return
        if not HAS_YF:
            BOT.send("⚠️ 종목발굴에 필요한 일괄 일봉 모듈(yfinance)을 사용할 수 없습니다.",chat,buttons=self.main_buttons());return
        with self.finder_lock:
            if self.finder_running:
                BOT.send(f"⏳ 종목발굴이 이미 실행 중입니다.\n진행: {html.escape(self.finder_progress)}",chat,buttons=self.main_buttons());return
            self.finder_running=True;self.finder_progress="준비중"
        try:
            STATE.save_vip()
        except Exception as e:
            with self.finder_lock:self.finder_running=False;self.finder_progress="대기"
            BOT.send(f"⚠️ VIP 안전백업 실패로 종목발굴을 시작하지 않았습니다.\n{html.escape(str(e)[:160])}",chat,buttons=self.main_buttons());return
        vip_snapshot=dict(STATE.vip_targets)
        def job():
            started=time.monotonic();last_progress_sent=0
            try:
                universe=[(c,MASTER.code_market.get(c),n) for c,n in MASTER.code_to_name.items() if MASTER.code_market.get(c) and self._finder_name_allowed(n)]
                total=len(universe)
                if total<=0:
                    log.error("V finder universe empty: master_names=%s market_map=%s",len(MASTER.code_to_name),len(MASTER.code_market))
                    BOT.send("⚠️ 종목발굴 대상이 0개입니다.\n종목 마스터 또는 KOSPI/KOSDAQ 시장구분 복구에 실패했습니다.\n봇 자체는 계속 동작하며, 다음 마스터 갱신 후 다시 시도해주세요.",chat,buttons=self.main_buttons());return
                chunk_size=max(5,min(20,SETTINGS.finder_chunk_size))
                log.info("V finder START total=%s chunk=%s vip=%s rss=%.1fMB",total,chunk_size,len(vip_snapshot),self._rss_mb())
                BOT.send(f"🔎 <b>VIP 반복 V자 종목발굴 시작</b>\n대상 {total:,}종목 · 저부하 {chunk_size}종목씩 순차 분석\n2만원 이상 · 반복 지지/반등 · 거래대금 · 저점 안정성 · 현재 위치\n※ VIP {len(vip_snapshot)}개는 안전백업 후 그대로 유지합니다.",chat)
                results=[];fail_chunks=0
                for start_idx in range(0,total,chunk_size):
                    if not self.finder_allowed():
                        log.warning("V finder STOP market window reached progress=%s/%s",start_idx,total)
                        BOT.send("⏹️ 종목발굴 중단\n장중 보호시간(07:50~20:10)에 진입하여 안전하게 중단했습니다.",chat,buttons=self.main_buttons());return
                    elapsed=time.monotonic()-started
                    if elapsed>SETTINGS.finder_max_seconds:
                        log.warning("V finder STOP timeout progress=%s/%s elapsed=%.1fs",start_idx,total,elapsed)
                        BOT.send(f"⏹️ 종목발굴 안전중단\n최대 실행시간 {SETTINGS.finder_max_seconds//60}분을 넘어 서버 보호를 위해 중단했습니다.",chat,buttons=self.main_buttons());return
                    rss=self._rss_mb()
                    if rss and rss>SETTINGS.finder_max_rss_mb:
                        log.warning("V finder STOP memory progress=%s/%s rss=%.1fMB",start_idx,total,rss)
                        BOT.send(f"⏹️ 종목발굴 안전중단\n메모리 사용량 {rss:.0f}MB로 보호기준을 넘어 중단했습니다.\nVIP 목록은 유지됩니다.",chat,buttons=self.main_buttons());return
                    chunk=universe[start_idx:start_idx+chunk_size]
                    pairs=[(c,mk) for c,mk,_ in chunk]
                    done=min(start_idx+len(chunk),total)
                    self.finder_progress=f"{done}/{total}"
                    data=self._yf_daily_chunk(pairs)
                    if not data:fail_chunks+=1
                    for c,mk,nm in chunk:
                        rows=data.get(c)
                        if not rows:continue
                        item=self.analyze_v_pattern(c,nm,rows)
                        if item:results.append(item)
                    try:del data,chunk,pairs
                    except Exception:pass
                    gc.collect()
                    if done==total or done-last_progress_sent>=max(60,SETTINGS.finder_progress_every):
                        last_progress_sent=done
                        log.info("V finder PROGRESS %s/%s candidates=%s rss=%.1fMB",done,total,len(results),self._rss_mb())
                        if done<total:BOT.send(f"⏳ 종목발굴 진행 {done:,}/{total:,}\n현재 조건통과 후보 {len(results)}개",chat)
                    time.sleep(max(0.5,SETTINGS.finder_pause_seconds))
                results.sort(key=lambda x:(x["score"],-abs(x["current_gap_pct"]-4)),reverse=True)
                self.finder_last_results=results[:SETTINGS.finder_result_limit];self.finder_last_at=now()
                elapsed=time.monotonic()-started
                log.info("V finder DONE total=%s candidates=%s fail_chunks=%s elapsed=%.1fs rss=%.1fMB",total,len(results),fail_chunks,elapsed,self._rss_mb())
                if not self.finder_last_results:
                    BOT.send(f"🔎 종목발굴 완료 ({elapsed/60:.1f}분)\n현재 조건을 충분히 충족한 반복 V자 후보가 없습니다.\n조건을 억지로 완화해 종목을 만들지 않았습니다.",chat,buttons=self.main_buttons());return
                lines=["🔎 <b>VIP 반복 V자 후보 TOP</b>","※ 자동추천/자동주문 아님 · 실제 과거 반복구조 기반"]
                for i,x in enumerate(self.finder_last_results,1):
                    pos="진입구간 근접" if -1<=x["current_gap_pct"]<=8 else "눌림 대기"
                    lines.append(f"\n<b>{i}. {html.escape(x['name'])}</b> ({x['code']}) · {x['score']:.0f}점\n현재 {x['price']:,.0f}원 · {pos}\n반복 진입관찰 {x['support_low']:,.0f}~{x['support_high']:,.0f}원\n과거구조 익절후보 {x['exit_low']:,.0f}~{x['exit_high']:,.0f}원\n지지 {x['touches']}회 · 5%+ 반등성공 {x['success_rate']*100:.0f}% · 중앙반등 {x['median_rebound_pct']:.1f}%")
                lines.append(f"\n⏱️ 분석시간 {elapsed/60:.1f}분 · VIP 목록 변경 없음")
                BOT.send("\n".join(lines),chat,buttons=self.main_buttons())
            except Exception as e:
                log.exception("V finder failed");BOT.send(f"⚠️ 종목발굴 오류: {html.escape(str(e)[:180])}\nVIP 목록은 변경하지 않았습니다.",chat,buttons=self.main_buttons())
            finally:
                if dict(STATE.vip_targets)!=vip_snapshot:
                    log.warning("V finder observed VIP change during scan: before=%s after=%s",list(vip_snapshot),list(STATE.vip_targets))
                with self.finder_lock:self.finder_running=False;self.finder_progress="대기"
                gc.collect()
        threading.Thread(target=job,daemon=True,name="vip-v-finder").start()


    def nextday_allowed(self,n=None):
        """Manual next-day analysis is disabled during the protected live-session window."""
        n=n or now()
        if n.weekday()>=5:
            return True
        sh,sm=map(int,SETTINGS.nextday_block_start.split(":"))
        eh,em=map(int,SETTINGS.nextday_block_end.split(":"))
        cur=n.hour*60+n.minute
        return not (sh*60+sm<=cur<eh*60+em)

    def _nextday_daily_rows(self,c):
        ds=self.daily_price_structure.get(c) or {}
        rows=list(ds.get("rows") or [])
        if len(rows)>=SETTINGS.nextday_min_daily_bars:
            return rows
        # Reuse the existing KIS daily sync; no extra provider is introduced.
        self.sync_daily_for(c,True)
        ds=self.daily_price_structure.get(c) or {}
        return list(ds.get("rows") or [])

    def _nextday_intraday_rows(self,c):
        """Return current-day bars already held by the live bot.

        We intentionally do not invent missing history.  If Render restarted late and the
        regular-session history is insufficient, the analyzer returns DATA_NOT_READY.
        """
        rows=list(STATE.bars.get(c,[]))
        cur=STATE.current.get(c)
        if cur: rows.append(cur)
        bars=normalize_intraday_bars(rows)
        out=[]
        for b in bars:
            # For next-day analysis use the regular KRX session only.
            m=kst_dt(b.minute)
            minute=m.hour*60+m.minute
            if 9*60<=minute<=15*60+30:
                out.append({
                    "time":m.isoformat(),
                    "open":b.open,"high":b.high,"low":b.low,"close":b.close,
                    "volume":b.volume,
                })
        return out

    def nextday_analysis(self,c,force=False):
        c=str(c).zfill(6)
        if c not in STATE.vip_targets:
            return {"ok":False,"error":"not_vip"}
        if not self.nextday_allowed():
            return {
                "ok":False,"error":"market_protection",
                "message":"평일 07:50~20:10에는 실시간 봇 보호를 위해 내일흐름 분석이 잠겨 있습니다."
            }

        cached=self.nextday_cache.get(c)
        if cached and not force:
            try:
                if cached.get("date")==str(now().date()):
                    return cached["payload"]
            except Exception:
                pass

        with self.nextday_lock:
            if c in self.nextday_running:
                return {"ok":False,"error":"already_running","message":"이 종목의 내일흐름 분석이 이미 실행 중입니다."}
            self.nextday_running.add(c)

        try:
            daily_rows=self._nextday_daily_rows(c)
            intraday_rows=self._nextday_intraday_rows(c)

            # Full-day integrity guard.  09:00~15:30 regular session should normally
            # aggregate to roughly 13~14 30-minute bars.
            if len(intraday_rows)<60:
                return {
                    "ok":False,
                    "error":"data_not_ready",
                    "message":(
                        f"당일 정규장 분봉이 {len(intraday_rows)}개뿐이라 예측을 만들지 않았습니다. "
                        "Render 재시작 등으로 장중 데이터가 비었을 수 있습니다."
                    ),
                    "daily_bars":len(daily_rows),
                    "intraday_bars":len(intraday_rows),
                }

            result=self.nextday_analyzer.analyze(
                c,STATE.vip_targets[c],daily_rows,intraday_rows,intraday_is_30m=False
            )
            rule_ai_payload=result.ai_payload()
            ai_advice=self.gemini_advisor.advise(rule_ai_payload)
            payload={
                "ok":True,
                "engine_version":"0.2.0",
                "analysis":result.to_dict(),
                "text":render_nextday_text(result),
                "ai_payload":rule_ai_payload,
                "ai":ai_advice,
                "ai_status":"ok" if ai_advice.get("ok") else ai_advice.get("error","unavailable"),
                "generated_at":now().isoformat(),
                "notice":"규칙 엔진이 1차 계산하고 Gemini는 보조 해석만 합니다. AI 실패 시 규칙 결과는 그대로 유지됩니다.",
            }
            # Persist one prediction per code/asof. Re-analysis updates the same record.
            pred={
                "id":f"{c}:{result.asof}",
                "code":c,
                "name":STATE.vip_targets[c],
                "asof":result.asof,
                "created_at":now().isoformat(),
                "current_price":result.current_price,
                "primary_view":result.primary_view,
                "setup_score":result.setup_score,
                "support_zone":list(result.support_zone),
                "resistance_zone":list(result.resistance_zone),
                "preferred_entry_zone":list(result.preferred_entry_zone),
                "invalidation_price":result.invalidation_price,
                "scenarios":[asdict(x) for x in result.scenarios],
                "reasons":list(result.reasons),
                "cautions":list(result.cautions),
                "features":dict(result.features),
                "ai":ai_advice,
                "evaluated":False,
                "outcome":None,
            }
            replaced=False
            for i,row in enumerate(self.nextday_predictions):
                if row.get("id")==pred["id"]:
                    # Preserve already-evaluated outcome if user reopens the same analysis later.
                    if row.get("evaluated"):
                        pred["evaluated"]=True;pred["outcome"]=row.get("outcome")
                    self.nextday_predictions[i]=pred;replaced=True;break
            if not replaced:self.nextday_predictions.append(pred)
            STORE.save_nextday_predictions(self.nextday_predictions)

            self.nextday_cache[c]={"date":str(now().date()),"payload":payload}
            log.info(
                "NEXTDAY analysis %s setup=%.1f view=%s intraday=%s daily=%s",
                c,result.setup_score,result.primary_view,len(intraday_rows),len(daily_rows)
            )
            return payload
        except Exception as e:
            log.exception("NEXTDAY analysis failed %s",c)
            return {"ok":False,"error":"analysis_failed","message":str(e)[:240]}
        finally:
            with self.nextday_lock:
                self.nextday_running.discard(c)


    @staticmethod
    def _daily_row_date(row):
        return str(row.get("date") or row.get("stck_bsop_date") or "").replace("-","")[:8]

    def _nextday_target_daily_bar(self,c,asof):
        """Find the first KIS daily bar strictly after prediction asof."""
        raw=KIS_CLIENT.daily_bars(c,80)
        rows=[]
        for r in raw:
            try:
                d=str(r.get("stck_bsop_date") or "")
                cp=num(r.get("stck_clpr"));op=num(r.get("stck_oprc"));hi=num(r.get("stck_hgpr"));lo=num(r.get("stck_lwpr"));vol=integer(r.get("acml_vol"))
                if d and min(cp,op or cp,hi,lo)>0:
                    rows.append({"date":d,"open":op or cp,"high":hi,"low":lo,"close":cp,"volume":max(0,vol)})
            except Exception:pass
        rows.sort(key=lambda x:x["date"])
        base=str(asof or "").replace("-","")[:8]
        return next((r for r in rows if r["date"]>base),None)

    @staticmethod
    def _score_nextday_outcome(pred,bar):
        entry=num(pred.get("current_price"))
        if entry<=0:return None
        o=num(bar.get("open"));h=num(bar.get("high"));l=num(bar.get("low"));c=num(bar.get("close"))
        sz=pred.get("support_zone") or [0,0]
        rz=pred.get("resistance_zone") or [0,0]
        ez=pred.get("preferred_entry_zone") or [0,0]
        support_low=num(sz[0] if len(sz)>0 else 0);support_high=num(sz[1] if len(sz)>1 else 0)
        resistance_low=num(rz[0] if len(rz)>0 else 0);resistance_high=num(rz[1] if len(rz)>1 else 0)
        entry_low=num(ez[0] if len(ez)>0 else 0);entry_high=num(ez[1] if len(ez)>1 else 0)
        invalid=num(pred.get("invalidation_price"))

        range_hits_entry=bool(entry_low and entry_high and l<=entry_high and h>=entry_low)
        hit_resistance=bool(resistance_low and h>=resistance_low)
        hit_resistance_high=bool(resistance_high and h>=resistance_high)
        invalidated=bool(invalid and l<=invalid)
        support_defended=bool(support_low and c>=support_low and not invalidated)
        max_up=pct(h,entry);max_down=pct(l,entry);close_ret=pct(c,entry)

        view=str(pred.get("primary_view") or "")
        if "반등" in view:
            success=(hit_resistance or (close_ret>0 and support_defended)) and not invalidated
        elif "박스" in view or "압축" in view:
            success=(not invalidated and not hit_resistance_high and abs(close_ret)<=3.0)
        elif "추격 금지" in view or "관찰" in view:
            success=True if invalidated or close_ret<=1.5 else not hit_resistance_high
        else:
            success=not invalidated

        ambiguous=bool(hit_resistance and invalidated)
        score=0
        if success:score+=60
        if hit_resistance:score+=20
        if support_defended:score+=10
        if range_hits_entry:score+=5
        if invalidated:score-=35
        if ambiguous:score-=5
        score=int(clamp(score,0,100))

        return {
            "date":str(bar.get("date") or ""),
            "open":o,"high":h,"low":l,"close":c,"volume":integer(bar.get("volume")),
            "open_gap_pct":round(pct(o,entry),3),
            "max_up_pct":round(max_up,3),
            "max_down_pct":round(max_down,3),
            "close_return_pct":round(close_ret,3),
            "entry_zone_touched":range_hits_entry,
            "support_defended":support_defended,
            "resistance_reached":hit_resistance,
            "resistance_high_reached":hit_resistance_high,
            "invalidated":invalidated,
            "ambiguous_intraday_order":ambiguous,
            "success":bool(success),
            "score":score,
            "note":"일봉 OHLC 기준 채점이라 고가/저가 발생 순서는 알 수 없음" if ambiguous else "일봉 OHLC 기준 자동채점",
        }

    def evaluate_nextday_predictions(self):
        """Evaluate unresolved predictions once the next trading day's daily bar exists."""
        changed=False
        for pred in self.nextday_predictions[-300:]:
            if pred.get("evaluated"):continue
            c=str(pred.get("code") or "").zfill(6)
            asof=str(pred.get("asof") or "")
            if not c or not asof:continue
            try:
                bar=self._nextday_target_daily_bar(c,asof)
                if not bar:continue
                outcome=self._score_nextday_outcome(pred,bar)
                if not outcome:continue
                pred["outcome"]=outcome;pred["evaluated"]=True;pred["evaluated_at"]=now().isoformat();changed=True
                log.info("NEXTDAY outcome %s %s success=%s score=%s close=%+.2f%%",
                         c,bar.get("date"),outcome.get("success"),outcome.get("score"),outcome.get("close_return_pct"))
            except Exception as e:
                log.warning("NEXTDAY outcome eval %s failed: %s",c,e)
        if changed:STORE.save_nextday_predictions(self.nextday_predictions)

    def nextday_stats(self):
        rows=[x for x in self.nextday_predictions if x.get("evaluated") and isinstance(x.get("outcome"),dict)]
        if not rows:
            return {"count":0,"success_rate":None,"avg_score":None,"avg_close_return_pct":None,"recent":[]}
        wins=sum(1 for x in rows if x["outcome"].get("success"))
        avg_score=sum(num(x["outcome"].get("score")) for x in rows)/len(rows)
        avg_ret=sum(num(x["outcome"].get("close_return_pct")) for x in rows)/len(rows)
        recent=[]
        for x in reversed(rows[-20:]):
            o=x["outcome"]
            recent.append({
                "code":x.get("code"),"name":x.get("name"),"asof":x.get("asof"),
                "target_date":o.get("date"),"primary_view":x.get("primary_view"),
                "success":bool(o.get("success")),"score":o.get("score"),
                "max_up_pct":o.get("max_up_pct"),"max_down_pct":o.get("max_down_pct"),
                "close_return_pct":o.get("close_return_pct"),
                "resistance_reached":o.get("resistance_reached"),
                "invalidated":o.get("invalidated"),
                "ambiguous_intraday_order":o.get("ambiguous_intraday_order"),
            })
        return {
            "count":len(rows),
            "success_rate":round(wins/len(rows)*100,1),
            "avg_score":round(avg_score,1),
            "avg_close_return_pct":round(avg_ret,2),
            "recent":recent,
        }

    def on_book(self,b):STATE.books[b.code]=b
    def on_tick(self,t):
        # 미래/과거 비정상 틱은 지표에 섞지 않는다.
        age=(now()-kst_dt(t.timestamp)).total_seconds()
        if t.price<=0 or age>180 or age<-10:return
        with STATE.lock:
            prev=STATE.last_cum.get(t.code);inc=t.cumulative_volume-prev if prev is not None and t.cumulative_volume>=prev else max(0,t.volume)
            STATE.last_cum[t.code]=t.cumulative_volume;minute=kst_dt(t.timestamp).replace(second=0,microsecond=0);cur=STATE.current.get(t.code)
            if not cur:
                STATE.current[t.code]=Bar(minute,t.price,t.price,t.price,t.price,inc,t.cumulative_volume,t.trade_strength)
            elif cur.minute==minute:
                cur.high=max(cur.high,t.price);cur.low=min(cur.low,t.price);cur.close=t.price;cur.volume+=inc;cur.cumulative_volume=max(cur.cumulative_volume,t.cumulative_volume);cur.trade_strength=t.trade_strength
            else:
                completed=list(STATE.bars.get(t.code,[]))+[cur]
                STATE.bars[t.code]=deque(normalize_intraday_bars(completed,minute.date()),maxlen=390)
                STATE.current[t.code]=Bar(minute,t.price,t.price,t.price,t.price,inc,t.cumulative_volume,t.trade_strength)
            STATE.runtime["last_tick"]=kst_dt(t.timestamp).isoformat();STATE.last_tick_at[t.code]=kst_dt(t.timestamp)
        for msg in POSITIONS.check(t.code,t.price):BOT.send_async(msg,buttons=self.position_buttons(t.code))
        if t.code in STATE.vip_targets:
            if t.code not in STATE.daily_trend:WORKER.submit(self.sync_daily_for,t.code)
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
    def target_health(self,c):
        tick=STATE.last_tick_at.get(c);ob=STATE.books.get(c);n=now()
        tick_age=(n-kst_dt(tick)).total_seconds() if tick else None
        ob_age=(n-kst_dt(ob.updated_at)).total_seconds() if ob else None
        bars=normalize_intraday_bars(list(STATE.bars.get(c,[]))+([STATE.current[c]] if STATE.current.get(c) else []))
        delayed=bool(in_session(SETTINGS.nxt_start,SETTINGS.nxt_end) and tick_age is not None and tick_age>SETTINGS.stale_tick_seconds)
        return {"status":"DELAYED" if delayed else "OK","tick_age":round(tick_age,1) if tick_age is not None else None,"orderbook_age":round(ob_age,1) if ob_age is not None else None,"bars":len(bars)}

    def track_signal_outcomes(self):
        changed=False;n=now()
        for s in STATE.signals[-200:]:
            if s.get("outcome_final"):continue
            try:created=datetime.fromisoformat(s.get("time"));age_min=(n-created).total_seconds()/60.0
            except Exception:continue
            c=str(s.get("code") or "");t=STATE.targets.get(c);price=t.last_price if t else 0;entry=num(s.get("price"))
            if not entry or not price:continue
            up=pct(price,entry);o=s.setdefault("outcome",{})
            o["max_up_pct"]=round(max(num(o.get("max_up_pct"),-999),up),3)
            o["max_down_pct"]=round(min(num(o.get("max_down_pct"),999),up),3)
            o["hit_plus_1"]=bool(o.get("hit_plus_1") or up>=1.0);o["hit_plus_3"]=bool(o.get("hit_plus_3") or up>=3.0)
            stop=num(s.get("stop"));o["hit_stop"]=bool(o.get("hit_stop") or (stop>0 and price<=stop));o["age_min"]=round(age_min,1);changed=True
            if age_min>=SETTINGS.signal_result_minutes or (n.hour>=20 and n.minute>=5):s["outcome_final"]=True
        if changed:STORE.save_signals(STATE.signals)

    def update_focus(self):
        pri={ST_HOLD:100,ST_BREAKOUT:95,ST_SIGNAL:92,ST_MOMENTUM:82,ST_READY:75,ST_PULLBACK:60,ST_WAIT:20,ST_COOLDOWN:5};STATE.focus_codes=[c for _,c in sorted(((pri.get(t.stage,0)+t.score/10,c) for c,t in STATE.targets.items()),reverse=True)[:SETTINGS.focus_orderbook_slots]]
    def evaluate_target(self,c):
        t=STATE.targets[c];st,m,reason=BRAIN.evaluate(c)
        if not st:return
        changed=STATE.set_stage(c,st,reason);self.update_focus()
        if st in (ST_SIGNAL,ST_BREAKOUT) and changed:
            t.last_signal_at=now().isoformat();t.signal_price=t.last_price;t.signal_kind="SECOND_WAVE" if st==ST_BREAKOUT else "REENTRY";t.signal_grade=signal_grade(t.score);t.signal_count_today+=1
            stop=(t.support*0.995 if t.support>0 else t.last_price*(1-SETTINGS.default_stop_pct/100))
            ob=STATE.books.get(c);target_est=self.estimate_upside(c,t.last_price,m)
            STATE.signals.append({
                "code":c,"name":t.name,"time":t.last_signal_at,"price":t.last_price,"stop":stop,
                "support":t.support,"resistance":t.resistance,"vwap":t.vwap,"strength":t.trade_strength,
                "score":t.score,"grade":t.signal_grade,"volume_ratio":t.volume_ratio,
                "projected_volume_ratio":t.projected_volume_ratio,"orderbook_imbalance":ob.imbalance if ob else None,
                "ma5d":t.ma5d,"ma20d":t.ma20d,"daily_trend":t.daily_trend,"signal_type":t.signal_kind,
                "market_risk":STATE.market_risk,"day_gain_pct":m.get("day_gain_pct"),"day_high_gap_pct":m.get("day_high_gap_pct"),
                "chase_risk":m.get("chase_risk"),"wave_pullback_pct":m.get("wave_pullback_pct"),"reason":reason,
                "target_estimate":target_est,
                "outcome":{},"outcome_final":False,
            });STORE.save_signals(STATE.signals);WORKER.submit(self.send_signal_alert,c);self.stage_alert[(c,"signal")]=now()
        elif changed and st==ST_MOMENTUM and self.allowed(c,"momentum",20):
            BOT.send_async(f"🔵 <b>{html.escape(t.name)} 상승추세 관찰</b>\n현재 {t.last_price:,.0f} · VWAP {t.vwap:,.0f}\n체결강도 {t.trade_strength:.0f} · 타점점수 {t.score:.0f}({t.signal_grade})\n⚠️ 아직 진입신호 아님 · 고점 추격 금지")
            self.stage_alert[(c,"momentum")]=now()
        elif changed and st==ST_PULLBACK and self.allowed(c,"pull",30):
            BOT.send_async(f"🟡 <b>{html.escape(t.name)} 눌림관찰</b>\n현재 {t.last_price:,.0f} · VWAP {t.vwap:,.0f} · 지지 {t.support:,.0f}\n거래량비 {t.volume_ratio:.2f} · 체결강도 {t.trade_strength:.0f}\n아직 매수신호 아님")
            self.stage_alert[(c,"pull")]=now()
    def allowed(self,c,k,m):
        x=self.stage_alert.get((c,k));return not x or now()-x>=timedelta(minutes=m)
    def signal_text(self,t):
        ob=STATE.books.get(t.code);obs=f"\n호가잔량비 {ob.imbalance:.2f}배" if ob and (now()-ob.updated_at).total_seconds()<SETTINGS.stale_orderbook_seconds else ""
        proj=f" · 예상1분 {t.projected_volume_ratio:.2f}배" if t.projected_volume_ratio>0 else "";daily=""
        if t.ma5d and t.ma20d:daily=f"\n일봉 MA5 {t.ma5d:,.0f} · MA20 {t.ma20d:,.0f} · {t.daily_trend}"
        est=self.estimate_upside(t.code,t.last_price)
        if est.get("ok"):
            target=(f"\n\n🎯 <b>구조상 예상 상승구간</b> {est['low']:,.0f}~{est['high']:,.0f}원"
                    f"\n중심 {est['center']:,.0f}원 · 현재가 대비 여력 {est['upside_pct']:+.1f}%"
                    f"\n타점무효 참고 {est['stop']:,.0f}원 · 예상 손익비 {est['risk_reward']:.1f}:1"
                    f"\n근거: {html.escape(est['source'])}")
        else:
            target=f"\n\n🎯 구조상 예상 상승가: {html.escape(est.get('reason','계산자료 부족'))}"
        if t.stage==ST_BREAKOUT:
            return (f"🟣 <b>[명하 2차상승 확인 · 보조신호] {html.escape(t.name)}</b>\n현재 <b>{t.last_price:,.0f}원</b>\nVWAP {t.vwap:,.0f} · 저항 {t.resistance:,.0f}\n체결강도 {t.trade_strength:.0f} · 거래량비 {t.volume_ratio:.2f}{proj}\n타점점수 <b>{t.score:.0f}점 ({t.signal_grade})</b>{obs}{daily}{target}\n\n✅ 상승 후 눌림 확인\n✅ 지지 후 거래량/체결 재가속\n✅ 고점추격 차단 필터 통과\n⚠️ 예상 상승구간은 실제 저항/고점 구조 기반이며 보장값이 아님\n⚠️ 실제 주문은 사용자가 직접 수행")
        return (f"🟢 <b>[명하 VIP 재진입 타점] {html.escape(t.name)}</b>\n현재 <b>{t.last_price:,.0f}원</b>\nVWAP {t.vwap:,.0f} · 지지 {t.support:,.0f} · 저항 {t.resistance:,.0f}\n체결강도 {t.trade_strength:.0f} · 거래량비 {t.volume_ratio:.2f}{proj}\n타점점수 <b>{t.score:.0f}점 ({t.signal_grade})</b>{obs}{daily}{target}\n\n✅ 눌림/추세 맥락 확인\n✅ 체결강도 + 단기고점 돌파\n✅ 거래량 재유입 확인\n⚠️ 타점 가격에서 이격되면 자동 만료 · 추격 금지\n⚠️ 예상 상승구간은 실제 저항/고점 구조 기반이며 보장값이 아님\n⚠️ 실제 주문은 사용자가 직접 수행")
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
            ([{"text":"🔎 VIP 종목발굴","callback_data":"finder"}] if self.finder_allowed() else [{"text":"🔒 VIP 종목발굴 (장중잠금)","callback_data":"finder_locked"}]),
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
        rows=list(STATE.bars.get(c,[]));cur=STATE.current.get(c)
        if cur:rows.append(cur)
        bars=normalize_intraday_bars(rows)
        if not bars:return None
        try:
            x=list(range(len(bars)));y=[b.close for b in bars];labels=[kst_dt(b.minute).strftime("%H:%M") for b in bars]
            fig=plt.figure(figsize=(8,3.8));plt.plot(x,y,label="Price");t=STATE.targets[c]
            if t.vwap:plt.axhline(t.vwap,linestyle="--",label="VWAP")
            if t.support:plt.axhline(t.support,linestyle=":",label="Support")
            if t.resistance:plt.axhline(t.resistance,linestyle=":",label="Resistance")
            step=max(1,len(x)//7);ticks=x[::step]
            if x and (not ticks or ticks[-1]!=x[-1]):ticks=list(ticks)+[x[-1]]
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
    def diag(self):return f"🩺 <b>VIP V5 진단</b>\nREADY {STATE.runtime.get('ready')} · {STATE.runtime.get('boot_phase')}\nNXT {STATE.runtime['ws']}\n체결 {STATE.runtime['trade_subscribed']} · 호가 {STATE.runtime['order_subscribed']}\nVIP {len(STATE.vip_targets)}/{STATE.vip_limit} · 보유 {len(POSITIONS.data)}\n종목발굴 {'실행중 '+self.finder_progress if self.finder_running else '대기'} · {'사용가능' if self.finder_allowed() else '장중잠금'}\n내일흐름 {'사용가능' if self.nextday_allowed() else '장중잠금'} · Gemini {'ON' if self.gemini_advisor.enabled else 'KEY없음'}\n마지막체결 {STATE.runtime['last_tick'] or '-'}\n오류 {STATE.runtime['last_error'] or '-'}"
    def handle_cb(self,cb):
        cid=cb.get("id");d=str(cb.get("data") or "");chat=str(((cb.get("message") or {}).get("chat") or {}).get("id") or SETTINGS.chat_id);BOT.answer_callback(cid,"처리중")
        if d in ("main","vip"):BOT.send(self.dashboard(),chat,buttons=self.main_buttons())
        elif d=="ready":BOT.send("🟢 <b>타점대기</b>\n"+"\n".join(f"• {t.name} {STATE_LABEL[t.stage]}" for t in STATE.targets.values() if t.stage in (ST_PULLBACK,ST_READY,ST_MOMENTUM,ST_SIGNAL,ST_BREAKOUT)),chat,buttons=self.main_buttons())
        elif d=="hold":BOT.send("💼 <b>보유관리</b>\n"+"\n".join(f"• {p.name} {pct(STATE.targets[c].last_price or p.entry,p.entry):+.2f}%" for c,p in POSITIONS.data.items()) if POSITIONS.data else "💼 보유 없음",chat,buttons=self.main_buttons())
        elif d=="market":BOT.send(f"📊 시장 {STATE.market_risk}\nKOSPI {STATE.market_changes.get('KOSPI',0):+.2f}% · KOSDAQ {STATE.market_changes.get('KOSDAQ',0):+.2f}%",chat,buttons=self.main_buttons())
        elif d=="add":STATE.pending_input[chat]={"action":"add"};BOT.send("추가할 종목명/코드 입력",chat)
        elif d=="remove":BOT.send("삭제할 종목 선택",chat,buttons={"inline_keyboard":[[{"text":"❌ "+n,"callback_data":f"rm:{c}"}] for c,n in STATE.vip_targets.items()]})
        elif d=="newsall":WORKER.submit(lambda:BOT.send(self.news_text(),chat,buttons=self.main_buttons()))
        elif d=="diag":BOT.send(self.diag(),chat,buttons=self.main_buttons())
        elif d=="finder":self.run_v_finder(chat)
        elif d=="finder_locked":BOT.send("🔒 평일 오전 7:50 ~ 오후 8:10(KST)에는 실시간 감시 보호를 위해 VIP 종목발굴이 잠겨 있습니다.\n오후 8:10 이후에 실행해주세요.",chat,buttons=self.main_buttons())
        elif d=="bm":BOT.send(self.morning(),chat,buttons=self.main_buttons())
        elif d=="bn":BOT.send(self.nxt_brief(),chat,buttons=self.main_buttons())
        elif d.startswith("detail:"):c=d.split(":",1)[1];BOT.send(self.detail(c),chat,buttons=self.target_buttons(c))
        elif d.startswith("chart:"):c=d.split(":",1)[1];b=self.chart(c);BOT.photo(b,f"📈 {STATE.vip_targets.get(c,c)} 당일 전체",chat,self.target_buttons(c)) if b else BOT.send("차트 데이터 부족",chat)
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
        if txt=="/종목발굴":self.run_v_finder(chat);return
        if txt.startswith("/추가 "):self.add_vip(txt[4:].strip(),chat);return
        if txt.startswith("/삭제 "):
            c,n=MASTER.resolve(txt[4:].strip());self.remove_vip(c,chat) if c in STATE.vip_targets else BOT.send("VIP에서 못 찾음",chat);return
        if txt.startswith("/매수 "):
            parts=txt.split();price=num(parts[-2] if len(parts)>=4 else parts[-1]);qty=num(parts[-1]) if len(parts)>=4 else 1;stock=" ".join(parts[1:-2] if len(parts)>=4 else parts[1:-1]);c,n=MASTER.resolve(stock)
            if c in STATE.vip_targets and price>0:t=STATE.targets[c];p=POSITIONS.register(c,n,price,qty,t.support);BOT.send(f"💼 {n} 등록 · {p.entry:,.0f} · 방어 {p.stop:,.0f}",chat,buttons=self.position_buttons(c));return
        if txt.startswith("/매도 "):
            parts=txt.split();price=num(parts[-1]);stock=" ".join(parts[1:-1]);c,n=MASTER.resolve(stock)
            if c in POSITIONS.data and price>0:r=POSITIONS.close(c,price,"직접입력 매도");BOT.send(f"✅ {r['name']} 종료 {r['return_pct']:+.2f}%",chat);return
        BOT.send("명령: /메뉴 /상태 /종목발굴 /추가 종목 /삭제 종목 /매수 종목 가격 [수량] /매도 종목 가격",chat,buttons=self.main_buttons())
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
                def due(hhmm):
                    h,m=map(int,hhmm.split(":"));return n.hour*60+n.minute>=h*60+m
                # Morning brief: late start is allowed only until noon; after that it is stale.
                if due(SETTINGS.morning_brief) and n.hour<12 and self.brief_sent.get("m")!=d:
                    WORKER.submit(lambda:BOT.send(self.morning(),buttons=self.main_buttons()));self.brief_sent["m"]=d
                # If Render restarts after the exact window, send the missing close/NXT brief once.
                if due(SETTINGS.close_brief) and self.brief_sent.get("c")!=d:
                    BOT.send_async(self.close_brief(),buttons=self.main_buttons());self.brief_sent["c"]=d
                if due(SETTINGS.nxt_brief) and self.brief_sent.get("n")!=d:
                    BOT.send_async(self.nxt_brief(),buttons=self.main_buttons());self.brief_sent["n"]=d
                if n.weekday()==4 and due(SETTINGS.weekly_brief) and self.brief_sent.get("w")!=d:
                    BOT.send_async(self.weekly(),buttons=self.main_buttons());self.brief_sent["w"]=d
            self.update_focus()
            health=[self.target_health(c) for c in STATE.vip_targets];STATE.runtime["stale_targets"]=sum(1 for h in health if h.get("status")=="DELAYED")
            if time.time()-self.last_outcome_check>=30:
                self.last_outcome_check=time.time();WORKER.submit(self.track_signal_outcomes)
            # Next-day prediction grading is lightweight but uses KIS REST; check every 10 min
            # after the regular close (or on weekends) until unresolved records are graded.
            if time.time()-self.nextday_last_eval>=600 and (n.weekday()>=5 or n.hour>15 or (n.hour==15 and n.minute>=35)):
                self.nextday_last_eval=time.time();WORKER.submit(self.evaluate_nextday_predictions)
            # 틱이 잠시 없어도 SIGNAL 만료/COOLDOWN 해제를 처리
            for c,t in list(STATE.targets.items()):
                if t.stage in (ST_SIGNAL,ST_BREAKOUT,ST_COOLDOWN):
                    st,_,reason=BRAIN.evaluate(c)
                    if st and st!=t.stage:STATE.set_stage(c,st,reason)
            time.sleep(5)
    def start(self):
        with self.start_lock:
            if self.started:return
            self.started=True
        WORKER.start();STATE.runtime["boot_phase"]="master_loading"
        try:MASTER.load()
        except Exception as e:STATE.runtime["last_error"]=f"master: {e}";log.exception("master load")
        for c in list(STATE.vip_targets):
            if c in MASTER.code_to_name:STATE.vip_targets[c]=MASTER.code_to_name[c]
            STATE.ensure_target(c,STATE.vip_targets[c])
        STATE.save_vip();POSITIONS.load();BOT.text_handler=self.handle_text;BOT.callback_handler=self.handle_cb

        # 재배포 직후 신호보다 데이터 복구가 먼저다.
        STATE.runtime["boot_phase"]="restoring_market_data"
        for c in list(STATE.vip_targets):
            try:self.sync_bars_for(c)
            except Exception as e:log.warning("initial minute restore %s: %s",c,e)
            try:self.sync_daily_for(c,True)
            except Exception as e:log.warning("initial daily restore %s: %s",c,e)
        STATE.runtime["ready"]=True;STATE.runtime["boot_phase"]="ready"

        if SETTINGS.telegram_token:threading.Thread(target=BOT.poll,daemon=True,name="telegram-poll").start();WORKER.submit(BOT.configure_miniapp_menu)
        threading.Thread(target=self.scheduler,daemon=True,name="scheduler").start()
        if SETTINGS.kis_app_key and SETTINGS.kis_app_secret:threading.Thread(target=KIS_CLIENT.stream,args=(self.on_tick,self.on_book),daemon=True,name="KIS-NXT-websocket").start()
        else:STATE.runtime["ws"]="disabled_missing_credentials"
        BOT.send_async(f"🚀 <b>명하 VIP V{SETTINGS.version}</b>\nREADY · VIP {len(STATE.vip_targets)}/{STATE.vip_limit} · 집중호가 {SETTINGS.focus_orderbook_slots}\n반복매매 쿨다운 {SETTINGS.cooldown_minutes}분\n자동주문 없음",buttons=self.main_buttons())

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
def _google_news_items(q,limit=6):return _fetch_google_news(q,limit)

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
            "score":round(t.score or 0,1),"grade":t.signal_grade,"note":t.note or "","signal_count_today":t.signal_count_today,
            "last_signal_at":t.last_signal_at,
            "projected_volume_ratio":round(t.projected_volume_ratio or 0,2),
            "ma5d":round(t.ma5d or 0,2),"ma20d":round(t.ma20d or 0,2),"daily_trend":t.daily_trend,
            "orderbook_imbalance":round(ob.imbalance,2) if ob else None,
            "best_ask":round(ob.best_ask,2) if ob else None,"best_bid":round(ob.best_bid,2) if ob else None,
            "last_tick_at":STATE.last_tick_at.get(c).isoformat() if STATE.last_tick_at.get(c) else None,"data_health":APP.target_health(c)})
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
    bars=normalize_intraday_bars(bars);p=POSITIONS.data.get(code);ob=STATE.books.get(code)
    return _no_store(make_response(jsonify({"ok":True,"code":code,"name":STATE.vip_targets.get(code,code),
        "state":{"stage":t.stage,"stage_label":STATE_LABEL.get(t.stage,t.stage),"price":t.last_price,"vwap":t.vwap,
                 "support":t.support,"resistance":t.resistance,"strength":t.trade_strength,"volume_ratio":t.volume_ratio,
                 "pullback_pct":t.pullback_pct,"score":t.score,"grade":t.signal_grade,"note":t.note,
                 "projected_volume_ratio":t.projected_volume_ratio,
                 "ma5d":t.ma5d,"ma20d":t.ma20d,"daily_trend":t.daily_trend},
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
    STATE.vip_targets[c]=n;STATE.ensure_target(c,n);STATE.save_vip();WORKER.submit(APP.sync_bars_for,c);WORKER.submit(APP.sync_daily_for,c,True);APP.update_focus()
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
        "news":[{"title":r.get("title",""),"published":r.get("published"),"time_text":r.get("time_text") or r.get("published") or "시간정보 없음","link":r.get("link","")} for r in rows]})))


@web.post("/api/miniapp/stock/<code>/nextday")
def miniapp_nextday(code):
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    code,t=_mini_target(code)
    if not t:return jsonify({"ok":False,"error":"not_found"}),404
    force=bool((_mini_json() or {}).get("force"))
    result=APP.nextday_analysis(code,force=force)
    status=200
    if not result.get("ok"):
        if result.get("error")=="market_protection":status=423
        elif result.get("error")=="already_running":status=409
        elif result.get("error")=="data_not_ready":status=422
        else:status=500
    return _no_store(make_response(jsonify(result),status))


@web.get("/api/miniapp/nextday/stats")
def miniapp_nextday_stats():
    if not _mini_auth():return jsonify({"ok":False,"error":"telegram_auth_required"}),401
    return _no_store(make_response(jsonify({"ok":True,"stats":APP.nextday_stats(),"server_time":now().isoformat()})))

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
