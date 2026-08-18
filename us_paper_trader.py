from __future__ import annotations

import json
import math
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, time as dtime, date
from pathlib import Path
from statistics import median
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
KST = ZoneInfo("Asia/Seoul")


def _f(v, default=0.0):
    try:
        if v is None:return default
        return float(v)
    except Exception:
        return default


def _i(v, default=0):
    try:return int(float(v))
    except Exception:return default


def _pct(new, old):
    return (new/old-1.0)*100.0 if old else 0.0


def _clamp(v, lo, hi):
    return max(lo,min(hi,v))


@dataclass
class USCandidate:
    symbol:str
    name:str
    price:float
    prev_close:float
    gap_pct:float
    rvol:float
    vwap:float
    atr:float
    premarket_high:float
    premarket_low:float
    day_high:float
    day_low:float
    avg_dollar_volume:float
    score:float
    setup:str
    updated_at:str

    def to_dict(self):
        return self.__dict__.copy()


class USPaperService:
    """
    미국장 전용 저부하 Paper Trading 엔진.

    설계 원칙
    - 기존 KR/NXT 엔진과 분리된 독립 스레드.
    - 프리마켓: 분석만. 실제 가상체결은 미국 정규장 09:30~16:00 ET만.
    - DST는 America/New_York zoneinfo가 자동 처리.
    - 전체 미국 종목을 실시간 감시하지 않고, 설정된 유동성 Universe를 저빈도로 스캔한 뒤
      상위 후보만 1분봉으로 정밀감시.
    - Gap / RVOL / Premarket H/L / VWAP / ATR / 시간대별 전략을 사용.
    - 실제 증권사 주문 API 호출 없음.
    """

    VERSION="1.1.0"

    DEFAULT_UNIVERSE=(
        "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AMD","AVGO","NFLX",
        "PLTR","CRM","ORCL","MU","QCOM","ARM","INTC","JPM","BAC","GS",
        "XOM","CVX","LLY","UNH","COST","WMT","HD","MCD","CAT","GE","BA"
    )

    def __init__(
        self, path:Path, now_fn, yf_module=None,
        universe=None, candidate_limit=12,
        scan_seconds=300, focus_seconds=60,
        min_price=5.0, min_avg_dollar_volume=50_000_000.0,
        min_rvol=1.20, min_gap_pct=0.40, max_gap_pct=10.0,
        max_positions=3, max_position_pct=25.0, max_daily_loss_pct=2.0,
        risk_per_trade_pct=0.60,
        buy_fee_pct=0.01, sell_fee_pct=0.01, fx_cost_pct=0.05,
        base_slippage_pct=0.03, max_slippage_pct=0.40,
        execution_poll_seconds=5, pending_expire_seconds=120, stale_quote_seconds=120,
        save_callback=None, load_callback=None
    ):
        self.path=Path(path);self.now_fn=now_fn;self.yf=yf_module
        self.universe=tuple(dict.fromkeys(universe or self.DEFAULT_UNIVERSE))
        self.candidate_limit=max(3,int(candidate_limit))
        self.scan_seconds=max(120,int(scan_seconds))
        self.focus_seconds=max(30,int(focus_seconds))
        self.min_price=float(min_price)
        self.min_avg_dollar_volume=float(min_avg_dollar_volume)
        self.min_rvol=float(min_rvol)
        self.min_gap_pct=float(min_gap_pct)
        self.max_gap_pct=float(max_gap_pct)
        self.max_positions=max(1,int(max_positions))
        self.max_position_pct=float(max_position_pct)
        self.max_daily_loss_pct=float(max_daily_loss_pct)
        self.risk_per_trade_pct=float(risk_per_trade_pct)
        self.buy_fee_pct=float(buy_fee_pct);self.sell_fee_pct=float(sell_fee_pct)
        self.fx_cost_pct=float(fx_cost_pct)
        self.base_slippage_pct=float(base_slippage_pct);self.max_slippage_pct=float(max_slippage_pct)
        self.execution_poll_seconds=max(2,int(execution_poll_seconds))
        self.pending_expire_seconds=max(15,int(pending_expire_seconds))
        self.stale_quote_seconds=max(30,int(stale_quote_seconds))
        self.save_callback=save_callback;self.load_callback=load_callback

        self.lock=threading.RLock()
        self.candidates={}
        self.last_scan_at=None;self.last_focus_at=None;self.last_error=None
        self.running=False
        self.market_status="OFF"
        self.fx_usdkrw=0.0
        self.fx_last_refresh=0.0
        self.daily_cache={}
        self.daily_cache_at=0.0
        self.state={
            "version":self.VERSION,"season":0,"active":False,"paused":False,
            "capital_initial_krw":0.0,"capital_initial_usd":0.0,
            "cash_usd":0.0,"duration_days":0,"start_date":None,"end_date":None,
            "last_trade_date":None,"day_start_equity_usd":0.0,
            "positions":{},"pending_orders":{},"trades":[],"logs":[],
            "equity_curve":[],"season_history":[],"manual_interventions":0,
            "aftermarket_context":{},"cancelled_order_count":0,"stale_quote_count":0,
        }
        self._load()
        self.state.setdefault("aftermarket_context",{})
        self.state.setdefault("cancelled_order_count",0)
        self.state.setdefault("stale_quote_count",0)

    def _now_et(self):
        n=self.now_fn()
        if n.tzinfo is None:n=n.replace(tzinfo=KST)
        return n.astimezone(NY)

    def _load(self):
        raw=None
        try:raw=self.load_callback() if self.load_callback else None
        except Exception:raw=None
        if not isinstance(raw,dict):
            try:raw=json.loads(self.path.read_text(encoding="utf-8")) if self.path.exists() else None
            except Exception:raw=None
        if isinstance(raw,dict):self.state.update(raw)
        self.state["version"]=self.VERSION

    def _save(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        tmp=self.path.with_suffix(self.path.suffix+".tmp")
        tmp.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding="utf-8")
        tmp.replace(self.path)
        if self.save_callback:
            try:self.save_callback(self.state)
            except Exception:pass

    def _log(self,action,**kw):
        self.state["logs"].append({"time":self.now_fn().isoformat(),"action":action,**kw})
        self.state["logs"]=self.state["logs"][-3000:]

    @staticmethod
    def _observed(d):
        if d.weekday()==5:return d-timedelta(days=1)
        if d.weekday()==6:return d+timedelta(days=1)
        return d

    @staticmethod
    def _easter(year):
        # Anonymous Gregorian algorithm.
        a=year%19;b=year//100;c=year%100;d=b//4;e=b%4
        f=(b+8)//25;g=(b-f+1)//3;h=(19*a+b-d-g+15)%30
        i=c//4;k=c%4;l=(32+2*e+2*i-h-k)%7;m=(a+11*h+22*l)//451
        month=(h+l-7*m+114)//31;day=((h+l-7*m+114)%31)+1
        return date(year,month,day)

    @classmethod
    def _is_market_holiday(cls,d):
        y=d.year
        # NYSE recurring full-day holidays. One-off closures are intentionally not guessed.
        holidays={
            cls._observed(date(y,1,1)),
            # MLK: third Monday Jan
            date(y,1,1)+timedelta(days=(0-date(y,1,1).weekday())%7+14),
            # Presidents: third Monday Feb
            date(y,2,1)+timedelta(days=(0-date(y,2,1).weekday())%7+14),
            cls._easter(y)-timedelta(days=2), # Good Friday
            # Memorial: last Monday May
            date(y,5,31)-timedelta(days=(date(y,5,31).weekday()-0)%7),
            cls._observed(date(y,6,19)),
            cls._observed(date(y,7,4)),
            # Labor: first Monday Sep
            date(y,9,1)+timedelta(days=(0-date(y,9,1).weekday())%7),
            # Thanksgiving: fourth Thursday Nov
            date(y,11,1)+timedelta(days=(3-date(y,11,1).weekday())%7+21),
            cls._observed(date(y,12,25)),
        }
        return d in holidays

    @classmethod
    def _regular_close(cls,d):
        # Common NYSE early-close days. If an exchange announces an exception,
        # no-data protection still prevents artificial fills.
        thanksgiving=date(d.year,11,1)+timedelta(days=(3-date(d.year,11,1).weekday())%7+21)
        day_after=thanksgiving+timedelta(days=1)
        july3=date(d.year,7,3)
        christmas_eve=date(d.year,12,24)
        if d in {day_after,july3,christmas_eve} and d.weekday()<5 and not cls._is_market_holiday(d):
            return dtime(13,0)
        return dtime(16,0)

    @classmethod
    def _session(cls,et):
        """US/Eastern session classification with DST/major holiday awareness."""
        if et.weekday()>=5 or cls._is_market_holiday(et.date()):return "CLOSED"
        t=et.timetz().replace(tzinfo=None);close=cls._regular_close(et.date())
        if dtime(4,0)<=t<dtime(9,30):return "PREMARKET"
        if dtime(9,30)<=t<close:return "REGULAR"
        if close<=t<dtime(20,0):return "AFTER"
        return "CLOSED"

    @staticmethod
    def _business_end(start,days):
        d=start;n=1
        while n<days:
            d+=timedelta(days=1)
            if d.weekday()<5:n+=1
        return d

    def _fx(self,force=False):
        """USD/KRW cached for 15 minutes so 3-second dashboard polling never creates market-data load."""
        if self.fx_usdkrw>0 and not force and time.time()-self.fx_last_refresh<900:
            return self.fx_usdkrw
        if not self.yf:return self.fx_usdkrw or 1350.0
        try:
            d=self.yf.Ticker("KRW=X").history(period="2d",interval="1h",auto_adjust=False)
            if d is not None and len(d):
                v=_f(d["Close"].dropna().iloc[-1])
                if v>500:
                    self.fx_usdkrw=v;self.fx_last_refresh=time.time()
        except Exception:pass
        return self.fx_usdkrw or 1350.0

    def start_new_season(self,capital_krw,duration_days):
        cap=float(capital_krw);days=int(duration_days)
        if cap<=0:raise ValueError("투자금은 0원보다 커야 합니다.")
        if days<1 or days>260:raise ValueError("투자기간은 1~260 영업일이어야 합니다.")
        with self.lock:
            if self.state.get("positions") or self.state.get("pending_orders"):
                raise RuntimeError("미국 가상보유/대기주문을 먼저 정리해야 새 시즌을 시작할 수 있습니다.")
            fx=self._fx()
            et=self._now_et();s=et.date()
            while s.weekday()>=5:s+=timedelta(days=1)
            # Season dates count actual US trading days, not just weekdays.
            e=s;n=1
            while n<days:
                e+=timedelta(days=1)
                if e.weekday()<5 and not self._is_market_holiday(e):n+=1
            if self.state.get("capital_initial_krw",0)>0:
                self.state["season_history"].append({
                    "season":self.state.get("season"),
                    "closed_at":self.now_fn().isoformat(),
                    "capital_initial_krw":self.state.get("capital_initial_krw"),
                    "trades":self.state.get("trades",[]),
                })
                self.state["season_history"]=self.state["season_history"][-50:]
            usd=cap/fx
            self.state.update({
                "season":int(self.state.get("season",0))+1,"active":True,"paused":False,
                "capital_initial_krw":cap,"capital_initial_usd":usd,"cash_usd":usd,
                "duration_days":days,"start_date":str(s),"end_date":str(e),
                "last_trade_date":None,"day_start_equity_usd":usd,
                "positions":{},"pending_orders":{},"trades":[],"logs":[],
                "equity_curve":[],"manual_interventions":0,
            })
            self._log("US_SEASON_START",capital_krw=cap,capital_usd=round(usd,2),fx=fx,days=days)
            self._save()
            return self.summary()

    def pause(self):
        with self.lock:self.state["paused"]=True;self._log("PAUSE");self._save()
    def resume(self):
        with self.lock:self.state["paused"]=False;self._log("RESUME");self._save()

    def _download(self,symbols,period,interval,prepost=True):
        if not self.yf or not symbols:return None
        try:
            return self.yf.download(
                list(symbols),period=period,interval=interval,prepost=prepost,
                group_by="ticker",auto_adjust=False,threads=False,progress=False
            )
        except Exception as e:
            self.last_error=f"yfinance: {e}"
            return None

    @staticmethod
    def _frame(data,symbol):
        if data is None:return None
        try:
            cols=getattr(data,"columns",None)
            if getattr(cols,"nlevels",1)>=2:
                f=data[symbol].dropna(how="all")
            else:
                f=data.dropna(how="all")
            return f if f is not None and len(f) else None
        except Exception:return None

    @staticmethod
    def _atr14(daily):
        if daily is None or len(daily)<15:return 0.0
        trs=[]
        for i in range(1,len(daily)):
            hi=_f(daily["High"].iloc[i]);lo=_f(daily["Low"].iloc[i]);pc=_f(daily["Close"].iloc[i-1])
            if min(hi,lo,pc)<=0:continue
            trs.append(max(hi-lo,abs(hi-pc),abs(lo-pc)))
        return sum(trs[-14:])/len(trs[-14:]) if trs[-14:] else 0.0

    def _feature_one(self,symbol,intra,daily,et):
        if intra is None or daily is None or len(daily)<15 or len(intra)<2:return None
        try:
            # yfinance timestamps are tz-aware for US symbols.
            idx=intra.index
            local=idx.tz_convert(NY) if getattr(idx,"tz",None) else idx.tz_localize(NY)
            f=intra.copy();f.index=local
            today=f[f.index.date==et.date()]
            if today.empty:return None
            price=_f(today["Close"].dropna().iloc[-1])
            if price<self.min_price:return None

            prev_close=0.0
            try:
                dd=daily.dropna(subset=["Close"])
                prev_close=_f(dd["Close"].iloc[-2] if str(dd.index[-1].date())==str(et.date()) and len(dd)>=2 else dd["Close"].iloc[-1])
            except Exception:pass
            if prev_close<=0:return None

            pre=today[(today.index.time>=dtime(4,0))&(today.index.time<dtime(9,30))]
            reg=today[(today.index.time>=dtime(9,30))&(today.index.time<dtime(16,0))]
            pmh=_f(pre["High"].max()) if len(pre) else 0.0
            pml=_f(pre["Low"].min()) if len(pre) else 0.0

            base=reg if len(reg) else today
            vol=base["Volume"].fillna(0)
            typ=(base["High"].fillna(price)+base["Low"].fillna(price)+base["Close"].fillna(price))/3.0
            vwap=float((typ*vol).sum()/vol.sum()) if float(vol.sum())>0 else price

            atr=self._atr14(daily)
            day_high=_f(base["High"].max());day_low=_f(base["Low"].min())
            gap=_pct(price if self._session(et)=="PREMARKET" else (_f(reg["Open"].iloc[0]) if len(reg) else price),prev_close)

            # Time-adjusted RVOL approximation:
            # today's regular accumulated volume vs average daily volume * elapsed regular-session fraction.
            avgvol=0.0
            try:
                vols=daily["Volume"].dropna().astype(float)
                avgvol=float(vols.iloc[-21:-1].median() if len(vols)>=21 else vols.tail(10).median())
            except Exception:pass
            if self._session(et)=="REGULAR":
                elapsed=max(1,(et.hour*60+et.minute)-(9*60+30))
                frac=_clamp(elapsed/390.0,1/390,1.0)
                expected=max(1.0,avgvol*frac)
                rvol=float(reg["Volume"].fillna(0).sum())/expected if len(reg) else 0.0
            elif self._session(et)=="PREMARKET":
                # Premarket volume is naturally thinner; scale with 15% of normal daily volume by 09:30.
                elapsed=max(1,(et.hour*60+et.minute)-4*60)
                frac=_clamp(elapsed/330.0,1/330,1.0)
                expected=max(1.0,avgvol*0.15*frac)
                rvol=float(pre["Volume"].fillna(0).sum())/expected if len(pre) else 0.0
            else:rvol=0.0

            avg_dollar=avgvol*prev_close
            if avg_dollar<self.min_avg_dollar_volume:return None

            # Long-only initial experiment. Avoid huge gap/chasing.
            gap_abs=abs(gap)
            if gap_abs<self.min_gap_pct or gap_abs>self.max_gap_pct:return None

            setup="PREMARKET_SCAN"
            score=50.0
            # Previous session after-hours context is informational/secondary only.
            ah=(self.state.get("aftermarket_context") or {}).get(symbol) or {}
            try:
                ah_date=date.fromisoformat(str(ah.get("date")))
                if ah_date < et.date():
                    ah_gap=_f(ah.get("gap_pct")); ah_vol=_f(ah.get("volume"))
                    if ah_gap>=0.5:score+=min(6.0,ah_gap*1.5)
                    elif ah_gap<=-0.5:score-=min(8.0,abs(ah_gap)*1.5)
                    if ah_vol>0:score+=2.0
            except Exception:pass
            score+=_clamp((rvol-1.0)*18,-8,28)
            score+=12 if gap>0 else -12
            if pmh>0:
                dist=_pct(price,pmh)
                if -1.2<=dist<=0.4:score+=12
                elif dist>1.0:score-=10
            if vwap>0 and price>=vwap:score+=10
            elif vwap>0:score-=8
            if atr>0:
                atrpct=atr/price*100
                if 1.0<=atrpct<=6.0:score+=8
                elif atrpct>8:score-=8

            if self._session(et)=="REGULAR":
                # Setup classification is time-aware.
                mins=(et.hour*60+et.minute)-(9*60+30)
                if mins<45:setup="OPENING_RANGE"
                elif mins<240:setup="VWAP_PULLBACK"
                else:setup="LATE_TREND"

            return USCandidate(
                symbol=symbol,name=symbol,price=price,prev_close=prev_close,gap_pct=gap,
                rvol=rvol,vwap=vwap,atr=atr,premarket_high=pmh,premarket_low=pml,
                day_high=day_high,day_low=day_low,avg_dollar_volume=avg_dollar,
                score=_clamp(score,0,100),setup=setup,updated_at=self.now_fn().isoformat()
            )
        except Exception as e:
            self.last_error=f"{symbol}: {e}"
            return None

    def _daily_frames(self,symbols,force=False):
        """Daily OHLC is refreshed at most every 30 minutes; focus loop reuses it."""
        if force or not self.daily_cache or time.time()-self.daily_cache_at>=1800:
            d=self._download(self.universe,"3mo","1d",False)
            if d is not None:
                cache={}
                for x in self.universe:
                    f=self._frame(d,x)
                    if f is not None:cache[x]=f.copy()
                if cache:
                    self.daily_cache=cache;self.daily_cache_at=time.time()
        return {x:self.daily_cache.get(x) for x in symbols}

    def scan_universe(self):
        if not self.yf:return
        et=self._now_et();session=self._session(et)
        if session not in ("PREMARKET","REGULAR"):return
        intra=self._download(self.universe,"2d","5m",True)
        daily_map=self._daily_frames(self.universe)
        rows=[]
        for s in self.universe:
            x=self._feature_one(s,self._frame(intra,s),daily_map.get(s),et)
            if x:rows.append(x)
        rows.sort(key=lambda x:x.score,reverse=True)
        with self.lock:
            self.candidates={x.symbol:x for x in rows[:self.candidate_limit]}
            self.last_scan_at=self.now_fn().isoformat()
            self.market_status=session

    def scan_aftermarket(self):
        """Collect after-hours context for the next US session; never trades in AFTER."""
        if not self.yf:return
        et=self._now_et()
        if self._session(et)!="AFTER":return
        syms=list(dict.fromkeys(self._focused_symbols() or list(self.universe)[:self.candidate_limit]))
        intra=self._download(syms,"2d","5m",True)
        updates={}
        for s in syms:
            try:
                f=self._frame(intra,s)
                if f is None or len(f)==0:continue
                idx=f.index
                local=idx.tz_convert(NY) if getattr(idx,"tz",None) else idx.tz_localize(NY)
                x=f.copy();x.index=local
                today=x[x.index.date==et.date()]
                close=self._regular_close(et.date())
                reg=today[(today.index.time>=dtime(9,30))&(today.index.time<close)]
                aft=today[(today.index.time>=close)&(today.index.time<dtime(20,0))]
                if len(aft)==0:continue
                regular_close=_f(reg["Close"].dropna().iloc[-1]) if len(reg) else 0.0
                last=_f(aft["Close"].dropna().iloc[-1])
                if regular_close<=0 or last<=0:continue
                updates[s]={
                    "date":str(et.date()),"last":last,"gap_pct":_pct(last,regular_close),
                    "high":_f(aft["High"].max()),"low":_f(aft["Low"].min()),
                    "volume":_f(aft["Volume"].fillna(0).sum()),"updated_at":self.now_fn().isoformat(),
                }
            except Exception as e:
                self.last_error=f"after {s}: {e}"
        if updates:
            with self.lock:
                self.state.setdefault("aftermarket_context",{}).update(updates)
                # Keep state bounded to current universe only.
                self.state["aftermarket_context"]={k:v for k,v in self.state["aftermarket_context"].items() if k in self.universe}
                self._save()

    def _focused_symbols(self):
        with self.lock:
            syms=list(self.candidates)[:self.candidate_limit]
            syms+=list(self.state.get("positions",{}))
            syms+=list(self.state.get("pending_orders",{}))
        return list(dict.fromkeys(syms))

    def _slippage(self,c:USCandidate,side):
        s=self.base_slippage_pct
        if c.rvol>=2.5:s+=0.03
        if abs(c.gap_pct)>=5:s+=0.04
        if c.atr and c.price and c.atr/c.price*100>=5:s+=0.05
        if c.avg_dollar_volume<100_000_000:s+=0.05
        return min(self.max_slippage_pct,s)

    def _fill_price(self,c,side):
        sp=self._slippage(c,side)
        return c.price*(1+sp/100 if side=="BUY" else 1-sp/100),sp

    def _entry_signal(self,c:USCandidate,intra):
        et=self._now_et()
        if self._session(et)!="REGULAR":return False,"PREMARKET_ANALYSIS"
        if c.gap_pct<=0 or c.rvol<self.min_rvol or c.score<70:return False,"FILTER"
        if c.atr<=0 or c.vwap<=0:return False,"DATA"
        if c.price<c.vwap:return False,"BELOW_VWAP"

        # No blind chasing: reject if > 0.8 ATR above VWAP or too extended beyond PM high.
        if c.price-c.vwap>0.8*c.atr:return False,"EXTENDED_VWAP"
        if c.premarket_high>0 and c.price>c.premarket_high+0.45*c.atr:return False,"EXTENDED_PM_HIGH"

        try:
            idx=intra.index
            local=idx.tz_convert(NY) if getattr(idx,"tz",None) else idx.tz_localize(NY)
            f=intra.copy();f.index=local
            close=self._regular_close(et.date())
            reg=f[(f.index.date==et.date())&(f.index.time>=dtime(9,30))&(f.index.time<close)]
            # Decisions use completed 5-minute bars only; the forming bar may repaint.
            if len(reg) and et < reg.index[-1].to_pydatetime()+timedelta(minutes=5):reg=reg.iloc[:-1]
            if len(reg)<4:return False,"BARS"
            last=reg.tail(5)
            closes=[_f(x) for x in last["Close"]]
            lows=[_f(x) for x in last["Low"]]
            highs=[_f(x) for x in last["High"]]
            rising=closes[-1]>closes[-2]>=closes[-3]
            touched_vwap=min(lows)<=c.vwap*1.003 and closes[-1]>=c.vwap*1.001
            opening_break=False
            if len(reg)>=3 and c.premarket_high>0:
                opening_break=closes[-1]>=c.premarket_high and closes[-2]<c.premarket_high*1.003
            recent_high=max(highs[:-1]) if len(highs)>1 else highs[-1]
            late_break=closes[-1]>=recent_high and rising

            mins=(et.hour*60+et.minute)-(9*60+30)
            if mins<45 and opening_break:return True,"PM_HIGH_BREAK"
            if 20<=mins<=300 and touched_vwap and rising:return True,"VWAP_RECLAIM"
            if mins>240 and late_break and c.rvol>=1.4:return True,"LATE_TREND_BREAK"
        except Exception:return False,"PARSE"
        return False,"WAIT"

    def _equity_usd(self,prices=None):
        prices=prices or {s:c.price for s,c in self.candidates.items()}
        v=float(self.state.get("cash_usd",0))
        for s,p in self.state.get("positions",{}).items():
            px=float(prices.get(s) or p.get("last_price") or p.get("entry"))
            # liquidation value after sell cost + slippage approximation
            sp=self.base_slippage_pct
            gross=px*(1-sp/100)*int(p["qty"])
            cost=gross*(self.sell_fee_pct+self.fx_cost_pct)/100
            v+=max(0,gross-cost)
        return v

    def _queue_buy(self,c,reason):
        if c.symbol in self.state["positions"] or c.symbol in self.state["pending_orders"]:return
        eq=self._equity_usd()
        day_start=float(self.state.get("day_start_equity_usd") or eq)
        if day_start and _pct(eq,day_start)<=-abs(self.max_daily_loss_pct):return
        if len(self.state["positions"])>=self.max_positions:return

        # ATR-risk position sizing with hard portfolio cap.
        stop=max(c.vwap-0.25*c.atr,c.price-1.15*c.atr)
        risk_per_share=max(0.01,c.price-stop)
        risk_budget=eq*self.risk_per_trade_pct/100
        qty_risk=int(risk_budget//risk_per_share)
        qty_cap=int((eq*self.max_position_pct/100)//max(c.price,0.01))
        qty=max(0,min(qty_risk,qty_cap))
        if qty<=0:return
        self.state["pending_orders"][c.symbol]={
            "side":"BUY","symbol":c.symbol,"qty":qty,"reason":reason,
            "signal_price":c.price,"created_at":self.now_fn().isoformat(),
            "execute_after":(self.now_fn()+timedelta(seconds=self.execution_poll_seconds)).isoformat(),
            "expires_at":(self.now_fn()+timedelta(seconds=self.pending_expire_seconds)).isoformat(),
            "stop_hint":stop,
        }
        self._log("US_ORDER_QUEUED",side="BUY",symbol=c.symbol,qty=qty,reason=reason)

    def _process_pending(self,c):
        o=self.state.get("pending_orders",{}).get(c.symbol)
        if not o:return
        try:
            if self.now_fn()<datetime.fromisoformat(o["execute_after"]):return
            if self.now_fn()>datetime.fromisoformat(o.get("expires_at") or o["execute_after"]):
                self.state["pending_orders"].pop(c.symbol,None)
                self.state["cancelled_order_count"]=int(self.state.get("cancelled_order_count",0))+1
                self._log("US_ORDER_CANCELLED",symbol=c.symbol,reason="PENDING_EXPIRED")
                return
        except Exception:pass
        side=o["side"];fill,sp=self._fill_price(c,side);qty=int(o["qty"])
        if side=="BUY":
            gross=fill*qty;cost=gross*(self.buy_fee_pct+self.fx_cost_pct)/100
            if gross+cost>self.state["cash_usd"]:
                qty=int(self.state["cash_usd"]//(fill*(1+(self.buy_fee_pct+self.fx_cost_pct)/100)))
                if qty<=0:self.state["pending_orders"].pop(c.symbol,None);return
                gross=fill*qty;cost=gross*(self.buy_fee_pct+self.fx_cost_pct)/100
            stop=max(_f(o.get("stop_hint")),fill-1.15*c.atr,c.vwap-0.25*c.atr)
            self.state["cash_usd"]-=gross+cost
            self.state["positions"][c.symbol]={
                "symbol":c.symbol,"name":c.name,"qty":qty,"entry":fill,"stop":stop,
                "highest":fill,"opened_at":self.now_fn().isoformat(),"buy_cost":cost,
                "entry_reason":o["reason"],"entry_slippage_pct":sp,"last_price":c.price,
                "atr_entry":c.atr,
            }
            self._log("US_BUY_FILLED",symbol=c.symbol,qty=qty,fill=round(fill,4),reason=o["reason"])
        else:
            self._sell(c.symbol,c,o["reason"])
        self.state["pending_orders"].pop(c.symbol,None)

    def _sell(self,symbol,c,reason):
        p=self.state["positions"].get(symbol)
        if not p:return None
        fill,sp=self._fill_price(c,"SELL");qty=int(p["qty"])
        gross=fill*qty;cost=gross*(self.sell_fee_pct+self.fx_cost_pct)/100
        self.state["cash_usd"]+=gross-cost
        invested=float(p["entry"])*qty+float(p.get("buy_cost") or 0)
        net=(gross-cost)-invested
        ret=net/invested*100 if invested else 0
        r={
            "symbol":symbol,"name":p.get("name",symbol),"qty":qty,
            "entry":round(float(p["entry"]),4),"exit":round(fill,4),
            "opened_at":p["opened_at"],"closed_at":self.now_fn().isoformat(),
            "reason":reason,"net_pnl_usd":round(net,2),"net_return_pct":round(ret,3),
            "sell_cost_usd":round(cost,2),"sell_slippage_pct":round(sp,4),
        }
        self.state["trades"].append(r);self.state["positions"].pop(symbol,None)
        self._log("US_SELL_FILLED",**r)
        return r

    def _manage_position(self,c):
        p=self.state["positions"].get(c.symbol)
        if not p:return
        p["last_price"]=c.price;p["highest"]=max(float(p.get("highest") or c.price),c.price)
        entry=float(p["entry"]);gain=_pct(c.price,entry)
        atr=max(c.atr,float(p.get("atr_entry") or 0))
        stop=float(p["stop"])

        # ATR + VWAP + profit-protecting trailing stop.
        if c.price>entry:
            stop=max(stop,entry*1.001)
            if c.vwap<c.price:stop=max(stop,c.vwap-0.15*atr)
            if gain>=2:stop=max(stop,p["highest"]-0.85*atr)
            if gain>=4:stop=max(stop,p["highest"]-0.60*atr)
        p["stop"]=stop

        reasons=[]
        exit_score=0
        if c.price<=stop:exit_score+=5;reasons.append("ATR_STOP")
        if c.vwap and c.price<c.vwap:exit_score+=2;reasons.append("VWAP_LOSS")
        if c.rvol>=1.5 and c.price<p["highest"]-0.65*atr:exit_score+=2;reasons.append("SELL_PRESSURE")
        if c.premarket_low and c.price<c.premarket_low:exit_score+=3;reasons.append("PM_LOW_BREAK")
        et=self._now_et()
        close=self._regular_close(et.date())
        close_dt=et.replace(hour=close.hour,minute=close.minute,second=0,microsecond=0)
        if et>=close_dt-timedelta(minutes=10):exit_score+=5;reasons.append("EOD")
        if exit_score>=5 and c.symbol not in self.state["pending_orders"]:
            self.state["pending_orders"][c.symbol]={
                "side":"SELL","symbol":c.symbol,"qty":int(p["qty"]),
                "reason":"AUTO_EXIT:"+"/".join(reasons[:3]),
                "signal_price":c.price,"created_at":self.now_fn().isoformat(),
                "execute_after":(self.now_fn()+timedelta(seconds=self.execution_poll_seconds)).isoformat(),
                "expires_at":(self.now_fn()+timedelta(seconds=self.pending_expire_seconds)).isoformat(),
            }
            self._log("US_ORDER_QUEUED",side="SELL",symbol=c.symbol,reason=reasons)

    def execution_update(self):
        """Fast, pending-only execution loop. Keeps 2~5s execution semantics separate from strategy cadence."""
        if not self.yf:return
        et=self._now_et()
        if self._session(et)!="REGULAR":return
        with self.lock:
            syms=list(dict.fromkeys(list(self.state.get("pending_orders",{}))+list(self.state.get("positions",{}))))
        if not syms:return
        intra=self._download(syms,"1d","1m",True)
        for s in syms:
            try:
                f=self._frame(intra,s)
                if f is None or len(f)==0:continue
                idx=f.index
                local=idx.tz_convert(NY) if getattr(idx,"tz",None) else idx.tz_localize(NY)
                last_ts=local[-1].to_pydatetime()
                age=max(0.0,(et-last_ts).total_seconds())
                if age>self.stale_quote_seconds:
                    self.state["stale_quote_count"]=int(self.state.get("stale_quote_count",0))+1
                    self.last_error=f"stale quote {s}: {age:.0f}s"
                    continue
                px=_f(f["Close"].dropna().iloc[-1])
                base=self.candidates.get(s)
                if px<=0 or not base:continue
                c=replace(base,price=px,updated_at=self.now_fn().isoformat())
                with self.lock:self.candidates[s]=c
                self._process_pending(c)
                # Hard-stop/risk checks run on the fast loop for held positions.
                if s in self.state.get("positions",{}):self._manage_position(c)
            except Exception as e:self.last_error=f"execution {s}: {e}"
        with self.lock:self._save()

    def focus_update(self):
        if not self.yf:return
        et=self._now_et();session=self._session(et)
        self.market_status=session
        syms=self._focused_symbols()
        if session!="REGULAR" or not syms:return

        # Strategy decisions are intentionally 5-minute based to reduce noise/latency sensitivity.
        intra=self._download(syms,"2d","5m",True)
        daily_map=self._daily_frames(syms)
        for s in syms:
            c=self._feature_one(s,self._frame(intra,s),daily_map.get(s),et)
            if not c:continue
            with self.lock:self.candidates[s]=c
            if s in self.state["positions"]:
                self._manage_position(c)
            else:
                ok,why=self._entry_signal(c,self._frame(intra,s))
                if ok:self._queue_buy(c,why)

        # Day rollover and season end.
        with self.lock:
            today=str(et.date())
            if self.state.get("last_trade_date")!=today:
                self.state["last_trade_date"]=today
                self.state["day_start_equity_usd"]=self._equity_usd()
            end=str(self.state.get("end_date") or "9999-12-31")
            close=self._regular_close(et.date())
            close_dt=et.replace(hour=close.hour,minute=close.minute,second=0,microsecond=0)
            season_end=(today>end) or (today==end and et>=close_dt-timedelta(minutes=10))
            if season_end:
                for s in list(self.state["positions"]):
                    c=self.candidates.get(s)
                    if c:self._sell(s,c,"SEASON_END")
                self.state["pending_orders"]={}
                self.state["active"]=False;self._log("US_SEASON_COMPLETE")
            eq=self._equity_usd()
            ts=self.now_fn().replace(second=0,microsecond=0).isoformat()
            if not self.state["equity_curve"] or self.state["equity_curve"][-1]["time"]!=ts:
                self.state["equity_curve"].append({"time":ts,"equity_usd":round(eq,2)})
                self.state["equity_curve"]=self.state["equity_curve"][-5000:]
            self._save()

    def force_liquidate(self):
        with self.lock:
            rows=[]
            self.state["pending_orders"]={}
            for s in list(self.state["positions"]):
                c=self.candidates.get(s)
                if not c:
                    p=self.state["positions"][s]
                    c=USCandidate(s,s,float(p.get("last_price") or p["entry"]),0,0,0,0,float(p.get("atr_entry") or 0),0,0,0,0,0,0,"FORCE",self.now_fn().isoformat())
                r=self._sell(s,c,"USER_FORCE_EXIT")
                if r:rows.append(r)
            self.state["manual_interventions"]=int(self.state.get("manual_interventions",0))+1
            self._save();return rows

    def run(self):
        self.running=True
        next_scan=0.0;next_focus=0.0;next_execution=0.0;next_after=0.0
        while True:
            try:
                et=self._now_et();session=self._session(et);self.market_status=session
                if self.state.get("active") and not self.state.get("paused"):
                    now_ts=time.time()
                    if session in ("PREMARKET","REGULAR") and now_ts>=next_scan:
                        self.scan_universe();next_scan=now_ts+self.scan_seconds
                    if session=="REGULAR" and now_ts>=next_focus:
                        self.focus_update();next_focus=now_ts+self.focus_seconds
                    if session=="REGULAR" and now_ts>=next_execution:
                        self.execution_update();next_execution=now_ts+self.execution_poll_seconds
                    if session=="AFTER" and now_ts>=next_after:
                        self.scan_aftermarket();next_after=now_ts+self.scan_seconds
                time.sleep(2)
            except Exception as e:
                self.last_error=str(e);time.sleep(30)

    def summary(self):
        with self.lock:
            fx=self._fx()
            prices={s:c.price for s,c in self.candidates.items()}
            eq_usd=self._equity_usd(prices)
            ini_usd=float(self.state.get("capital_initial_usd") or 0)
            ini_krw=float(self.state.get("capital_initial_krw") or 0)
            eq_krw=eq_usd*fx
            tr=list(self.state.get("trades",[]))
            wins=[x for x in tr if _f(x.get("net_pnl_usd"))>0]
            losses=[x for x in tr if _f(x.get("net_pnl_usd"))<0]
            gp=sum(_f(x.get("net_pnl_usd")) for x in wins)
            gl=abs(sum(_f(x.get("net_pnl_usd")) for x in losses))
            pf=(gp/gl) if gl>0 else (None if gp<=0 else 999.0)
            peak=0.0;maxdd=0.0
            for x in self.state.get("equity_curve",[]):
                v=_f(x.get("equity_usd"));peak=max(peak,v)
                if peak:maxdd=min(maxdd,(v/peak-1)*100)

            pos=[]
            for s,p in self.state.get("positions",{}).items():
                px=float(prices.get(s) or p.get("last_price") or p["entry"])
                invested=float(p["entry"])*int(p["qty"])+float(p.get("buy_cost") or 0)
                liq=px*(1-self.base_slippage_pct/100)*int(p["qty"])
                liq-=liq*(self.sell_fee_pct+self.fx_cost_pct)/100
                net=liq-invested
                pos.append({**p,"current":px,"unrealized_pnl_usd":round(net,2),
                            "return_pct":round(net/invested*100,3) if invested else 0})

            return {
                "engine_version":self.VERSION,"enabled":bool(self.yf),"running":self.running,
                "market_status":self.market_status,"market_time_et":self._now_et().isoformat(),
                "active":bool(self.state.get("active")),"paused":bool(self.state.get("paused")),
                "season":self.state.get("season"),"capital_initial_krw":round(ini_krw,2),
                "capital_initial_usd":round(ini_usd,2),"cash_usd":round(float(self.state.get("cash_usd",0)),2),
                "equity_usd":round(eq_usd,2),"equity_krw":round(eq_krw,2),"usdkrw":round(fx,2),
                "strategy_return_pct":round((eq_usd/ini_usd-1)*100,3) if ini_usd else 0,
                "krw_return_pct":round((eq_krw/ini_krw-1)*100,3) if ini_krw else 0,
                "start_date":self.state.get("start_date"),"end_date":self.state.get("end_date"),
                "duration_days":int(self.state.get("duration_days") or 0),
                "positions":pos,"pending_orders":list(self.state.get("pending_orders",{}).values()),
                "trade_count":len(tr),"win_rate":round(len(wins)/len(tr)*100,1) if tr else None,
                "profit_factor":round(pf,2) if isinstance(pf,(int,float)) else None,
                "max_drawdown_pct":round(maxdd,3),
                "recent_trades":list(reversed(tr[-20:])),
                "candidates":[x.to_dict() for x in sorted(self.candidates.values(),key=lambda z:z.score,reverse=True)[:self.candidate_limit]],
                "last_scan_at":self.last_scan_at,"last_error":self.last_error,
                "manual_interventions":int(self.state.get("manual_interventions",0)),
                "cancelled_order_count":int(self.state.get("cancelled_order_count",0)),
                "stale_quote_count":int(self.state.get("stale_quote_count",0)),
                "aftermarket_context":self.state.get("aftermarket_context",{}),
                "strategy_note":"US V1.1: 프리마켓 Gap/RVOL → 5분봉 VWAP·PM H/L·ATR 의사결정 → pending 전용 빠른 체결루프. 애프터마켓은 다음 거래일 컨텍스트로만 사용하며 거래하지 않음.",
                "market_data_note":"Paper 단계는 yfinance 5분 전략데이터 + pending 시 1분 가격확인 기반입니다. stale quote는 체결 금지합니다. 실전 자동주문 전환 전에는 KIS 해외주식 실시간 시세/거래정지·LULD 상태/주문거부·부분체결 검증이 필요합니다.",
                "load_guard":{"universe":len(self.universe),"candidate_limit":self.candidate_limit,
                              "scan_seconds":self.scan_seconds,"focus_seconds":self.focus_seconds,
                              "execution_poll_seconds":self.execution_poll_seconds,
                              "pending_expire_seconds":self.pending_expire_seconds,
                              "stale_quote_seconds":self.stale_quote_seconds},
            }
