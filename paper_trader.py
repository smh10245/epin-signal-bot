from __future__ import annotations
import json, threading, math
from datetime import timedelta, datetime
from pathlib import Path


class PaperTradingEngine:
    """
    V2 실전형 포워드 테스트 엔진.
    - 실제 주문 API 호출 없음
    - 신호 시점 즉시체결 대신 지연 후 다음 실시간 틱으로 체결
    - 유동성/변동성 기반 동적 슬리피지
    - 분봉 거래량/호가잔량 기반 부분체결
    - 수수료/매도세금 포함
    - 보유 평가자산은 '지금 청산했을 때 받을 순현금' 기준
    - 기존 V1 paper_trading.json 상태와 하위호환
    """

    def __init__(
        self, path, now_fn,
        buy_fee_pct=0.01, sell_fee_pct=0.01, sell_tax_pct=0.20,
        slippage_pct=0.03,
        max_slippage_pct=0.50,
        latency_seconds=2.0,
        max_positions=3,
        max_position_pct=25.0,
        max_daily_loss_pct=2.0,
        min_entry_score=72.0,
        max_minute_participation_pct=3.0,
        max_book_participation_pct=10.0,
        min_fill_ratio=0.25,
        season_close_time="19:50",
        save_callback=None, load_callback=None
    ):
        self.path=Path(path); self.now=now_fn
        self.buy_fee_pct=float(buy_fee_pct)
        self.sell_fee_pct=float(sell_fee_pct)
        self.sell_tax_pct=float(sell_tax_pct)
        self.slippage_pct=max(0.0,float(slippage_pct))
        self.max_slippage_pct=max(self.slippage_pct,float(max_slippage_pct))
        self.latency_seconds=max(0.0,float(latency_seconds))
        self.max_positions=int(max_positions)
        self.max_position_pct=float(max_position_pct)
        self.max_daily_loss_pct=float(max_daily_loss_pct)
        self.min_entry_score=float(min_entry_score)
        self.max_minute_participation_pct=max(0.1,float(max_minute_participation_pct))
        self.max_book_participation_pct=max(0.1,float(max_book_participation_pct))
        self.min_fill_ratio=min(1.0,max(0.01,float(min_fill_ratio)))
        self.season_close_time=str(season_close_time or "19:50")
        self.save_callback=save_callback; self.load_callback=load_callback
        self.lock=threading.RLock()
        self.state={
            "version":"3.0.0","season":0,"active":False,"paused":False,
            "capital_initial":0.0,"cash":0.0,"duration_days":0,
            "start_date":None,"end_date":None,"last_trade_date":None,
            "day_start_equity":0.0,"positions":{},"pending_orders":{},
            "trades":[],"logs":[],"equity_curve":[],"season_history":[],
            "manual_interventions":0,"partial_fill_count":0,
            "cancelled_order_count":0
        }
        self._load()
        self.state["version"]="3.0.0"
        self.state.setdefault("pending_orders",{})
        self.state.setdefault("partial_fill_count",0)
        self.state.setdefault("cancelled_order_count",0)

    def _load(self):
        raw=None
        try: raw=self.load_callback() if self.load_callback else None
        except Exception: raw=None
        if not isinstance(raw,dict):
            try: raw=json.loads(self.path.read_text(encoding="utf-8")) if self.path.exists() else None
            except Exception: raw=None
        if isinstance(raw,dict): self.state.update(raw)

    def _save(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        tmp=self.path.with_suffix(self.path.suffix+".tmp")
        tmp.write_text(json.dumps(self.state,ensure_ascii=False,indent=2),encoding="utf-8")
        tmp.replace(self.path)
        if self.save_callback:
            try:self.save_callback(self.state)
            except Exception:pass

    def _log(self,action,**kw):
        self.state["logs"].append({"time":self.now().isoformat(),"action":action,**kw})
        self.state["logs"]=self.state["logs"][-3000:]

    def _business_end(self,start,days):
        d=start;n=1
        while n<days:
            d+=timedelta(days=1)
            if d.weekday()<5:n+=1
        return d

    def start_new_season(self,capital,duration_days):
        capital=float(capital); duration_days=int(duration_days)
        if capital<=0: raise ValueError("투자금은 0원보다 커야 합니다.")
        if duration_days<1 or duration_days>260: raise ValueError("투자기간은 1~260 영업일이어야 합니다.")
        with self.lock:
            if self.state.get("positions") or self.state.get("pending_orders"):
                raise RuntimeError("가상 보유종목/대기주문을 먼저 정리해야 새 시즌을 시작할 수 있습니다.")
            if self.state.get("capital_initial",0)>0:
                self.state["season_history"].append({
                    "season":self.state.get("season"),
                    "capital_initial":self.state.get("capital_initial"),
                    "cash":self.state.get("cash"),
                    "trades":self.state.get("trades",[]),
                    "closed_at":self.now().isoformat()
                })
                self.state["season_history"]=self.state["season_history"][-50:]
            s=self.now().date()
            while s.weekday()>=5:s+=timedelta(days=1)
            e=self._business_end(s,duration_days)
            self.state.update({
                "season":int(self.state.get("season",0))+1,"active":True,"paused":False,
                "capital_initial":capital,"cash":capital,"duration_days":duration_days,
                "start_date":str(s),"end_date":str(e),"last_trade_date":None,
                "day_start_equity":capital,"positions":{},"pending_orders":{},
                "trades":[],"logs":[],"equity_curve":[],"manual_interventions":0,
                "partial_fill_count":0,"cancelled_order_count":0
            })
            self._log("SEASON_START",capital=capital,duration_days=duration_days,
                      execution_model="latency+dynamic_slippage+partial_fill+liquidation_equity")
            self._save()
            return self.summary({})

    def tracked_codes(self):
        """VIP 여부와 무관하게 체결/청산을 위해 계속 실시간 구독해야 하는 종목."""
        with self.lock:
            return set(self.state.get("positions",{})) | set(self.state.get("pending_orders",{}))

    def has_position(self,code):
        with self.lock:return str(code).zfill(6) in self.state.get("positions",{})

    def has_pending(self,code):
        with self.lock:return str(code).zfill(6) in self.state.get("pending_orders",{})

    def cancel_pending_buy(self,code,reason="VIP_REMOVED"):
        code=str(code).zfill(6)
        with self.lock:
            o=self.state.get("pending_orders",{}).get(code)
            if not o or o.get("side")!="BUY":return False
            self.state["pending_orders"].pop(code,None)
            self.state["cancelled_order_count"]=int(self.state.get("cancelled_order_count",0))+1
            self._log("ORDER_CANCELLED",side="BUY",code=code,reason=reason)
            self._save();return True

    def pause(self):
        with self.lock:
            self.state["paused"]=True; self._log("PAUSE"); self._save()

    def resume(self):
        with self.lock:
            self.state["paused"]=False; self._log("RESUME"); self._save()

    def _slippage_pct(self, side, metrics):
        """기본값에서 유동성/급등/호가상태가 나쁠수록 불리하게 확대."""
        m=metrics or {}
        slip=self.slippage_pct
        vr=max(0.0,float(m.get("volume_ratio") or 0))
        projected=max(0.0,float(m.get("projected_volume_ratio") or 0))
        imb=max(0.0,float(m.get("imbalance") or 1.0))
        day_gain=abs(float(m.get("day_gain_pct") or 0))
        extension=abs(float(m.get("breakout_extension_pct") or 0))

        # 급변/급등 구간 패널티
        if vr>=2.0 or projected>=2.0: slip+=0.03
        if vr>=4.0 or projected>=4.0: slip+=0.05
        if day_gain>=5.0: slip+=0.04
        if day_gain>=10.0: slip+=0.08
        if extension>=0.8: slip+=0.03

        # 매수는 매도호가가 얇을수록, 매도는 매수호가가 얇을수록 불리
        if side=="BUY":
            if imb<0.80: slip+=0.08
            elif imb<0.95: slip+=0.03
        else:
            if imb<0.80: slip+=0.08
            elif imb<0.95: slip+=0.03

        return min(self.max_slippage_pct,max(self.slippage_pct,slip))

    def _fill_price(self,market_price,side,metrics):
        sp=self._slippage_pct(side,metrics)
        mult=(1+sp/100.0) if side=="BUY" else (1-sp/100.0)
        return float(market_price)*mult,sp

    def _buy_fee(self,amount): return amount*self.buy_fee_pct/100.0
    def _sell_cost(self,amount): return amount*(self.sell_fee_pct+self.sell_tax_pct)/100.0

    def _liquidation_value(self,p,market_price,metrics=None):
        """현재 즉시 매도한다고 가정한 순현금 가치."""
        fill,_=self._fill_price(float(market_price),"SELL",metrics or {})
        gross=fill*int(p["qty"])
        return max(0.0,gross-self._sell_cost(gross))

    def _equity(self,prices,metrics_by_code=None):
        v=float(self.state.get("cash",0))
        metrics_by_code=metrics_by_code or {}
        for c,p in self.state.get("positions",{}).items():
            px=float(prices.get(c) or p.get("entry") or 0)
            v+=self._liquidation_value(p,px,metrics_by_code.get(c) or {})
        return v

    def _max_fill_qty(self,want_qty,metrics,side):
        """
        보수적 부분체결:
        - 현재 1분 누적 거래량의 일정 비율
        - 해당 방향 호가잔량의 일정 비율
        두 값 중 유효한 더 작은 값으로 제한.
        """
        want=max(0,int(want_qty))
        if want<=0:return 0
        m=metrics or {}
        caps=[]

        minute_vol=int(float(m.get("current_minute_volume") or 0))
        if minute_vol>0:
            caps.append(max(1,int(minute_vol*self.max_minute_participation_pct/100.0)))

        book_qty=int(float((m.get("total_ask") if side=="BUY" else m.get("total_bid")) or 0))
        if book_qty>0:
            caps.append(max(1,int(book_qty*self.max_book_participation_pct/100.0)))

        if not caps:return want
        cap=max(1,min(caps))
        filled=min(want,cap)
        # 너무 적게 채워지는 신호는 아예 취소해서 비현실적인 1~2주 체결 남발 방지
        if filled/want < self.min_fill_ratio:return 0
        return filled

    def _order_due(self,o):
        try:
            return self.now()>=datetime.fromisoformat(o["execute_after"])
        except Exception:
            return True

    def _queue_order(self,side,code,name,qty,reason,metrics,market_price):
        if code in self.state["pending_orders"]: return False
        execute_after=self.now()+timedelta(seconds=self.latency_seconds)
        self.state["pending_orders"][code]={
            "side":side,"code":code,"name":name,"qty":int(qty),"reason":reason,
            "created_at":self.now().isoformat(),"execute_after":execute_after.isoformat(),
            "signal_price":float(market_price),
            "signal_score":float((metrics or {}).get("score") or 0)
        }
        self._log("ORDER_QUEUED",side=side,code=code,name=name,qty=int(qty),
                  signal_price=market_price,execute_after=execute_after.isoformat(),reason=reason)
        return True

    def _process_pending(self,code,price,metrics):
        o=self.state.get("pending_orders",{}).get(code)
        if not o or not self._order_due(o):return
        side=o["side"]; want=int(o["qty"])
        fill_qty=self._max_fill_qty(want,metrics,side)
        if fill_qty<=0:
            self.state["pending_orders"].pop(code,None)
            self.state["cancelled_order_count"]=int(self.state.get("cancelled_order_count",0))+1
            self._log("ORDER_CANCELLED_LIQUIDITY",side=side,code=code,want_qty=want,market_price=price)
            return

        if fill_qty<want:
            self.state["partial_fill_count"]=int(self.state.get("partial_fill_count",0))+1

        fill,slip=self._fill_price(price,side,metrics)
        if side=="BUY":
            if code in self.state["positions"]:
                self.state["pending_orders"].pop(code,None);return
            amount=fill*fill_qty; fee=self._buy_fee(amount)
            if amount+fee>self.state["cash"]:
                fill_qty=int(self.state["cash"]//(fill*(1+self.buy_fee_pct/100.0)))
                if fill_qty<=0:
                    self.state["pending_orders"].pop(code,None);return
                amount=fill*fill_qty;fee=self._buy_fee(amount)
            support=float((metrics or {}).get("support") or 0)
            stop=max(fill*0.98,support*0.995 if 0<support<fill else 0)
            self.state["cash"]-=amount+fee
            self.state["positions"][code]={
                "code":code,"name":o["name"],"qty":fill_qty,"entry":fill,"stop":stop,
                "highest":fill,"opened_at":self.now().isoformat(),
                "entry_reason":o["reason"],"buy_fee":fee,
                "requested_qty":want,"entry_slippage_pct":slip,
                "signal_price":o.get("signal_price")
            }
            self._log("BUY_FILLED",code=code,name=o["name"],requested_qty=want,qty=fill_qty,
                      fill=round(fill,4),slippage_pct=round(slip,4),cash=round(self.state["cash"],2))
        else:
            p=self.state["positions"].get(code)
            if not p:
                self.state["pending_orders"].pop(code,None);return
            fill_qty=min(fill_qty,int(p["qty"]))
            self._sell_qty(code,price,fill_qty,o["reason"],metrics,forced_fill=(fill,slip))

        self.state["pending_orders"].pop(code,None)

    def _sell_qty(self,code,market_price,qty,reason,metrics=None,forced_fill=None):
        p=self.state["positions"].get(code)
        if not p:return None
        qty=min(max(0,int(qty)),int(p["qty"]))
        if qty<=0:return None
        fill,slip=forced_fill if forced_fill else self._fill_price(market_price,"SELL",metrics or {})
        gross=fill*qty;sell_cost=self._sell_cost(gross)
        self.state["cash"]+=gross-sell_cost

        # 매수수수료를 수량비례로 배분
        original_qty=int(p["qty"])
        fee_alloc=float(p.get("buy_fee") or 0)*(qty/original_qty if original_qty else 1)
        buy_total=float(p["entry"])*qty+fee_alloc
        net=(gross-sell_cost)-buy_total
        ret=net/buy_total*100 if buy_total else 0

        remaining=original_qty-qty
        if remaining<=0:
            self.state["positions"].pop(code,None)
        else:
            p["qty"]=remaining
            p["buy_fee"]=max(0.0,float(p.get("buy_fee") or 0)-fee_alloc)

        r={
            "code":code,"name":p["name"],"qty":qty,"entry":round(float(p["entry"]),4),
            "exit":round(fill,4),"market_exit":round(float(market_price),4),
            "opened_at":p["opened_at"],"closed_at":self.now().isoformat(),
            "reason":reason,"sell_slippage_pct":round(slip,4),
            "sell_cost":round(sell_cost,2),"net_pnl":round(net,2),
            "net_return_pct":round(ret,3),"remaining_qty":remaining
        }
        self.state["trades"].append(r); self._log("SELL_FILLED",**r)
        return r

    def force_liquidate(self,prices,metrics_by_code=None):
        """사용자 비상 전량매도: 대기주문 취소 후 각 종목 전체를 현재가 기반으로 즉시 가상청산."""
        rows=[];metrics_by_code=metrics_by_code or {}
        with self.lock:
            self.state["pending_orders"]={}
            for c in list(self.state.get("positions",{})):
                p=self.state["positions"][c]
                px=float(prices.get(c) or p.get("entry") or 0)
                if px<=0:continue
                r=self._sell_qty(c,px,int(p["qty"]),"USER_FORCE_EXIT",metrics_by_code.get(c) or {})
                if r:rows.append(r)
            self.state["manual_interventions"]=int(self.state.get("manual_interventions",0))+1
            self._log("USER_FORCE_LIQUIDATE",count=len(rows));self._save()
        return rows

    def on_market(self,code,name,price,metrics,stage,market_risk,all_prices,metrics_by_code=None,allow_entry=True):
        with self.lock:
            if not self.state.get("active") or self.state.get("paused") or price<=0:return
            m=metrics or {}
            metrics_by_code=metrics_by_code or {code:m}

            # 먼저 이전 신호의 지연 주문을 '현재 실제 틱'으로 처리
            self._process_pending(code,price,m)

            today=str(self.now().date());end=str(self.state.get("end_date") or "9999-12-31")
            try:
                ch,cm=[int(x) for x in self.season_close_time.split(":",1)]
            except Exception:
                ch,cm=19,50
            season_close=(today>end) or (today==end and (self.now().hour>ch or (self.now().hour==ch and self.now().minute>=cm)))
            if season_close:
                if code in self.state.get("positions",{}) and code not in self.state.get("pending_orders",{}):
                    p=self.state["positions"][code]
                    self._queue_order("SELL",code,p["name"],int(p["qty"]),"SEASON_END",m,price)
                    # 시즌 종료는 대기 없이 즉시 현재 틱으로 처리
                    self.state["pending_orders"][code]["execute_after"]=self.now().isoformat()
                    self._process_pending(code,price,m)
                if not self.state.get("positions") and not self.state.get("pending_orders"):
                    self.state["active"]=False;self._log("SEASON_COMPLETE")
                self._save();return

            if self.state.get("last_trade_date")!=today:
                self.state["last_trade_date"]=today
                self.state["day_start_equity"]=self._equity(all_prices,metrics_by_code)

            pos=self.state.get("positions",{}).get(code)
            if pos:
                pos["highest"]=max(float(pos["highest"]),price)
                entry=float(pos["entry"]);gain=(price/entry-1)*100
                dd=(price/float(pos["highest"])-1)*100
                stop=float(pos["stop"]);vwap=float(m.get("vwap") or 0)
                support=float(m.get("support") or 0);strength=float(m.get("strength") or 0)
                imb=float(m.get("imbalance") or 1);score=float(m.get("score") or 50)
                vr=float(m.get("volume_ratio") or 0)

                if gain>0:
                    stop=max(stop,entry*1.001)
                    if vwap and vwap<price:stop=max(stop,vwap*0.995)
                    if support and support<price:stop=max(stop,support*0.995)
                    trail=0.030 if gain<3 else 0.022 if gain<6 else 0.016
                    stop=max(stop,float(pos["highest"])*(1-trail))
                pos["stop"]=stop

                es=0;reasons=[]
                if vwap and price<vwap:es+=2;reasons.append("VWAP")
                if support and price<support:es+=3;reasons.append("SUPPORT")
                if strength and strength<90:es+=2;reasons.append("STRENGTH")
                elif strength and strength<100:es+=1
                if imb<0.80:es+=2;reasons.append("BOOK")
                elif imb<0.95:es+=1
                if vr>=1.35 and price<float(pos["highest"])*0.99:es+=2;reasons.append("SELL_VOLUME")
                if dd<=-2:es+=2;reasons.append("DRAWDOWN")
                elif dd<=-1.2:es+=1
                if score<45:es+=2;reasons.append("SCORE")
                if market_risk=="RISK":es+=2;reasons.append("MARKET")

                if (price<=stop or es>=7) and code not in self.state.get("pending_orders",{}):
                    self._queue_order("SELL",code,pos["name"],int(pos["qty"]),
                                      "AUTO_EXIT:"+"/".join(reasons[:4]),m,price)
            else:
                if not allow_entry:
                    self._save();return
                day_start=float(self.state.get("day_start_equity") or 0)
                if day_start>0 and (self._equity(all_prices,metrics_by_code)/day_start-1)*100<=-abs(self.max_daily_loss_pct):return
                if code in self.state.get("pending_orders",{}):return
                if market_risk=="RISK" or stage not in ("SIGNAL","BREAKOUT"):return
                score=float(m.get("score") or 0);strength=float(m.get("strength") or 0)
                imb=float(m.get("imbalance") or 1)
                if score<self.min_entry_score or strength<105 or imb<0.90 or bool(m.get("chase_risk")):return
                if len(self.state.get("positions",{}))>=self.max_positions:return

                eq=self._equity(all_prices,metrics_by_code)
                budget=min(float(self.state["cash"]),eq*self.max_position_pct/100.0)
                rough_fill=price*(1+self.max_slippage_pct/100.0)
                qty=int(budget//rough_fill)
                if qty<=0:return
                self._queue_order("BUY",code,name,qty,f"{stage} score={score:.1f}",m,price)

            eq=self._equity(all_prices,metrics_by_code)
            ts=self.now().replace(second=0,microsecond=0).isoformat()
            if not self.state["equity_curve"] or self.state["equity_curve"][-1]["time"]!=ts:
                self.state["equity_curve"].append({"time":ts,"equity":round(eq,2)})
                self.state["equity_curve"]=self.state["equity_curve"][-5000:]
            self._save()

    def summary(self,prices,metrics_by_code=None):
        with self.lock:
            metrics_by_code=metrics_by_code or {}
            eq=self._equity(prices,metrics_by_code)
            ini=float(self.state.get("capital_initial") or 0)
            tr=list(self.state.get("trades",[]))
            wins=sum(1 for x in tr if float(x.get("net_pnl",0))>0)
            peak=0.0;maxdd=0.0
            for x in self.state.get("equity_curve",[]):
                v=float(x.get("equity",0));peak=max(peak,v)
                if peak:maxdd=min(maxdd,(v/peak-1)*100)

            pos=[]
            for c,p in self.state.get("positions",{}).items():
                px=float(prices.get(c) or p["entry"]);entry=float(p["entry"]);q=int(p["qty"])
                liquidation=self._liquidation_value(p,px,metrics_by_code.get(c) or {})
                invested=entry*q+float(p.get("buy_fee") or 0)
                net=liquidation-invested
                pos.append({
                    **p,"current":px,"liquidation_value":round(liquidation,2),
                    "unrealized_pnl":round(net,2),
                    "return_pct":round(net/invested*100,3) if invested else 0
                })

            return {
                "engine_version":"3.0.0",
                "season":self.state.get("season"),"active":bool(self.state.get("active")),
                "paused":bool(self.state.get("paused")),"capital_initial":round(ini,2),
                "cash":round(float(self.state.get("cash",0)),2),"equity":round(eq,2),
                "total_pnl":round(eq-ini,2),
                "total_return_pct":round((eq/ini-1)*100,3) if ini else 0,
                "duration_days":int(self.state.get("duration_days") or 0),
                "start_date":self.state.get("start_date"),"end_date":self.state.get("end_date"),
                "positions":pos,"pending_orders":list(self.state.get("pending_orders",{}).values()),
                "trade_count":len(tr),"win_rate":round(wins/len(tr)*100,1) if tr else None,
                "max_drawdown_pct":round(maxdd,3),
                "manual_interventions":int(self.state.get("manual_interventions",0)),
                "partial_fill_count":int(self.state.get("partial_fill_count",0)),
                "cancelled_order_count":int(self.state.get("cancelled_order_count",0)),
                "recent_trades":list(reversed(tr[-20:])),
                "cost_model":{
                    "buy_fee_pct":self.buy_fee_pct,"sell_fee_pct":self.sell_fee_pct,
                    "sell_tax_pct":self.sell_tax_pct,"base_slippage_pct":self.slippage_pct,
                    "max_slippage_pct":self.max_slippage_pct,"latency_seconds":self.latency_seconds,
                    "minute_participation_pct":self.max_minute_participation_pct,
                    "book_participation_pct":self.max_book_participation_pct,
                    "season_close_time":self.season_close_time
                },
                "strategy_note":"V3: VIP 신규진입 + VIP 해제 보유종목 백그라운드 추적 + NXT 종료까지 운용 + 실전형 체결모델"
            }
