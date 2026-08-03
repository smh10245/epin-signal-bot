from __future__ import annotations

# 뽕실 V10 AI - 단일 파일 배포본
# 자동 주문 기능 없음. 실제 매매는 사용자가 직접 수행합니다.


# ===== config.py =====
import os
from dataclasses import dataclass
from zoneinfo import ZoneInfo
KST=ZoneInfo('Asia/Seoul')
def b(n,d): return os.getenv(n,str(d)).lower() in {'1','true','yes','on'}
def i(n,d):
    try:return int(os.getenv(n,str(d)))
    except:return d
def f(n,d):
    try:return float(os.getenv(n,str(d)))
    except:return d
@dataclass(frozen=True)
class Settings:
    version:str='12.0.0-verified'
    telegram_token:str=os.getenv('TELEGRAM_TOKEN','').strip()
    chat_id:str=(os.getenv('CHAT_ID') or os.getenv('TELEGRAM_CHAT_ID') or '').strip()
    telegram_polling:bool=b('ENABLE_TELEGRAM_POLLING',True)
    kis_app_key:str=os.getenv('KIS_APP_KEY','').strip()
    kis_app_secret:str=os.getenv('KIS_APP_SECRET','').strip()
    kis_env:str=os.getenv('KIS_ENV','real').strip().lower()
    enable_nxt:bool=b('ENABLE_NXT',True)
    supabase_url:str=os.getenv('SUPABASE_URL','').rstrip('/')
    supabase_key:str=os.getenv('SUPABASE_SECRET_KEY','').strip()
    port:int=i('PORT',10000)
    max_candidates:int=i('MAX_CANDIDATES',200)
    realtime_limit:int=i('REALTIME_SUBSCRIPTION_LIMIT',12)
    ws_trade_limit:int=i('WS_TRADE_LIMIT',12)
    ws_orderbook_limit:int=i('WS_ORDERBOOK_LIMIT',4)
    ws_subscribe_delay:float=f('WS_SUBSCRIBE_DELAY',0.25)
    swing_scan_limit:int=i('SWING_SCAN_LIMIT',40)
    swing_history_days:int=i('SWING_HISTORY_DAYS',220)
    swing_cache_minutes:int=i('SWING_CACHE_MINUTES',360)
    min_intraday_bars:int=i('MIN_INTRADAY_BARS',12)
    daily_limit:int=i('DAILY_RECOMMEND_LIMIT',15)
    min_market_cap:float=f('MIN_MARKET_CAP',100_000_000_000)
    min_daily_volume:int=i('MIN_DAILY_VOLUME',20_000)
    max_chase_pct:float=f('MAX_CHASE_PCT',12.0)
    min_reliability:float=f('MIN_RELIABILITY',67)
    day_signal:float=f('DAYTRADE_SIGNAL_SCORE',84)
    swing_signal:float=f('SWING_SIGNAL_SCORE',84)
    day_stop:float=f('DAYTRADE_STOP_PCT',3.0)
    swing_stop:float=f('SWING_STOP_PCT',5.0)
    krx_start:str=os.getenv('KRX_WS_START','08:30')
    krx_end:str=os.getenv('KRX_WS_END','15:40')
    nxt_start:str=os.getenv('NXT_WS_START','08:00')
    nxt_end:str=os.getenv('NXT_WS_END','20:00')
    krx_trade_tr:str=os.getenv('KIS_KRX_TRADE_TR_ID','H0STCNT0')
    nxt_trade_tr:str=os.getenv('KIS_NXT_TRADE_TR_ID','H0NXCNT0')
    krx_order_tr:str=os.getenv('KIS_KRX_ORDERBOOK_TR_ID','H0STASP0')
    nxt_order_tr:str=os.getenv('KIS_NXT_ORDERBOOK_TR_ID','H0UNASP0')
SETTINGS=Settings()


# ===== models.py =====
from dataclasses import dataclass,field
from datetime import datetime
from typing import Dict,List
@dataclass
class Tick: code:str; name:str; market:str; price:float; volume:int; cumulative_volume:int; trade_strength:float; timestamp:datetime
@dataclass
class Bar: code:str; name:str; market:str; minute:datetime; open:float; high:float; low:float; close:float; volume:int; cumulative_volume:int; trade_strength:float
@dataclass
class OrderBook: code:str; market:str; asks:List[float]; bids:List[float]; ask_qty:List[int]; bid_qty:List[int]; total_ask:int; total_bid:int; imbalance:float; updated_at:datetime
@dataclass
class ScoreCard:
    code:str; name:str; kind:str; score:float; acceleration:float; reliability:float; stage:str
    reasons:List[str]; blockers:List[str]; strategy:str; price:float; target1:float; target2:float; target3:float; stop:float
    components:Dict[str,float]=field(default_factory=dict)
@dataclass
class Position:
    code:str; name:str; kind:str; entry:float; qty:float; highest:float; stop:float; target1:float; target2:float; target3:float
    stop_notified:bool=False; recovered:bool=False; t1:bool=False; t2:bool=False; state:str='매수등록'


# ===== utils.py =====
from datetime import datetime
def now(): return datetime.now(KST)
def num(v,d=0.0):
    try:return float(str(v).replace(',','').strip())
    except:return d
def integer(v,d=0):
    try:return int(float(str(v).replace(',','').strip()))
    except:return d
def clamp(v,a=0,b=100): return max(a,min(b,v))
def pct(n,o): return (n/o-1)*100 if o else 0
def in_session(start_hhmm,end_hhmm):
    n=now()
    if n.weekday()>=5:
        return False
    try:
        sh,sm=map(int,start_hhmm.split(':'))
        eh,em=map(int,end_hhmm.split(':'))
    except Exception:
        return False
    start=n.replace(hour=sh,minute=sm,second=0,microsecond=0)
    end=n.replace(hour=eh,minute=em,second=0,microsecond=0)
    return start<=n<=end

def ema(values,p):
    if not values:return 0
    a=2/(p+1); out=values[0]
    for v in values[1:]: out=a*v+(1-a)*out
    return out


# ===== infrastructure.py =====
import json,logging,threading,time
from datetime import datetime,timedelta,timezone
from typing import Any,Callable,Dict,List,Optional
import requests,websocket
log=logging.getLogger('v10.infra')
REAL_REST='https://openapi.koreainvestment.com:9443'; VIRTUAL_REST='https://openapivts.koreainvestment.com:29443'
REAL_WS='ws://ops.koreainvestment.com:21000'; VIRTUAL_WS='ws://ops.koreainvestment.com:31000'
class DB:
    def __init__(self): self.enabled=bool(SETTINGS.supabase_url and SETTINGS.supabase_key); self.s=requests.Session()
    @property
    def h(self): return {'apikey':SETTINGS.supabase_key,'Authorization':f'Bearer {SETTINGS.supabase_key}','Content-Type':'application/json','Prefer':'return=representation'}
    def select(self,t,p=None):
        if not self.enabled:return []
        try:
            r=self.s.get(f'{SETTINGS.supabase_url}/rest/v1/{t}',headers=self.h,params=p or {'select':'*'},timeout=20); r.raise_for_status(); d=r.json(); return d if isinstance(d,list) else []
        except Exception as e: log.warning('DB select %s: %s',t,e); return []
    def insert(self,t,payload):
        if not self.enabled:return None
        try:
            r=self.s.post(f'{SETTINGS.supabase_url}/rest/v1/{t}',headers=self.h,json=payload,timeout=20); r.raise_for_status(); return r.json()
        except Exception as e: log.warning('DB insert %s: %s',t,e); return None
    def upsert(self,t,payload,key):
        if not self.enabled:return False
        h=dict(self.h); h['Prefer']='resolution=merge-duplicates,return=minimal'
        try:
            r=self.s.post(f'{SETTINGS.supabase_url}/rest/v1/{t}',headers=h,params={'on_conflict':key},json=payload,timeout=20); r.raise_for_status(); return True
        except Exception as e: log.warning('DB upsert %s: %s',t,e); return False
DATABASE=DB()
class Telegram:
    def __init__(self): self.s=requests.Session(); self.offset=0; self.handler:Optional[Callable[[str,str],None]]=None
    def send(self,text,chat=None):
        target=str(chat or SETTINGS.chat_id).strip()
        if not SETTINGS.telegram_token or not target:return False
        try:
            r=self.s.post(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/sendMessage',json={'chat_id':target,'text':text,'parse_mode':'HTML','disable_web_page_preview':True},timeout=15); r.raise_for_status(); return bool(r.json().get('ok'))
        except Exception as e: log.warning('Telegram send: %s',e); return False
    def poll(self):
        if not SETTINGS.telegram_token:
            return
        while True:
            try:
                r=self.s.get(f'https://api.telegram.org/bot{SETTINGS.telegram_token}/getUpdates',params={'timeout':25,'offset':self.offset},timeout=35)
                if r.status_code==409:
                    log.warning('Telegram 409: 이전 배포 프로세스 종료 대기 후 재시도')
                    time.sleep(20)
                    continue
                r.raise_for_status()
                for u in r.json().get('result',[]):
                    self.offset=max(self.offset,int(u['update_id'])+1); m=u.get('message') or {}; text=str(m.get('text') or '').strip(); chat=str((m.get('chat') or {}).get('id') or '')
                    if text and chat and self.handler: threading.Thread(target=self.handler,args=(text,chat),daemon=True).start()
            except Exception as e: log.warning('Telegram poll: %s',e); time.sleep(10)
BOT=Telegram()
class KIS:
    def __init__(self):
        self.rest=VIRTUAL_REST if SETTINGS.kis_env=='virtual' else REAL_REST; self.ws=VIRTUAL_WS if SETTINGS.kis_env=='virtual' else REAL_WS
        self.s=requests.Session(); self.token=None; self.token_exp=None; self.approval=None; self.approval_exp=None; self.lock=threading.Lock()
    def access(self):
        if self.token and self.token_exp and datetime.now(timezone.utc)<self.token_exp:return self.token
        r=self.s.post(f'{self.rest}/oauth2/tokenP',json={'grant_type':'client_credentials','appkey':SETTINGS.kis_app_key,'appsecret':SETTINGS.kis_app_secret},timeout=20); r.raise_for_status(); d=r.json(); self.token=d['access_token']; self.token_exp=datetime.now(timezone.utc)+timedelta(seconds=max(60,int(d.get('expires_in',86400))-300)); return self.token
    def approval_key(self):
        with self.lock:
            if self.approval and self.approval_exp and datetime.now(timezone.utc)<self.approval_exp:return self.approval
            r=self.s.post(f'{self.rest}/oauth2/Approval',json={'grant_type':'client_credentials','appkey':SETTINGS.kis_app_key,'secretkey':SETTINGS.kis_app_secret},timeout=20); r.raise_for_status(); self.approval=r.json()['approval_key']; self.approval_exp=datetime.now(timezone.utc)+timedelta(hours=12); return self.approval
    def price(self,code):
        t=self.access(); r=self.s.get(f'{self.rest}/uapi/domestic-stock/v1/quotations/inquire-price',headers={'authorization':f'Bearer {t}','appkey':SETTINGS.kis_app_key,'appsecret':SETTINGS.kis_app_secret,'tr_id':'FHKST01010100','custtype':'P'},params={'FID_COND_MRKT_DIV_CODE':'J','FID_INPUT_ISCD':code},timeout=20); r.raise_for_status(); d=r.json(); return d.get('output') or {}
    @staticmethod
    def parse_trade(msg,market,names):
        if isinstance(msg,(bytes,bytearray,memoryview)):
            msg=bytes(msg).decode('utf-8',errors='ignore')
        elif msg is not None and not isinstance(msg,str):
            msg=str(msg)
        if not msg or msg.startswith('{'):return []
        p=msg.split('|',3)
        if len(p)<4 or p[0]!='0':return []
        count=max(1,integer(p[2],1)); f=p[3].split('^'); width=len(f)//count if count else len(f)
        if width<19:return []
        out=[]
        for i in range(count):
            r=f[i*width:(i+1)*width]; code=str(r[0]).zfill(6); ts=now(); h=str(r[1]).zfill(6)
            try: ts=ts.replace(hour=int(h[:2]),minute=int(h[2:4]),second=int(h[4:6]),microsecond=0)
            except: pass
            out.append(Tick(code,names.get(code,code),market,num(r[2]),integer(r[12]),integer(r[13]),num(r[18]),ts))
        return out
    @staticmethod
    def parse_book(msg,market,tr):
        if isinstance(msg,(bytes,bytearray,memoryview)):
            msg=bytes(msg).decode('utf-8',errors='ignore')
        elif msg is not None and not isinstance(msg,str):
            msg=str(msg)
        if not msg or msg.startswith('{'):return None
        p=msg.split('|',3)
        if len(p)<4 or p[1]!=tr:return None
        f=p[3].split('^')
        if len(f)<45:return None
        code=str(f[0]).zfill(6); asks=[num(x) for x in f[3:13]]; bids=[num(x) for x in f[13:23]]; aq=[integer(x) for x in f[23:33]]; bq=[integer(x) for x in f[33:43]]; ta=integer(f[43]) or sum(aq); tb=integer(f[44]) or sum(bq)
        return OrderBook(code,market,asks,bids,aq,bq,ta,tb,tb/ta if ta else 0,now())
    def stream(self,market,codes_fn,names,on_tick,on_book,state):
        trade=SETTINGS.krx_trade_tr if market=='KRX' else SETTINGS.nxt_trade_tr
        order=SETTINGS.krx_order_tr if market=='KRX' else SETTINGS.nxt_order_tr
        retry=5

        while True:
            ws_app=None
            try:
                session_ok=in_session(SETTINGS.krx_start,SETTINGS.krx_end) if market=='KRX' else in_session(SETTINGS.nxt_start,SETTINGS.nxt_end)
                if not session_ok:
                    state[f'ws_{market.lower()}']='waiting_market_session'
                    time.sleep(30)
                    continue

                key=self.approval_key()
                state[f'ws_{market.lower()}']='connecting'
                state[f'{market.lower()}_last_error']=''

                def sub(ws,tr,code):
                    payload={
                        'header':{
                            'approval_key':key,
                            'custtype':'P',
                            'tr_type':'1',
                            'content-type':'utf-8'
                        },
                        'body':{'input':{'tr_id':tr,'tr_key':code}}
                    }
                    ws.send(json.dumps(payload,ensure_ascii=False))

                def opened(ws):
                    state[f'ws_{market.lower()}']='connected'
                    all_codes=list(dict.fromkeys(codes_fn()))
                    trade_codes=all_codes[:max(1,SETTINGS.ws_trade_limit)]

                    priority=list(dict.fromkeys(
                        list(getattr(STATE,'positions',{}))+
                        list(getattr(STATE,'watch',{}))+
                        list(getattr(STATE,'active',{}))
                    ))
                    order_codes=(priority+[c for c in trade_codes if c not in priority])[:max(0,SETTINGS.ws_orderbook_limit)]

                    state[f'{market.lower()}_trade_subscribed']=0
                    state[f'{market.lower()}_orderbook_subscribed']=0

                    # KIS websocket has a finite subscription capacity.
                    # Do not subscribe trade + orderbook for all 200 candidates.
                    for code in trade_codes:
                        sub(ws,trade,code)
                        state[f'{market.lower()}_trade_subscribed']+=1
                        time.sleep(SETTINGS.ws_subscribe_delay)

                    for code in order_codes:
                        sub(ws,order,code)
                        state[f'{market.lower()}_orderbook_subscribed']+=1
                        time.sleep(SETTINGS.ws_subscribe_delay)

                    log.info(
                        '%s WS 구독 완료: 체결 %s개 / 호가 %s개',
                        market,
                        state[f'{market.lower()}_trade_subscribed'],
                        state[f'{market.lower()}_orderbook_subscribed']
                    )

                def message(ws,msg):
                    if isinstance(msg,(bytes,bytearray,memoryview)):
                        msg=bytes(msg).decode('utf-8',errors='ignore')
                    elif not isinstance(msg,str):
                        msg=str(msg)

                    if msg.startswith('{'):
                        try:
                            d=json.loads(msg)
                            header=d.get('header') or {}
                            body=d.get('body') or {}

                            if header.get('tr_id')=='PINGPONG':
                                ws.send(msg)
                                return

                            rt_cd=str(body.get('rt_cd','0'))
                            msg1=str(body.get('msg1') or '')
                            tr_id=str(header.get('tr_id') or body.get('tr_id') or '')
                            if rt_cd not in ('0',''):
                                state[f'{market.lower()}_last_error']=f'{tr_id} {msg1}'
                                log.warning('%s WS 구독응답 오류: tr=%s, rt_cd=%s, msg=%s',market,tr_id,rt_cd,msg1)
                        except Exception as e:
                            log.debug('%s WS JSON 처리 실패: %s',market,e)
                        return

                    ob=self.parse_book(msg,market,order)
                    if ob:
                        on_book(ob)
                        return

                    for t in self.parse_trade(msg,market,names):
                        on_tick(t)

                def error(ws,e):
                    state[f'ws_{market.lower()}']='error'
                    state[f'{market.lower()}_last_error']=str(e)
                    state['last_error']=f'{market}: {e}'
                    log.warning('%s WS 오류: %s',market,e)

                def closed(ws,status,msg):
                    state[f'ws_{market.lower()}']='closed'
                    log.warning('%s WS 종료: status=%s, msg=%s',market,status,msg)

                ws_app=websocket.WebSocketApp(
                    self.ws,
                    on_open=opened,
                    on_message=message,
                    on_error=error,
                    on_close=closed
                )
                ws_app.run_forever(
                    ping_interval=25,
                    ping_timeout=10,
                    skip_utf8_validation=True
                )

                time.sleep(retry)
                retry=min(120,retry*2)

            except Exception as e:
                state[f'ws_{market.lower()}']='failed'
                state[f'{market.lower()}_last_error']=str(e)
                state['last_error']=f'{market}: {e}'
                log.exception('%s stream 실패',market)
                time.sleep(retry)
                retry=min(120,retry*2)

KIS_CLIENT=KIS()


# ===== core.py =====
import html,logging,math,threading,time
from collections import deque
from datetime import timedelta
from typing import Any,Dict,List,Optional,Tuple
import FinanceDataReader as fdr
log=logging.getLogger('v10.core')
LABEL={'volume':'거래량 증가','strength':'체결강도','trend':'상승추세','breakout':'돌파','pullback':'눌림목 재돌파','v':'V자 반등','kiyoung':'기영이 패턴','order':'호가 매수우위','sector':'섹터 강도','bottom':'바닥권','base':'바닥 다지기','volume_return':'거래량 재유입','ma_turn':'이평선 전환','box_break':'박스권 돌파 가능성','rebound':'바닥 반등 초기'}
class State:
    def __init__(self):
        self.lock=threading.RLock(); self.names={}; self.meta={}; self.candidates={}; self.watch={}; self.positions={}; self.active={}; self.bars={}; self.current={}; self.last_cum={}; self.books={}; self.ticks={}; self.score_history={}; self.sectors={}; self.daily_cache={}; self.runtime={'ws_krx':'stopped','ws_nxt':'stopped','last_tick':None,'last_error':None}
    def load(self):
        df=fdr.StockListing('KRX')
        with self.lock:
            for _,r in df.iterrows():
                c=str(r.get('Code') or r.get('Symbol') or '').zfill(6); n=str(r.get('Name') or c)
                if c and n:self.names[c]=n; self.meta[c]={str(k):r[k] for k in df.columns}
    def refresh(self):
        rows=[]
        with self.lock:
            for c,m in self.meta.items():
                close=num(m.get('Close')); vol=integer(m.get('Volume')); cap=num(m.get('Marcap')); amount=num(m.get('Amount')) or close*vol
                if cap and cap<SETTINGS.min_market_cap:continue
                if vol and vol<SETTINGS.min_daily_volume:continue
                rows.append((amount,cap,c))
            rows.sort(reverse=True); priority=list(dict.fromkeys(list(self.positions)+list(self.watch)+list(self.active))); codes=priority+[c for _,_,c in rows if c not in priority]; self.candidates={c:self.meta.get(c,{}) for c in codes[:SETTINGS.max_candidates]}
    def realtime_codes(self):
        with self.lock:
            p=list(dict.fromkeys(list(self.positions)+list(self.watch)+list(self.active))); return (p+[c for c in self.candidates if c not in p])[:SETTINGS.realtime_limit]
    def push(self,t:Tick):
        key=(t.market,t.code); minute=t.timestamp.replace(second=0,microsecond=0); last=self.last_cum.get(key,t.cumulative_volume); inc=max(0,t.cumulative_volume-last); self.last_cum[key]=t.cumulative_volume; b=self.current.get(key); self.ticks[key]=t; self.runtime['last_tick']=t.timestamp.isoformat()
        if not b:self.current[key]=Bar(t.code,t.name,t.market,minute,t.price,t.price,t.price,t.price,max(t.volume,inc),t.cumulative_volume,t.trade_strength);return None
        if b.minute==minute:b.high=max(b.high,t.price);b.low=min(b.low,t.price);b.close=t.price;b.volume+=max(t.volume,inc);b.cumulative_volume=max(b.cumulative_volume,t.cumulative_volume);b.trade_strength=t.trade_strength;return None
        self.current[key]=Bar(t.code,t.name,t.market,minute,t.price,t.price,t.price,t.price,max(t.volume,inc),t.cumulative_volume,t.trade_strength); self.bars.setdefault(key,deque(maxlen=240)).append(b); return b
STATE=State()
class Brain:
    def sector_refresh(self):
        buckets={}
        for c,m in STATE.meta.items():
            s=str(m.get('Sector') or m.get('Industry') or '기타')
            ch=num(m.get('ChagesRatio') or m.get('ChangesRatio'))
            amount=num(m.get('Amount')) or num(m.get('Close'))*num(m.get('Volume'))
            buckets.setdefault(s,[]).append(ch*max(1,math.log10(max(amount,10))))
        STATE.sectors={k:clamp(50+sum(v)/max(1,len(v))*2) for k,v in buckets.items()}

    def sector(self,c):
        m=STATE.meta.get(c,{})
        return STATE.sectors.get(str(m.get('Sector') or m.get('Industry') or '기타'),50)

    def patterns_day(self,bars):
        o={'volume':0,'strength':0,'trend':0,'breakout':0,'pullback':0,'v':0,'kiyoung':0}
        if len(bars)<SETTINGS.min_intraday_bars:
            return o
        r=bars[-30:]
        closes=[b.close for b in r]; highs=[b.high for b in r]
        lows=[b.low for b in r]; vols=[b.volume for b in r]
        latest=r[-1]
        old=sum(vols[-12:-3])/max(1,len(vols[-12:-3]))
        new=sum(vols[-3:])/3
        vr=new/old if old else 0
        o['volume']=clamp(vr*35)
        o['strength']=clamp((latest.trade_strength-80)*1.4)
        e5=ema(closes,5); e12=ema(closes,12)
        if latest.close>=e5>e12:
            o['trend']=clamp(65+pct(latest.close,e5)*8)
        prev=max(highs[-8:-1])
        o['breakout']=90 if latest.close>=prev else 0
        pl=min(lows[-6:]); depth=pct(pl,prev)
        if -5.5<=depth<=-.7 and latest.close>=max(highs[-5:-1]):
            o['pullback']=88
        hi=max(highs[:-2]); lo=min(lows)
        dd=pct(lo,hi); reb=pct(latest.close,lo)
        if dd<=-2 and reb>=1.1:
            o['v']=clamp(abs(dd)*11+reb*13)
        w=r[-18:]
        support=sorted([b.low for b in w])[max(0,len(w)//4-1)]
        touch=sum(1 for b in w if support and abs(b.low/support-1)<=.012)
        rng=pct(max(b.high for b in w),min(b.low for b in w))
        if touch>=3 and rng<=12 and vr>=1.2 and latest.close>support*1.01:
            o['kiyoung']=clamp(42+touch*10+vr*12)
        return o

    def daily_history(self,c):
        cached=STATE.daily_cache.get(c)
        if cached and now()-cached[0] < timedelta(minutes=SETTINGS.swing_cache_minutes):
            return cached[1]
        end=now().date()+timedelta(days=1)
        start=end-timedelta(days=SETTINGS.swing_history_days)
        try:
            df=fdr.DataReader(c,start,end)
            required={'Open','High','Low','Close','Volume'}
            if df is None or df.empty or not required.issubset(set(df.columns)):
                STATE.daily_cache[c]=(now(),None)
                return None
            df=df.dropna(subset=['High','Low','Close','Volume']).copy()
            STATE.daily_cache[c]=(now(),df)
            return df
        except Exception as e:
            log.warning('일봉 조회 실패 %s: %s',c,e)
            STATE.daily_cache[c]=(now(),None)
            return None

    def patterns_swing_daily(self,df):
        o={'bottom':0,'base':0,'volume_return':0,'ma_turn':0,'box_break':0,'rebound':0}
        if df is None or len(df)<60:
            return o
        r=df.tail(120)
        close=r['Close'].astype(float)
        high=r['High'].astype(float)
        low=r['Low'].astype(float)
        volume=r['Volume'].astype(float)
        latest=float(close.iloc[-1])
        low60=float(low.tail(60).min())
        high60=float(high.tail(60).max())
        pos=(latest-low60)/(high60-low60) if high60>low60 else .5
        o['bottom']=clamp(95-pos*120)
        touches=int(((low.tail(40)/low60-1).abs()<=.025).sum()) if low60 else 0
        o['base']=clamp(touches*16)
        old=float(volume.tail(25).head(20).mean())
        new=float(volume.tail(5).mean())
        vr=new/old if old>0 else 0
        o['volume_return']=clamp(vr*38)
        ma5=float(close.tail(5).mean())
        ma20=float(close.tail(20).mean())
        prev_ma20=float(close.iloc[-25:-5].mean()) if len(close)>=25 else ma20
        if ma5>=ma20 and ma20>=prev_ma20*.995 and latest>=ma5:
            o['ma_turn']=85
        box=float(high.iloc[-21:-1].max())
        if box>0 and latest>=box*.985:
            o['box_break']=82
        rebound=pct(latest,float(low.tail(20).min()))
        if 2<=rebound<=15:
            o['rebound']=clamp(55+rebound*3)
        return o

    def acceleration(self,c,k,s):
        q=STATE.score_history.setdefault((k,c),deque(maxlen=20))
        n=now(); q.append((n,s))
        v=[x for t,x in q if n-t<=timedelta(minutes=10)]
        return 50 if len(v)<2 else clamp(50+(v[-1]-v[0])*3)

    def blockers(self,c,price,change=None):
        m=STATE.meta.get(c,{})
        ch=num(change if change is not None else (m.get('ChagesRatio') or m.get('ChangesRatio')))
        out=[]
        if ch>=SETTINGS.max_chase_pct:
            out.append('당일 급등으로 추격 위험')
        if price<=0:
            out.append('현재가 확인 불가')
        n=STATE.names.get(c,c)
        if any(x in n for x in ['스팩','ETN']):
            out.append('대상 제외 상품')
        return out

    def evaluate_day(self,c,market='KRX'):
        bars=list(STATE.bars.get((market,c),[])) or list(STATE.bars.get(('NXT',c),[])) or list(STATE.bars.get(('KRX',c),[]))
        if len(bars)<SETTINGS.min_intraday_bars:
            return ScoreCard(c,STATE.names.get(c,c),'단타',0,50,0,'데이터수집중',[],[],
                f'1분봉 {len(bars)}/{SETTINGS.min_intraday_bars}개 수집 중',0,0,0,0,0,{})
        price=bars[-1].close
        ob=STATE.books.get((market,c)) or STATE.books.get(('NXT',c)) or STATE.books.get(('KRX',c))
        order=clamp(50+(((ob.imbalance if ob else 1)-1)*35))
        sector=self.sector(c)
        block=self.blockers(c,price)
        d=self.patterns_day(bars)
        dc={**d,'order':order,'sector':sector}
        dw={'volume':.18,'strength':.14,'trend':.12,'breakout':.14,'pullback':.12,'v':.10,'kiyoung':.10,'order':.06,'sector':.04}
        ds=sum(dc[k]*w for k,w in dw.items())
        da=self.acceleration(c,'단타',ds)
        dr=clamp(35+ds*.35+da*.2+sum(1 for v in dc.values() if v>=65)*4-(max(dc.values())-min(dc.values()))*.08-len(block)*18)
        dstage='매수 시그널' if ds>=SETTINGS.day_signal and dr>=SETTINGS.min_reliability and not block else '예비후보' if ds>=72 else '관찰' if ds>=58 else '대기'
        dre=[k for k,v in sorted(dc.items(),key=lambda x:x[1],reverse=True) if v>=65][:5]
        strategy='돌파 확인 후 분할진입' if d['breakout']>=80 else '눌림 후 재상승 확인' if d['pullback']>=70 else '추격매수 금지·관찰'
        return ScoreCard(c,STATE.names.get(c,c),'단타',round(ds,1),round(da,1),round(dr,1),dstage,dre,block,strategy,price,price*1.03,price*1.06,price*1.10,price*(1-SETTINGS.day_stop/100),dc)

    def evaluate_swing(self,c):
        df=self.daily_history(c)
        if df is None or len(df)<60:
            return ScoreCard(c,STATE.names.get(c,c),'스윙',0,50,0,'데이터부족',[],[],
                '일봉 60개 이상 필요',0,0,0,0,0,{})
        price=float(df['Close'].iloc[-1])
        prev=float(df['Close'].iloc[-2]) if len(df)>1 else price
        change=pct(price,prev)
        block=self.blockers(c,price,change)
        s=self.patterns_swing_daily(df)
        sector=self.sector(c)
        sc={**s,'sector':sector}
        sw={'bottom':.20,'base':.17,'volume_return':.18,'ma_turn':.18,'box_break':.12,'rebound':.10,'sector':.05}
        ss=sum(sc[k]*w for k,w in sw.items())
        sa=self.acceleration(c,'스윙',ss)
        sr=clamp(35+ss*.35+sa*.2+sum(1 for v in sc.values() if v>=65)*4-(max(sc.values())-min(sc.values()))*.08-len(block)*18)
        sstage='매수 시그널' if ss>=SETTINGS.swing_signal and sr>=SETTINGS.min_reliability and not block else '예비후보' if ss>=74 else '관찰' if ss>=62 else '대기'
        sre=[k for k,v in sorted(sc.items(),key=lambda x:x[1],reverse=True) if v>=65][:5]
        return ScoreCard(c,STATE.names.get(c,c),'스윙',round(ss,1),round(sa,1),round(sr,1),sstage,sre,block,'바닥 지지 확인 후 분할진입',price,price*1.08,price*1.14,price*1.20,price*(1-SETTINGS.swing_stop/100),sc)

    def evaluate(self,c,market='KRX'):
        return self.evaluate_day(c,market),self.evaluate_swing(c)
BRAIN=Brain()
class PositionEngine:
    def __init__(self):self.data={}
    def register(self,c,n,p,q,k):
        stop=p*(1-(SETTINGS.day_stop if k=='단타' else SETTINGS.swing_stop)/100);x=Position(c,n,k,p,q,p,stop,p*1.03,p*1.06,p*1.10);self.data[c]=x;return x
    def close(self,c):self.data.pop(c,None)
    def evaluate(self,c,current):
        p=self.data.get(c);out=[]
        if not p:return out
        p.highest=max(p.highest,current);rate=pct(current,p.entry)
        if current<=p.stop and not p.stop_notified:p.stop_notified=True;p.state='손절 신호';out.append(f'⛔ {p.name} 손절 신호 · {current:,.0f}원 · {rate:+.2f}%')
        elif p.stop_notified and current>p.entry and not p.recovered:p.recovered=True;p.state='손절 해제';out.append(f'🟢 {p.name} 손절 해제 · 매수가 회복')
        if current>=p.target1 and not p.t1:p.t1=True;p.state='1차 익절권';out.append(f'🎯 {p.name} 1차 익절권 · {rate:+.2f}%')
        if current>=p.target2 and not p.t2:p.t2=True;p.state='2차 익절권';out.append(f'🔥 {p.name} 2차 익절권 · {rate:+.2f}%')
        if rate>3 and pct(current,p.highest)<=-3:out.append(f'🔻 {p.name} 고점 대비 하락 · 추적익절 검토')
        return out
POSITION=PositionEngine()


# ===== app.py =====
import html,threading,time,logging
from datetime import timedelta
log=logging.getLogger('v10.app')
class App:
 def __init__(self):self.recs=[];self.last_rec=now();self.last_none=now()
 def score_text(self,c):
  rs='\n'.join(f'• {LABEL.get(x,x)}' for x in c.reasons) or '• 핵심 조건 추가 확인 필요'; bs='\n'.join(f'• {html.escape(x)}' for x in c.blockers)
  return f"{'🔥' if c.stage=='매수 시그널' else '🟡'} <b>{c.kind} {c.stage}</b>\n\n<b>{html.escape(c.name)}</b> ({c.code})\n현재가 <b>{c.price:,.0f}원</b>\n현재 강도 {c.score:.1f}점\n상승 속도 {c.acceleration:.1f}점\n추천 신뢰도 <b>{c.reliability:.1f}%</b>\n\n<b>추천 사유</b>\n{rs}"+(f'\n\n<b>주의</b>\n{bs}' if bs else '')+f'\n\n<b>매매 한 줄 전략</b>\n{c.strategy}\n\n1차 {c.target1:,.0f}원\n2차 {c.target2:,.0f}원\n최종 {c.target3:,.0f}원\n손절 {c.stop:,.0f}원'
 def on_book(self,o):STATE.books[(o.market,o.code)]=o
 def on_tick(self,t):
  b=STATE.push(t)
  for m in POSITION.evaluate(t.code,t.price):BOT.send(m)
  if not b or t.code not in STATE.candidates:return
  c=BRAIN.evaluate_day(t.code,t.market)
  if c.stage=='매수 시그널':self.recommend(c)
 def recommend(self,c):
  today=now().date();self.recs=[x for x in self.recs if x['time'].date()==today]
  if len(self.recs)>=SETTINGS.daily_limit or any(x['code']==c.code and x['kind']==c.kind for x in self.recs):return
  self.recs.append({'code':c.code,'kind':c.kind,'time':now(),'price':c.price});self.last_rec=now();STATE.active[c.code]=c.kind;BOT.send(self.score_text(c));DATABASE.insert('recommendations',{'stock_code':c.code,'stock_name':c.name,'recommended_price':c.price,'confidence_score':c.reliability,'recommendation_time':now().isoformat()})
 def rank(self,k,limit=10):
  rows=[]
  codes=list(STATE.candidates)
  if k=='스윙':codes=codes[:SETTINGS.swing_scan_limit]
  for code in codes:
   try:
    c=BRAIN.evaluate_day(code) if k=='단타' else BRAIN.evaluate_swing(code)
    if c.stage not in ('데이터수집중','데이터부족') and c.score>0:rows.append(c)
   except Exception as e:log.warning('%s 후보 계산 실패 %s: %s',k,code,e)
  if not rows:
   if k=='단타':
    counts=[len(STATE.bars.get(('KRX',c),[])) or len(STATE.bars.get(('NXT',c),[])) for c in codes[:10]]
    collected=max(counts) if counts else 0
    return f'⏳ <b>단타 분석 데이터 수집 중</b>\n1분봉 {collected}/{SETTINGS.min_intraday_bars}개\n데이터가 충분해지기 전에는 임의 순위를 표시하지 않습니다.'
   return '⏳ <b>스윙 일봉 데이터 수집 중</b>\n최근 일봉을 불러온 뒤 바닥권·거래량·이평선 기준으로 별도 순위를 계산합니다.'
  rows.sort(key=lambda x:(x.stage=='매수 시그널',x.stage=='예비후보',x.score,x.acceleration,x.reliability),reverse=True)
  lines=[f'🏆 <b>{k} 후보</b>','']
  for i,c in enumerate(rows[:limit],1):
   lines += [f'{i}. <b>{html.escape(c.name)}</b> ({c.code}) · {c.stage}',f'   강도 {c.score:.1f} · 속도 {c.acceleration:.1f} · 신뢰도 {c.reliability:.1f}%']
  return '\n'.join(lines)
 def resolve(self,q):
  if q.isdigit():return q.zfill(6)
  key=q.replace(' ','').lower();x=[c for c,n in STATE.names.items() if n.replace(' ','').lower()==key];return x[0] if len(x)==1 else None
 def handle(self,text,chat):
  p=text.split();cmd=p[0]
  if cmd in ('/도움말','/help'):BOT.send('<b>명령어</b>\n/상태 /단타 /스윙 /예비후보 /시장 /성과\n/매수 종목 매수가 수량 [단타|스윙]\n/매도 종목 /보유 /보유리셋\n/관심등록 종목 /관심삭제 종목 /관심목록\n/호가 종목 /후보갱신',chat)
  elif cmd=='/상태':BOT.send(f'🤖 <b>뽕실 V{SETTINGS.version}</b>\n후보 {len(STATE.candidates)}개\nKRX {STATE.runtime.get("ws_krx")} · 체결 {STATE.runtime.get("krx_trade_subscribed",0)} · 호가 {STATE.runtime.get("krx_orderbook_subscribed",0)}\nNXT {STATE.runtime.get("ws_nxt")} · 체결 {STATE.runtime.get("nxt_trade_subscribed",0)} · 호가 {STATE.runtime.get("nxt_orderbook_subscribed",0)}\n오늘 추천 {len(self.recs)}/{SETTINGS.daily_limit}\n마지막 체결 {STATE.runtime.get("last_tick") or "-"}\n최근 오류 {STATE.runtime.get("last_error") or "-"}',chat)
  elif cmd=='/단타':BOT.send(self.rank('단타'),chat)
  elif cmd=='/스윙':BOT.send(self.rank('스윙'),chat)
  elif cmd=='/예비후보':BOT.send(self.rank('단타',5)+'\n\n'+self.rank('스윙',5),chat)
  elif cmd=='/시장':
   top=sorted(STATE.sectors.items(),key=lambda x:x[1],reverse=True)[:5];BOT.send('📊 <b>시장·섹터</b>\n\n'+'\n'.join(f'• {html.escape(k)} {v:.0f}점' for k,v in top),chat)
  elif cmd=='/매수':
   if len(p)<4:
    BOT.send('사용법: /매수 종목명 매수가 수량 [단타|스윙]',chat);return
   c=self.resolve(p[1]);price=num(p[2]);qty=num(p[3]);kind=p[4] if len(p)>4 and p[4] in ('단타','스윙') else '단타'
   if c and price>0 and qty>0:
    x=POSITION.register(c,STATE.names.get(c,c),price,qty,kind);STATE.positions[c]=kind;DATABASE.upsert('tracked_positions',{'stock_code':c,'stock_name':x.name,'market':'INTEGRATED','entry_price':price,'current_price':price,'quantity':qty,'position_status':'entered','updated_at':now().isoformat()},'stock_code');BOT.send(f'✅ {x.name} {kind} 보유등록\n손절 {x.stop:,.0f}원',chat)
   else:BOT.send('종목명·매수가·수량을 확인해 주세요.',chat)
  elif cmd in ('/매도','/삭제'):
   if len(p)>1:
    c=self.resolve(' '.join(p[1:]))
    if c:
     POSITION.close(c);STATE.positions.pop(c,None);BOT.send(f'✅ {STATE.names.get(c,c)} 보유감시 종료',chat)
    else:BOT.send('종목명을 정확히 입력해 주세요.',chat)
  elif cmd=='/보유':
   if not POSITION.data:BOT.send('📭 등록된 보유종목이 없습니다.',chat)
   else:BOT.send('💼 <b>보유종목</b>\n'+'\n'.join(f'• {x.name} ({x.code}) · {x.state}' for x in POSITION.data.values()),chat)
  elif cmd=='/보유리셋':POSITION.data.clear();STATE.positions.clear();BOT.send('✅ 보유종목 초기화',chat)
  elif cmd=='/관심등록' and len(p)>1:
   c=self.resolve(' '.join(p[1:]))
   if c:
    STATE.watch[c]=STATE.names.get(c,c);BOT.send(f'✅ {STATE.names.get(c,c)} 관심등록',chat)
   else:BOT.send('종목명을 정확히 입력해 주세요.',chat)
  elif cmd=='/관심삭제' and len(p)>1:
   c=self.resolve(' '.join(p[1:]))
   if c:
    STATE.watch.pop(c,None);BOT.send(f'✅ {STATE.names.get(c,c)} 관심삭제',chat)
   else:BOT.send('종목명을 정확히 입력해 주세요.',chat)
  elif cmd=='/관심목록':BOT.send('👀 <b>관심목록</b>\n'+'\n'.join(f'• {n} ({c})' for c,n in STATE.watch.items()),chat)
  elif cmd=='/호가' and len(p)>1:
   c=self.resolve(' '.join(p[1:]))
   if not c:BOT.send('종목명을 정확히 입력해 주세요.',chat)
   else:
    o=STATE.books.get(('NXT',c)) or STATE.books.get(('KRX',c))
    BOT.send(f'📚 {STATE.names.get(c,c)}\n시장 {o.market}\n매수잔량 {o.total_bid:,}\n매도잔량 {o.total_ask:,}\n잔량비 {o.imbalance:.2f}배' if o else '호가 데이터가 아직 없습니다.',chat)
  elif cmd=='/후보갱신':STATE.refresh();BOT.send(f'✅ 후보 {len(STATE.candidates)}개 갱신',chat)
  elif cmd=='/성과':BOT.send(f'📈 오늘 추천 {len(self.recs)}건\n성과는 30분·1시간·종가·익일·3일·1주·2주 기준으로 누적 검증합니다.',chat)
  else:BOT.send('명령어는 /도움말',chat)
 def scheduler(self):
  sent={}
  while True:
   n=now();d=str(n.date())
   if n.weekday()<5:
    if n.hour==7 and 30<=n.minute<35 and sent.get('m')!=d:BOT.send('🌅 <b>장전 브리핑</b>\n\n'+self.rank('스윙',5)+'\n\n전략: 급등 추격 금지, 상승 초입만 선별');sent['m']=d
    if n.hour==15 and 45<=n.minute<50 and sent.get('c')!=d:BOT.send(f'📊 <b>장마감 브리핑</b>\n오늘 추천 {len(self.recs)}건');sent['c']=d
    if n.hour==20 and 5<=n.minute<10 and sent.get('n')!=d:BOT.send('🌙 <b>NXT 마감·익일 준비</b>\n\n'+self.rank('단타',5));sent['n']=d
    if n-self.last_rec>=timedelta(hours=2) and n-self.last_none>=timedelta(hours=2):BOT.send('📢 최근 2시간 동안 조건 충족이 없어 추천 내역이 없습니다.\n시장을 계속 감시 중입니다.');self.last_none=n
   time.sleep(20)
 def swing_scan_loop(self):
  while True:
   try:
    n=now()
    if n.weekday()<5 and 8<=n.hour<=20:
     for code in list(STATE.candidates)[:SETTINGS.swing_scan_limit]:
      c=BRAIN.evaluate_swing(code)
      if c.stage=='매수 시그널':self.recommend(c)
   except Exception as e:log.warning('스윙 자동스캔 실패: %s',e)
   time.sleep(1800)
 def refresh_loop(self):
  while True:
   try:STATE.refresh();BRAIN.sector_refresh()
   except Exception as e:log.warning('refresh %s',e)
   time.sleep(600)
 def start(self):
  STATE.load();STATE.refresh();BRAIN.sector_refresh();BOT.handler=self.handle
  if SETTINGS.telegram_polling:threading.Thread(target=BOT.poll,daemon=True).start()
  threading.Thread(target=self.scheduler,daemon=True).start();threading.Thread(target=self.refresh_loop,daemon=True).start();threading.Thread(target=self.swing_scan_loop,daemon=True).start();threading.Thread(target=KIS_CLIENT.stream,args=('KRX',STATE.realtime_codes,STATE.names,self.on_tick,self.on_book,STATE.runtime),daemon=True).start()
  if SETTINGS.enable_nxt and SETTINGS.kis_env!='virtual':threading.Thread(target=KIS_CLIENT.stream,args=('NXT',STATE.realtime_codes,STATE.names,self.on_tick,self.on_book,STATE.runtime),daemon=True).start()
  BOT.send(f'🤖 <b>뽕실 V{SETTINGS.version} 시작</b>\n전체 후보 {len(STATE.candidates)}개\n실시간 체결구독 시장별 최대 {SETTINGS.ws_trade_limit}개\n실시간 호가구독 시장별 최대 {SETTINGS.ws_orderbook_limit}개\n자동주문 없음')
APP=App()


# ===== main.py =====
import logging
from flask import Flask,jsonify
logging.basicConfig(level=logging.INFO,format='%(asctime)s | %(levelname)s | %(threadName)s | %(message)s')
web=Flask(__name__)
@web.get('/')
def root():return f'뽕실 V{SETTINGS.version} running',200
@web.get('/health')
def health():return jsonify({'status':'ok','version':SETTINGS.version,'runtime':STATE.runtime,'candidates':len(STATE.candidates),'positions':len(POSITION.data)})
if __name__=='__main__':APP.start();web.run(host='0.0.0.0',port=SETTINGS.port,threaded=True,use_reloader=False)
