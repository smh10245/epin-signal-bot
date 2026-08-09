from __future__ import annotations

# 뽕실 V3 AUTO 경량 운영본
# 08:00~14:30 단타 / 14:40 종배 1차 / 15:15 최종 / 15:22 발송 / 20:00 NXT 확인
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
    version:str='3.0.1-auto-safe'
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
    realtime_limit:int=i('REALTIME_SUBSCRIPTION_LIMIT',200)
    ws_trade_limit:int=i('WS_TRADE_LIMIT',6)
    ws_orderbook_limit:int=i('WS_ORDERBOOK_LIMIT',1)
    ws_subscribe_delay:float=f('WS_SUBSCRIBE_DELAY',0.25)
    swing_scan_limit:int=i('SWING_SCAN_LIMIT',40)
    swing_history_days:int=i('SWING_HISTORY_DAYS',220)
    swing_cache_minutes:int=i('SWING_CACHE_MINUTES',360)
    min_intraday_bars:int=i('MIN_INTRADAY_BARS',12)
    daily_limit:int=i('DAILY_RECOMMEND_LIMIT',5)
    # 신규 추천 품질 필터: 저가주·소형주·저유동성 종목 기본 제외
    min_price:float=f('MIN_PRICE',20_000)
    min_market_cap:float=f('MIN_MARKET_CAP',200_000_000_000)
    min_daily_amount:float=f('MIN_DAILY_AMOUNT',10_000_000_000)
    min_daily_volume:int=i('MIN_DAILY_VOLUME',20_000)
    max_chase_pct:float=f('MAX_CHASE_PCT',12.0)
    min_reliability:float=f('MIN_RELIABILITY',67)
    day_signal:float=f('DAYTRADE_SIGNAL_SCORE',84)
    swing_signal:float=f('SWING_SIGNAL_SCORE',84)
    day_stop:float=f('DAYTRADE_STOP_PCT',3.0)
    swing_stop:float=f('SWING_STOP_PCT',5.0)
    nxt_start:str=os.getenv('NXT_WS_START','08:00')
    nxt_end:str=os.getenv('NXT_WS_END','20:00')
    nxt_trade_tr:str=os.getenv('KIS_NXT_TRADE_TR_ID','H0NXCNT0')
    nxt_order_tr:str=os.getenv('KIS_NXT_ORDERBOOK_TR_ID','H0UNASP0')
    briefing_store:str=os.getenv('BRIEFING_STORE','bong_picks.json')
    close_store:str=os.getenv('CLOSE_STORE','bong_close.json')
    close_result_store:str=os.getenv('CLOSE_RESULT_STORE','bong_close_results.json')
    close_signal:float=f('CLOSE_BET_SIGNAL_SCORE',78)
    close_limit:int=i('CLOSE_BET_LIMIT',3)
    close_stop:float=f('CLOSE_BET_STOP_PCT',3.0)
    render_start_delay:int=i('RENDER_START_DELAY',240)
    close_stage1_limit:int=i('CLOSE_STAGE1_LIMIT',40)
    close_stage2_limit:int=i('CLOSE_STAGE2_LIMIT',12)
    rotation_seconds:int=i('ROTATION_SECONDS',180)
    position_store:str=os.getenv('POSITION_STORE','bong_positions.json')
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
log=logging.getLogger('v1.infra')
REAL_REST='https://openapi.koreainvestment.com:9443'; VIRTUAL_REST='https://openapivts.koreainvestment.com:29443'
REAL_WS='ws://ops.koreainvestment.com:21000'; VIRTUAL_WS='ws://ops.koreainvestment.com:31000'
class DB:
    def __init__(self): self.enabled=b('ENABLE_SUPABASE',False) and bool(SETTINGS.supabase_url and SETTINGS.supabase_key); self.s=requests.Session()
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
        self.stream_lock=threading.Lock()
        self.stream_running=False
    def access(self):
        if self.token and self.token_exp and datetime.now(timezone.utc)<self.token_exp:return self.token
        r=self.s.post(f'{self.rest}/oauth2/tokenP',json={'grant_type':'client_credentials','appkey':SETTINGS.kis_app_key,'appsecret':SETTINGS.kis_app_secret},timeout=20); r.raise_for_status(); d=r.json(); self.token=d['access_token']; self.token_exp=datetime.now(timezone.utc)+timedelta(seconds=max(60,int(d.get('expires_in',86400))-300)); return self.token
    def approval_key(self):
        with self.lock:
            if self.approval and self.approval_exp and datetime.now(timezone.utc)<self.approval_exp:return self.approval
            r=self.s.post(f'{self.rest}/oauth2/Approval',json={'grant_type':'client_credentials','appkey':SETTINGS.kis_app_key,'secretkey':SETTINGS.kis_app_secret},timeout=20); r.raise_for_status(); self.approval=r.json()['approval_key']; self.approval_exp=datetime.now(timezone.utc)+timedelta(hours=12); return self.approval
    def price(self,code):
        t=self.access(); r=self.s.get(f'{self.rest}/uapi/domestic-stock/v1/quotations/inquire-price',headers={'authorization':f'Bearer {t}','appkey':SETTINGS.kis_app_key,'appsecret':SETTINGS.kis_app_secret,'tr_id':'FHKST01010100','custtype':'P'},params={'FID_COND_MRKT_DIV_CODE':'J','FID_INPUT_ISCD':code},timeout=20); r.raise_for_status(); d=r.json(); return d.get('output') or {}
    def get_minute_bars(self,code,hour='153000'):
        """KRX REST 1분봉. 종배 2차 압축용이며 NXT 실시간 데이터와 분리 저장한다."""
        try:
            t=self.access()
            headers={'authorization':f'Bearer {t}','appkey':SETTINGS.kis_app_key,'appsecret':SETTINGS.kis_app_secret,'tr_id':'FHKST03010200','custtype':'P'}
            params={'FID_ETC_CLS_CODE':'','FID_COND_MRKT_DIV_CODE':'J','FID_INPUT_ISCD':code,'FID_INPUT_HOUR_1':hour,'FID_PW_DATA_INCU_YN':'Y'}
            r=self.s.get(f'{self.rest}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice',headers=headers,params=params,timeout=12)
            r.raise_for_status()
            return (r.json().get('output2') or [])
        except Exception as e:
            log.debug('KRX 분봉 조회 실패 %s: %s',code,e)
            return []
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
    def stream(self,codes_fn,names,on_tick,on_book,state):
        """NXT 전용 실시간 스트림. KRX 데이터는 REST/일봉 보조 조회로만 사용한다."""
        with self.stream_lock:
            if self.stream_running:
                log.warning('NXT WebSocket 중복 실행 차단')
                return
            self.stream_running=True
        try:
            retry=5
            duplicate_wait=60
            while True:
                if not (SETTINGS.enable_nxt and SETTINGS.kis_env!='virtual'):
                    state['ws_krx']='rest_only'
                    state['ws_nxt']='disabled'
                    time.sleep(60)
                    continue
                if not in_session(SETTINGS.nxt_start,SETTINGS.nxt_end):
                    state['ws_krx']='rest_only'
                    state['ws_nxt']='waiting_market_session'
                    state['nxt_trade_subscribed']=0
                    state['nxt_orderbook_subscribed']=0
                    time.sleep(30)
                    continue

                refresh_stop=threading.Event()
                send_lock=threading.Lock()
                requested={'trade':set(),'order':set()}
                confirmed={'trade':set(),'order':set()}
                pending={}
                duplicate_appkey=False
                subscribe_over=False

                try:
                    key=self.approval_key()
                    state['ws_krx']='rest_only'
                    state['ws_nxt']='connecting'
                    state['nxt_last_error']=''
                    state['nxt_trade_subscribed']=0
                    state['nxt_orderbook_subscribed']=0
                    state['nxt_trade_requested']=0
                    state['nxt_orderbook_requested']=0

                    def send_sub(ws,tr,code,tr_type='1'):
                        payload={
                            'header':{
                                'approval_key':key,
                                'custtype':'P',
                                'tr_type':tr_type,
                                'content-type':'utf-8'
                            },
                            'body':{'input':{'tr_id':tr,'tr_key':code}}
                        }
                        with send_lock:
                            ws.send(json.dumps(payload,ensure_ascii=False))
                        pending[(tr,code)]=tr_type

                    def desired_codes():
                        all_codes=list(dict.fromkeys(codes_fn()))
                        trade_codes=all_codes[:max(1,SETTINGS.ws_trade_limit)]
                        order_codes=trade_codes[:max(0,SETTINGS.ws_orderbook_limit)]
                        return trade_codes,order_codes

                    def refresh_state():
                        state['nxt_trade_requested']=len(requested['trade'])
                        state['nxt_orderbook_requested']=len(requested['order'])
                        state['nxt_trade_subscribed']=len(confirmed['trade'])
                        state['nxt_orderbook_subscribed']=len(confirmed['order'])

                    def sync_nxt(ws):
                        if not in_session(SETTINGS.nxt_start,SETTINGS.nxt_end):
                            refresh_stop.set()
                            try: ws.close()
                            except Exception: pass
                            return
                        trade_codes,order_codes=desired_codes()
                        wanted_trade=set(trade_codes)
                        wanted_order=set(order_codes)

                        # 먼저 불필요한 호가/체결 구독을 해제한 뒤 새 종목을 추가한다.
                        for code in list(requested['order']-wanted_order):
                            send_sub(ws,SETTINGS.nxt_order_tr,code,'2')
                            requested['order'].discard(code)
                            confirmed['order'].discard(code)
                            time.sleep(SETTINGS.ws_subscribe_delay)
                        for code in list(requested['trade']-wanted_trade):
                            send_sub(ws,SETTINGS.nxt_trade_tr,code,'2')
                            requested['trade'].discard(code)
                            confirmed['trade'].discard(code)
                            time.sleep(SETTINGS.ws_subscribe_delay)

                        for code in trade_codes:
                            if code not in requested['trade']:
                                send_sub(ws,SETTINGS.nxt_trade_tr,code,'1')
                                requested['trade'].add(code)
                                time.sleep(SETTINGS.ws_subscribe_delay)
                        for code in order_codes:
                            if code not in requested['order']:
                                send_sub(ws,SETTINGS.nxt_order_tr,code,'1')
                                requested['order'].add(code)
                                time.sleep(SETTINGS.ws_subscribe_delay)

                        refresh_state()
                        state['ws_nxt']='connected' if confirmed['trade'] or confirmed['order'] else 'subscription_pending'

                    def refresh_subscriptions(ws):
                        while not refresh_stop.wait(30):
                            try:
                                sync_nxt(ws)
                            except Exception as e:
                                state['last_error']=f'NXT WS 구독 갱신: {e}'
                                state['nxt_last_error']=str(e)
                                log.warning('NXT WS 구독 갱신 실패: %s',e)
                                try: ws.close()
                                except Exception: pass
                                return

                    def initial_subscribe(ws):
                        try:
                            time.sleep(1.0)
                            if refresh_stop.is_set():
                                return
                            sync_nxt(ws)
                            if refresh_stop.is_set():
                                return
                            threading.Thread(
                                target=refresh_subscriptions,
                                args=(ws,),
                                daemon=True,
                                name='NXT-subscription-refresh'
                            ).start()
                            log.info(
                                'NXT WS 구독 요청: 체결 최대 %s / 호가 최대 %s, 30초마다 재평가',
                                SETTINGS.ws_trade_limit,SETTINGS.ws_orderbook_limit
                            )
                        except Exception as e:
                            refresh_stop.set()
                            state['last_error']=f'NXT 초기 구독: {e}'
                            state['nxt_last_error']=str(e)
                            log.warning('NXT 초기 구독 실패: %s',e)
                            try: ws.close()
                            except Exception: pass

                    def opened(ws):
                        nonlocal retry
                        retry=5
                        threading.Thread(
                            target=initial_subscribe,
                            args=(ws,),
                            daemon=True,
                            name='NXT-initial-subscribe'
                        ).start()

                    def confirm_from_response(header,body):
                        tr_id=str(header.get('tr_id') or body.get('tr_id') or '')
                        tr_key=str(
                            header.get('tr_key')
                            or body.get('tr_key')
                            or (body.get('output') or {}).get('tr_key')
                            or ''
                        ).zfill(6)
                        if not tr_key or tr_key=='000000':
                            return
                        action=pending.pop((tr_id,tr_key),None)
                        target='trade' if tr_id==SETTINGS.nxt_trade_tr else 'order' if tr_id==SETTINGS.nxt_order_tr else None
                        if not target:
                            return
                        if action=='1':
                            confirmed[target].add(tr_key)
                        elif action=='2':
                            confirmed[target].discard(tr_key)
                        refresh_state()

                    def message(ws,msg):
                        nonlocal duplicate_appkey,subscribe_over
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
                                    with send_lock: ws.send(msg)
                                    return
                                rt_cd=str(body.get('rt_cd','0'))
                                msg1=str(body.get('msg1') or '')
                                tr_id=str(header.get('tr_id') or body.get('tr_id') or '')
                                error_code=str(
                                    body.get('msg_cd')
                                    or body.get('error_code')
                                    or header.get('msg_cd')
                                    or ''
                                ).upper()
                                if rt_cd in ('0',''):
                                    confirm_from_response(header,body)
                                    return

                                err=f'{tr_id} {msg1}'.strip()
                                state['last_error']=err
                                state['nxt_last_error']=msg1
                                duplicate_error=(
                                    rt_cd=='9'
                                    or error_code=='OPSP8996'
                                    or 'ALREADY IN USE APPKEY' in msg1.upper()
                                )
                                max_over='MAX SUBSCRIBE OVER' in msg1.upper()
                                if duplicate_error:
                                    duplicate_appkey=True
                                    state['ws_nxt']='duplicate_appkey'
                                    log.error('NXT WS 앱키 중복 사용 감지: %s',msg1)
                                    refresh_stop.set()
                                    try: ws.close()
                                    except Exception: pass
                                    return
                                if max_over:
                                    subscribe_over=True
                                    state['ws_nxt']='subscribe_limit'
                                    log.error('NXT WS 구독 한도 초과: %s',msg1)
                                    refresh_stop.set()
                                    try: ws.close()
                                    except Exception: pass
                                    return
                                log.warning('NXT WS 구독응답 오류: tr=%s, rt_cd=%s, msg=%s',tr_id,rt_cd,msg1)
                            except Exception as e:
                                log.debug('NXT WS JSON 처리 실패: %s',e)
                            return

                        parts=msg.split('|',3)
                        tr_id=parts[1] if len(parts)>=2 else ''
                        if tr_id==SETTINGS.nxt_order_tr:
                            ob=self.parse_book(msg,'NXT',SETTINGS.nxt_order_tr)
                            if ob:
                                confirmed['order'].add(ob.code)
                                refresh_state()
                                on_book(ob)
                            return
                        if tr_id==SETTINGS.nxt_trade_tr:
                            for tick in self.parse_trade(msg,'NXT',names):
                                confirmed['trade'].add(tick.code)
                                refresh_state()
                                on_tick(tick)

                    def error(ws,e):
                        refresh_stop.set()
                        state['last_error']=f'NXT WS: {e}'
                        state['nxt_last_error']=str(e)
                        if state.get('ws_nxt') not in ('duplicate_appkey','subscribe_limit'):
                            state['ws_nxt']='error'
                        log.warning('NXT WS 오류: %s',e)

                    def closed(ws,status,msg):
                        refresh_stop.set()
                        if state.get('ws_nxt') not in ('duplicate_appkey','subscribe_limit','waiting_market_session','disabled'):
                            state['ws_nxt']='closed'
                        log.warning('NXT WS 종료: status=%s, msg=%s',status,msg)

                    ws_app=websocket.WebSocketApp(
                        self.ws,on_open=opened,on_message=message,on_error=error,on_close=closed
                    )
                    ws_app.run_forever(
                        ping_interval=25,ping_timeout=10,skip_utf8_validation=True
                    )
                    refresh_stop.set()

                    if duplicate_appkey:
                        with self.lock:
                            self.approval=None
                            self.approval_exp=None
                        log.warning('앱키 중복 세션 해제를 위해 %s초 후 재연결합니다.',duplicate_wait)
                        time.sleep(duplicate_wait)
                        retry=5
                    elif subscribe_over:
                        # 기본 8/2에서도 한도가 발생하면 즉시 반복하지 않고 충분히 기다린다.
                        log.warning('NXT 구독 한도 오류 후 120초 대기합니다.')
                        time.sleep(120)
                        retry=5
                    else:
                        time.sleep(retry)
                        retry=min(120,retry*2)

                except Exception as e:
                    refresh_stop.set()
                    state['ws_krx']='rest_only'
                    state['ws_nxt']='failed'
                    state['last_error']=f'NXT WS: {e}'
                    state['nxt_last_error']=str(e)
                    log.exception('NXT stream 실패')
                    time.sleep(retry)
                    retry=min(120,retry*2)
        finally:
            with self.stream_lock:
                self.stream_running=False

KIS_CLIENT=KIS()


# ===== core.py =====
import html,logging,math,threading,time
from collections import deque
from datetime import timedelta
from typing import Any,Dict,List,Optional,Tuple
import FinanceDataReader as fdr
log=logging.getLogger('v1.core')
LABEL={'volume':'거래량 증가','strength':'체결강도','trend':'상승추세','breakout':'돌파','pullback':'눌림목 재돌파','v':'V자 반등','kiyoung':'기영이 패턴','order':'호가 매수우위','sector':'섹터 강도','bottom':'바닥권','base':'바닥 다지기','volume_return':'거래량 재유입','ma_turn':'이평선 전환','box_break':'박스권 돌파 가능성','rebound':'바닥 반등 초기','vwap':'VWAP 지지','liquidity':'거래대금·회전율','high_position':'고가 부근 마감','late_volume':'장 후반 거래량'}
class State:
    def __init__(self):
        self.lock=threading.RLock(); self.names={}; self.meta={}; self.candidates={}; self.watch={}; self.positions={}; self.active={}; self.bars={}; self.current={}; self.last_cum={}; self.books={}; self.ticks={}; self.score_history={}; self.sectors={}; self.daily_cache={}; self.realtime_cache=[]; self.realtime_cache_at=None; self.realtime_priority_key=(); self.runtime={'mode':'AUTO','phase':'대기','ws_krx':'rest_only','ws_nxt':'stopped','last_tick':None,'last_error':None,'nxt_trade_requested':0,'nxt_orderbook_requested':0,'nxt_trade_subscribed':0,'nxt_orderbook_subscribed':0}
    def load(self):
        df=fdr.StockListing('KRX')
        names={}; meta={}
        for _,r in df.iterrows():
            raw=str(r.get('Code') or r.get('Symbol') or '').strip()
            if not raw or raw.lower()=='nan':
                continue
            c=raw.zfill(6); n=str(r.get('Name') or c).strip()
            if c and n:
                names[c]=n
                meta[c]={str(k):r[k] for k in df.columns}
        if not names:
            raise RuntimeError('KRX 종목 목록이 비어 있습니다.')
        with self.lock:
            self.names=names
            self.meta=meta
    def refresh(self):
        """거래대금 순위 50% + 시가총액 대비 회전율 순위 50%로 후보를 고른다."""
        items=[]
        with self.lock:
            for c,m in self.meta.items():
                close=num(m.get('Close')); vol=integer(m.get('Volume')); cap=num(m.get('Marcap'))
                amount=num(m.get('Amount')) or close*vol
                # 일반 신규 후보는 최소주가/시총/거래대금/거래량 기준을 모두 통과해야 한다.
                if close and close<SETTINGS.min_price:continue
                if cap and cap<SETTINGS.min_market_cap:continue
                if amount and amount<SETTINGS.min_daily_amount:continue
                if vol and vol<SETTINGS.min_daily_volume:continue
                turnover=(amount/cap) if cap>0 else 0
                items.append((c,amount,turnover))
            by_amount=sorted(items,key=lambda x:x[1],reverse=True)
            by_turn=sorted(items,key=lambda x:x[2],reverse=True)
            amount_rank={c:idx for idx,(c,_,_) in enumerate(by_amount)}
            turn_rank={c:idx for idx,(c,_,_) in enumerate(by_turn)}
            ranked=sorted(items,key=lambda x:(amount_rank[x[0]]*.5+turn_rank[x[0]]*.5))
            priority=list(dict.fromkeys(list(self.positions)+list(self.watch)+list(self.active)))
            codes=priority+[c for c,_,_ in ranked if c not in priority]
            self.candidates={c:self.meta.get(c,{}) for c in codes[:SETTINGS.max_candidates]}
    def realtime_codes(self):
        """AUTO 경량모드: 보유·관심 우선, 집중감시는 최소 3분간 유지한다."""
        with self.lock:
            priority=list(dict.fromkeys(list(self.positions)+list(self.watch)+list(self.active)))
            priority_key=tuple(priority)
            n=now()
            if (self.realtime_cache and self.realtime_cache_at is not None
                and (n-self.realtime_cache_at).total_seconds()<SETTINGS.rotation_seconds
                and priority_key==self.realtime_priority_key):
                return list(self.realtime_cache)
            scored=[]
            for idx,c in enumerate(self.candidates):
                tick=self.ticks.get(('NXT',c))
                bars=list(self.bars.get(('NXT',c),[]))
                meta=self.meta.get(c,{})
                score=0.0
                if c in self.positions: score+=100000
                if c in self.active: score+=75000
                if c in self.watch: score+=50000
                if tick:
                    score+=max(0,tick.trade_strength)*20
                    score+=min(50000,max(0,tick.volume))
                if bars:
                    score+=min(50000,max(0,bars[-1].volume))
                amount=num(meta.get('Amount')) or num(meta.get('Close'))*num(meta.get('Volume'))
                score+=min(20000,math.log10(max(amount,10))*1000)
                score-=idx*.01
                scored.append((score,c))
            scored.sort(reverse=True)
            ordered=priority+[c for _,c in scored if c not in priority]
            selected=ordered[:max(SETTINGS.ws_trade_limit,SETTINGS.ws_orderbook_limit)]
            self.realtime_cache=list(selected)
            self.realtime_cache_at=n
            self.realtime_priority_key=priority_key
            return list(selected)
    def push(self,t:Tick):
        with self.lock:
            key=(t.market,t.code)
            minute=t.timestamp.replace(second=0,microsecond=0)
            last=self.last_cum.get(key,t.cumulative_volume)
            inc=max(0,t.cumulative_volume-last)
            self.last_cum[key]=t.cumulative_volume
            b=self.current.get(key)
            self.ticks[key]=t
            self.runtime['last_tick']=t.timestamp.isoformat()
            tick_volume=max(t.volume,inc)
            if not b:
                self.current[key]=Bar(t.code,t.name,t.market,minute,t.price,t.price,t.price,t.price,tick_volume,t.cumulative_volume,t.trade_strength)
                return None
            if b.minute==minute:
                b.high=max(b.high,t.price);b.low=min(b.low,t.price);b.close=t.price
                b.volume+=tick_volume;b.cumulative_volume=max(b.cumulative_volume,t.cumulative_volume)
                b.trade_strength=t.trade_strength
                return None
            self.current[key]=Bar(t.code,t.name,t.market,minute,t.price,t.price,t.price,t.price,tick_volume,t.cumulative_volume,t.trade_strength)
            self.bars.setdefault(key,deque(maxlen=240)).append(b)
            return b
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
        o={'volume':0,'strength':0,'trend':0,'breakout':0,'pullback':0,'v':0,'kiyoung':0,'vwap':0}
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
        total_vol=sum(max(0,b.volume) for b in r)
        if total_vol>0:
            vwap=sum(((b.high+b.low+b.close)/3)*max(0,b.volume) for b in r)/total_vol
            distance=abs(pct(latest.close,vwap))
            if distance<=1.5 and latest.close>=vwap*.995:
                o['vwap']=clamp(90-distance*20)
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
        elif price<SETTINGS.min_price:
            out.append(f'최소 추천주가 {SETTINGS.min_price:,.0f}원 미만')
        n=STATE.names.get(c,c)
        if any(x in n for x in ['스팩','ETN']):
            out.append('대상 제외 상품')
        return out

    def evaluate_day(self,c,market='NXT'):
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
        dw={'volume':.16,'strength':.13,'trend':.10,'breakout':.12,'pullback':.11,'v':.09,'kiyoung':.09,'vwap':.10,'order':.06,'sector':.04}
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

    def evaluate_close(self,c):
        """종가배팅 전용 점수. 일봉 + 당일 NXT 분봉 + 섹터를 결합한다."""
        df=self.daily_history(c)
        bars=list(STATE.bars.get(('NXT',c),[])) or list(STATE.bars.get(('KRX',c),[]))
        if df is None or len(df)<25 or len(bars)<SETTINGS.min_intraday_bars:
            return ScoreCard(c,STATE.names.get(c,c),'종배',0,50,0,'데이터부족',[],[],
                '일봉 25개와 NXT 1분봉이 필요',0,0,0,0,0,{})
        price=bars[-1].close
        highs=[b.high for b in bars]; lows=[b.low for b in bars]; vols=[max(0,b.volume) for b in bars]
        day_high=max(highs); day_low=min(lows)
        high_pos=clamp((price-day_low)/(day_high-day_low)*100 if day_high>day_low else 50)
        total_vol=sum(vols)
        vwap=sum(((b.high+b.low+b.close)/3)*max(0,b.volume) for b in bars)/total_vol if total_vol else price
        vwap_score=clamp(100-abs(pct(price,vwap))*25) if price>=vwap*.995 else 0
        late=bars[-30:] if len(bars)>=30 else bars
        prev=bars[-60:-30] if len(bars)>=60 else bars[:-len(late)] or bars
        late_avg=sum(max(0,b.volume) for b in late)/max(1,len(late))
        prev_avg=sum(max(0,b.volume) for b in prev)/max(1,len(prev))
        late_volume=clamp((late_avg/prev_avg if prev_avg>0 else 0)*45)
        strength=clamp((bars[-1].trade_strength-80)*1.4)
        close=df['Close'].astype(float)
        ma5=float(close.tail(5).mean()); ma20=float(close.tail(20).mean())
        trend=90 if price>=ma5>=ma20 else 65 if price>=ma20 else 20
        box=float(df['High'].astype(float).iloc[-21:-1].max())
        breakout=90 if box>0 and price>=box*.995 else 65 if box>0 and price>=box*.975 else 20
        sector=self.sector(c)
        m=STATE.meta.get(c,{})
        amount=num(m.get('Amount')) or num(m.get('Close'))*num(m.get('Volume'))
        cap=num(m.get('Marcap'))
        turnover=(amount/cap*100) if cap>0 else 0
        liquidity=clamp(math.log10(max(amount,10))*6 + min(35,turnover*8))
        components={'liquidity':liquidity,'high_position':high_pos,'late_volume':late_volume,
                    'vwap':vwap_score,'strength':strength,'trend':trend,'breakout':breakout,'sector':sector}
        weights={'liquidity':.20,'high_position':.15,'late_volume':.15,'vwap':.10,
                 'strength':.10,'trend':.10,'breakout':.10,'sector':.10}
        score=sum(components[k]*w for k,w in weights.items())
        blockers=self.blockers(c,price)
        if high_pos<60:blockers.append('종가가 당일 고가에서 멀음')
        if price<vwap*.99:blockers.append('VWAP 이탈')
        if pct(price,float(close.iloc[-2]))>=10:blockers.append('당일 급등으로 익일 갭 위험')
        reliability=clamp(35+score*.45+sum(1 for v in components.values() if v>=70)*4-len(blockers)*18)
        stage='종배 시그널' if score>=SETTINGS.close_signal and reliability>=SETTINGS.min_reliability and not blockers else '종배 예비' if score>=68 else '관찰'
        reasons=[k for k,v in sorted(components.items(),key=lambda x:x[1],reverse=True) if v>=65][:5]
        return ScoreCard(c,STATE.names.get(c,c),'종배',round(score,1),50,round(reliability,1),stage,reasons,blockers,
            '종가 부근 분할 접근·익일 갭상승 추격 금지',price,price*1.045,price*1.075,price*1.10,price*(1-SETTINGS.close_stop/100),components)

    def evaluate(self,c,market='NXT'):
        return self.evaluate_day(c,market),self.evaluate_swing(c)
BRAIN=Brain()
class PositionEngine:
    def __init__(self):self.data={}
    def register(self,c,n,p,q,k):
        if k=='단타':
            stop_pct=SETTINGS.day_stop; targets=(1.03,1.06,1.10)
        elif k=='종배':
            stop_pct=SETTINGS.close_stop; targets=(1.045,1.075,1.10)
        else:
            stop_pct=SETTINGS.swing_stop; targets=(1.08,1.14,1.20)
        stop=p*(1-stop_pct/100)
        x=Position(c,n,k,p,q,p,stop,p*targets[0],p*targets[1],p*targets[2])
        self.data[c]=x
        return x
    def close(self,c):self.data.pop(c,None)
    def evaluate(self,c,current):
        p=self.data.get(c);out=[]
        if not p:return out
        old_stop=p.stop
        p.highest=max(p.highest,current);rate=pct(current,p.entry)
        if rate>=10:
            p.stop=max(p.stop,p.highest*.96)
        elif rate>=3:
            p.stop=max(p.stop,max(p.entry*1.01,p.highest*.97))
        if p.stop>old_stop and rate>=3:
            p.state='방어선 상향'
        if current<=p.stop and not p.stop_notified:
            p.stop_notified=True;p.state='방어선 이탈'
            out.append(f'🛡️ {p.name} 방어선 이탈 · {current:,.0f}원 · {rate:+.2f}% · 매도 검토')
        elif p.stop_notified and current>p.entry and not p.recovered:
            p.recovered=True;p.state='방어선 회복';out.append(f'🟢 {p.name} 방어선 회복 · 매수가 상단 재진입')
        if current>=p.target1 and not p.t1:p.t1=True;p.state='1차 익절권';out.append(f'🎯 {p.name} 1차 익절권 · {rate:+.2f}%')
        if current>=p.target2 and not p.t2:p.t2=True;p.state='2차 익절권';out.append(f'🔥 {p.name} 2차 익절권 · {rate:+.2f}%')
        return out
POSITION=PositionEngine()


# ===== app.py =====
import html,threading,time,logging
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus
from pathlib import Path
import yfinance as yf
from datetime import timedelta
log=logging.getLogger('v1.app')
class App:
 def __init__(self):
  self.recs=[]
  self.last_rec=now()
  self.last_none=now()
  self._start_lock=threading.Lock()
  self._started=False
  self.close_picks=[]
  self.close_scan_stage='대기'
  self.close_stage1_codes=[]
  self.close_stage2_codes=[]
  self.close_scan_running=False
 def score_text(self,c):
  rs='\n'.join(f'• {LABEL.get(x,x)}' for x in c.reasons) or '• 핵심 조건 추가 확인 필요'; bs='\n'.join(f'• {html.escape(x)}' for x in c.blockers)
  return f"{'🔥' if c.stage=='매수 시그널' else '🟡'} <b>{c.kind} {c.stage}</b>\n\n<b>{html.escape(c.name)}</b> ({c.code})\n현재가 <b>{c.price:,.0f}원</b>\n현재 강도 {c.score:.1f}점\n상승 속도 {c.acceleration:.1f}점\n추천 신뢰도 <b>{c.reliability:.1f}%</b>\n\n<b>추천 사유</b>\n{rs}"+(f'\n\n<b>주의</b>\n{bs}' if bs else '')+f'\n\n<b>매매 한 줄 전략</b>\n{c.strategy}\n\n1차 {c.target1:,.0f}원\n2차 {c.target2:,.0f}원\n최종 {c.target3:,.0f}원\n손절 {c.stop:,.0f}원'
 def on_book(self,o):STATE.books[(o.market,o.code)]=o
 def auto_phase(self):
  n=now()
  if n.weekday()>=5:return '휴장'
  hm=n.hour*60+n.minute
  if hm<8*60:return '장전'
  if hm<14*60+30:return '단타 감시'
  if hm<15*60+15:return '종배 준비'
  if hm<15*60+30:return '종배 최종분석'
  if hm<20*60:return 'NXT 사후확인'
  return '마감'

 def on_tick(self,t):
  b=STATE.push(t)
  for m in POSITION.evaluate(t.code,t.price):BOT.send(m)
  STATE.runtime['phase']=self.auto_phase()
  if not b or t.code not in STATE.candidates:return
  if self.auto_phase()!='단타 감시':return
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
    counts=[len(STATE.bars.get(('NXT',c),[])) for c in codes[:10]]
    collected=max(counts) if counts else 0
    return f'⏳ <b>단타 분석 데이터 수집 중</b>\n1분봉 {collected}/{SETTINGS.min_intraday_bars}개\n데이터가 충분해지기 전에는 임의 순위를 표시하지 않습니다.'
   return '⏳ <b>스윙 일봉 데이터 수집 중</b>\n최근 일봉을 불러온 뒤 바닥권·거래량·이평선 기준으로 별도 순위를 계산합니다.'
  rows.sort(key=lambda x:(x.stage=='매수 시그널',x.stage=='예비후보',x.score,x.acceleration,x.reliability),reverse=True)
  lines=[f'🏆 <b>{k} 후보</b>','']
  for i,c in enumerate(rows[:limit],1):
   lines += [f'{i}. <b>{html.escape(c.name)}</b> ({c.code}) · {c.stage}',f'   강도 {c.score:.1f} · 속도 {c.acceleration:.1f} · 신뢰도 {c.reliability:.1f}%']
  return '\n'.join(lines)

 def load_close_intraday(self,codes):
  """압축된 종목의 KRX 1분봉만 REST로 적재한다. NXT 분봉과 섞지 않는다."""
  for code in codes:
   raw=KIS_CLIENT.get_minute_bars(code)
   if not raw:continue
   q=deque(maxlen=240)
   for r in reversed(raw):
    try:
     h=str(r.get('stck_cntg_hour') or '').zfill(6)
     minute=now().replace(hour=int(h[:2]),minute=int(h[2:4]),second=0,microsecond=0)
     q.append(Bar(code,STATE.names.get(code,code),'KRX',minute,num(r.get('stck_oprc')),num(r.get('stck_hgpr')),num(r.get('stck_lwpr')),num(r.get('stck_prpr')),integer(r.get('cntg_vol')),integer(r.get('acml_vol')),100.0))
    except Exception:pass
   if q:
    with STATE.lock:STATE.bars[('KRX',code)]=q
   time.sleep(.08)

 def close_stage1(self):
  """14:40: 최신 거래대금·회전율 기준으로 40개를 빠르게 압축한다."""
  rows=[]
  try:
   STATE.load();STATE.refresh();BRAIN.sector_refresh()
  except Exception as e:
   log.warning('종배 직전 시장 데이터 갱신 실패: %s',e)
  for code,m in list(STATE.candidates.items()):
   close=num(m.get('Close'));vol=num(m.get('Volume'));cap=num(m.get('Marcap'))
   amount=num(m.get('Amount')) or close*vol
   change=num(m.get('ChagesRatio') or m.get('ChangesRatio'))
   if close<=0 or amount<=0 or change>=SETTINGS.max_chase_pct:continue
   if close<SETTINGS.min_price:continue
   if cap>0 and cap<SETTINGS.min_market_cap:continue
   if amount<SETTINGS.min_daily_amount:continue
   turnover=amount/cap*100 if cap>0 else 0
   score=min(70,math.log10(max(amount,10))*5)+min(30,turnover*8)
   rows.append((score,code))
  rows.sort(reverse=True)
  self.close_stage1_codes=[c for _,c in rows[:SETTINGS.close_stage1_limit]]
  self.close_scan_stage='1차완료'
  return self.close_stage1_codes

 def close_stage2(self):
  """15:15: 40개 중 일봉 조건을 통과한 12개만 최신 분봉으로 정밀 분석한다."""
  codes=self.close_stage1_codes or self.close_stage1()
  daily_rows=[]
  for code in codes:
   try:
    df=BRAIN.daily_history(code)
    if df is None or len(df)<25:continue
    close=float(df['Close'].iloc[-1]);ma5=float(df['Close'].tail(5).mean());ma20=float(df['Close'].tail(20).mean())
    if close<ma20:continue
    daily_rows.append((20 if close>=ma5>=ma20 else 8,code))
   except Exception:pass
  daily_rows.sort(reverse=True)
  inspect=[c for _,c in daily_rows[:SETTINGS.close_stage2_limit]]
  self.load_close_intraday(inspect)
  rows=[]
  for code in inspect:
   try:
    card=BRAIN.evaluate_close(code)
    if card.score>0:rows.append(card)
   except Exception as e:log.debug('종배 최종 실패 %s: %s',code,e)
  rows.sort(key=lambda x:(x.stage=='종배 시그널',x.score,x.reliability),reverse=True)
  self.close_stage2_codes=[x.code for x in rows]
  self.close_scan_stage='최종완료'
  return rows

 def close_candidates(self,limit=None):
  limit=limit or SETTINGS.close_limit
  rows=self.close_stage2()
  final=[]
  for c in rows:
   tick=STATE.ticks.get(('NXT',c.code))
   if tick:
    c.score=round(min(100,c.score+clamp((tick.trade_strength-100)*.08,0,5)),1)
    c.reliability=round(min(100,c.reliability+3),1)
   if c.stage!='데이터부족':final.append(c)
  final.sort(key=lambda x:(x.stage=='종배 시그널',x.score,x.reliability),reverse=True)
  return final[:limit]

 def close_rank_text(self,limit=None):
  rows=self.close_candidates(limit)
  if not rows:return '⏳ <b>종가배팅 분석 데이터가 부족합니다.</b>'
  lines=['🌆 <b>종가배팅 후보</b>','']
  for i,c in enumerate(rows,1):
   why=', '.join(LABEL.get(x,x) for x in c.reasons) or '점수 상위'
   caution=' / '.join(c.blockers[:2]) if c.blockers else '특이 위험 없음'
   lines += [f'{i}. <b>{html.escape(c.name)}</b> ({c.code}) · {c.stage}',f'   현재 {c.price:,.0f}원 · 점수 {c.score:.1f} · 신뢰 {c.reliability:.1f}%',f'   근거: {html.escape(why)}',f'   손절 {c.stop:,.0f}원 · 1차 {c.target1:,.0f}원 · 주의: {html.escape(caution)}','']
  return '\n'.join(lines)

 def save_close_picks(self,rows):
  try:
   payload={'date':str(now().date()),'saved_at':now().isoformat(),'picks':[{'code':c.code,'name':c.name,'price':c.price,'score':c.score,'reliability':c.reliability,'stop':c.stop,'target1':c.target1,'target2':c.target2,'stage':c.stage} for c in rows]}
   Path(SETTINGS.close_store).write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')
  except Exception as e:log.warning('종배 후보 저장 실패: %s',e)

 def load_close_picks(self):
  try:
   p=Path(SETTINGS.close_store)
   return json.loads(p.read_text(encoding='utf-8')).get('picks',[]) if p.exists() else []
  except Exception as e:log.warning('종배 후보 읽기 실패: %s',e);return []

 def run_close_scan(self,stage,send=False,chat=None):
  if self.close_scan_running:
   if send:BOT.send('⏳ 종가배팅 분석이 이미 진행 중입니다.',chat)
   return []
  self.close_scan_running=True
  try:
   if stage=='1차':
    self.close_stage1();rows=[]
    log.info('AUTO 종배 1차 압축 완료: %s개',len(self.close_stage1_codes))
   else:
    rows=self.close_candidates(SETTINGS.close_limit)
    self.close_picks=rows;self.save_close_picks(rows)
    log.info('AUTO 종배 최종 추천 완료: %s개',len(rows))
   if send:BOT.send(self.close_rank_text_from_rows(rows),chat)
   return rows
  finally:
   self.close_scan_running=False

 def close_rank_text_from_rows(self,rows):
  if not rows:return '📭 최종 종가배팅 후보가 없습니다.'
  lines=['🌆 <b>AUTO 종가배팅 최종 후보</b>','']
  for i,c in enumerate(rows,1):
   why=', '.join(LABEL.get(x,x) for x in c.reasons) or '점수 상위'
   lines += [f'{i}. <b>{html.escape(c.name)}</b> ({c.code})',f'   현재 {c.price:,.0f}원 · 점수 {c.score:.1f} · 신뢰 {c.reliability:.1f}%',f'   근거: {html.escape(why)}',f'   손절 {c.stop:,.0f}원 · 1차 {c.target1:,.0f}원','']
  return '\n'.join(lines)

 def update_close_results(self):
  """저장된 종배 추천의 익일/3거래일 성과를 일봉으로 실제 계산해 JSON에 누적한다."""
  picks=self.load_close_picks()
  if not picks:return []
  try:pick_date=datetime.fromisoformat(json.loads(Path(SETTINGS.close_store).read_text(encoding='utf-8')).get('date')).date()
  except Exception:return []
  results=[]
  for x in picks:
   try:
    df=BRAIN.daily_history(x['code'])
    if df is None or df.empty:continue
    future=df[df.index.date>pick_date].head(3)
    if future.empty:continue
    rec=num(x.get('price')); first=future.iloc[0]
    low1=float(first['Low']); high1=float(first['High'])
    results.append({'date':str(pick_date),'code':x['code'],'name':x.get('name',x['code']),'recommended_price':rec,'next_open_pct':pct(float(first['Open']),rec),'next_high_pct':pct(high1,rec),'next_low_pct':pct(low1,rec),'next_close_pct':pct(float(first['Close']),rec),'three_day_high_pct':pct(float(future['High'].max()),rec),'stop_hit':low1<=num(x.get('stop')),'success':high1>=num(x.get('target1'))})
   except Exception as e:log.debug('종배 성과 계산 실패 %s: %s',x.get('code'),e)
  if results:
   try:
    p=Path(SETTINGS.close_result_store);old=json.loads(p.read_text(encoding='utf-8')) if p.exists() else []
    index={(r.get('date'),r.get('code')):r for r in old if isinstance(r,dict)}
    for r in results:index[(r['date'],r['code'])]=r
    p.write_text(json.dumps(list(index.values()),ensure_ascii=False,indent=2),encoding='utf-8')
   except Exception as e:log.warning('종배 성과 저장 실패: %s',e)
  return results

 def performance_text(self):
  self.update_close_results()
  try:
   p=Path(SETTINGS.close_result_store);rows=json.loads(p.read_text(encoding='utf-8')) if p.exists() else []
  except Exception:rows=[]
  if not rows:return '📭 아직 확정된 종가배팅 성과가 없습니다.'
  total=len(rows);success=sum(1 for r in rows if r.get('success'));stop=sum(1 for r in rows if r.get('stop_hit'))
  avg=sum(num(r.get('next_close_pct')) for r in rows)/total
  return f'📊 <b>종가배팅 누적 성과</b>\n총 {total}건 · 1차 목표 성공 {success}건 ({success/total*100:.1f}%)\n손절선 도달 {stop}건 · 익일 종가 평균 {avg:+.2f}%'

 def close_result_text(self):
  picks=self.load_close_picks()
  if not picks:return '📭 저장된 종가배팅 추천이 없습니다.'
  lines=['📈 <b>NXT 종가배팅 사후 확인</b>','']
  for x in picks:
   code=x.get('code');name=x.get('name',code);rec=num(x.get('price'));current=0;source='NXT'
   tick=STATE.ticks.get(('NXT',code))
   if tick:current=tick.price
   if current<=0:
    try:current=num(KIS_CLIENT.price(code).get('stck_prpr'));source='KRX REST'
    except Exception:current=0;source='확인 불가'
   rate=pct(current,rec) if current and rec else 0
   status='유지' if rate>=0 else '주의' if rate>-1.5 else '제외 검토'
   lines.append(f"• <b>{html.escape(name)}</b> ({code})\n  추천 {rec:,.0f}원 · {source} {current:,.0f}원 · {rate:+.2f}% · <b>{status}</b>")
  return '\n\n'.join(lines)

 def holding_text(self):
  if not POSITION.data:return '📭 등록된 보유종목이 없습니다.'
  blocks=[]
  for x in POSITION.data.values():
   tick=STATE.ticks.get(('NXT',x.code))
   cur=tick.price if tick else 0
   source='NXT'
   if cur<=0:
    bar=STATE.current.get(('NXT',x.code))
    if bar:cur=bar.close
   if cur<=0:
    try:cur=num(KIS_CLIENT.price(x.code).get('stck_prpr'));source='KRX REST'
    except Exception:cur=0;source='확인 불가'
   gain=pct(cur,x.entry) if cur else 0
   profit=(cur-x.entry)*x.qty if cur else 0
   icon='🟢' if gain>0 else '🔴' if gain<0 else '⚪'
   all_bars=list(STATE.bars.get(('NXT',x.code),[]));current_bar=STATE.current.get(('NXT',x.code))
   highs=[b.high for b in all_bars]+([current_bar.high] if current_bar else [])
   today_high=max(highs) if highs else cur
   high_drawdown=pct(cur,today_high) if cur and today_high else 0
   ob=STATE.books.get(('NXT',x.code))
   strength=tick.trade_strength if tick else None
   ob_text=f'{ob.imbalance:.2f}배' if ob else '호가 집중감시 대상 아님'
   strength_text=f'{strength:.0f}%' if strength is not None else '수신 없음'
   opinion='보유 유지 관찰'
   if cur and cur<=x.stop:opinion='방어선 이탈 · 매도 검토'
   elif gain>=3:opinion='수익구간 · 방어선 확인'
   elif strength is not None and strength<90:opinion='체결강도 약화 · 보수적 대응'
   blocks.append(
    f'<b>{html.escape(x.name)}</b> ({x.code}) · {x.kind} · {x.state}\n'
    f'매수가 {x.entry:,.0f}원 · 수량 {x.qty:g}주\n'
    f'현재가({source}) <b>{cur:,.0f}원</b> · {icon} <b>{gain:+.2f}%</b>\n'
    f'평가손익 {profit:+,.0f}원 · 오늘고가 {today_high:,.0f}원 ({high_drawdown:+.2f}%)\n'
    f'체결강도 {strength_text} · 호가 잔량비 {ob_text}\n'
    f'방어선 {x.stop:,.0f}원 · 1차 {x.target1:,.0f}원 · 2차 {x.target2:,.0f}원 · 최종 {x.target3:,.0f}원\n'
    f'뽕실 의견: {opinion}'
   )
  return '💼 <b>보유종목</b>\n\n'+'\n\n'.join(blocks)

 def save_night_picks(self,picks):
  try:
   payload={'date':str(now().date()),'picks':picks}
   Path(SETTINGS.briefing_store).write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')
  except Exception as e:
   log.warning('야간 후보 저장 실패: %s',e)

 def load_night_picks(self):
  try:
   p=Path(SETTINGS.briefing_store)
   if not p.exists():return []
   data=json.loads(p.read_text(encoding='utf-8'))
   return data.get('picks') or []
  except Exception as e:
   log.warning('야간 후보 읽기 실패: %s',e)
   return []

 def us_market_text(self):
  rows=[]
  for name,symbol in {'S&P500':'^GSPC','나스닥':'^IXIC','다우':'^DJI','필라델피아반도체':'^SOX'}.items():
   try:
    df=yf.Ticker(symbol).history(period='5d')
    if len(df)>=2:
     close=float(df['Close'].iloc[-1]);prev=float(df['Close'].iloc[-2])
     rows.append(f'• {name} {pct(close,prev):+.2f}%')
   except Exception as e:
    log.debug('미국지수 조회 실패 %s: %s',name,e)
  return '\n'.join(rows) or '• 미국 증시 조회 실패'

 def morning_compare_text(self):
  previous=self.load_night_picks()
  current=self.tomorrow_candidates(5)
  prev_codes={x.get('code') for x in previous}
  cur_codes={x.get('code') for x in current}
  keep=[x for x in current if x.get('code') in prev_codes]
  new=[x for x in current if x.get('code') not in prev_codes]
  removed=[x for x in previous if x.get('code') not in cur_codes]
  def names(rows):
   return ', '.join(html.escape(x.get('name',x.get('code','-'))) for x in rows) or '없음'
  news=self.news_headlines(4)
  return (
   '🌅 <b>07:30 장전 재검증 브리핑</b>\n\n'
   f'<b>미국 증시</b>\n{self.us_market_text()}\n\n'
   f'<b>밤사이 주요 뉴스</b>\n'+('\n'.join(f'• {html.escape(x)}' for x in news) or '• 뉴스 없음')+'\n\n'
   f'<b>전날 후보 유지</b>: {names(keep)}\n'
   f'<b>신규 편입</b>: {names(new)}\n'
   f'<b>제외</b>: {names(removed)}\n\n'
   '※ 전날 22시 후보와 아침 데이터를 비교한 관찰 목록입니다.'
  )

 def news_headlines(self,limit=5):
  """Google News RSS에서 국내 증시 관련 최신 제목만 짧게 수집한다."""
  queries=[
   '한국 증시 코스피 코스닥',
   '반도체 AI 원전 방산 전력 주식',
   '환율 유가 금리 국내 증시'
  ]
  titles=[]
  for q in queries:
   try:
    url='https://news.google.com/rss/search?q='+quote_plus(q)+'&hl=ko&gl=KR&ceid=KR:ko'
    r=requests.get(url,timeout=10)
    r.raise_for_status()
    root=ET.fromstring(r.text)
    for item in root.findall('.//item'):
     title=(item.findtext('title') or '').strip()
     if title and title not in titles:
      titles.append(title)
     if len(titles)>=limit:
      return titles
   except Exception as e:
    log.warning('뉴스 조회 실패 %s: %s',q,e)
  return titles[:limit]

 def tomorrow_candidates(self,limit=5):
  """당일 흐름과 스윙 일봉을 함께 반영해 다음 거래일 관찰 후보를 만든다."""
  rows=[]
  for code in list(dict.fromkeys(self.close_stage2_codes+list(STATE.candidates)[:20])):
   try:
    day=BRAIN.evaluate_day(code)
    swing=BRAIN.evaluate_swing(code)
    close=BRAIN.evaluate_close(code)
    day_score=day.score if day.stage not in ('데이터수집중','데이터부족') else 0
    swing_score=swing.score if swing.stage not in ('데이터수집중','데이터부족') else 0
    close_score=close.score if close.stage!='데이터부족' else 0
    reliability=max(day.reliability,swing.reliability,close.reliability)
    combined=day_score*.30+swing_score*.35+close_score*.25+reliability*.10
    if combined<=0:
     continue
    kind='단타 우선' if day_score>=swing_score else '스윙 우선'
    reasons=[]
    source=day if day_score>=swing_score else swing
    for x in source.reasons[:3]:
     reasons.append(LABEL.get(x,x))
    rows.append((combined,reliability,code,kind,reasons,day_score,swing_score,close_score))
   except Exception as e:
    log.debug('익일 후보 계산 실패 %s: %s',code,e)
  rows.sort(reverse=True)
  result=[]
  for _,rel,code,kind,reasons,ds,ss,cs in rows[:limit]:
   result.append({
    'code':code,
    'name':STATE.names.get(code,code),
    'kind':kind,
    'reasons':reasons,
    'day_score':ds,
    'swing_score':ss,
    'close_score':cs,
    'reliability':rel
   })
  return result

 def night_briefing_text(self):
  """22시 브리핑: 당일 시장·뉴스·익일 후보 5개."""
  top=sorted(STATE.sectors.items(),key=lambda x:x[1],reverse=True)[:5]
  sector_text='\n'.join(f'• {html.escape(k)} {v:.0f}점' for k,v in top) or '• 섹터 데이터 없음'
  news=self.news_headlines(5)
  news_text='\n'.join(f'• {html.escape(x)}' for x in news) or '• 뉴스 조회 실패'
  picks=self.tomorrow_candidates(5)
  self.save_night_picks(picks)
  if picks:
   blocks=[]
   for idx,x in enumerate(picks,1):
    why=', '.join(x['reasons']) or '점수 상위'
    blocks.append(
     f"{idx}. <b>{html.escape(x['name'])}</b> ({x['code']}) · {x['kind']}\n"
     f"   단타 {x['day_score']:.1f} · 스윙 {x['swing_score']:.1f} · 종배 {x.get('close_score',0):.1f} · 신뢰 {x['reliability']:.1f}%\n"
     f"   근거: {html.escape(why)}"
    )
   pick_text='\n\n'.join(blocks)
  else:
   pick_text='• 조건을 충족한 후보 없음'
  return (
   '🌙 <b>22:00 종합 장마감 브리핑</b>\n\n'
   f'<b>오늘 추천</b> {len(self.recs)}건\n\n'
   f'<b>강한 섹터</b>\n{sector_text}\n\n'
   f'<b>주요 뉴스</b>\n{news_text}\n\n'
   f'<b>내일 우선 관찰 5종목</b>\n{pick_text}\n\n'
   '※ 당일 데이터와 뉴스 제목을 바탕으로 계산한 뽕실봇 의견입니다. '
   '다음 날 07:30 장전 후보와 비교해 최종 판단하세요.'
  )

 def send_night_briefing(self,chat=None):
  try:
   BOT.send(self.night_briefing_text(),chat)
  except Exception as e:
   log.warning('22시 브리핑 생성 실패: %s',e)

 def resolve(self,q):
  if q.isdigit():return q.zfill(6)
  key=q.replace(' ','').lower();x=[c for c,n in STATE.names.items() if n.replace(' ','').lower()==key];return x[0] if len(x)==1 else None
 def save_positions(self):
  try:
   payload={c:vars(x) for c,x in POSITION.data.items()}
   Path(SETTINGS.position_store).write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')
  except Exception as e:log.warning('보유 저장 실패: %s',e)

 def load_positions(self):
  try:
   p=Path(SETTINGS.position_store)
   if not p.exists():return
   raw=json.loads(p.read_text(encoding='utf-8'))
   for c,v in raw.items():
    POSITION.data[c]=Position(**v);STATE.positions[c]=v.get('kind','단타')
  except Exception as e:log.warning('보유 복원 실패: %s',e)

 def handle(self,text,chat):
  p=text.split();cmd=p[0]
  if cmd in ('/도움말','/help'):BOT.send('<b>명령어</b>\n/상태 /단타 /스윙 /종배 /종배후보 /종배결과 /예비후보 /시장 /성과\n/매수 종목 매수가 수량 [단타|스윙|종배]\n/매도 종목 /보유 /보유리셋\n/관심등록 종목 /관심삭제 종목 /관심목록\n/호가 종목 /후보갱신\n/야간브리핑 /내일추천 /아침비교',chat)
  elif cmd=='/상태':BOT.send(f'🤖 <b>뽕실 V{SETTINGS.version}</b>\n운영 모드 AUTO · 현재 단계 {self.auto_phase()}\n후보 {len(STATE.candidates)}개\nKRX 보조데이터 {STATE.runtime.get("ws_krx","rest_only")}\nNXT {STATE.runtime.get("ws_nxt")} · 체결 확인 {STATE.runtime.get("nxt_trade_subscribed",0)}/{STATE.runtime.get("nxt_trade_requested",0)} · 호가 확인 {STATE.runtime.get("nxt_orderbook_subscribed",0)}/{STATE.runtime.get("nxt_orderbook_requested",0)}\n추천 필터: {SETTINGS.min_price:,.0f}원↑ · 시총 {SETTINGS.min_market_cap/100_000_000:,.0f}억↑ · 거래대금 {SETTINGS.min_daily_amount/100_000_000:,.0f}억↑\n오늘 추천 {len(self.recs)}/{SETTINGS.daily_limit}\n마지막 체결 {STATE.runtime.get("last_tick") or "-"}\n최근 오류 {STATE.runtime.get("last_error") or "-"}',chat)
  elif cmd=='/단타':BOT.send(self.rank('단타'),chat)
  elif cmd=='/스윙':BOT.send(self.rank('스윙'),chat)
  elif cmd in ('/종배','/종배후보'):
   threading.Thread(target=lambda:BOT.send(self.close_rank_text(),chat),daemon=True,name='close-rank-manual').start()
  elif cmd=='/종배결과':
   BOT.send(self.close_result_text(),chat)
  elif cmd=='/예비후보':BOT.send(self.rank('단타',5)+'\n\n'+self.rank('스윙',5),chat)
  elif cmd=='/시장':
   top=sorted(STATE.sectors.items(),key=lambda x:x[1],reverse=True)[:5];BOT.send('📊 <b>시장·섹터</b>\n\n'+'\n'.join(f'• {html.escape(k)} {v:.0f}점' for k,v in top),chat)
  elif cmd=='/매수':
   if len(p)<4:
    BOT.send('사용법: /매수 종목명 매수가 수량 [단타|스윙]',chat);return
   c=self.resolve(p[1]);price=num(p[2]);qty=num(p[3]);kind=p[4] if len(p)>4 and p[4] in ('단타','스윙','종배') else '단타'
   if c and price>0 and qty>0:
    x=POSITION.register(c,STATE.names.get(c,c),price,qty,kind)
    STATE.positions[c]=kind
    self.save_positions()
    DATABASE.upsert('tracked_positions',{'stock_code':c,'stock_name':x.name,'market':'NXT','entry_price':price,'current_price':price,'quantity':qty,'position_status':'entered','updated_at':now().isoformat()},'stock_code')
    BOT.send(f'✅ <b>{html.escape(x.name)} {kind} 보유등록</b>\n\n매수가 {x.entry:,.0f}원\n수량 {x.qty:g}주\n1차 목표 {x.target1:,.0f}원\n2차 목표 {x.target2:,.0f}원\n최종 목표 {x.target3:,.0f}원\n손절 {x.stop:,.0f}원',chat)
   else:BOT.send('종목명·매수가·수량을 확인해 주세요.',chat)
  elif cmd in ('/매도','/삭제'):
   if len(p)>1:
    c=self.resolve(' '.join(p[1:]))
    if c:
     x=POSITION.data.get(c)
     POSITION.close(c)
     STATE.positions.pop(c,None)
     self.save_positions()
     if x:
      BOT.send(f'✅ <b>{html.escape(x.name)} 매도·보유감시 종료</b>\n\n매수가 {x.entry:,.0f}원\n수량 {x.qty:g}주\n등록 유형 {x.kind}\n마지막 상태 {x.state}',chat)
     else:
      BOT.send(f'✅ <b>{html.escape(STATE.names.get(c,c))} 보유감시 종료</b>',chat)
    else:BOT.send('종목명을 정확히 입력해 주세요.',chat)
   else:BOT.send('사용법: /매도 종목명',chat)
  elif cmd=='/보유':
   BOT.send(self.holding_text(),chat)
  elif cmd=='/보유리셋':POSITION.data.clear();STATE.positions.clear();self.save_positions();BOT.send('✅ 보유종목 초기화',chat)
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
    o=STATE.books.get(('NXT',c))
    BOT.send(f'📚 {STATE.names.get(c,c)}\n시장 {o.market}\n매수잔량 {o.total_bid:,}\n매도잔량 {o.total_ask:,}\n잔량비 {o.imbalance:.2f}배' if o else '호가 데이터가 아직 없습니다.',chat)
  elif cmd=='/후보갱신':STATE.refresh();BOT.send(f'✅ 후보 {len(STATE.candidates)}개 갱신',chat)
  elif cmd=='/성과':BOT.send(self.performance_text(),chat)
  elif cmd in ('/야간브리핑','/내일추천'):
   threading.Thread(target=self.send_night_briefing,args=(chat,),daemon=True,name='night-briefing-manual').start()
  elif cmd=='/아침비교':
   threading.Thread(target=lambda:BOT.send(self.morning_compare_text(),chat),daemon=True,name='morning-compare-manual').start()
  else:BOT.send('명령어는 /도움말',chat)
 def scheduler(self):
  sent={}
  while True:
   n=now();d=str(n.date());STATE.runtime['phase']=self.auto_phase()
   if n.weekday()<5:
    if n.hour==7 and n.minute>=30 and sent.get('morning')!=d:
     threading.Thread(target=self.update_close_results,daemon=True,name='close-result-update').start()
     threading.Thread(target=lambda:BOT.send(self.morning_compare_text()),daemon=True,name='morning-briefing').start()
     sent['morning']=d
    if n.hour==14 and n.minute>=40 and sent.get('close1')!=d:
     threading.Thread(target=self.run_close_scan,args=('1차',False),daemon=True,name='close-scan-1440').start()
     sent['close1']=d
    if n.hour==15 and n.minute>=15 and sent.get('close_final')!=d:
     threading.Thread(target=self.run_close_scan,args=('최종',False),daemon=True,name='close-scan-1515').start()
     sent['close_final']=d
    if n.hour==15 and n.minute>=22 and sent.get('close_send')!=d:
     BOT.send(self.close_rank_text_from_rows(self.close_picks))
     sent['close_send']=d
    if n.hour==20 and sent.get('nxt_close')!=d:
     BOT.send('🌙 <b>NXT 마감 확인</b>\n\n'+self.close_result_text())
     sent['nxt_close']=d
    if n.hour==22 and sent.get('night')!=d:
     threading.Thread(target=self.send_night_briefing,daemon=True,name='night-briefing').start()
     sent['night']=d
    if 8<=n.hour<20 and n-self.last_rec>=timedelta(hours=2) and n-self.last_none>=timedelta(hours=2):
     BOT.send('📢 최근 2시간 동안 신규 단타 조건 충족 종목이 없습니다. 계속 감시 중입니다.')
     self.last_none=n
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
  with self._start_lock:
   if self._started:
    log.warning('APP.start 중복 호출 차단')
    return
   self._started=True
  BOT.handler=self.handle
  if SETTINGS.telegram_polling:
   threading.Thread(target=BOT.poll,daemon=True,name='telegram-poll').start()
  threading.Thread(target=self.scheduler,daemon=True,name='scheduler').start()
  while True:
   try:
    STATE.load();STATE.refresh();BRAIN.sector_refresh()
    break
   except Exception as e:
    STATE.runtime['last_error']=f'초기화: {e}'
    log.exception('초기 종목 데이터 로드 실패')
    time.sleep(60)
  self.load_positions()
  threading.Thread(target=self.refresh_loop,daemon=True,name='candidate-refresh').start()
  if SETTINGS.kis_app_key and SETTINGS.kis_app_secret:
   threading.Thread(target=KIS_CLIENT.stream,args=(STATE.realtime_codes,STATE.names,self.on_tick,self.on_book,STATE.runtime),daemon=True,name='KIS-NXT-websocket').start()
  else:
   STATE.runtime['ws_krx']='rest_only'
   STATE.runtime['ws_nxt']='disabled_missing_credentials'
   STATE.runtime['last_error']='KIS_APP_KEY 또는 KIS_APP_SECRET 미설정'
   log.error('KIS 자격증명이 없어 실시간 시세 연결을 시작하지 않습니다.')
  BOT.send(
   f'🤖 <b>뽕실 V{SETTINGS.version} 시작</b>\n'
   f'운영 모드 AUTO\n'
   f'현재 단계 {self.auto_phase()}\n'
   f'후보 {len(STATE.candidates)}개\n'
   f'NXT 체결 {SETTINGS.ws_trade_limit} / 호가 {SETTINGS.ws_orderbook_limit}\n'
   f'신규추천 필터 {SETTINGS.min_price:,.0f}원↑ · 시총 {SETTINGS.min_market_cap/100_000_000:,.0f}억↑ · 거래대금 {SETTINGS.min_daily_amount/100_000_000:,.0f}억↑\n'
   f'집중감시 재평가 {SETTINGS.rotation_seconds}초\n'
   f'14:40 종배 1차 · 15:15 최종 · 15:22 발송\n'
   f'자동주문 없음'
  )
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
def delayed_app_start():
 delay=max(0,SETTINGS.render_start_delay)
 if delay:
  log.info('Render 이전 인스턴스 종료 대기: %s초 후 봇 본체 시작',delay)
  time.sleep(delay)
 APP.start()
if __name__=='__main__':
 threading.Thread(target=delayed_app_start,daemon=True,name='app-initializer').start()
 web.run(host='0.0.0.0',port=SETTINGS.port,threaded=True,use_reloader=False)
