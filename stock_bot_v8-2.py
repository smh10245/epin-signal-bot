from __future__ import annotations

import html
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
import websocket
import yfinance as yf
from flask import Flask, jsonify

from v8_engine import BarBuilder, Signal, Tick, V8SignalEngine


APP_VERSION = "8.0.1-fix"
KST = ZoneInfo("Asia/Seoul")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID = (os.getenv("CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID") or "").strip()
ENABLE_TELEGRAM_POLLING = os.getenv("ENABLE_TELEGRAM_POLLING", "true").strip().lower() in ("1", "true", "yes", "on")
ENABLE_NXT_ON_VIRTUAL = os.getenv("ENABLE_NXT_ON_VIRTUAL", "false").strip().lower() in ("1", "true", "yes", "on")

KIS_APP_KEY = os.getenv("KIS_APP_KEY", "").strip()
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET", "").strip()
KIS_ENV = os.getenv("KIS_ENV", "virtual").strip().lower()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SECRET_KEY = os.getenv("SUPABASE_SECRET_KEY", "").strip()

# 예: 005930:삼성전자,000660:SK하이닉스
WATCHLIST_RAW = os.getenv(
    "V8_WATCHLIST",
    "005930:삼성전자,000660:SK하이닉스",
)
BUY_CONFIDENCE = float(os.getenv("V8_BUY_CONFIDENCE", "78"))

# 공식 문서 TR ID가 바뀔 경우 Render 환경변수로 즉시 교체 가능
KIS_KRX_TRADE_TR_ID = os.getenv("KIS_KRX_TRADE_TR_ID", "H0STCNT0").strip()
KIS_NXT_TRADE_TR_ID = os.getenv("KIS_NXT_TRADE_TR_ID", "H0NXCNT0").strip()

REAL_REST = "https://openapi.koreainvestment.com:9443"
VIRTUAL_REST = "https://openapivts.koreainvestment.com:29443"
REAL_WS = "ws://ops.koreainvestment.com:21000"
VIRTUAL_WS = "ws://ops.koreainvestment.com:31000"

app = Flask(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
logger = logging.getLogger("ppongsil-v8")
http = requests.Session()

runtime: Dict[str, Any] = {
    "started_at": None,
    "kis_auth": "not_tested",
    "kis_last_error": None,
    "supabase": "not_tested",
    "telegram": "not_tested",
    "ws_krx": "stopped",
    "ws_nxt": "stopped",
    "last_tick_at": None,
    "last_signal_at": None,
    "signals_today": 0,
    "note": "모의투자 키에서는 NXT 실시간이 미지원일 수 있습니다.",
}


def now_kst() -> datetime:
    return datetime.now(KST)


def parse_watchlist() -> Dict[str, str]:
    result: Dict[str, str] = {}
    for item in WATCHLIST_RAW.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            code, name = item.split(":", 1)
        else:
            code, name = item, item
        code = code.strip().zfill(6)
        result[code] = name.strip()
    return result


WATCHLIST = parse_watchlist()


class Telegram:
    def __init__(self) -> None:
        self.offset = 0

    def send(self, text: str, chat_id: Optional[str] = None) -> bool:
        target = str(chat_id or CHAT_ID).strip()
        if not TELEGRAM_TOKEN or not target:
            logger.warning("텔레그램 토큰 또는 CHAT_ID가 없습니다.")
            return False
        try:
            response = http.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={
                    "chat_id": target,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
            ok = bool(data.get("ok"))
            runtime["telegram"] = "ok" if ok else "failed"
            return ok
        except Exception as exc:
            runtime["telegram"] = "failed"
            logger.exception("텔레그램 전송 실패: %s", exc)
            return False

    def poll_forever(self) -> None:
        if not TELEGRAM_TOKEN:
            return
        while True:
            try:
                response = http.get(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                    params={"timeout": 25, "offset": self.offset},
                    timeout=35,
                )
                response.raise_for_status()
                for update in response.json().get("result", []):
                    self.offset = max(self.offset, int(update["update_id"]) + 1)
                    message = update.get("message") or {}
                    text = str(message.get("text") or "").strip()
                    chat_id = str((message.get("chat") or {}).get("id") or "")
                    if text:
                        handle_command(text, chat_id)
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status == 409:
                    # 같은 봇 토큰을 다른 프로세스가 getUpdates로 사용 중입니다.
                    # 발신 기능은 유지하고 폴링만 길게 대기하여 로그 폭주를 막습니다.
                    runtime["telegram"] = "polling_conflict_409"
                    logger.error(
                        "텔레그램 409 충돌: 같은 봇을 다른 서비스/프로세스가 폴링 중입니다. "
                        "기존 V7 서비스 또는 중복 Render 인스턴스를 중지하세요."
                    )
                    time.sleep(60)
                else:
                    logger.warning("텔레그램 폴링 HTTP 오류: %s", exc)
                    time.sleep(10)
            except Exception as exc:
                logger.warning("텔레그램 폴링 오류: %s", exc)
                time.sleep(10)


telegram = Telegram()


class SupabaseStore:
    def __init__(self) -> None:
        self.enabled = bool(SUPABASE_URL and SUPABASE_SECRET_KEY)

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "apikey": SUPABASE_SECRET_KEY,
            "Authorization": f"Bearer {SUPABASE_SECRET_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

    def test(self) -> bool:
        if not self.enabled:
            runtime["supabase"] = "missing_env"
            return False
        try:
            # 이미 생성한 recommendations 테이블을 0건 조회
            response = http.get(
                f"{SUPABASE_URL}/rest/v1/recommendations",
                headers=self.headers,
                params={"select": "id", "limit": "1"},
                timeout=15,
            )
            response.raise_for_status()
            runtime["supabase"] = "ok"
            return True
        except Exception as exc:
            runtime["supabase"] = "failed"
            logger.warning("Supabase 연결 실패: %s", exc)
            return False

    def insert(self, table: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
        try:
            response = http.post(
                f"{SUPABASE_URL}/rest/v1/{table}",
                headers=self.headers,
                json=payload,
                timeout=15,
            )
            response.raise_for_status()
            rows = response.json()
            return rows[0] if isinstance(rows, list) and rows else None
        except Exception as exc:
            logger.warning("Supabase %s 저장 실패: %s", table, exc)
            return None

    def upsert_position(self, payload: Dict[str, Any]) -> None:
        if not self.enabled:
            return
        headers = dict(self.headers)
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        try:
            response = http.post(
                f"{SUPABASE_URL}/rest/v1/tracked_positions",
                headers=headers,
                params={"on_conflict": "stock_code"},
                json=payload,
                timeout=15,
            )
            response.raise_for_status()
        except Exception as exc:
            logger.warning("포지션 저장 실패: %s", exc)


store = SupabaseStore()


class KISClient:
    def __init__(self) -> None:
        self.rest_base = VIRTUAL_REST if KIS_ENV == "virtual" else REAL_REST
        self.ws_url = VIRTUAL_WS if KIS_ENV == "virtual" else REAL_WS
        self.access_token: Optional[str] = None
        self.access_expires_at: Optional[datetime] = None
        self.approval_key: Optional[str] = None
        self.approval_expires_at: Optional[datetime] = None
        self._approval_lock = threading.Lock()

    def _check_env(self) -> None:
        if not KIS_APP_KEY or not KIS_APP_SECRET:
            raise RuntimeError("KIS_APP_KEY/KIS_APP_SECRET 환경변수가 없습니다.")

    def get_access_token(self, force: bool = False) -> str:
        self._check_env()
        if (
            not force
            and self.access_token
            and self.access_expires_at
            and datetime.now(timezone.utc) < self.access_expires_at
        ):
            return self.access_token
        response = http.post(
            f"{self.rest_base}/oauth2/tokenP",
            json={
                "grant_type": "client_credentials",
                "appkey": KIS_APP_KEY,
                "appsecret": KIS_APP_SECRET,
            },
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        token = data.get("access_token")
        if not token:
            raise RuntimeError(f"접근토큰 없음: {data}")
        expires_in = int(data.get("expires_in", 86400))
        self.access_token = token
        self.access_expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=max(60, expires_in - 300)
        )
        runtime["kis_auth"] = "ok"
        return token

    def get_approval_key(self, force: bool = False) -> str:
        """
        WebSocket 접속키는 KRX/NXT가 함께 재사용합니다.
        두 스레드가 동시에 /oauth2/Approval을 호출하면 모의 서버에서
        403 또는 호출 제한이 발생할 수 있어 잠금과 캐시를 사용합니다.
        """
        self._check_env()
        with self._approval_lock:
            if (
                not force
                and self.approval_key
                and self.approval_expires_at
                and datetime.now(timezone.utc) < self.approval_expires_at
            ):
                return self.approval_key

            response = http.post(
                f"{self.rest_base}/oauth2/Approval",
                json={
                    "grant_type": "client_credentials",
                    "appkey": KIS_APP_KEY,
                    "secretkey": KIS_APP_SECRET,
                },
                timeout=20,
            )
            response.raise_for_status()
            data = response.json()
            key = data.get("approval_key")
            if not key:
                raise RuntimeError(f"웹소켓 접속키 없음: {data}")
            self.approval_key = key
            # 접속키 만료시간이 응답에 명시되지 않는 경우가 있어 보수적으로 12시간 캐시
            self.approval_expires_at = datetime.now(timezone.utc) + timedelta(hours=12)
            return key

    def inquire_price(self, code: str) -> Dict[str, Any]:
        token = self.get_access_token()
        response = http.get(
            f"{self.rest_base}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers={
                "authorization": f"Bearer {token}",
                "appkey": KIS_APP_KEY,
                "appsecret": KIS_APP_SECRET,
                "tr_id": "FHKST01010100",
                "custtype": "P",
            },
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": code,
            },
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        if str(data.get("rt_cd")) != "0":
            raise RuntimeError(data.get("msg1") or str(data))
        return data.get("output") or {}

    def weekend_test(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        try:
            self.get_access_token(force=True)
            result["access_token"] = "ok"
            runtime["kis_auth"] = "ok"
        except Exception as exc:
            runtime["kis_auth"] = "failed"
            runtime["kis_last_error"] = str(exc)
            result["access_token"] = f"failed: {exc}"
            return result
        try:
            self.get_approval_key()
            result["approval_key"] = "ok"
        except Exception as exc:
            result["approval_key"] = f"failed: {exc}"
        try:
            output = self.inquire_price("005930")
            result["samsung_price"] = output.get("stck_prpr")
        except Exception as exc:
            # 휴장일에도 마지막 가격이 내려올 수 있으나, 계정/환경에 따라 실패할 수 있음
            result["samsung_price"] = f"failed: {exc}"
        return result


kis = KISClient()
bar_builder = BarBuilder()
engine = V8SignalEngine(buy_confidence=BUY_CONFIDENCE)


def to_float(value: Any) -> float:
    try:
        return float(str(value).replace(",", "").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def to_int(value: Any) -> int:
    try:
        return int(float(str(value).replace(",", "").strip() or 0))
    except (TypeError, ValueError):
        return 0


def parse_trade_frame(message: str, market: str) -> List[Tick]:
    """
    KIS 국내주식 실시간 체결가 공통 스키마의 핵심 필드만 사용합니다.
    암호화 체결통보 프레임은 대상이 아니며, 일반 시세 프레임만 처리합니다.
    """
    if not message or message.startswith("{"):
        return []
    parts = message.split("|", 3)
    if len(parts) < 4 or parts[0] != "0":
        return []
    count = max(1, to_int(parts[2]))
    fields = parts[3].split("^")
    field_count = len(fields) // count if count else len(fields)
    if field_count < 19:
        return []

    ticks: List[Tick] = []
    for index in range(count):
        row = fields[index * field_count:(index + 1) * field_count]
        code = row[0].zfill(6)
        hhmmss = row[1].zfill(6)
        now = now_kst()
        try:
            ts = now.replace(
                hour=int(hhmmss[0:2]),
                minute=int(hhmmss[2:4]),
                second=int(hhmmss[4:6]),
                microsecond=0,
            )
        except Exception:
            ts = now
        ticks.append(
            Tick(
                code=code,
                name=WATCHLIST.get(code, code),
                market=market,
                price=to_float(row[2]),
                volume=to_int(row[12]),
                cumulative_volume=to_int(row[13]),
                trade_strength=to_float(row[18]),
                timestamp=ts,
            )
        )
    return ticks


def format_signal(signal: Signal) -> str:
    title_map = {
        "buy_valid": "🚨 <b>매수 유효 신호</b>",
        "partial_sell": "🟠 <b>부분매도 경고</b>",
        "full_sell": "🔻 <b>전량매도 신호</b>",
        "stop_loss": "⛔ <b>손절·무효화 신호</b>",
    }
    lines = [
        title_map.get(signal.signal_type, f"🔔 <b>{html.escape(signal.signal_type)}</b>"),
        "",
        f"종목: <b>{html.escape(signal.name)}</b> ({signal.code})",
        f"시장: {signal.market}",
        f"가격: <b>{signal.price:,.0f}원</b>",
        f"확신도: <b>{signal.confidence:.0f}%</b>",
    ]
    if signal.invalidation_price:
        lines.append(f"무효화 가격: {signal.invalidation_price:,.0f}원")
    if signal.protection_price:
        lines.append(f"추적 보호가격: {signal.protection_price:,.0f}원")
    lines.extend(["", "근거"])
    lines.extend(f"• {html.escape(reason)}" for reason in signal.reason)
    return "\n".join(lines)


def persist_signal(signal: Signal) -> None:
    row = store.insert(
        "trade_signals",
        {
            "stock_code": signal.code,
            "stock_name": signal.name,
            "signal_type": signal.signal_type,
            "signal_price": signal.price,
            "confidence_score": signal.confidence,
            "signal_time": signal.timestamp.isoformat(),
        },
    )
    if signal.signal_type == "buy_valid":
        position = engine.register_position(signal)
        store.upsert_position(
            {
                "stock_code": position.code,
                "stock_name": position.name,
                "market": position.market,
                "entry_price": position.entry_price,
                "current_price": position.entry_price,
                "highest_price": position.highest_price,
                "protection_price": position.protection_price,
                "invalidation_price": position.invalidation_price,
                "position_status": "entered",
                "updated_at": now_kst().isoformat(),
            }
        )
    elif signal.signal_type in ("full_sell", "stop_loss"):
        engine.close_position(signal.code)
        store.upsert_position(
            {
                "stock_code": signal.code,
                "stock_name": signal.name,
                "market": signal.market,
                "current_price": signal.price,
                "protection_price": signal.protection_price,
                "position_status": "closed",
                "updated_at": now_kst().isoformat(),
            }
        )


def process_tick(tick: Tick) -> None:
    runtime["last_tick_at"] = tick.timestamp.isoformat()
    completed = bar_builder.push(tick)
    if not completed:
        return
    for signal in engine.on_bar(completed):
        runtime["last_signal_at"] = signal.timestamp.isoformat()
        runtime["signals_today"] += 1
        telegram.send(format_signal(signal))
        persist_signal(signal)


def run_ws(market: str, tr_id: str) -> None:
    state_key = "ws_krx" if market == "KRX" else "ws_nxt"
    while True:
        try:
            approval_key = kis.get_approval_key()
            runtime[state_key] = "connecting"

            def on_open(ws: websocket.WebSocketApp) -> None:
                runtime[state_key] = "connected"
                for code in WATCHLIST:
                    ws.send(
                        json.dumps(
                            {
                                "header": {
                                    "approval_key": approval_key,
                                    "custtype": "P",
                                    "tr_type": "1",
                                    "content-type": "utf-8",
                                },
                                "body": {
                                    "input": {
                                        "tr_id": tr_id,
                                        "tr_key": code,
                                    }
                                },
                            }
                        )
                    )
                    time.sleep(0.08)

            def on_message(ws: websocket.WebSocketApp, message: str) -> None:
                if message.startswith("{"):
                    try:
                        data = json.loads(message)
                        header = data.get("header") or {}
                        if header.get("tr_id") == "PINGPONG":
                            ws.send(message)
                    except Exception:
                        pass
                    return
                for tick in parse_trade_frame(message, market):
                    process_tick(tick)

            def on_error(ws: websocket.WebSocketApp, error: Any) -> None:
                runtime[state_key] = "error"
                runtime["kis_last_error"] = str(error)
                logger.warning("%s 웹소켓 오류: %s", market, error)

            def on_close(ws: websocket.WebSocketApp, status: Any, msg: Any) -> None:
                runtime[state_key] = "closed"
                logger.warning("%s 웹소켓 종료: %s %s", market, status, msg)

            websocket.WebSocketApp(
                kis.ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            ).run_forever(ping_interval=30, ping_timeout=10)
        except Exception as exc:
            runtime[state_key] = "failed"
            runtime["kis_last_error"] = str(exc)
            logger.exception("%s 웹소켓 시작 실패", market)
        time.sleep(10)


def morning_us_briefing() -> str:
    symbols = {
        "^IXIC": "나스닥",
        "^GSPC": "S&P500",
        "^SOX": "필라델피아 반도체",
        "CL=F": "WTI 유가",
        "KRW=X": "원·달러",
    }
    lines = ["🌎 <b>미국증시 아침 브리핑</b>", ""]
    changes: Dict[str, float] = {}
    for symbol, label in symbols.items():
        try:
            hist = yf.Ticker(symbol).history(period="7d", interval="1d", auto_adjust=False)
            closes = hist["Close"].dropna()
            if len(closes) < 2:
                raise ValueError("데이터 부족")
            change = (closes.iloc[-1] / closes.iloc[-2] - 1) * 100
            changes[label] = float(change)
            lines.append(f"• {label}: <b>{change:+.2f}%</b>")
        except Exception as exc:
            logger.warning("%s 브리핑 조회 실패: %s", symbol, exc)
            lines.append(f"• {label}: 확인 불가")

    nasdaq = changes.get("나스닥", 0)
    sox = changes.get("필라델피아 반도체", 0)
    if nasdaq > 0.7 and sox > 1:
        view = "기술주·반도체 우호적. 갭상승 추격은 피하고 눌림 확인."
    elif nasdaq < -0.7:
        view = "장 초반 변동성 확대 가능. 신규 매수 기준 강화."
    else:
        view = "중립권. 국내 거래량과 업종 강도를 우선 확인."
    lines.extend(["", f"오늘 국내장 관점: {view}"])
    return "\n".join(lines)


def status_text() -> str:
    return (
        f"🤖 <b>뽕실로봇 V{APP_VERSION}</b>\n\n"
        f"KIS 환경: <b>{html.escape(KIS_ENV)}</b>\n"
        f"KIS 인증: {runtime['kis_auth']}\n"
        f"Supabase: {runtime['supabase']}\n"
        f"KRX WS: {runtime['ws_krx']}\n"
        f"NXT WS: {runtime['ws_nxt']}\n"
        f"감시종목: {len(WATCHLIST)}개\n"
        f"텔레그램 폴링: {ENABLE_TELEGRAM_POLLING}\n"
        f"마지막 체결: {runtime['last_tick_at'] or '-'}\n"
        f"오늘 신호: {runtime['signals_today']}건\n\n"
        f"주의: 모의투자에서는 NXT 실시간이 미지원될 수 있어 "
        f"실전 앱키 연결 후 최종 확인이 필요합니다."
    )


def handle_command(text: str, chat_id: str) -> None:
    cmd = text.split()[0]
    if cmd == "/상태":
        telegram.send(status_text(), chat_id)
    elif cmd == "/테스트":
        telegram.send("✅ V8 텔레그램 연결 정상", chat_id)
    elif cmd == "/한투테스트":
        result = kis.weekend_test()
        telegram.send(
            "🔌 <b>한투 API 점검</b>\n\n"
            + "\n".join(
                f"• {html.escape(str(k))}: {html.escape(str(v))}"
                for k, v in result.items()
            ),
            chat_id,
        )
    elif cmd == "/미국증시":
        telegram.send(morning_us_briefing(), chat_id)
    elif cmd == "/감시목록":
        lines = ["👀 <b>V8 감시목록</b>", ""]
        lines.extend(f"• {html.escape(name)} ({code})" for code, name in WATCHLIST.items())
        telegram.send("\n".join(lines), chat_id)
    else:
        telegram.send(
            "명령어\n"
            "/상태\n/테스트\n/한투테스트\n/미국증시\n/감시목록",
            chat_id,
        )


def scheduler_loop() -> None:
    sent_morning: Optional[str] = None
    last_reset: Optional[str] = None
    while True:
        now = now_kst()
        today = now.date().isoformat()
        if last_reset != today:
            runtime["signals_today"] = 0
            last_reset = today
        # 평일 07:30~07:39 한 번 발송
        if now.weekday() < 5 and now.hour == 7 and 30 <= now.minute < 40:
            if sent_morning != today:
                telegram.send(morning_us_briefing())
                sent_morning = today
        time.sleep(20)


def startup() -> None:
    runtime["started_at"] = now_kst().isoformat()
    store.test()
    result = kis.weekend_test()
    logger.info("KIS 시작 점검: %s", result)
    telegram.send(
        f"🤖 <b>뽕실로봇 V{APP_VERSION} 시작</b>\n"
        f"KIS 인증: {runtime['kis_auth']}\n"
        f"Supabase: {runtime['supabase']}\n"
        f"감시종목: {len(WATCHLIST)}개"
    )

    if ENABLE_TELEGRAM_POLLING:
        threading.Thread(target=telegram.poll_forever, daemon=True, name="telegram-poll").start()
    else:
        runtime["telegram"] = "send_only"
    threading.Thread(target=scheduler_loop, daemon=True, name="scheduler").start()
    threading.Thread(
        target=run_ws,
        args=("KRX", KIS_KRX_TRADE_TR_ID),
        daemon=True,
        name="ws-krx",
    ).start()

    # 모의투자 NXT는 지원 범위가 제한될 수 있습니다.
    # 기본값은 건너뛰고, 실전 키 또는 ENABLE_NXT_ON_VIRTUAL=true일 때만 연결합니다.
    if KIS_ENV != "virtual" or ENABLE_NXT_ON_VIRTUAL:
        threading.Thread(
            target=run_ws,
            args=("NXT", KIS_NXT_TRADE_TR_ID),
            daemon=True,
            name="ws-nxt",
        ).start()
    else:
        runtime["ws_nxt"] = "skipped_virtual"


@app.route("/")
def health() -> Tuple[str, int]:
    return f"뽕실로봇 V{APP_VERSION} running", 200


@app.route("/health")
def health_json() -> Any:
    return jsonify(
        {
            "status": "ok",
            "version": APP_VERSION,
            "runtime": runtime,
            "watchlist": WATCHLIST,
            "time_kst": now_kst().isoformat(),
        }
    )


if __name__ == "__main__":
    startup()
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
        threaded=True,
        use_reloader=False,
    )
