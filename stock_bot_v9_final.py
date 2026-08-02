from __future__ import annotations

import html
import json
import logging
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
import websocket
import yfinance as yf
import FinanceDataReader as fdr
from flask import Flask, jsonify



# =========================
# V8 SIGNAL ENGINE (INLINE)
# =========================

@dataclass
class Tick:
    code: str
    name: str
    market: str
    price: float
    volume: int
    cumulative_volume: int
    trade_strength: float
    timestamp: datetime


@dataclass
class MinuteBar:
    code: str
    name: str
    market: str
    minute: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    cumulative_volume: int = 0
    trade_strength: float = 0.0

    def update(self, tick: Tick, incremental_volume: int) -> None:
        self.high = max(self.high, tick.price)
        self.low = min(self.low, tick.price)
        self.close = tick.price
        self.volume += max(0, incremental_volume)
        self.cumulative_volume = max(self.cumulative_volume, tick.cumulative_volume)
        self.trade_strength = tick.trade_strength


@dataclass
class Signal:
    code: str
    name: str
    market: str
    signal_type: str
    price: float
    confidence: float
    reason: List[str]
    invalidation_price: Optional[float] = None
    protection_price: Optional[float] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class PositionState:
    code: str
    name: str
    market: str
    entry_price: float
    entry_time: datetime
    highest_price: float
    protection_price: float
    invalidation_price: float
    partial_sold: bool = False
    failed_high_count: int = 0


class BarBuilder:
    """체결 틱을 1분봉으로 합칩니다."""

    def __init__(self) -> None:
        self.current: Dict[Tuple[str, str], MinuteBar] = {}
        self.last_cumulative: Dict[Tuple[str, str], int] = {}

    @staticmethod
    def _minute(ts: datetime) -> datetime:
        return ts.replace(second=0, microsecond=0)

    def push(self, tick: Tick) -> Optional[MinuteBar]:
        key = (tick.market, tick.code)
        minute = self._minute(tick.timestamp)
        previous_cumulative = self.last_cumulative.get(key, tick.cumulative_volume)
        incremental = max(0, tick.cumulative_volume - previous_cumulative)
        self.last_cumulative[key] = tick.cumulative_volume

        bar = self.current.get(key)
        if bar is None:
            self.current[key] = MinuteBar(
                code=tick.code,
                name=tick.name,
                market=tick.market,
                minute=minute,
                open=tick.price,
                high=tick.price,
                low=tick.price,
                close=tick.price,
                volume=max(tick.volume, incremental),
                cumulative_volume=tick.cumulative_volume,
                trade_strength=tick.trade_strength,
            )
            return None

        if bar.minute == minute:
            bar.update(tick, max(tick.volume, incremental))
            return None

        completed = bar
        self.current[key] = MinuteBar(
            code=tick.code,
            name=tick.name,
            market=tick.market,
            minute=minute,
            open=tick.price,
            high=tick.price,
            low=tick.price,
            close=tick.price,
            volume=max(tick.volume, incremental),
            cumulative_volume=tick.cumulative_volume,
            trade_strength=tick.trade_strength,
        )
        return completed


class V8SignalEngine:
    """
    V8 핵심 엔진.
    - 거래량 없는 반등은 배제
    - 저점 재이탈 실패 + 직전 단기고점 돌파를 확인
    - 상승 중 눌림 후 재돌파도 별도 감지
    - 매도는 고정 목표가가 아니라 최고가/거래량/직전 저점으로 추적
    """

    def __init__(
        self,
        min_bars: int = 12,
        buy_confidence: float = 78.0,
        partial_sell_confidence: float = 70.0,
        max_history: int = 120,
    ) -> None:
        self.min_bars = min_bars
        self.buy_confidence = buy_confidence
        self.partial_sell_confidence = partial_sell_confidence
        self.history: Dict[Tuple[str, str], Deque[MinuteBar]] = {}
        self.positions: Dict[str, PositionState] = {}
        self.last_signal: Dict[Tuple[str, str], Tuple[str, datetime]] = {}
        self.max_history = max_history

    @staticmethod
    def _avg(values: List[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    @staticmethod
    def _pct(a: float, b: float) -> float:
        return (a - b) / b * 100 if b else 0.0

    def _cooldown_ok(self, code: str, market: str, signal_type: str, now: datetime) -> bool:
        key = (market, code)
        last = self.last_signal.get(key)
        if not last:
            return True
        last_type, last_time = last
        seconds = (now - last_time).total_seconds()
        if last_type == signal_type and seconds < 600:
            return False
        return True

    def _remember_signal(self, signal: Signal) -> None:
        self.last_signal[(signal.market, signal.code)] = (signal.signal_type, signal.timestamp)

    def on_bar(self, bar: MinuteBar) -> List[Signal]:
        key = (bar.market, bar.code)
        bars = self.history.setdefault(key, deque(maxlen=self.max_history))
        bars.append(bar)
        signals: List[Signal] = []

        if bar.code in self.positions:
            sell = self._evaluate_sell(bar, list(bars), self.positions[bar.code])
            if sell:
                signals.append(sell)
                self._remember_signal(sell)

        if len(bars) < self.min_bars or bar.code in self.positions:
            return signals

        data = list(bars)
        buy = self._evaluate_v_reversal(data) or self._evaluate_trend_pullback(data)
        if buy and self._cooldown_ok(bar.code, bar.market, buy.signal_type, bar.minute):
            signals.append(buy)
            self._remember_signal(buy)
        return signals

    def _evaluate_v_reversal(self, bars: List[MinuteBar]) -> Optional[Signal]:
        latest = bars[-1]
        recent = bars[-12:]
        pre = recent[:-3]
        confirm = recent[-3:]

        prior_high = max(b.high for b in pre)
        bottom = min(b.low for b in recent)
        bottom_index = min(range(len(recent)), key=lambda i: recent[i].low)
        decline_pct = self._pct(bottom, prior_high)

        avg_volume = self._avg([b.volume for b in bars[-12:-3]])
        confirm_volume = self._avg([b.volume for b in confirm])
        volume_ratio = confirm_volume / avg_volume if avg_volume > 0 else 0.0

        short_break = max(b.high for b in recent[-6:-1])
        lows_after_bottom = [b.low for b in recent[bottom_index + 1:]]
        defended_bottom = bool(lows_after_bottom) and min(lows_after_bottom) >= bottom * 0.998
        bullish_turn = latest.close > latest.open and latest.close >= short_break
        strength_ok = latest.trade_strength <= 0 or latest.trade_strength >= 100

        score = 0.0
        reasons: List[str] = []
        if decline_pct <= -2.0:
            score += 22
            reasons.append(f"고점 대비 {decline_pct:.1f}% 조정")
        if bottom_index <= len(recent) - 3:
            score += 16
            reasons.append("저점 형성 후 확인봉 확보")
        if defended_bottom:
            score += 18
            reasons.append("저점 재이탈 실패")
        if volume_ratio >= 1.8:
            score += 24
            reasons.append(f"확인 거래량 {volume_ratio:.1f}배")
        elif volume_ratio >= 1.35:
            score += 14
            reasons.append(f"거래량 {volume_ratio:.1f}배 증가")
        if bullish_turn:
            score += 15
            reasons.append("직전 단기고점 돌파")
        if strength_ok:
            score += 5
            reasons.append("체결강도 매수 우위")

        if score < self.buy_confidence:
            return None

        invalidation = bottom * 0.997
        return Signal(
            code=latest.code,
            name=latest.name,
            market=latest.market,
            signal_type="buy_valid",
            price=latest.close,
            confidence=min(99.0, score),
            reason=reasons,
            invalidation_price=invalidation,
            protection_price=max(invalidation, latest.close * 0.985),
            timestamp=latest.minute,
        )

    def _evaluate_trend_pullback(self, bars: List[MinuteBar]) -> Optional[Signal]:
        latest = bars[-1]
        closes = [b.close for b in bars]
        ema5 = self._ema(closes[-20:], 5)
        ema12 = self._ema(closes[-30:], 12)
        if ema5 <= ema12:
            return None

        last10 = bars[-10:]
        pullback_low = min(b.low for b in last10[-6:])
        recent_high = max(b.high for b in last10[:-1])
        pullback_depth = self._pct(pullback_low, recent_high)

        base_volume = self._avg([b.volume for b in last10[:-3]])
        recent_volume = self._avg([b.volume for b in last10[-3:]])
        volume_ratio = recent_volume / base_volume if base_volume > 0 else 0.0

        score = 0.0
        reasons: List[str] = []
        if latest.close > ema5 > ema12:
            score += 30
            reasons.append("단기 상승추세 유지")
        if -5.5 <= pullback_depth <= -0.8:
            score += 20
            reasons.append(f"정상 눌림 {pullback_depth:.1f}%")
        if latest.close >= recent_high:
            score += 25
            reasons.append("눌림 후 전고점 재돌파")
        if volume_ratio >= 1.5:
            score += 20
            reasons.append(f"재상승 거래량 {volume_ratio:.1f}배")
        if latest.trade_strength <= 0 or latest.trade_strength >= 100:
            score += 5

        if score < self.buy_confidence:
            return None

        invalidation = pullback_low * 0.997
        return Signal(
            code=latest.code,
            name=latest.name,
            market=latest.market,
            signal_type="buy_valid",
            price=latest.close,
            confidence=min(99.0, score),
            reason=reasons,
            invalidation_price=invalidation,
            protection_price=max(invalidation, latest.close * 0.987),
            timestamp=latest.minute,
        )

    @staticmethod
    def _ema(values: List[float], period: int) -> float:
        if not values:
            return 0.0
        alpha = 2 / (period + 1)
        result = values[0]
        for value in values[1:]:
            result = alpha * value + (1 - alpha) * result
        return result

    def register_position(self, signal: Signal) -> PositionState:
        invalidation = signal.invalidation_price or signal.price * 0.98
        protection = signal.protection_price or max(invalidation, signal.price * 0.985)
        position = PositionState(
            code=signal.code,
            name=signal.name,
            market=signal.market,
            entry_price=signal.price,
            entry_time=signal.timestamp,
            highest_price=signal.price,
            protection_price=protection,
            invalidation_price=invalidation,
        )
        self.positions[signal.code] = position
        return position

    def register_manual_position(self, code: str, name: str, market: str, entry_price: float, entry_time: Optional[datetime] = None) -> PositionState:
        signal = Signal(code=code, name=name, market=market, signal_type="buy_valid", price=entry_price, confidence=100.0, reason=["사용자 직접 매수 등록"], invalidation_price=entry_price * 0.97, protection_price=entry_price * 0.985, timestamp=entry_time or datetime.now(timezone.utc))
        return self.register_position(signal)

    def close_position(self, code: str) -> None:
        self.positions.pop(code, None)

    def _evaluate_sell(
        self,
        bar: MinuteBar,
        bars: List[MinuteBar],
        position: PositionState,
    ) -> Optional[Signal]:
        position.highest_price = max(position.highest_price, bar.high)
        gain = self._pct(bar.close, position.entry_price)
        drawdown = self._pct(bar.close, position.highest_price)

        # 수익이 커질수록 보호가격을 점진적으로 올림
        if gain >= 8:
            candidate = position.highest_price * 0.975
        elif gain >= 5:
            candidate = position.highest_price * 0.970
        elif gain >= 2.5:
            candidate = max(position.entry_price * 1.002, position.highest_price * 0.965)
        else:
            candidate = position.protection_price
        position.protection_price = max(position.protection_price, candidate)

        if bar.close <= position.invalidation_price:
            return Signal(
                code=bar.code,
                name=bar.name,
                market=bar.market,
                signal_type="stop_loss",
                price=bar.close,
                confidence=98,
                reason=["무효화 가격 이탈"],
                invalidation_price=position.invalidation_price,
                protection_price=position.protection_price,
                timestamp=bar.minute,
            )

        if bar.close <= position.protection_price:
            return Signal(
                code=bar.code,
                name=bar.name,
                market=bar.market,
                signal_type="full_sell",
                price=bar.close,
                confidence=92,
                reason=[
                    f"추적 보호가격 {position.protection_price:,.0f}원 이탈",
                    f"최고가 대비 {drawdown:.1f}%",
                ],
                invalidation_price=position.invalidation_price,
                protection_price=position.protection_price,
                timestamp=bar.minute,
            )

        if len(bars) >= 8:
            avg_prev = self._avg([b.volume for b in bars[-8:-3]])
            avg_recent = self._avg([b.volume for b in bars[-3:]])
            volume_drop = 1 - (avg_recent / avg_prev) if avg_prev > 0 else 0
            previous_high = max(b.high for b in bars[-6:-1])

            if bar.high < previous_high:
                position.failed_high_count += 1
            else:
                position.failed_high_count = 0

            if (
                gain >= 3
                and not position.partial_sold
                and position.failed_high_count >= 2
                and volume_drop >= 0.35
            ):
                position.partial_sold = True
                return Signal(
                    code=bar.code,
                    name=bar.name,
                    market=bar.market,
                    signal_type="partial_sell",
                    price=bar.close,
                    confidence=82,
                    reason=[
                        f"고점 갱신 실패 {position.failed_high_count}회",
                        f"최근 거래량 {volume_drop * 100:.0f}% 감소",
                    ],
                    protection_price=position.protection_price,
                    timestamp=bar.minute,
                )
        return None


APP_VERSION = "9.0.1-simulation"
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

KRX_WS_START = os.getenv("KRX_WS_START", "08:30")
KRX_WS_END = os.getenv("KRX_WS_END", "15:40")
NXT_WS_START = os.getenv("NXT_WS_START", "08:00")
NXT_WS_END = os.getenv("NXT_WS_END", "20:00")

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

manual_holdings: Dict[str, Dict[str, Any]] = {}
watchlist_lock = threading.Lock()


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
STOCK_MASTER: Dict[str, str] = {}
STOCK_NAME_INDEX: Dict[str, List[Tuple[str, str]]] = {}


def load_stock_master() -> None:
    """KRX 종목명/종목코드 사전을 불러옵니다. 실패해도 봇은 계속 실행됩니다."""
    global STOCK_MASTER, STOCK_NAME_INDEX
    try:
        df = fdr.StockListing("KRX")
        master: Dict[str, str] = {}
        index: Dict[str, List[Tuple[str, str]]] = {}
        for _, row in df.iterrows():
            code = str(row.get("Code") or row.get("Symbol") or "").strip().zfill(6)
            name = str(row.get("Name") or "").strip()
            if not code or not name:
                continue
            master[code] = name
            key = name.replace(" ", "").lower()
            index.setdefault(key, []).append((code, name))
        STOCK_MASTER = master
        STOCK_NAME_INDEX = index
        logger.info("KRX 종목사전 로드: %s개", len(master))
    except Exception as exc:
        logger.warning("KRX 종목사전 로드 실패: %s", exc)



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
                    runtime["telegram"] = "send_only_conflict_409"
                    logger.error(
                        "텔레그램 409 충돌: 다른 서비스가 같은 봇을 폴링 중입니다. "
                        "V8은 발신 전용으로 계속 실행합니다. 명령어 수신을 되살리려면 "
                        "기존 V7/중복 서비스를 중지한 뒤 V8을 재배포하세요."
                    )
                    return
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

    def select(self, table: str, params: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []
        try:
            response = http.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=self.headers, params=params or {"select": "*"}, timeout=15)
            response.raise_for_status()
            data = response.json()
            return data if isinstance(data, list) else []
        except Exception as exc:
            logger.warning("Supabase %s 조회 실패: %s", table, exc)
            return []

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
        except requests.HTTPError as exc:
            # 초기 모바일 SQL로 생성한 tracked_positions에는 quantity 컬럼이 없을 수 있습니다.
            # 그 경우 quantity만 제외하고 다시 저장하여 봇 전체가 멈추지 않게 합니다.
            if exc.response is not None and exc.response.status_code == 400 and "quantity" in payload:
                retry_payload = dict(payload)
                retry_payload.pop("quantity", None)
                try:
                    response = http.post(
                        f"{SUPABASE_URL}/rest/v1/tracked_positions",
                        headers=headers,
                        params={"on_conflict": "stock_code"},
                        json=retry_payload,
                        timeout=15,
                    )
                    response.raise_for_status()
                    logger.warning("tracked_positions에 quantity 컬럼이 없어 수량 제외 후 저장했습니다.")
                    return
                except Exception as retry_exc:
                    logger.warning("포지션 재저장 실패: %s", retry_exc)
            else:
                logger.warning("포지션 저장 HTTP 실패: %s", exc)
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


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour, minute = value.strip().split(":", 1)
    return int(hour), int(minute)


def market_session_open(market: str, now: Optional[datetime] = None) -> bool:
    now = now or now_kst()
    if now.weekday() >= 5:
        return False

    start_text = KRX_WS_START if market == "KRX" else NXT_WS_START
    end_text = KRX_WS_END if market == "KRX" else NXT_WS_END
    start_h, start_m = _parse_hhmm(start_text)
    end_h, end_m = _parse_hhmm(end_text)

    current = now.hour * 60 + now.minute
    start = start_h * 60 + start_m
    end = end_h * 60 + end_m
    return start <= current <= end


def seconds_until_session(market: str, now: Optional[datetime] = None) -> int:
    now = now or now_kst()
    start_text = KRX_WS_START if market == "KRX" else NXT_WS_START
    start_h, start_m = _parse_hhmm(start_text)
    candidate = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)

    if now.weekday() >= 5 or now >= candidate:
        candidate += timedelta(days=1)

    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)

    return max(30, int((candidate - now).total_seconds()))


def run_ws(market: str, tr_id: str) -> None:
    state_key = "ws_krx" if market == "KRX" else "ws_nxt"
    retry_seconds = 10

    while True:
        if not market_session_open(market):
            runtime[state_key] = "waiting_market_session"
            sleep_for = min(seconds_until_session(market), 900)
            logger.info("%s 휴장/운영시간 외: %s초 후 재확인", market, sleep_for)
            time.sleep(sleep_for)
            continue

        opened_at: Optional[float] = None
        try:
            approval_key = kis.get_approval_key()
            runtime[state_key] = "connecting"

            def on_open(ws: websocket.WebSocketApp) -> None:
                nonlocal opened_at
                opened_at = time.monotonic()
                runtime[state_key] = "connected"
                logger.info("%s WebSocket connected", market)

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
            ).run_forever(
                ping_interval=30,
                ping_timeout=10,
            )

            connected_seconds = (
                time.monotonic() - opened_at if opened_at is not None else 0
            )
            retry_seconds = 10 if connected_seconds >= 300 else min(retry_seconds * 2, 300)

        except Exception as exc:
            runtime[state_key] = "failed"
            runtime["kis_last_error"] = str(exc)
            logger.exception("%s 웹소켓 실행 실패", market)
            retry_seconds = min(retry_seconds * 2, 300)

        if market_session_open(market):
            runtime[state_key] = f"retry_in_{retry_seconds}s"
            logger.info("%s %s초 후 자동 재접속", market, retry_seconds)
            time.sleep(retry_seconds)


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
        f"실전 시세 전용 · 자동주문 기능 없음"
    )


def normalize_code_or_name(token: str) -> Tuple[Optional[str], Optional[str]]:
    token = token.strip()
    if token.isdigit():
        code = token.zfill(6)
        return code, WATCHLIST.get(code) or STOCK_MASTER.get(code) or code

    key = token.replace(" ", "").lower()
    for code, name in WATCHLIST.items():
        if key == name.replace(" ", "").lower():
            return code, name

    matches = STOCK_NAME_INDEX.get(key, [])
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        return None, None

    # 정확한 이름이 없으면 유일한 부분 일치만 허용합니다.
    partial = []
    for stock_key, items in STOCK_NAME_INDEX.items():
        if key and key in stock_key:
            partial.extend(items)
            if len(partial) > 5:
                break
    if len(partial) == 1:
        return partial[0]
    return None, None


def load_holdings_from_supabase() -> None:
    rows = store.select("tracked_positions", {"select": "*", "position_status": "in.(entered,watching,partial_sold)"})
    for row in rows:
        code = str(row.get("stock_code") or "").zfill(6)
        if not code:
            continue
        item = {
            "code": code,
            "name": row.get("stock_name") or code,
            "entry_price": to_float(row.get("entry_price")),
            "quantity": to_float(row.get("quantity")),
            "market": row.get("market") or "INTEGRATED",
            "position_status": row.get("position_status") or "entered",
        }
        manual_holdings[code] = item
        WATCHLIST[code] = item["name"]
        if item["entry_price"] > 0:
            engine.register_manual_position(code, item["name"], item["market"], item["entry_price"], now_kst())


def register_manual_holding(code: str, name: str, price: float, quantity: float) -> None:
    engine.register_manual_position(code, name, "INTEGRATED", price, now_kst())
    manual_holdings[code] = {"code": code, "name": name, "entry_price": price, "quantity": quantity, "market": "INTEGRATED", "position_status": "entered"}
    with watchlist_lock:
        WATCHLIST[code] = name
    store.upsert_position({"stock_code": code, "stock_name": name, "market": "INTEGRATED", "entry_price": price, "current_price": price, "highest_price": price, "quantity": quantity, "position_status": "entered", "updated_at": now_kst().isoformat()})


def remove_manual_holding(code: str) -> None:
    item = manual_holdings.pop(code, None)
    engine.close_position(code)
    store.upsert_position({"stock_code": code, "stock_name": (item or {}).get("name") or WATCHLIST.get(code, code), "market": "INTEGRATED", "position_status": "closed", "updated_at": now_kst().isoformat()})



def reset_all_holdings() -> int:
    """보유종목 등록만 모두 해제하고 관심목록 항목은 유지합니다."""
    codes = list(manual_holdings.keys())
    for code in codes:
        remove_manual_holding(code)
    return len(codes)


def reset_interest_watchlist() -> int:
    """
    보유종목을 제외한 관심종목을 모두 삭제합니다.
    보유종목은 매도 감시가 필요하므로 감시목록에 남겨둡니다.
    """
    with watchlist_lock:
        removable = [code for code in WATCHLIST if code not in manual_holdings]
        for code in removable:
            WATCHLIST.pop(code, None)
    return len(removable)


def holding_report() -> str:
    if not manual_holdings:
        return "📭 등록된 보유종목이 없습니다."
    lines = ["💼 <b>보유종목</b>", ""]
    total_cost = total_value = 0.0
    for code, item in manual_holdings.items():
        try:
            current = to_float(kis.inquire_price(code).get("stck_prpr"))
        except Exception:
            current = item["entry_price"]
        entry = float(item["entry_price"])
        qty = float(item["quantity"])
        rate = ((current / entry) - 1) * 100 if entry else 0.0
        total_cost += entry * qty
        total_value += current * qty
        lines += [f"• <b>{html.escape(item['name'])}</b> ({code})", f"  매수가 {entry:,.0f}원 · 현재가 {current:,.0f}원", f"  수량 {qty:g}주 · 수익률 <b>{rate:+.2f}%</b>"]
    total_rate = ((total_value / total_cost) - 1) * 100 if total_cost else 0.0
    lines += ["", f"총 매입금액: {total_cost:,.0f}원", f"총 평가금액: {total_value:,.0f}원", f"총 수익률: <b>{total_rate:+.2f}%</b>"]
    return "\n".join(lines)


def watchlist_report() -> str:
    lines = ["👀 <b>V9 관심·감시목록</b>", ""]
    with watchlist_lock:
        for code, name in WATCHLIST.items():
            lines.append(f"• {html.escape(name)} ({code})" + (" · 보유" if code in manual_holdings else ""))
    return "\n".join(lines)


def current_candidate_report() -> str:
    rows = []
    with watchlist_lock:
        items = list(WATCHLIST.items())
    for code, name in items:
        try:
            out = kis.inquire_price(code)
            price = to_float(out.get("stck_prpr"))
            chg = to_float(out.get("prdy_ctrt"))
            vol = to_int(out.get("acml_vol"))
            score = max(0.0, min(100.0, 50 + chg * 5 + (10 if vol > 1000000 else 0)))
            rows.append((score, code, name, price))
        except Exception:
            pass
    if not rows:
        return "추천 후보를 계산할 수 없습니다."
    rows.sort(reverse=True)
    lines = ["📌 <b>현재 감시목록 후보</b>", ""]
    for score, code, name, price in rows[:5]:
        lines.append(f"• <b>{html.escape(name)}</b> ({code})\n  현재가 {price:,.0f}원 · 기초점수 {score:.0f}점")
    lines += ["", "※ 등록된 감시목록 안에서 계산합니다.", "※ 장중 V자 반등·거래량 조건 충족 시 별도 신호가 발송됩니다."]
    return "\n".join(lines)


def premarket_briefing() -> str:
    return (
        "🕗 <b>장 시작 전 브리핑</b>\n\n"
        f"감시종목: {len(WATCHLIST)}개\n"
        f"보유종목: {len(manual_holdings)}개\n"
        "매수 기준: V자 반등 또는 상승추세 눌림 후 재돌파\n"
        "필수 확인: 거래량 증가·저점 방어·단기고점 돌파\n\n"
        + current_candidate_report()
    )


def close_briefing() -> str:
    return (
        "📊 <b>장마감 브리핑</b>\n\n"
        f"오늘 발생 신호: {runtime['signals_today']}건\n"
        f"보유종목: {len(manual_holdings)}개\n"
        f"마지막 체결: {runtime['last_tick_at'] or '-'}\n\n"
        "추천·매수·매도 신호는 Supabase에 저장됩니다."
    )


def performance_report() -> str:
    rows = store.select(
        "performance_reports",
        {"select": "*", "order": "created_at.desc", "limit": "1"},
    )
    if not rows:
        return (
            "📈 <b>성과보고</b>\n\n"
            "아직 생성된 성과보고가 없습니다.\n"
            "장중 신호와 장마감 평가가 누적되면 표시됩니다."
        )
    row = rows[0]
    return (
        "📈 <b>최근 성과보고</b>\n\n"
        f"기간: {row.get('period_start', '-')} ~ {row.get('period_end', '-')}\n"
        f"추천 수: {row.get('total_recommendations', 0)}\n"
        f"성공: {row.get('success_count', 0)}\n"
        f"실패: {row.get('failure_count', 0)}\n"
        f"적중률: {row.get('hit_rate', '-')}\n"
        f"평균수익률: {row.get('average_return', '-')}"
    )


def help_text() -> str:
    return (
        "<b>명령어</b>\n"
        "/상태\n/테스트\n/한투테스트\n/미국증시\n"
        "/장전브리핑\n/장마감브리핑\n/감시목록\n/추천\n/성과\n\n"
        "<b>보유종목</b>\n"
        "/매수 종목명(또는 코드) 매수가 수량\n"
        "/매도 종목명(또는 코드)\n"
        "/보유\n/평가\n"
        "/보유리셋\n\n"
        "<b>관심종목</b>\n"
        "/관심등록 종목명(또는 코드)\n"
        "/관심삭제 종목명(또는 코드)\n"
        "/관심리셋"
    )


def handle_command(text: str, chat_id: str) -> None:
    parts = text.split()
    cmd = parts[0]
    if cmd in ("/도움말", "/help"):
        telegram.send(help_text(), chat_id)
    elif cmd == "/상태":
        telegram.send(status_text(), chat_id)
    elif cmd == "/테스트":
        telegram.send("✅ V9 텔레그램 연결 정상", chat_id)
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
    elif cmd == "/장전브리핑":
        telegram.send(premarket_briefing(), chat_id)
    elif cmd == "/장마감브리핑":
        telegram.send(close_briefing(), chat_id)
    elif cmd == "/성과":
        telegram.send(performance_report(), chat_id)
    elif cmd == "/감시목록":
        telegram.send(watchlist_report(), chat_id)
    elif cmd == "/추천":
        telegram.send(current_candidate_report(), chat_id)
    elif cmd == "/매수":
        if len(parts) < 4:
            telegram.send(
                "사용법: /매수 종목명(또는 코드) 매수가 수량\n"
                "예: /매수 삼성전자 70000 10",
                chat_id,
            )
            return
        code, name = normalize_code_or_name(parts[1])
        if not code or not name:
            telegram.send("종목명 또는 종목코드를 확인하세요. 이름이 비슷한 종목은 코드로 입력하세요.", chat_id)
            return
        price = to_float(parts[2])
        qty = to_float(parts[3])
        if price <= 0 or qty <= 0:
            telegram.send("매수가와 수량은 0보다 커야 합니다.", chat_id)
            return
        register_manual_holding(code, name, price, qty)
        telegram.send(
            f"✅ <b>보유종목 등록</b>\n\n"
            f"{html.escape(name)} ({code})\n"
            f"매수가: {price:,.0f}원\n"
            f"수량: {qty:g}주\n\n"
            f"감시목록과 매도 엔진이 함께 추적합니다.",
            chat_id,
        )
    elif cmd in ("/매도", "/삭제"):
        if len(parts) < 2:
            telegram.send("사용법: /매도 종목명(또는 코드)", chat_id)
            return
        code, name = normalize_code_or_name(parts[1])
        if not code:
            telegram.send("종목코드 6자리를 확인하세요.", chat_id)
            return
        remove_manual_holding(code)
        telegram.send(
            f"✅ {html.escape(name or code)} 보유 감시를 종료했습니다.",
            chat_id,
        )
    elif cmd in ("/보유", "/평가"):
        telegram.send(holding_report(), chat_id)
    elif cmd == "/보유리셋":
        count = reset_all_holdings()
        telegram.send(
            f"✅ 보유종목 {count}개를 모두 초기화했습니다.\n"
            f"관심종목 목록은 그대로 유지됩니다.",
            chat_id,
        )

    elif cmd == "/관심등록":
        if len(parts) < 2:
            telegram.send(
                "사용법: /관심등록 종목명(또는 코드)\n"
                "예: /관심등록 삼성전자",
                chat_id,
            )
            return

        query = " ".join(parts[1:]).strip()
        code, name = normalize_code_or_name(query)
        if not code or not name:
            telegram.send(
                "종목을 찾지 못했거나 비슷한 이름이 여러 개입니다.\n"
                "정확한 종목명 또는 6자리 종목코드를 입력하세요.",
                chat_id,
            )
            return

        with watchlist_lock:
            already_exists = code in WATCHLIST
            WATCHLIST[code] = name

        telegram.send(
            f"{'ℹ️ 이미 등록된 종목입니다.' if already_exists else '✅ 관심종목 등록 완료'}\n"
            f"{html.escape(name)} ({code})",
            chat_id,
        )

    elif cmd == "/관심삭제":
        if len(parts) < 2:
            telegram.send(
                "사용법: /관심삭제 종목명(또는 코드)\n"
                "예: /관심삭제 삼성전자",
                chat_id,
            )
            return

        query = " ".join(parts[1:]).strip()
        code, name = normalize_code_or_name(query)
        if not code:
            telegram.send(
                "종목을 찾지 못했습니다. 정확한 종목명 또는 코드를 입력하세요.",
                chat_id,
            )
            return

        if code in manual_holdings:
            telegram.send(
                "이 종목은 보유종목으로 등록되어 있어 삭제할 수 없습니다.\n"
                "먼저 /매도 종목명 명령으로 보유 감시를 종료하세요.",
                chat_id,
            )
            return

        with watchlist_lock:
            removed = WATCHLIST.pop(code, None)

        if removed:
            telegram.send(
                f"✅ 관심종목 삭제 완료\n{html.escape(removed)} ({code})",
                chat_id,
            )
        else:
            telegram.send(
                f"ℹ️ {html.escape(name or code)}은(는) 관심목록에 없습니다.",
                chat_id,
            )

    elif cmd == "/관심리셋":
        count = reset_interest_watchlist()
        telegram.send(
            f"✅ 관심종목 {count}개를 모두 초기화했습니다.\n"
            f"보유종목 {len(manual_holdings)}개는 감시목록에 유지됩니다.",
            chat_id,
        )

    else:
        telegram.send(help_text(), chat_id)


def scheduler_loop() -> None:
    last_reset: Optional[str] = None
    sent: Dict[str, str] = {}
    while True:
        now = now_kst()
        today = now.date().isoformat()
        if last_reset != today:
            runtime["signals_today"] = 0
            last_reset = today
        if now.weekday() < 5:
            if now.hour == 7 and 30 <= now.minute < 35 and sent.get("us") != today:
                telegram.send(morning_us_briefing())
                sent["us"] = today
            if now.hour == 8 and 20 <= now.minute < 25 and sent.get("pre") != today:
                telegram.send(premarket_briefing())
                sent["pre"] = today
            if now.hour == 15 and 45 <= now.minute < 50 and sent.get("close") != today:
                telegram.send(close_briefing())
                sent["close"] = today
            if now.hour == 20 and 5 <= now.minute < 10 and sent.get("nxt") != today:
                telegram.send("🌙 <b>NXT 마감 후 점검</b>\n\n" + current_candidate_report())
                sent["nxt"] = today
        time.sleep(20)


def startup() -> None:
    runtime["started_at"] = now_kst().isoformat()
    load_stock_master()
    store.test()
    load_holdings_from_supabase()
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
