from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple


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

    def register_manual_position(
        self,
        code: str,
        name: str,
        market: str,
        entry_price: float,
        entry_time: Optional[datetime] = None,
    ) -> PositionState:
        """사용자가 직접 매수한 종목을 동적 매도 감시에 등록합니다."""
        signal = Signal(
            code=code,
            name=name,
            market=market,
            signal_type="buy_valid",
            price=entry_price,
            confidence=100.0,
            reason=["사용자 직접 매수 등록"],
            invalidation_price=entry_price * 0.97,
            protection_price=entry_price * 0.985,
            timestamp=entry_time or datetime.now(timezone.utc),
        )
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
