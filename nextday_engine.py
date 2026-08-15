
from __future__ import annotations

"""
명하 - 다음 거래일 대응 시나리오 엔진 v0.2.0

목적
----
- 기존 실시간 봇(a1.py)과 분리된 순수 분석 모듈
- 일봉 60~120개 + 당일 1분봉/30분봉 OHLCV를 입력받아
  캔들 구조, 꼬리, 거래량, 지지/저항, 가격압축, 반등 위치를 계산
- "내일 오른다/내린다" 단정 대신 대응 시나리오와 무효조건을 생성
- 다음 단계에서 OpenAI 모델이 해석할 수 있도록 compact AI payload 제공

중요
----
이 모듈은 네트워크 호출을 하지 않는다.
KIS 수집은 기존 a1.py가 담당하고, 이 엔진은 받은 숫자만 분석한다.
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
import math
import statistics

VERSION = "0.2.0"


def _num(v: Any, default: float = 0.0) -> float:
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return default


def _pct(new: float, old: float) -> float:
    return (new / old - 1.0) * 100.0 if old else 0.0


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _median(values: Iterable[float], default: float = 0.0) -> float:
    vals = [float(v) for v in values if v is not None]
    return statistics.median(vals) if vals else default


def _mean(values: Iterable[float], default: float = 0.0) -> float:
    vals = [float(v) for v in values if v is not None]
    return sum(vals) / len(vals) if vals else default


def _safe_ratio(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


def _parse_dt(v: Any) -> Optional[datetime]:
    if isinstance(v, datetime):
        return v
    s = str(v or "").strip()
    if not s:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y%m%d%H%M%S",
        "%Y%m%d%H%M",
        "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


@dataclass
class Candle:
    time: Optional[datetime]
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def range(self) -> float:
        return max(0.0, self.high - self.low)

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def bull(self) -> bool:
        return self.close > self.open

    @property
    def bear(self) -> bool:
        return self.close < self.open

    @property
    def upper_wick(self) -> float:
        return max(0.0, self.high - max(self.open, self.close))

    @property
    def lower_wick(self) -> float:
        return max(0.0, min(self.open, self.close) - self.low)

    @property
    def body_ratio(self) -> float:
        return _safe_ratio(self.body, self.range)

    @property
    def upper_wick_ratio(self) -> float:
        return _safe_ratio(self.upper_wick, self.range)

    @property
    def lower_wick_ratio(self) -> float:
        return _safe_ratio(self.lower_wick, self.range)

    @property
    def close_location(self) -> float:
        # 0=저가마감, 1=고가마감
        return _safe_ratio(self.close - self.low, self.range, 0.5) if self.range else 0.5


@dataclass
class Scenario:
    name: str
    relative_score: float
    condition: str
    action: str


@dataclass
class NextDayResult:
    code: str
    name: str
    asof: str
    data_quality: str
    current_price: float

    structure_score: float
    candle_score: float
    volume_score: float
    support_score: float
    compression_score: float
    chase_risk: float
    setup_score: float

    support_zone: Tuple[float, float]
    resistance_zone: Tuple[float, float]
    preferred_entry_zone: Tuple[float, float]
    invalidation_price: float

    primary_view: str
    scenarios: List[Scenario]
    reasons: List[str]
    cautions: List[str]
    features: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def ai_payload(self) -> Dict[str, Any]:
        """2단계 AI 해석부에 넘길 압축 입력."""
        keep = {
            "ma5", "ma20", "ma60", "day_change_pct", "day_low_recovery_pct",
            "close_location_day", "last_30m_close_location",
            "lower_wick_bias", "upper_wick_bias", "late_volume_ratio",
            "range_compression", "support_touches", "support_distance_pct",
            "resistance_distance_pct", "daily_gain_20_pct", "high20_gap_pct",
            "intraday_higher_low", "intraday_lower_high", "bars_30m",
        }
        return {
            "version": VERSION,
            "code": self.code,
            "name": self.name,
            "asof": self.asof,
            "current_price": self.current_price,
            "scores": {
                "structure": self.structure_score,
                "candle": self.candle_score,
                "volume": self.volume_score,
                "support": self.support_score,
                "compression": self.compression_score,
                "chase_risk": self.chase_risk,
                "setup": self.setup_score,
            },
            "zones": {
                "support": self.support_zone,
                "resistance": self.resistance_zone,
                "preferred_entry": self.preferred_entry_zone,
                "invalidation": self.invalidation_price,
            },
            "features": {k: self.features.get(k) for k in keep},
            "rule_view": self.primary_view,
            "rule_reasons": self.reasons[:8],
            "rule_cautions": self.cautions[:6],
        }


def candle_from_row(row: Dict[str, Any], time_key: str = "time") -> Optional[Candle]:
    o = _num(row.get("open", row.get("Open")))
    h = _num(row.get("high", row.get("High")))
    l = _num(row.get("low", row.get("Low")))
    c = _num(row.get("close", row.get("Close")))
    v = max(0.0, _num(row.get("volume", row.get("Volume"))))
    t = _parse_dt(row.get(time_key) or row.get("minute") or row.get("date"))
    if min(o, h, l, c) <= 0 or h < l:
        return None
    return Candle(t, o, h, l, c, v)


def parse_daily_rows(rows: Iterable[Dict[str, Any]]) -> List[Candle]:
    out = []
    for r in rows or []:
        c = candle_from_row(r, "date")
        if c:
            out.append(c)
    out.sort(key=lambda x: x.time or datetime.min)
    return out


def parse_intraday_rows(rows: Iterable[Dict[str, Any]]) -> List[Candle]:
    out = []
    for r in rows or []:
        c = candle_from_row(r, "time")
        if c:
            out.append(c)
    out.sort(key=lambda x: x.time or datetime.min)
    return out


def aggregate_to_30m(rows: Iterable[Dict[str, Any]]) -> List[Candle]:
    """
    1분봉 또는 더 세밀한 봉을 30분봉으로 집계.
    이미 30분봉이어도 동일 버킷이면 안전하게 합쳐진다.
    """
    src = parse_intraday_rows(rows)
    buckets: Dict[datetime, List[Candle]] = {}
    for c in src:
        if not c.time:
            continue
        t = c.time.replace(minute=(c.time.minute // 30) * 30, second=0, microsecond=0)
        buckets.setdefault(t, []).append(c)

    out: List[Candle] = []
    for t in sorted(buckets):
        g = buckets[t]
        out.append(Candle(
            time=t,
            open=g[0].open,
            high=max(x.high for x in g),
            low=min(x.low for x in g),
            close=g[-1].close,
            volume=sum(x.volume for x in g),
        ))
    return out


def _local_levels(values: List[float], mode: str, radius: int = 2) -> List[float]:
    out: List[float] = []
    if len(values) < radius * 2 + 1:
        return out
    for i in range(radius, len(values) - radius):
        w = values[i - radius:i + radius + 1]
        if mode == "low" and values[i] <= min(w):
            out.append(values[i])
        elif mode == "high" and values[i] >= max(w):
            out.append(values[i])
    return out


def _cluster_levels(levels: List[float], tolerance_pct: float = 1.5) -> List[Tuple[float, int]]:
    groups: List[List[float]] = []
    for v in sorted(x for x in levels if x > 0):
        placed = False
        for g in groups:
            center = _median(g)
            if abs(_pct(v, center)) <= tolerance_pct:
                g.append(v)
                placed = True
                break
        if not placed:
            groups.append([v])
    return [(_median(g), len(g)) for g in groups]


class NextDayAnalyzer:
    def __init__(self, min_daily_bars: int = 40, min_30m_bars: int = 8):
        self.min_daily_bars = min_daily_bars
        self.min_30m_bars = min_30m_bars

    def analyze(
        self,
        code: str,
        name: str,
        daily_rows: Iterable[Dict[str, Any]],
        intraday_rows: Iterable[Dict[str, Any]],
        *,
        intraday_is_30m: bool = False,
    ) -> NextDayResult:
        daily = parse_daily_rows(daily_rows)
        intra = parse_intraday_rows(intraday_rows) if intraday_is_30m else aggregate_to_30m(intraday_rows)

        if len(daily) < self.min_daily_bars:
            raise ValueError(f"일봉 부족: {len(daily)}개 / 최소 {self.min_daily_bars}개")
        if len(intra) < self.min_30m_bars:
            raise ValueError(f"30분봉 부족: {len(intra)}개 / 최소 {self.min_30m_bars}개")

        # 최신 거래일만 사용
        last_date = intra[-1].time.date() if intra[-1].time else None
        if last_date:
            day30 = [x for x in intra if x.time and x.time.date() == last_date]
        else:
            day30 = intra[-14:]
        if len(day30) < self.min_30m_bars:
            day30 = intra[-max(self.min_30m_bars, 14):]

        closes = [x.close for x in daily]
        highs = [x.high for x in daily]
        lows = [x.low for x in daily]
        volumes = [x.volume for x in daily]

        current = day30[-1].close
        ma5 = _mean(closes[-5:])
        ma20 = _mean(closes[-20:])
        ma60 = _mean(closes[-60:]) if len(closes) >= 60 else _mean(closes)
        gain20 = _pct(current, closes[-21]) if len(closes) >= 21 else 0.0

        day_open = day30[0].open
        day_high = max(x.high for x in day30)
        day_low = min(x.low for x in day30)
        day_change = _pct(current, day_open)
        day_low_recovery = _pct(current, day_low)
        day_range = max(1e-9, day_high - day_low)
        close_location_day = (current - day_low) / day_range

        recent30 = day30[-6:]
        lower_wick_bias = _mean(x.lower_wick_ratio for x in recent30)
        upper_wick_bias = _mean(x.upper_wick_ratio for x in recent30)
        last30 = day30[-1]

        # 후반 거래량의 변화
        n = len(day30)
        third = max(2, n // 3)
        early_vol = _mean(x.volume for x in day30[:third])
        late_vol = _mean(x.volume for x in day30[-third:])
        late_volume_ratio = _safe_ratio(late_vol, early_vol, 1.0)

        # 후반 변동폭 압축
        ranges = [_safe_ratio(x.range, x.close) * 100 for x in day30]
        head = _mean(ranges[:min(3, len(ranges))])
        tail = _mean(ranges[-min(3, len(ranges)):])
        range_compression = _clamp(1.0 - _safe_ratio(tail, head, 1.0), 0.0, 1.0)

        # HH/HL 또는 LH/LL 간단 판정
        intraday_higher_low = False
        intraday_lower_high = False
        if len(day30) >= 8:
            a = day30[-8:-4]
            b = day30[-4:]
            intraday_higher_low = min(x.low for x in b) >= min(x.low for x in a)
            intraday_lower_high = max(x.high for x in b) < max(x.high for x in a)

        # 일봉 지지/저항 후보
        recent_daily = daily[-60:]
        low_levels = _cluster_levels(_local_levels([x.low for x in recent_daily], "low"), 1.5)
        high_levels = _cluster_levels(_local_levels([x.high for x in recent_daily], "high"), 1.5)

        supports = sorted(
            [(center, touches) for center, touches in low_levels if center <= current * 1.02],
            key=lambda x: x[0],
            reverse=True,
        )
        resistances = sorted(
            [(center, touches) for center, touches in high_levels if center >= current * 0.98],
            key=lambda x: x[0],
        )

        support, support_touches = supports[0] if supports else (day_low, 1)
        resistance, _ = resistances[0] if resistances else (day_high, 1)

        # 당일 강한 저점/고점도 병합
        if abs(_pct(day_low, support)) <= 3.0:
            support = _median([support, day_low])
            support_touches += 1
        if resistance < current * 1.002:
            resistance = max(day_high, current * 1.01)

        support_distance = _pct(current, support)
        resistance_distance = _pct(resistance, current)

        high20 = max(highs[-20:])
        high20_gap = _pct(current, high20)

        # 데이터 품질
        data_quality = "GOOD"
        cautions: List[str] = []
        if len(day30) < 12:
            data_quality = "FAIR"
            cautions.append(f"당일 30분봉 {len(day30)}개로 전체장 정보가 다소 부족")
        if len(daily) < 60:
            data_quality = "FAIR"
            cautions.append("60일 미만 일봉으로 중기 구조 신뢰도 제한")

        # 1) 구조 점수
        structure = 50.0
        if current >= ma5 >= ma20:
            structure += 15
        elif current >= ma20:
            structure += 7
        else:
            structure -= 8
        structure += 8 if ma20 >= ma60 else -6
        structure += 7 if intraday_higher_low else 0
        structure -= 6 if intraday_lower_high and not intraday_higher_low else 0
        structure = _clamp(structure)

        # 2) 캔들 점수
        candle_score = 50.0
        candle_score += _clamp((lower_wick_bias - upper_wick_bias) * 45, -18, 18)
        candle_score += (close_location_day - 0.5) * 24
        candle_score += (last30.close_location - 0.5) * 12
        if last30.lower_wick_ratio >= 0.40:
            candle_score += 8
        if last30.upper_wick_ratio >= 0.45:
            candle_score -= 8
        candle_score = _clamp(candle_score)

        # 3) 거래량 구조
        volume_score = 50.0
        if 0 < late_volume_ratio <= 0.70:
            volume_score += 14
        elif late_volume_ratio <= 0.90:
            volume_score += 7
        elif late_volume_ratio >= 1.45 and day_change < 0:
            volume_score -= 15
        # 저점 반등 후 후반 거래량이 완전히 마르면 중립 이상, 폭락 거래량 확대는 감점
        if day_low_recovery >= 1.5 and late_volume_ratio <= 1.0:
            volume_score += 6
        volume_score = _clamp(volume_score)

        # 4) 지지 안정성
        support_score = 82.0 - max(0.0, support_distance) * 7.5
        support_score += min(12.0, max(0, support_touches - 1) * 4.0)
        if current < support * 0.985:
            support_score -= 35
        support_score = _clamp(support_score)

        # 5) 가격압축
        compression_score = _clamp(48 + range_compression * 48)
        if intraday_higher_low and intraday_lower_high:
            compression_score = _clamp(compression_score + 7)

        # 6) 추격위험
        chase = 15.0
        if gain20 >= 15:
            chase += 25
        elif gain20 >= 8:
            chase += 12
        if high20_gap >= -2.0:
            chase += 20
        if day_change >= 5:
            chase += 20
        if close_location_day >= 0.90 and day_change >= 3:
            chase += 10
        chase = _clamp(chase)

        setup = (
            structure * 0.23
            + candle_score * 0.24
            + volume_score * 0.16
            + support_score * 0.24
            + compression_score * 0.13
            - chase * 0.12
        )
        setup = _clamp(setup)

        # 가격대
        support_zone = (support * 0.994, support * 1.010)
        resistance_zone = (resistance * 0.995, resistance * 1.005)
        invalidation = support * 0.982

        # 좋은 매수는 지지 바로 위 + 반등 확인을 전제로 한다.
        entry_low = max(support * 0.998, support_zone[0])
        entry_high = min(current, support * 1.028)
        if entry_high <= entry_low:
            entry_high = support * 1.022
        preferred_entry = (entry_low, entry_high)

        reasons: List[str] = []
        if intraday_higher_low:
            reasons.append("후반 30분봉 저점이 높아지는 구조")
        if lower_wick_bias > upper_wick_bias * 1.2:
            reasons.append("최근 30분봉에서 아래꼬리 매수 방어가 상대적으로 우세")
        if late_volume_ratio < 0.85:
            reasons.append("후반 거래량 감소로 매도압력 둔화/가격압축 가능성")
        if range_compression >= 0.25:
            reasons.append("장 후반 봉 변동폭 축소")
        if support_touches >= 3:
            reasons.append(f"유사 지지대 반복 확인 {support_touches}회")
        if current >= ma20:
            reasons.append("현재가가 20일선 위 또는 부근")
        if day_low_recovery >= 2.0:
            reasons.append(f"당일 저점 대비 {day_low_recovery:.1f}% 회복")
        if close_location_day >= 0.65:
            reasons.append("당일 범위 중상단에서 마감")

        if chase >= 55:
            cautions.append("최근 상승폭/고점 위치로 추격위험이 높음")
        if resistance_distance < 1.5:
            cautions.append("현재가 바로 위에 단기 저항이 가까움")
        if current < ma20:
            cautions.append("20일선 아래라 중기 추세 확인 필요")
        if last30.upper_wick_ratio >= 0.45:
            cautions.append("마지막 30분봉 윗꼬리가 길어 상단 매물 확인 필요")

        # 규칙 기반 상대 시나리오 점수: '확률'로 표시하지 않는다.
        bull = (
            candle_score * 0.28 + support_score * 0.30 + volume_score * 0.16
            + compression_score * 0.12 + structure * 0.14
        )
        bear = (
            (100 - support_score) * 0.34 + chase * 0.22
            + (100 - candle_score) * 0.22 + (100 - structure) * 0.22
        )
        side = 50 + compression_score * 0.35 - abs(bull - bear) * 0.25
        side = _clamp(side)

        raw = {
            "눌림 후 반등": max(1.0, bull),
            "박스권/가격압축": max(1.0, side),
            "지지 이탈/추가 하락": max(1.0, bear),
        }
        total_raw = sum(raw.values())
        rel = {k: v / total_raw * 100 for k, v in raw.items()}

        scenarios = [
            Scenario(
                "눌림 후 반등",
                round(rel["눌림 후 반등"], 1),
                f"{support_zone[0]:,.0f}~{support_zone[1]:,.0f}원 지지와 재회복 확인",
                f"추격보다 {preferred_entry[0]:,.0f}~{preferred_entry[1]:,.0f}원에서 지지/체결 회복 확인 후 접근",
            ),
            Scenario(
                "박스권/가격압축",
                round(rel["박스권/가격압축"], 1),
                f"{support_zone[1]:,.0f}원~{resistance_zone[0]:,.0f}원 사이 횡보",
                "방향 확인 전 비중 확대보다 관찰",
            ),
            Scenario(
                "지지 이탈/추가 하락",
                round(rel["지지 이탈/추가 하락"], 1),
                f"{invalidation:,.0f}원 부근을 거래량 동반 하향 이탈",
                "기존 반등 가설 폐기하고 새로운 지지 확인까지 대기",
            ),
        ]
        scenarios.sort(key=lambda x: x.relative_score, reverse=True)

        primary = scenarios[0].name
        if setup < 48:
            primary = "진입보다 관찰 우선"
        elif chase >= 65:
            primary = "방향 긍정이어도 추격 금지"

        features = {
            "ma5": round(ma5, 2),
            "ma20": round(ma20, 2),
            "ma60": round(ma60, 2),
            "daily_gain_20_pct": round(gain20, 3),
            "high20": round(high20, 2),
            "high20_gap_pct": round(high20_gap, 3),
            "day_open": round(day_open, 2),
            "day_high": round(day_high, 2),
            "day_low": round(day_low, 2),
            "day_change_pct": round(day_change, 3),
            "day_low_recovery_pct": round(day_low_recovery, 3),
            "close_location_day": round(close_location_day, 3),
            "last_30m_close_location": round(last30.close_location, 3),
            "lower_wick_bias": round(lower_wick_bias, 3),
            "upper_wick_bias": round(upper_wick_bias, 3),
            "late_volume_ratio": round(late_volume_ratio, 3),
            "range_compression": round(range_compression, 3),
            "intraday_higher_low": intraday_higher_low,
            "intraday_lower_high": intraday_lower_high,
            "support_touches": support_touches,
            "support_distance_pct": round(support_distance, 3),
            "resistance_distance_pct": round(resistance_distance, 3),
            "bars_daily": len(daily),
            "bars_30m": len(day30),
        }

        return NextDayResult(
            code=str(code).zfill(6),
            name=str(name or code),
            asof=str(last_date or ""),
            data_quality=data_quality,
            current_price=round(current, 2),
            structure_score=round(structure, 1),
            candle_score=round(candle_score, 1),
            volume_score=round(volume_score, 1),
            support_score=round(support_score, 1),
            compression_score=round(compression_score, 1),
            chase_risk=round(chase, 1),
            setup_score=round(setup, 1),
            support_zone=tuple(round(x, 0) for x in support_zone),
            resistance_zone=tuple(round(x, 0) for x in resistance_zone),
            preferred_entry_zone=tuple(round(x, 0) for x in preferred_entry),
            invalidation_price=round(invalidation, 0),
            primary_view=primary,
            scenarios=scenarios,
            reasons=reasons,
            cautions=cautions,
            features=features,
        )


def render_text(result: NextDayResult) -> str:
    lines = [
        f"🧠 명하 다음 거래일 시나리오 v{VERSION}",
        f"{result.name} ({result.code}) · 기준 {result.asof}",
        f"현재 {result.current_price:,.0f}원 · 데이터 {result.data_quality}",
        "",
        f"종합 셋업 {result.setup_score:.0f}점",
        f"구조 {result.structure_score:.0f} · 캔들 {result.candle_score:.0f} · 거래량 {result.volume_score:.0f}",
        f"지지 {result.support_score:.0f} · 압축 {result.compression_score:.0f} · 추격위험 {result.chase_risk:.0f}",
        "",
        f"기본 판단: {result.primary_view}",
        f"관심 진입: {result.preferred_entry_zone[0]:,.0f}~{result.preferred_entry_zone[1]:,.0f}원",
        f"핵심 지지: {result.support_zone[0]:,.0f}~{result.support_zone[1]:,.0f}원",
        f"1차 저항: {result.resistance_zone[0]:,.0f}~{result.resistance_zone[1]:,.0f}원",
        f"가설 무효: {result.invalidation_price:,.0f}원",
        "",
        "근거:",
    ]
    lines.extend(f"• {x}" for x in result.reasons[:8])
    if result.cautions:
        lines.append("")
        lines.append("주의:")
        lines.extend(f"• {x}" for x in result.cautions[:6])
    lines.append("")
    lines.append("시나리오 상대점수(백테스트 확률 아님):")
    for s in result.scenarios:
        lines.append(f"• {s.name} {s.relative_score:.1f} · {s.condition}")
    return "\n".join(lines)
