import os
import time
import json
import html
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import FinanceDataReader as fdr
import yfinance as yf
from flask import Flask, jsonify
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# 뽕실로봇 V6 Ultimate
# - Wilder RSI / EMA / MACD / ATR / ADX / Bollinger Band
# - AI Score 100점
# - 시가총액 상위 200종목
# - 캐시 / 재시도 / 상태 명령어 / 강제 스캔
# - ATR 기반 손절 및 트레일링
# ============================================================

APP_VERSION = "6.1.0"
KST = timezone(timedelta(hours=9))

app = Flask(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
CHAT_ID = os.getenv("CHAT_ID", "").strip()
PORTFOLIO_FILE = os.getenv("PORTFOLIO_FILE", "portfolio.json")
LOG_FILE = os.getenv("LOG_FILE", "stock_bot.log")
SIGNAL_HISTORY_FILE = os.getenv("SIGNAL_HISTORY_FILE", "signal_history.json")

SCAN_TOP_N = int(os.getenv("SCAN_TOP_N", "200"))
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "180"))
PORTFOLIO_INTERVAL_SECONDS = int(os.getenv("PORTFOLIO_INTERVAL_SECONDS", "60"))
COMMAND_INTERVAL_SECONDS = int(os.getenv("COMMAND_INTERVAL_SECONDS", "5"))
MIN_AI_SCORE = float(os.getenv("MIN_AI_SCORE", "70"))
SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "3600"))
DATA_CACHE_SECONDS = int(os.getenv("DATA_CACHE_SECONDS", "300"))
LISTING_CACHE_SECONDS = int(os.getenv("LISTING_CACHE_SECONDS", "21600"))
INVESTOR_CACHE_SECONDS = int(os.getenv("INVESTOR_CACHE_SECONDS", "1800"))
MAX_TELEGRAM_LENGTH = 3900

state_lock = threading.RLock()
portfolio_lock = threading.RLock()
scan_lock = threading.Lock()

sent_signals_today: Dict[str, Dict[str, Any]] = {}
last_reset_date: Optional[str] = None
daily_summary_sent_date: Optional[str] = None
morning_briefing_sent_date: Optional[str] = None
nxt_open_sent_date: Optional[str] = None
reg_open_sent_date: Optional[str] = None
reg_close_sent_date: Optional[str] = None
nxt_close_sent_date: Optional[str] = None
last_update_id = 0

runtime_state: Dict[str, Any] = {
    "started_at": datetime.now(KST).isoformat(),
    "last_scan_at": None,
    "last_scan_count": 0,
    "last_signal_count": 0,
    "last_scan_error": None,
    "scanner_running": False,
    "market_score": None,
    "today_scan_runs": 0,
    "today_analyzed_total": 0,
    "today_signal_total": 0,
    "today_best_score": None,
    "today_best_v_score": None,
    "last_error_at": None,
}

_cache: Dict[str, Tuple[float, Any]] = {}


# ----------------------------- logging -----------------------------

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("bbongsilbot")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
    )

    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    try:
        file_handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=1_000_000,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError:
        pass

    return logger


logger = setup_logging()


# ----------------------------- HTTP session -----------------------------

def build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/124 Safari/537.36"
            )
        }
    )
    return session


http = build_http_session()


# ----------------------------- common utils -----------------------------

def get_kst_now() -> datetime:
    return datetime.now(KST).replace(tzinfo=None)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if np.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def cache_get(key: str) -> Any:
    with state_lock:
        item = _cache.get(key)
        if not item:
            return None
        expires_at, value = item
        if time.time() >= expires_at:
            _cache.pop(key, None)
            return None
        return value


def cache_set(key: str, value: Any, ttl: int) -> Any:
    with state_lock:
        _cache[key] = (time.time() + ttl, value)
    return value


def atomic_json_write(path: str, data: Dict[str, Any]) -> None:
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
        file.flush()
        os.fsync(file.fileno())
    os.replace(temp_path, path)


def load_portfolio() -> Dict[str, Any]:
    with portfolio_lock:
        if not os.path.exists(PORTFOLIO_FILE):
            return {}
        try:
            with open(PORTFOLIO_FILE, "r", encoding="utf-8") as file:
                data = json.load(file)
                return data if isinstance(data, dict) else {}
        except (OSError, json.JSONDecodeError) as exc:
            logger.error("포트폴리오 불러오기 실패: %s", exc)
            return {}


def save_portfolio(data: Dict[str, Any]) -> None:
    with portfolio_lock:
        try:
            atomic_json_write(PORTFOLIO_FILE, data)
        except OSError as exc:
            logger.error("포트폴리오 저장 실패: %s", exc)



def load_signal_history() -> list[Dict[str, Any]]:
    with state_lock:
        if not os.path.exists(SIGNAL_HISTORY_FILE):
            return []
        try:
            with open(SIGNAL_HISTORY_FILE, "r", encoding="utf-8") as file:
                data = json.load(file)
                return data if isinstance(data, list) else []
        except (OSError, json.JSONDecodeError) as exc:
            logger.error("추천 이력 불러오기 실패: %s", exc)
            return []


def save_signal_history(history: list[Dict[str, Any]]) -> None:
    with state_lock:
        try:
            temp_path = f"{SIGNAL_HISTORY_FILE}.tmp"
            with open(temp_path, "w", encoding="utf-8") as file:
                json.dump(history, file, ensure_ascii=False, indent=2)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temp_path, SIGNAL_HISTORY_FILE)
        except OSError as exc:
            logger.error("추천 이력 저장 실패: %s", exc)


def record_signal(
    code: str,
    name: str,
    signal: "SignalResult",
    market: Dict[str, Any],
    now: datetime,
) -> None:
    history = load_signal_history()
    history.append(
        {
            "date": now.strftime("%Y-%m-%d"),
            "time": now.isoformat(),
            "code": code,
            "name": name,
            "recommend_price": signal.current_price,
            "ai_score": signal.score,
            "grade": signal.grade,
            "v_score": signal.v_score,
            "v_stage": signal.v_stage,
            "target_price": signal.target_price,
            "stop_price": signal.stop_price,
            "market_score": market.get("score"),
            "rsi": round(signal.rsi, 2),
            "volume_ratio": round(signal.volume_ratio, 2),
            "atr_percent": round(signal.atr_percent, 2),
        }
    )
    save_signal_history(history[-5000:])


def get_today_history() -> list[Dict[str, Any]]:
    today = get_kst_now().strftime("%Y-%m-%d")
    return [item for item in load_signal_history() if item.get("date") == today]


def split_telegram_message(message: str) -> list[str]:
    if len(message) <= MAX_TELEGRAM_LENGTH:
        return [message]

    chunks = []
    remaining = message
    while len(remaining) > MAX_TELEGRAM_LENGTH:
        cut = remaining.rfind("\n", 0, MAX_TELEGRAM_LENGTH)
        if cut < 100:
            cut = MAX_TELEGRAM_LENGTH
        chunks.append(remaining[:cut])
        remaining = remaining[cut:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def send_telegram_msg(message: str, chat_id: Optional[str] = None) -> bool:
    target_chat = str(chat_id or CHAT_ID).strip()
    if not TELEGRAM_TOKEN or not target_chat:
        logger.warning("텔레그램 환경변수가 없어 메시지를 전송하지 않았습니다.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    success = True
    for chunk in split_telegram_message(message):
        try:
            response = http.post(
                url,
                json={
                    "chat_id": target_chat,
                    "text": chunk,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=(5, 15),
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.error("텔레그램 전송 실패: %s", exc)
            success = False
    return success


# ----------------------------- market/listing -----------------------------

def get_krx_listing(force: bool = False) -> pd.DataFrame:
    cache_key = "krx_listing"
    if not force:
        cached = cache_get(cache_key)
        if cached is not None:
            return cached.copy()

    listing = fdr.StockListing("KRX")
    required = {"Code", "Name"}
    if not required.issubset(listing.columns):
        raise ValueError("KRX 종목 목록 형식이 예상과 다릅니다.")

    listing = listing.copy()
    listing["Code"] = listing["Code"].astype(str).str.zfill(6)
    if "Marcap" not in listing.columns:
        listing["Marcap"] = 0
    listing["Marcap"] = pd.to_numeric(listing["Marcap"], errors="coerce").fillna(0)
    return cache_set(cache_key, listing, LISTING_CACHE_SECONDS).copy()


def get_name_code_maps() -> Tuple[Dict[str, str], Dict[str, str]]:
    listing = get_krx_listing()
    name_to_code = dict(zip(listing["Name"], listing["Code"]))
    code_to_name = dict(zip(listing["Code"], listing["Name"]))
    return name_to_code, code_to_name


def resolve_stock(query: str) -> Tuple[Optional[str], Optional[str]]:
    query = query.strip()
    try:
        name_to_code, code_to_name = get_name_code_maps()
    except Exception:
        return None, None

    if query.isdigit():
        code = query.zfill(6)
        return code, code_to_name.get(code, code)

    exact = name_to_code.get(query)
    if exact:
        return exact, query

    candidates = [
        (name, code)
        for name, code in name_to_code.items()
        if query.lower() in str(name).lower()
    ]
    if len(candidates) == 1:
        return candidates[0][1], candidates[0][0]
    return None, None


def get_price_data(
    code: str,
    days: int = 220,
    force: bool = False,
) -> pd.DataFrame:
    cache_key = f"price:{code}:{days}"
    if not force:
        cached = cache_get(cache_key)
        if cached is not None:
            return cached.copy()

    end = get_kst_now()
    start = end - timedelta(days=days)
    data = fdr.DataReader(code, start, end)
    if data is None or data.empty:
        raise ValueError(f"{code} 가격 데이터가 없습니다.")

    data = data.copy()
    for column in ("Open", "High", "Low", "Close", "Volume"):
        if column not in data.columns:
            raise ValueError(f"{code} 데이터에 {column} 열이 없습니다.")
        data[column] = pd.to_numeric(data[column], errors="coerce")

    data = data.dropna(subset=["High", "Low", "Close"])
    return cache_set(cache_key, data, DATA_CACHE_SECONDS).copy()


# ----------------------------- indicators -----------------------------

def calculate_wilder_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.where(avg_loss != 0, 100)
    rsi = rsi.where(avg_gain != 0, 0)
    return rsi.clip(0, 100)


def calculate_atr(data: pd.DataFrame, period: int = 14) -> pd.Series:
    previous_close = data["Close"].shift(1)
    true_range = pd.concat(
        [
            data["High"] - data["Low"],
            (data["High"] - previous_close).abs(),
            (data["Low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def calculate_adx(data: pd.DataFrame, period: int = 14) -> Tuple[pd.Series, pd.Series, pd.Series]:
    high_diff = data["High"].diff()
    low_diff = -data["Low"].diff()

    plus_dm = pd.Series(
        np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0.0),
        index=data.index,
    )
    minus_dm = pd.Series(
        np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0.0),
        index=data.index,
    )

    atr = calculate_atr(data, period)
    plus_di = 100 * plus_dm.ewm(
        alpha=1 / period, adjust=False, min_periods=period
    ).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(
        alpha=1 / period, adjust=False, min_periods=period
    ).mean() / atr.replace(0, np.nan)

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    return adx, plus_di, minus_di


def add_indicators(data: pd.DataFrame) -> pd.DataFrame:
    df = data.copy()
    close = df["Close"]

    df["RSI"] = calculate_wilder_rsi(close, 14)
    df["EMA20"] = close.ewm(span=20, adjust=False).mean()
    df["EMA60"] = close.ewm(span=60, adjust=False).mean()
    df["EMA120"] = close.ewm(span=120, adjust=False).mean()

    df["MACD"] = close.ewm(span=12, adjust=False).mean() - close.ewm(
        span=26, adjust=False
    ).mean()
    df["MACD_SIGNAL"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["MACD"] - df["MACD_SIGNAL"]

    df["ATR"] = calculate_atr(df, 14)
    adx, plus_di, minus_di = calculate_adx(df, 14)
    df["ADX"] = adx
    df["PLUS_DI"] = plus_di
    df["MINUS_DI"] = minus_di

    df["BB_MID"] = close.rolling(20).mean()
    bb_std = close.rolling(20).std(ddof=0)
    df["BB_UPPER"] = df["BB_MID"] + 2 * bb_std
    df["BB_LOWER"] = df["BB_MID"] - 2 * bb_std
    df["BB_WIDTH"] = (
        (df["BB_UPPER"] - df["BB_LOWER"]) / df["BB_MID"].replace(0, np.nan) * 100
    )

    df["VOL_MA20"] = df["Volume"].rolling(20).mean()
    df["HIGH20"] = df["High"].rolling(20).max()
    df["LOW20"] = df["Low"].rolling(20).min()
    df["RETURN_5D"] = close.pct_change(5) * 100
    df["RETURN_20D"] = close.pct_change(20) * 100
    return df


def get_estimated_daily_volume(current_volume: float, now: datetime) -> float:
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

    if now <= market_open or now >= market_close:
        return current_volume

    elapsed_minutes = (now - market_open).total_seconds() / 60
    elapsed_minutes = max(elapsed_minutes, 15)
    estimated = current_volume * (390 / elapsed_minutes)
    return min(estimated, current_volume * 26)


# ----------------------------- investor/market -----------------------------

def check_investor_buying(code: str) -> Optional[bool]:
    cache_key = f"investor:{code}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    url = f"https://finance.naver.com/item/frgn.naver?code={code}"
    try:
        response = http.get(url, timeout=(5, 10))
        response.raise_for_status()
        tables = pd.read_html(response.text, encoding="euc-kr", match="순매매량")
        if not tables:
            return cache_set(cache_key, None, INVESTOR_CACHE_SECONDS)

        df = tables[0]
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(-1)
        if "날짜" not in df.columns:
            return cache_set(cache_key, None, INVESTOR_CACHE_SECONDS)

        df = df.dropna(subset=["날짜"]).head(3)
        institution = next((c for c in df.columns if "기관" in str(c)), None)
        foreign = next((c for c in df.columns if "외국인" in str(c)), None)
        retail = next((c for c in df.columns if "개인" in str(c)), None)
        if not all([institution, foreign, retail]):
            return cache_set(cache_key, None, INVESTOR_CACHE_SECONDS)

        def parse_sum(column: str) -> int:
            cleaned = (
                df[column]
                .astype(str)
                .str.replace(r"[^0-9\-]", "", regex=True)
                .replace("", "0")
            )
            return pd.to_numeric(cleaned, errors="coerce").fillna(0).astype(int).sum()

        result = parse_sum(retail) < 0 and (
            parse_sum(institution) > 0 or parse_sum(foreign) > 0
        )
        return cache_set(cache_key, bool(result), INVESTOR_CACHE_SECONDS)
    except Exception as exc:
        logger.info("[%s] 수급 조회 실패: %s", code, exc)
        return cache_set(cache_key, None, 300)


def calculate_market_score(force: bool = False) -> Dict[str, Any]:
    cache_key = "market_score"
    if not force:
        cached = cache_get(cache_key)
        if cached is not None:
            return cached

    score = 50.0
    details = []
    index_results = {}

    for label, code in (("KOSPI", "KS11"), ("KOSDAQ", "KQ11")):
        try:
            df = get_price_data(code, 220, force=force)
            df = add_indicators(df)
            row = df.iloc[-1]

            local_score = 50.0
            if row["Close"] >= row["EMA20"]:
                local_score += 12
            else:
                local_score -= 12
            if row["EMA20"] >= row["EMA60"]:
                local_score += 10
            else:
                local_score -= 10
            if row["MACD_HIST"] > 0:
                local_score += 8
            else:
                local_score -= 8
            if row["PLUS_DI"] > row["MINUS_DI"]:
                local_score += 8
            else:
                local_score -= 8
            if row["RSI"] < 30:
                local_score -= 4
            elif 45 <= row["RSI"] <= 65:
                local_score += 4

            local_score = max(0, min(100, local_score))
            index_results[label] = round(local_score, 1)
            details.append(f"{label} {local_score:.0f}점")
        except Exception as exc:
            logger.warning("%s 시장점수 계산 실패: %s", label, exc)
            index_results[label] = 50.0
            details.append(f"{label} 확인불가")

    if index_results:
        score = sum(index_results.values()) / len(index_results)

    result = {
        "score": round(score, 1),
        "good": score >= 50,
        "detail": " · ".join(details),
        "indexes": index_results,
    }
    runtime_state["market_score"] = result["score"]
    return cache_set(cache_key, result, 600)


# ----------------------------- AI score -----------------------------


@dataclass
class SignalResult:
    score: float
    grade: str
    reasons: list[str]
    warnings: list[str]
    current_price: int
    target_price: int
    stop_price: int
    upside: float
    rsi: float
    volume_ratio: float
    atr_percent: float
    adx: float
    macd_hist: float
    investor_ok: Optional[bool]
    v_score: float
    v_stage: str
    v_reasons: list[str]


def calculate_v_reversal_score(df: pd.DataFrame) -> Tuple[float, str, list[str]]:
    if len(df) < 22:
        return 0.0, "데이터 부족", []

    row = df.iloc[-1]
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]

    close = safe_float(row["Close"])
    open_price = safe_float(row["Open"], close)
    high = safe_float(row["High"], close)
    low = safe_float(row["Low"], close)
    rsi = safe_float(row["RSI"], 50)
    prev_rsi = safe_float(prev["RSI"], 50)
    prev2_rsi = safe_float(prev2["RSI"], 50)
    macd_hist = safe_float(row["MACD_HIST"])
    prev_hist = safe_float(prev["MACD_HIST"])
    ema20 = safe_float(row["EMA20"], close)
    prev_ema20 = safe_float(prev["EMA20"], close)
    bb_lower = safe_float(row["BB_LOWER"])
    avg_volume = safe_float(prev["VOL_MA20"], 1)
    volume_ratio = safe_float(row["Volume"]) / max(avg_volume, 1)

    score = 0.0
    reasons: list[str] = []

    if prev_rsi < 35 <= rsi:
        score += 2.0
        reasons.append("RSI 과매도권 탈출")
    elif rsi > prev_rsi > prev2_rsi and prev2_rsi <= 40:
        score += 1.3
        reasons.append("RSI 연속 회복")

    if macd_hist > prev_hist and prev_hist <= 0:
        score += 1.8
        reasons.append("MACD 하락 둔화·반전")
    elif macd_hist > prev_hist:
        score += 0.8

    if volume_ratio >= 1.5:
        score += 1.5
        reasons.append("거래량 150% 이상")
    elif volume_ratio >= 1.15:
        score += 0.8
        reasons.append("거래량 증가")

    candle_range = max(high - low, 1)
    lower_wick = min(open_price, close) - low
    recovery = (close - low) / candle_range
    if lower_wick / candle_range >= 0.35 and close >= open_price:
        score += 1.5
        reasons.append("긴 아랫꼬리·양봉 회복")
    elif recovery >= 0.65:
        score += 0.8
        reasons.append("저점 대비 강한 회복")

    prev_close = safe_float(prev["Close"])
    if prev_close < prev_ema20 and close >= ema20:
        score += 1.5
        reasons.append("EMA20 재돌파")
    elif close >= ema20 * 0.99:
        score += 0.5

    prev_bb_lower = safe_float(prev["BB_LOWER"])
    if prev_bb_lower > 0 and prev_close < prev_bb_lower and close >= bb_lower:
        score += 1.2
        reasons.append("볼린저 하단 복귀")
    elif bb_lower > 0 and close <= bb_lower * 1.02:
        score += 0.5

    score = round(min(10.0, max(0.0, score)), 1)
    if score >= 8:
        stage = "강한 초기 반등"
    elif score >= 6:
        stage = "초기 반등"
    elif score >= 4:
        stage = "반등 관찰"
    else:
        stage = "확인 필요"

    return score, stage, reasons

def calculate_ai_score(
    df: pd.DataFrame,
    market_score: float,
    investor_ok: Optional[bool],
    now: Optional[datetime] = None,
) -> SignalResult:
    now = now or get_kst_now()
    row = df.iloc[-1]
    previous = df.iloc[-2]

    close = safe_float(row["Close"])
    atr = safe_float(row["ATR"])
    rsi = safe_float(row["RSI"], 50)
    adx = safe_float(row["ADX"])
    macd_hist = safe_float(row["MACD_HIST"])
    previous_macd_hist = safe_float(previous["MACD_HIST"])
    avg_volume = safe_float(previous["VOL_MA20"], 1)
    estimated_volume = get_estimated_daily_volume(safe_float(row["Volume"]), now)
    volume_ratio = estimated_volume / max(avg_volume, 1) * 100
    atr_percent = atr / close * 100 if close > 0 else 0

    score = 0.0
    reasons: list[str] = []
    warnings: list[str] = []

    # 1) RSI: 15점
    if 28 <= rsi <= 42:
        score += 15
        reasons.append(f"RSI {rsi:.1f} 저점 반등 구간")
    elif 42 < rsi <= 55:
        score += 11
        reasons.append(f"RSI {rsi:.1f} 회복 구간")
    elif 20 <= rsi < 28:
        score += 7
        warnings.append("RSI가 지나치게 낮아 추가 하락 가능")
    elif 55 < rsi <= 68:
        score += 6
    elif rsi > 75:
        score -= 8
        warnings.append("RSI 과열")

    # 2) 거래량: 15점
    if 120 <= volume_ratio <= 350:
        score += 15
        reasons.append(f"예상 거래량 {volume_ratio:.0f}%")
    elif 90 <= volume_ratio < 120:
        score += 10
    elif 350 < volume_ratio <= 600:
        score += 9
        warnings.append("거래량 과열 주의")
    elif volume_ratio < 60:
        score -= 5

    # 3) EMA 구조: 20점
    ema20 = safe_float(row["EMA20"])
    ema60 = safe_float(row["EMA60"])
    ema120 = safe_float(row["EMA120"])

    if close >= ema20:
        score += 8
        reasons.append("현재가 EMA20 위")
    elif close >= ema20 * 0.97:
        score += 4
    else:
        warnings.append("EMA20 아래")

    if ema20 >= ema60:
        score += 7
        reasons.append("EMA20 ≥ EMA60")
    if ema60 >= ema120:
        score += 5
        reasons.append("중장기 추세 우호적")

    # 4) MACD: 15점
    if macd_hist > 0 and macd_hist >= previous_macd_hist:
        score += 15
        reasons.append("MACD 상승 모멘텀")
    elif macd_hist > previous_macd_hist:
        score += 9
        reasons.append("MACD 하락 둔화")
    elif macd_hist > 0:
        score += 7
    else:
        warnings.append("MACD 모멘텀 약함")

    # 5) ADX/DI: 10점
    plus_di = safe_float(row["PLUS_DI"])
    minus_di = safe_float(row["MINUS_DI"])
    if adx >= 20 and plus_di > minus_di:
        score += 10
        reasons.append(f"ADX {adx:.1f} 상승추세")
    elif plus_di > minus_di:
        score += 6
    elif adx >= 30 and minus_di > plus_di:
        score -= 5
        warnings.append("강한 하락추세")

    # 6) Bollinger: 10점
    bb_lower = safe_float(row["BB_LOWER"])
    bb_mid = safe_float(row["BB_MID"])
    if bb_lower > 0 and bb_lower <= close <= bb_mid:
        score += 10
        reasons.append("볼린저 하단 반등 구간")
    elif close >= bb_mid:
        score += 6
    if close > safe_float(row["BB_UPPER"]):
        score -= 5
        warnings.append("볼린저 상단 과열")

    # 7) 시장: 10점
    score += max(0, min(10, market_score / 10))
    if market_score >= 60:
        reasons.append(f"시장강도 {market_score:.0f}점")
    elif market_score < 40:
        warnings.append("시장 추세 약세")

    # 8) 수급: 5점
    if investor_ok is True:
        score += 5
        reasons.append("외국인·기관 수급 우호적")
    elif investor_ok is False:
        warnings.append("외국인·기관 수급 미확인")

    # 위험 감점
    if atr_percent > 7:
        score -= 8
        warnings.append(f"ATR 변동성 {atr_percent:.1f}% 과다")
    elif atr_percent > 5:
        score -= 4

    return_5d = safe_float(row["RETURN_5D"])
    if return_5d > 18:
        score -= 8
        warnings.append("최근 5일 급등 후 추격 위험")
    elif return_5d < -18:
        score -= 5
        warnings.append("최근 5일 낙폭 과대")

    score = round(max(0, min(100, score)), 1)
    v_score, v_stage, v_reasons = calculate_v_reversal_score(df)

    if score >= 85:
        grade = "S"
    elif score >= 78:
        grade = "A"
    elif score >= 70:
        grade = "B"
    elif score >= 60:
        grade = "C"
    else:
        grade = "관망"

    recent_high = safe_float(row["HIGH20"], close)
    target_by_atr = close + max(atr * 2.2, close * 0.04)
    target_price = int(min(recent_high if recent_high > close else target_by_atr, target_by_atr))
    target_price = max(target_price, int(close * 1.03))

    stop_by_atr = close - max(atr * 1.5, close * 0.025)
    stop_price = max(1, int(stop_by_atr))
    upside = (target_price - close) / close * 100 if close else 0

    return SignalResult(
        score=score,
        grade=grade,
        reasons=reasons,
        warnings=warnings,
        current_price=int(close),
        target_price=target_price,
        stop_price=stop_price,
        upside=upside,
        rsi=rsi,
        volume_ratio=volume_ratio,
        atr_percent=atr_percent,
        adx=adx,
        macd_hist=macd_hist,
        investor_ok=investor_ok,
        v_score=v_score,
        v_stage=v_stage,
        v_reasons=v_reasons,
    )


def analyze_stock(
    code: str,
    name: str,
    force: bool = False,
    include_investor: bool = True,
) -> Tuple[pd.DataFrame, SignalResult, Dict[str, Any]]:
    df = add_indicators(get_price_data(code, 260, force=force))
    if len(df) < 130:
        raise ValueError("지표 계산에 필요한 데이터가 부족합니다.")

    market = calculate_market_score(force=force)
    investor_ok = check_investor_buying(code) if include_investor else None
    signal = calculate_ai_score(df, market["score"], investor_ok)
    return df, signal, market



def format_signal_message(
    code: str,
    name: str,
    signal: SignalResult,
    market: Dict[str, Any],
) -> str:
    reasons = "\n".join(f"• {html.escape(item)}" for item in signal.reasons[:6])
    warnings = "\n".join(f"• {html.escape(item)}" for item in signal.warnings[:4])
    v_reasons = "\n".join(f"• {html.escape(item)}" for item in signal.v_reasons[:5])

    message = (
        f"🚨 <b>[뽕실로봇 V{APP_VERSION} 매수 후보]</b>\n"
        f"🧠 <b>AI 점수 {signal.score:.1f}/100 · {signal.grade}등급</b>\n"
        f"📈 <b>V반등 {signal.v_score:.1f}/10 · {html.escape(signal.v_stage)}</b>\n\n"
        f"📌 <b>{html.escape(name)}</b> ({code})\n"
        f"💰 현재가: <b>{signal.current_price:,}원</b>\n"
        f"🎯 1차 목표가: <b>{signal.target_price:,}원</b> ({signal.upside:+.1f}%)\n"
        f"🛑 참고 손절가: <b>{signal.stop_price:,}원</b>\n\n"
        f"📊 RSI {signal.rsi:.1f} · ADX {signal.adx:.1f}\n"
        f"📈 예상 거래량 {signal.volume_ratio:.0f}% · ATR {signal.atr_percent:.1f}%\n"
        f"🌏 시장강도 {market['score']:.1f}점 ({html.escape(market['detail'])})\n\n"
        f"<b>[AI 가점 근거]</b>\n{reasons or '• 뚜렷한 가점 근거 없음'}\n\n"
        f"<b>[V반등 근거]</b>\n{v_reasons or '• 뚜렷한 V반등 근거 없음'}"
    )
    if warnings:
        message += f"\n\n⚠️ <b>[주의]</b>\n{warnings}"

    message += (
        f"\n\n💡 등록 명령어\n"
        f"• <code>/매수 {html.escape(name)} {signal.current_price} 단타</code>\n"
        f"• <code>/매수 {html.escape(name)} {signal.current_price} 스윙</code>\n\n"
        f"<i>AI·V반등 점수는 참고값이며 수익을 보장하지 않습니다.</i>"
    )
    return message

# ----------------------------- portfolio -----------------------------


def get_trade_rules(trade_type: str, atr_percent: float) -> Dict[str, float]:
    atr_percent = max(0.8, min(atr_percent, 8.0))
    if trade_type == "스윙":
        stop_abs = min(8.0, max(4.0, atr_percent * 1.35))
        return {
            "trigger": max(4.0, atr_percent * 1.35),
            "trailing": max(2.0, atr_percent * 0.75),
            "stop": -stop_abs,
        }

    stop_abs = min(5.5, max(2.5, atr_percent * 1.05))
    return {
        "trigger": max(2.0, atr_percent * 0.85),
        "trailing": max(1.0, atr_percent * 0.50),
        "stop": -stop_abs,
    }

def monitor_portfolio() -> None:
    portfolio = load_portfolio()
    if not portfolio:
        return

    changed = False
    now = get_kst_now()

    for name, info in list(portfolio.items()):
        code = str(info.get("code", "")).zfill(6)
        buy_price = safe_float(info.get("price"))
        if not code or code == "000000" or buy_price <= 0:
            continue

        try:
            df = add_indicators(get_price_data(code, 80))
            row = df.iloc[-1]
            current_price = int(row["Close"])
            atr_percent = safe_float(row["ATR"]) / current_price * 100
            trade_type = info.get("type", "단타")
            rules = get_trade_rules(trade_type, atr_percent)

            max_price = safe_float(info.get("max_price"), buy_price)
            trailing_active = bool(info.get("trailing_active", False))
            profit_rate = (current_price - buy_price) / buy_price * 100

            if current_price > max_price:
                max_price = current_price
                info["max_price"] = current_price
                changed = True

            alert_key = None
            signal_text = None

            if not trailing_active and profit_rate >= rules["trigger"]:
                info["trailing_active"] = True
                trailing_active = True
                changed = True
                alert_key = "trailing_start"
                signal_text = (
                    f"🚀 목표수익률 +{rules['trigger']:.1f}% 도달\n"
                    f"최고가 대비 -{rules['trailing']:.1f}% 하락 시 익절 알림"
                )
            elif not trailing_active and profit_rate <= rules["stop"]:
                alert_key = "stop"
                signal_text = (
                    f"🛑 ATR 기반 손절 기준 {rules['stop']:.1f}% 도달\n"
                    f"손실 확대 방지를 위해 대응 여부를 확인하세요."
                )
            elif trailing_active:
                drop_rate = (max_price - current_price) / max_price * 100
                if drop_rate >= rules["trailing"]:
                    alert_key = "take_profit"
                    signal_text = (
                        f"🎯 최고가 {int(max_price):,}원 대비 -{drop_rate:.1f}% 하락\n"
                        f"트레일링 익절 조건에 도달했습니다."
                    )

            if alert_key:
                cooldown_key = f"alert:{alert_key}"
                last_alert_text = info.get(cooldown_key)
                last_alert = (
                    datetime.fromisoformat(last_alert_text)
                    if last_alert_text
                    else None
                )
                if not last_alert or (now - last_alert).total_seconds() >= 1800:
                    send_telegram_msg(
                        f"🚨 <b>[{html.escape(name)} {trade_type} 매도관리]</b>\n\n"
                        f"{signal_text}\n\n"
                        f"매수가: {int(buy_price):,}원\n"
                        f"현재가: {current_price:,}원\n"
                        f"수익률: <b>{profit_rate:+.2f}%</b>\n\n"
                        f"매도 후 <code>/매도완료 {html.escape(name)}</code>"
                    )
                    info[cooldown_key] = now.isoformat()
                    changed = True

        except Exception as exc:
            logger.warning("[%s] 보유종목 감시 실패: %s", name, exc)

    if changed:
        save_portfolio(portfolio)


# ----------------------------- backtest -----------------------------

def run_backtest(code: str, name: str) -> str:
    try:
        end = get_kst_now()
        raw = fdr.DataReader(code, end - timedelta(days=760), end)
        df = add_indicators(raw).dropna().copy()
        if len(df) < 180:
            return "⚠️ 백테스트 데이터가 부족합니다."

        market_score = 55.0
        trades = []
        equity = 1.0
        equity_curve = [equity]
        position = None

        for index in range(121, len(df) - 1):
            history = df.iloc[: index + 1]
            row = history.iloc[-1]
            next_open = safe_float(df.iloc[index + 1]["Open"], row["Close"])

            if position is None:
                signal = calculate_ai_score(
                    history,
                    market_score,
                    investor_ok=None,
                    now=df.index[index].to_pydatetime()
                    if hasattr(df.index[index], "to_pydatetime")
                    else get_kst_now(),
                )
                if signal.score >= MIN_AI_SCORE:
                    position = {
                        "buy": next_open,
                        "max": next_open,
                        "days": 0,
                        "atr_percent": signal.atr_percent,
                    }
            else:
                current = safe_float(row["Close"])
                position["days"] += 1
                position["max"] = max(position["max"], current)
                profit = (current - position["buy"]) / position["buy"] * 100
                drop = (position["max"] - current) / position["max"] * 100
                rules = get_trade_rules("단타", position["atr_percent"])

                exit_reason = None
                if profit <= rules["stop"]:
                    exit_reason = "손절"
                elif profit >= rules["trigger"] and drop >= rules["trailing"]:
                    exit_reason = "트레일링"
                elif position["days"] >= 15:
                    exit_reason = "기간청산"

                if exit_reason:
                    net_return = profit - 0.30
                    trades.append(net_return)
                    equity *= 1 + net_return / 100
                    equity_curve.append(equity)
                    position = None

        if not trades:
            return (
                f"📊 <b>[{html.escape(name)}] V6 백테스트</b>\n"
                f"조건에 맞는 완료 거래가 없습니다."
            )

        wins = [value for value in trades if value > 0]
        win_rate = len(wins) / len(trades) * 100
        average = float(np.mean(trades))
        cumulative = (equity - 1) * 100
        curve = np.array(equity_curve)
        peaks = np.maximum.accumulate(curve)
        mdd = float(np.min((curve - peaks) / peaks) * 100)
        profit_factor = (
            sum(wins) / abs(sum(value for value in trades if value < 0))
            if any(value < 0 for value in trades)
            else float("inf")
        )

        return (
            f"📊 <b>[{html.escape(name)}] V6 백테스트</b>\n"
            f"<i>약 2년 · 다음 날 시가 진입 · 거래비용 0.30% 반영</i>\n\n"
            f"총 거래: {len(trades)}회\n"
            f"승률: <b>{win_rate:.1f}%</b>\n"
            f"평균 수익률: <b>{average:+.2f}%</b>\n"
            f"누적 수익률: <b>{cumulative:+.2f}%</b>\n"
            f"MDD: <b>{mdd:.2f}%</b>\n"
            f"손익비 지표: <b>{profit_factor:.2f}</b>\n\n"
            f"<i>과거 결과는 미래 수익을 보장하지 않습니다.</i>"
        )
    except Exception as exc:
        logger.exception("백테스트 오류")
        return f"⚠️ 백테스트 중 오류: {html.escape(str(exc))}"


# ----------------------------- scanner -----------------------------

def should_scan_now(now: datetime) -> bool:
    return now.weekday() < 5 and (
        (9 <= now.hour < 16) or (8 <= now.hour < 9) or (16 <= now.hour < 20)
    )


def scan_stocks(force: bool = False, requested_chat_id: Optional[str] = None) -> None:
    global last_reset_date

    if not scan_lock.acquire(blocking=False):
        if requested_chat_id:
            send_telegram_msg("⏳ 이미 종목 스캔이 진행 중입니다.", requested_chat_id)
        return

    try:
        runtime_state["scanner_running"] = True
        runtime_state["last_scan_error"] = None

        now = get_kst_now()
        today = now.strftime("%Y-%m-%d")
        with state_lock:
            if last_reset_date != today:
                sent_signals_today.clear()
                runtime_state["today_scan_runs"] = 0
                runtime_state["today_analyzed_total"] = 0
                runtime_state["today_signal_total"] = 0
                runtime_state["today_best_score"] = None
                runtime_state["today_best_v_score"] = None
                last_reset_date = today

        market = calculate_market_score(force=force)
        listing = get_krx_listing(force=force)
        top_stocks = (
            listing[listing["Marcap"] > 0]
            .sort_values("Marcap", ascending=False)
            .head(SCAN_TOP_N)
        )

        scanned = 0
        signal_count = 0
        candidates = []

        for _, row in top_stocks.iterrows():
            code = str(row["Code"]).zfill(6)
            name = str(row["Name"])

            with state_lock:
                previous = sent_signals_today.get(code)
            if previous and not force:
                elapsed = (now - previous["time"]).total_seconds()
                if elapsed < SIGNAL_COOLDOWN_SECONDS:
                    continue

            try:
                # 먼저 빠른 기술점수 산정 후 상위 후보에만 네이버 수급 조회
                df = add_indicators(get_price_data(code, 260, force=force))
                if len(df) < 130:
                    continue

                preliminary = calculate_ai_score(
                    df,
                    market["score"],
                    investor_ok=None,
                    now=now,
                )
                scanned += 1

                if preliminary.score < MIN_AI_SCORE - 5:
                    continue

                investor_ok = check_investor_buying(code)
                signal = calculate_ai_score(
                    df,
                    market["score"],
                    investor_ok=investor_ok,
                    now=now,
                )

                if signal.score >= MIN_AI_SCORE:
                    candidates.append((signal.score, code, name, signal))

            except Exception as exc:
                logger.info("[%s %s] 분석 제외: %s", name, code, exc)

            time.sleep(0.05)

        candidates.sort(key=lambda item: item[0], reverse=True)

        # 한 번의 스캔에서 최대 10개만 전송해 알림 폭주 방지
        for _, code, name, signal in candidates[:10]:
            send_telegram_msg(
                format_signal_message(code, name, signal, market),
                requested_chat_id,
            )
            with state_lock:
                sent_signals_today[code] = {
                    "name": name,
                    "time": now,
                    "score": signal.score,
                    "v_score": signal.v_score,
                    "price": signal.current_price,
                    "target_price": signal.target_price,
                    "stop_price": signal.stop_price,
                    "code": code,
                }
            record_signal(code, name, signal, market, now)
            signal_count += 1
            time.sleep(0.6)

        runtime_state["last_scan_at"] = now.isoformat()
        runtime_state["last_scan_count"] = scanned
        runtime_state["last_signal_count"] = signal_count
        runtime_state["today_scan_runs"] += 1
        runtime_state["today_analyzed_total"] += scanned
        runtime_state["today_signal_total"] += signal_count
        if candidates:
            best = candidates[0][3]
            runtime_state["today_best_score"] = max(
                safe_float(runtime_state.get("today_best_score")), best.score
            )
            runtime_state["today_best_v_score"] = max(
                safe_float(runtime_state.get("today_best_v_score")), best.v_score
            )

        if requested_chat_id:
            send_telegram_msg(
                f"✅ 강제 스캔 완료\n"
                f"분석 {scanned}종목 · 기준 {MIN_AI_SCORE:.0f}점 이상 "
                f"{signal_count}종목 전송",
                requested_chat_id,
            )

        logger.info(
            "스캔 완료: 분석=%s 후보=%s 시장점수=%s",
            scanned,
            signal_count,
            market["score"],
        )

    except Exception as exc:
        runtime_state["last_scan_error"] = str(exc)
        runtime_state["last_error_at"] = get_kst_now().isoformat()
        logger.exception("전체 스캔 실패")
        if requested_chat_id:
            send_telegram_msg(
                f"🚨 강제 스캔 실패: {html.escape(str(exc))}",
                requested_chat_id,
            )
    finally:
        runtime_state["scanner_running"] = False
        scan_lock.release()


# ----------------------------- Telegram commands -----------------------------

def command_help() -> str:
    return (
        "🤖 <b>[뽕실로봇 V6 명령어]</b>\n\n"
        "• <code>/매수 종목명 단가 단타</code>\n"
        "• <code>/매수 종목명 단가 스윙</code>\n"
        "• <code>/수정 종목명 단가 단타</code>\n"
        "• <code>/매도완료 종목명</code>\n"
        "• <code>/목록</code>\n"
        "• <code>/점수 종목명</code>\n"
        "• <code>/백테스트 종목명</code>\n"
        "• <code>/시장</code>\n"
        "• <code>/상태</code>\n"
        "• <code>/강제스캔</code>\n"
        "• <code>/도움말</code>"
    )


def handle_command(text: str, chat_id: str) -> None:
    parts = text.split()
    cmd = parts[0].split("@")[0]
    portfolio = load_portfolio()

    if cmd in ("/매수", "/수정"):
        if len(parts) < 3:
            send_telegram_msg("⚠️ 사용법: /매수 [종목명] [단가] [단타/스윙]", chat_id)
            return

        name_query = parts[1]
        try:
            price = int(parts[2].replace(",", ""))
        except ValueError:
            send_telegram_msg("⚠️ 매수단가는 숫자로 입력하세요.", chat_id)
            return

        trade_type = parts[3] if len(parts) >= 4 else "단타"
        if trade_type not in ("단타", "스윙"):
            trade_type = "단타"

        code, resolved_name = resolve_stock(name_query)
        if not code or not resolved_name:
            send_telegram_msg("⚠️ 종목을 정확히 찾지 못했습니다.", chat_id)
            return

        portfolio[resolved_name] = {
            "code": code,
            "price": price,
            "type": trade_type,
            "max_price": price,
            "trailing_active": False,
            "registered_at": get_kst_now().isoformat(),
        }
        save_portfolio(portfolio)
        send_telegram_msg(
            f"✅ <b>{html.escape(resolved_name)}</b> 등록 완료\n"
            f"종목코드: {code}\n"
            f"매수단가: {price:,}원\n"
            f"모드: {trade_type}",
            chat_id,
        )

    elif cmd == "/매도완료":
        query = " ".join(parts[1:]).strip()
        if query in portfolio:
            del portfolio[query]
            save_portfolio(portfolio)
            send_telegram_msg(f"🗑️ {html.escape(query)} 감시를 종료했습니다.", chat_id)
        else:
            send_telegram_msg("⚠️ 감시 목록에서 해당 종목을 찾지 못했습니다.", chat_id)

    elif cmd == "/목록":
        if not portfolio:
            send_telegram_msg("📂 현재 감시 중인 종목이 없습니다.", chat_id)
            return

        lines = ["📂 <b>[보유·감시 목록]</b>"]
        for name, info in portfolio.items():
            code = str(info.get("code", "")).zfill(6)
            buy_price = safe_float(info.get("price"))
            trade_type = info.get("type", "단타")
            status = "트레일링 가동" if info.get("trailing_active") else "일반 감시"

            try:
                df = add_indicators(get_price_data(code, 80, force=True))
                row = df.iloc[-1]
                current_price = int(row["Close"])
                atr_percent = safe_float(row["ATR"]) / max(current_price, 1) * 100
                rules = get_trade_rules(trade_type, atr_percent)
                profit_rate = (current_price - buy_price) / buy_price * 100
                stop_price = int(buy_price * (1 + rules["stop"] / 100))
                trigger_price = int(buy_price * (1 + rules["trigger"] / 100))
                max_price = int(safe_float(info.get("max_price"), buy_price))

                lines.extend(
                    [
                        "",
                        f"📌 <b>{html.escape(name)}</b> ({trade_type})",
                        f"매수가: {int(buy_price):,}원",
                        f"현재가: {current_price:,}원",
                        f"수익률: <b>{profit_rate:+.2f}%</b>",
                        f"손절가: {stop_price:,}원 ({rules['stop']:.1f}%)",
                        f"트레일링 시작가: {trigger_price:,}원 (+{rules['trigger']:.1f}%)",
                        f"최고가: {max_price:,}원",
                        f"상태: {status}",
                    ]
                )
            except Exception:
                lines.extend(
                    [
                        "",
                        f"📌 <b>{html.escape(name)}</b> ({trade_type})",
                        f"매수가: {int(buy_price):,}원",
                        "현재가: 확인 불가",
                        f"상태: {status}",
                    ]
                )

        send_telegram_msg("\n".join(lines), chat_id)

    elif cmd == "/점수":
        if len(parts) < 2:
            send_telegram_msg("⚠️ 사용법: /점수 [종목명]", chat_id)
            return
        query = " ".join(parts[1:])
        code, name = resolve_stock(query)
        if not code or not name:
            send_telegram_msg("⚠️ 종목을 찾지 못했습니다.", chat_id)
            return

        send_telegram_msg(f"⏳ {html.escape(name)} 분석 중...", chat_id)
        try:
            _, signal, market = analyze_stock(code, name, force=True)
            send_telegram_msg(format_signal_message(code, name, signal, market), chat_id)
        except Exception as exc:
            send_telegram_msg(f"⚠️ 분석 실패: {html.escape(str(exc))}", chat_id)

    elif cmd == "/백테스트":
        if len(parts) < 2:
            send_telegram_msg("⚠️ 사용법: /백테스트 [종목명]", chat_id)
            return
        query = " ".join(parts[1:])
        code, name = resolve_stock(query)
        if not code or not name:
            send_telegram_msg("⚠️ 종목을 찾지 못했습니다.", chat_id)
            return
        send_telegram_msg(f"⏳ {html.escape(name)} 백테스트 중...", chat_id)
        send_telegram_msg(run_backtest(code, name), chat_id)

    elif cmd == "/시장":
        market = calculate_market_score(force=True)
        send_telegram_msg(
            f"🌏 <b>[시장강도]</b>\n\n"
            f"종합점수: <b>{market['score']:.1f}/100</b>\n"
            f"{html.escape(market['detail'])}\n\n"
            f"판정: {'매수환경 우호적' if market['good'] else '보수적 대응 권장'}",
            chat_id,
        )

    elif cmd == "/상태":
        started = datetime.fromisoformat(runtime_state["started_at"]).replace(tzinfo=None)
        uptime = get_kst_now() - started
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes = remainder // 60
        today_history = get_today_history()

        send_telegram_msg(
            f"🛠️ <b>[뽕실로봇 V{APP_VERSION} 상태]</b>\n\n"
            f"스캐너: {'가동 중' if runtime_state['scanner_running'] else '대기'}\n"
            f"가동시간: {hours}시간 {minutes}분\n"
            f"마지막 스캔: {runtime_state['last_scan_at'] or '없음'}\n"
            f"최근 분석: {runtime_state['last_scan_count']}종목\n"
            f"최근 시그널: {runtime_state['last_signal_count']}종목\n\n"
            f"오늘 스캔: {runtime_state['today_scan_runs']}회\n"
            f"오늘 누적 분석: {runtime_state['today_analyzed_total']}종목\n"
            f"오늘 누적 추천: {len(today_history)}건\n"
            f"오늘 최고 AI: {runtime_state['today_best_score'] if runtime_state['today_best_score'] is not None else '없음'}\n"
            f"오늘 최고 V반등: {runtime_state['today_best_v_score'] if runtime_state['today_best_v_score'] is not None else '없음'}\n"
            f"감시 종목: {len(portfolio)}개\n"
            f"시장점수: {runtime_state['market_score'] or '미계산'}\n"
            f"마지막 오류: {html.escape(str(runtime_state['last_scan_error'] or '없음'))}",
            chat_id,
        )

    elif cmd == "/강제스캔":
        send_telegram_msg("🔍 시가총액 상위 종목 강제 스캔을 시작합니다.", chat_id)
        threading.Thread(
            target=scan_stocks,
            kwargs={"force": True, "requested_chat_id": chat_id},
            daemon=True,
            name="manual-scan",
        ).start()

    elif cmd in ("/도움말", "/start"):
        send_telegram_msg(command_help(), chat_id)


def process_telegram_commands() -> None:
    global last_update_id

    if not TELEGRAM_TOKEN:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    try:
        response = http.get(
            url,
            params={"offset": last_update_id, "timeout": 2},
            timeout=(5, 8),
        )
        response.raise_for_status()
        payload = response.json()

        for item in payload.get("result", []):
            last_update_id = item["update_id"] + 1
            message = item.get("message") or item.get("edited_message") or {}
            text = str(message.get("text", "")).strip()
            chat_id = str((message.get("chat") or {}).get("id", CHAT_ID))
            if text.startswith("/"):
                handle_command(text, chat_id)
    except requests.RequestException as exc:
        logger.debug("텔레그램 명령 조회 실패: %s", exc)
    except Exception:
        logger.exception("텔레그램 명령 처리 오류")


# ----------------------------- reports -----------------------------

def send_morning_briefing() -> None:
    tickers = {
        "나스닥": "^IXIC",
        "필라델피아 반도체": "^SOX",
        "엔비디아": "NVDA",
        "테슬라": "TSLA",
        "애플": "AAPL",
    }
    lines = [
        f"🌅 <b>[뽕실로봇 V6 장전 브리핑]</b>",
        get_kst_now().strftime("%Y-%m-%d"),
        "",
    ]

    for label, ticker in tickers.items():
        try:
            history = yf.Ticker(ticker).history(period="7d", auto_adjust=False)
            closes = history["Close"].dropna()
            if len(closes) < 2:
                raise ValueError("데이터 부족")
            change = (closes.iloc[-1] / closes.iloc[-2] - 1) * 100
            icon = "🟢" if change >= 0 else "🔴"
            lines.append(f"{icon} {label}: {change:+.2f}%")
        except Exception:
            lines.append(f"⚪ {label}: 확인 불가")

    try:
        market = calculate_market_score(force=True)
        lines.extend(["", f"국내 시장강도: {market['score']:.1f}점"])
    except Exception:
        pass

    send_telegram_msg("\n".join(lines))



def send_daily_closing_report() -> None:
    now = get_kst_now()
    signals = get_today_history()

    if not signals:
        send_telegram_msg(
            f"📋 <b>[{now:%Y-%m-%d} 장 마감]</b>\n\n"
            f"오늘 AI 점수 {MIN_AI_SCORE:.0f}점 이상 시그널이 없었습니다."
        )
        return

    results = []
    for item in signals:
        try:
            df = get_price_data(str(item["code"]).zfill(6), 10, force=True)
            today_rows = df[df.index.strftime("%Y-%m-%d") == now.strftime("%Y-%m-%d")]
            row = today_rows.iloc[-1] if not today_rows.empty else df.iloc[-1]
            recommendation = safe_float(item.get("recommend_price"))
            high = safe_float(row["High"])
            low = safe_float(row["Low"])
            close = safe_float(row["Close"])
            max_return = (high - recommendation) / recommendation * 100
            max_loss = (low - recommendation) / recommendation * 100
            close_return = (close - recommendation) / recommendation * 100
            results.append(
                {
                    **item,
                    "close_price": int(close),
                    "close_return": close_return,
                    "max_return": max_return,
                    "max_loss": max_loss,
                    "target_hit": high >= safe_float(item.get("target_price")),
                    "stop_hit": low <= safe_float(item.get("stop_price")),
                }
            )
        except Exception as exc:
            logger.info("장마감 성과 확인 실패 %s: %s", item.get("name"), exc)

    if not results:
        send_telegram_msg(
            f"📋 <b>[{now:%Y-%m-%d} 장 마감]</b>\n\n"
            f"추천 {len(signals)}건이 있었으나 종가 성과를 확인하지 못했습니다."
        )
        return

    avg_return = float(np.mean([item["close_return"] for item in results]))
    winners = [item for item in results if item["close_return"] > 0]
    best = max(results, key=lambda item: item["close_return"])
    worst = min(results, key=lambda item: item["close_return"])
    target_count = sum(bool(item["target_hit"]) for item in results)
    stop_count = sum(bool(item["stop_hit"]) for item in results)

    rows = "\n".join(
        f"• {html.escape(item['name'])}: "
        f"{item['recommend_price']:,}→{item['close_price']:,}원 "
        f"({item['close_return']:+.2f}%) · "
        f"AI {item['ai_score']:.1f} · V {item.get('v_score', 0):.1f}"
        for item in sorted(results, key=lambda x: x["close_return"], reverse=True)[:15]
    )

    send_telegram_msg(
        f"📋 <b>[{now:%Y-%m-%d} 장마감 성과]</b>\n\n"
        f"분석 누적: {runtime_state['today_analyzed_total']}종목\n"
        f"추천: <b>{len(results)}건</b>\n"
        f"상승 {len(winners)} · 하락 {len(results) - len(winners)}\n"
        f"종가 평균: <b>{avg_return:+.2f}%</b>\n"
        f"종가 성공률: <b>{len(winners) / len(results) * 100:.1f}%</b>\n"
        f"목표가 도달: {target_count}건\n"
        f"손절가 도달: {stop_count}건\n"
        f"최고: {html.escape(best['name'])} {best['close_return']:+.2f}%\n"
        f"최저: {html.escape(worst['name'])} {worst['close_return']:+.2f}%\n"
        f"시장점수: {runtime_state['market_score'] or '미계산'}\n\n"
        f"<b>[추천별 종가 성적]</b>\n{rows}"
    )

# ----------------------------- scheduler -----------------------------

def run_scanner() -> None:
    global morning_briefing_sent_date, daily_summary_sent_date
    global nxt_open_sent_date, reg_open_sent_date
    global reg_close_sent_date, nxt_close_sent_date

    next_command_at = 0.0
    next_scan_at = 0.0
    next_portfolio_at = 0.0

    logger.info("뽕실로봇 V%s 스케줄러 시작", APP_VERSION)

    while True:
        try:
            now = get_kst_now()
            timestamp = time.time()
            today = now.strftime("%Y-%m-%d")

            if timestamp >= next_command_at:
                process_telegram_commands()
                next_command_at = timestamp + COMMAND_INTERVAL_SECONDS

            if now.weekday() < 5:
                if (
                    now.hour == 7
                    and 30 <= now.minute < 35
                    and morning_briefing_sent_date != today
                ):
                    send_morning_briefing()
                    morning_briefing_sent_date = today

                if (
                    now.hour == 8
                    and now.minute < 5
                    and nxt_open_sent_date != today
                ):
                    send_telegram_msg("🔔 <b>[NXT 프리마켓 시작]</b>")
                    nxt_open_sent_date = today

                if (
                    now.hour == 9
                    and now.minute < 5
                    and reg_open_sent_date != today
                ):
                    send_telegram_msg("🔔 <b>[정규장 시작]</b>")
                    reg_open_sent_date = today

                if should_scan_now(now) and timestamp >= next_scan_at:
                    threading.Thread(
                        target=scan_stocks,
                        daemon=True,
                        name="scheduled-scan",
                    ).start()
                    next_scan_at = timestamp + SCAN_INTERVAL_SECONDS

                if 8 <= now.hour < 20 and timestamp >= next_portfolio_at:
                    monitor_portfolio()
                    next_portfolio_at = timestamp + PORTFOLIO_INTERVAL_SECONDS

                if (
                    now.hour == 15
                    and 30 <= now.minute < 35
                    and reg_close_sent_date != today
                ):
                    send_telegram_msg("🔔 <b>[정규장 마감]</b>")
                    reg_close_sent_date = today

                if (
                    now.hour == 15
                    and 35 <= now.minute < 40
                    and daily_summary_sent_date != today
                ):
                    send_daily_closing_report()
                    daily_summary_sent_date = today

                if (
                    now.hour == 20
                    and now.minute < 5
                    and nxt_close_sent_date != today
                ):
                    send_telegram_msg("🔔 <b>[NXT 애프터마켓 마감]</b>")
                    nxt_close_sent_date = today

        except Exception:
            logger.exception("메인 스케줄러 오류")

        time.sleep(1)


# ----------------------------- Flask -----------------------------

@app.route("/")
def health_check():
    return (
        f"뽕실로봇 V{APP_VERSION} 정상 작동 중 | "
        f"scanner={'running' if runtime_state['scanner_running'] else 'idle'}",
        200,
    )


@app.route("/health")
def health_json():
    return jsonify(
        {
            "status": "ok",
            "version": APP_VERSION,
            "runtime": runtime_state,
            "portfolio_count": len(load_portfolio()),
            "signal_history_count": len(load_signal_history()),
            "time_kst": get_kst_now().isoformat(),
        }
    )


def start_background_worker() -> None:
    worker = threading.Thread(
        target=run_scanner,
        daemon=True,
        name="main-scheduler",
    )
    worker.start()


if __name__ == "__main__":
    start_background_worker()
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
        threaded=True,
        use_reloader=False,
    )
