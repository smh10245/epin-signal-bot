import os
import time
import json
import html
import logging
import threading
import io
import zipfile
import xml.etree.ElementTree as ET
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
# 뽕실로봇 V7 Ultimate
# - Wilder RSI / EMA / MACD / ATR / ADX / Bollinger Band
# - AI Score 100점
# - 시가총액 상위 200종목
# - 캐시 / 재시도 / 상태 명령어 / 강제 스캔
# - ATR 기반 손절 및 트레일링
# - 안전/일반/공격 추천모드
# ============================================================

APP_VERSION = "8.0.0"
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
MODE_FILE = os.getenv("MODE_FILE", "bot_mode.json")
DEFAULT_MODE = os.getenv("DEFAULT_MODE", "일반").strip()

# OpenDART
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
DART_CORP_CACHE_SECONDS = int(os.getenv("DART_CORP_CACHE_SECONDS", "86400"))
DART_FINANCE_CACHE_SECONDS = int(os.getenv("DART_FINANCE_CACHE_SECONDS", "21600"))
DART_TARGET_PER = float(os.getenv("DART_TARGET_PER", "10"))
DART_TARGET_PBR = float(os.getenv("DART_TARGET_PBR", "1.0"))

MODE_CONFIG: Dict[str, Dict[str, Any]] = {
    "안전": {
        "label": "안전모드",
        "min_ai_score": float(os.getenv("SAFE_MODE_MIN_AI_SCORE", "85")),
        "max_recommendations": int(os.getenv("SAFE_MODE_MAX_RECOMMENDATIONS", "2")),
    },
    "일반": {
        "label": "일반모드",
        "min_ai_score": float(os.getenv("NORMAL_MODE_MIN_AI_SCORE", str(MIN_AI_SCORE))),
        "max_recommendations": int(os.getenv("NORMAL_MODE_MAX_RECOMMENDATIONS", "5")),
    },
    "공격": {
        "label": "공격모드",
        "min_ai_score": float(os.getenv("AGGRESSIVE_MODE_MIN_AI_SCORE", "65")),
        "max_recommendations": int(os.getenv("AGGRESSIVE_MODE_MAX_RECOMMENDATIONS", "8")),
    },
}
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
}

_cache: Dict[str, Tuple[float, Any]] = {}


# ----------------------------- recommendation mode -----------------------------

def normalize_mode_name(mode: str) -> Optional[str]:
    value = str(mode or "").strip().replace("모드", "")
    aliases = {
        "safe": "안전",
        "normal": "일반",
        "aggressive": "공격",
        "안전": "안전",
        "일반": "일반",
        "공격": "공격",
    }
    return aliases.get(value.lower(), aliases.get(value))


def load_bot_mode() -> str:
    default_mode = normalize_mode_name(DEFAULT_MODE) or "일반"
    if not os.path.exists(MODE_FILE):
        return default_mode
    try:
        with open(MODE_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
        saved_mode = normalize_mode_name(data.get("mode") if isinstance(data, dict) else "")
        return saved_mode or default_mode
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("추천모드 불러오기 실패: %s", exc)
        return default_mode


def save_bot_mode(mode: str) -> None:
    normalized = normalize_mode_name(mode)
    if not normalized:
        raise ValueError("지원하지 않는 추천모드입니다.")
    atomic_json_write(MODE_FILE, {
        "mode": normalized,
        "updated_at": get_kst_now().isoformat(),
    })


def get_mode_settings() -> Tuple[str, Dict[str, Any]]:
    mode = load_bot_mode()
    return mode, MODE_CONFIG[mode]


def get_mode_min_ai_score() -> float:
    return float(get_mode_settings()[1]["min_ai_score"])


def get_mode_max_recommendations() -> int:
    return int(get_mode_settings()[1]["max_recommendations"])


def format_mode_status() -> str:
    mode, config = get_mode_settings()
    return (
        f"🎛️ <b>[추천모드]</b>\n\n"
        f"현재 추천모드: <b>{config['label']}</b>\n"
        f"AI 점수 기준: <b>{float(config['min_ai_score']):.0f}점 이상</b>\n"
        f"스캔당 추천 수: <b>최대 {int(config['max_recommendations'])}종목</b>\n\n"
        f"변경 명령어\n"
        f"• <code>/모드 안전</code>\n"
        f"• <code>/모드 일반</code>\n"
        f"• <code>/모드 공격</code>"
    )


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
    if not os.path.exists(SIGNAL_HISTORY_FILE):
        return []
    try:
        with open(SIGNAL_HISTORY_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
            return data if isinstance(data, list) else []
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("시그널 기록 불러오기 실패: %s", exc)
        return []


def save_signal_history(data: list[Dict[str, Any]]) -> None:
    temp_path = f"{SIGNAL_HISTORY_FILE}.tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_path, SIGNAL_HISTORY_FILE)
    except OSError as exc:
        logger.error("시그널 기록 저장 실패: %s", exc)


def append_signal_history(record: Dict[str, Any]) -> None:
    history = load_signal_history()
    history.append(record)
    # 파일 과대화를 막기 위해 최근 5,000건 유지
    save_signal_history(history[-5000:])


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



# ----------------------------- OpenDART valuation -----------------------------

def _dart_request(endpoint: str, params: Dict[str, Any], timeout: Tuple[int, int] = (5, 20)) -> Dict[str, Any]:
    if not DART_API_KEY:
        raise ValueError("DART_API_KEY 환경변수가 없습니다.")

    query = {"crtfc_key": DART_API_KEY, **params}
    url = f"https://opendart.fss.or.kr/api/{endpoint}"
    response = http.get(url, params=query, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    status = str(payload.get("status", ""))
    if status and status != "000":
        message = str(payload.get("message", "OpenDART 오류"))
        raise ValueError(f"DART {status}: {message}")
    return payload


def get_dart_corp_map(force: bool = False) -> Dict[str, str]:
    cache_key = "dart_corp_map"
    if not force:
        cached = cache_get(cache_key)
        if cached is not None:
            return dict(cached)

    if not DART_API_KEY:
        raise ValueError("DART_API_KEY 환경변수가 없습니다.")

    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    response = http.get(
        url,
        params={"crtfc_key": DART_API_KEY},
        timeout=(5, 30),
    )
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        xml_name = next((name for name in archive.namelist() if name.lower().endswith(".xml")), None)
        if not xml_name:
            raise ValueError("DART 기업코드 XML을 찾지 못했습니다.")
        root = ET.fromstring(archive.read(xml_name))

    corp_map: Dict[str, str] = {}
    for item in root.findall("list"):
        stock_code = (item.findtext("stock_code") or "").strip()
        corp_code = (item.findtext("corp_code") or "").strip()
        if stock_code and corp_code:
            corp_map[stock_code.zfill(6)] = corp_code

    if not corp_map:
        raise ValueError("DART 상장사 코드 목록이 비어 있습니다.")

    return cache_set(cache_key, corp_map, DART_CORP_CACHE_SECONDS)


def check_dart_connection() -> Dict[str, Any]:
    try:
        corp_map = get_dart_corp_map(force=True)
        samsung = corp_map.get("005930")
        if not samsung:
            raise ValueError("삼성전자 기업코드를 찾지 못했습니다.")
        return {
            "success": True,
            "corp_name": "삼성전자",
            "stock_code": "005930",
            "corp_code": samsung,
            "count": len(corp_map),
        }
    except Exception as exc:
        return {
            "success": False,
            "status": "ERROR",
            "message": str(exc),
        }


def _parse_amount(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text or text in ("-", "nan", "None"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _pick_financial_amount(rows: list[Dict[str, Any]], account_names: Tuple[str, ...]) -> Optional[float]:
    for row in rows:
        account_nm = str(row.get("account_nm", "")).replace(" ", "")
        if any(name.replace(" ", "") in account_nm for name in account_names):
            amount = _parse_amount(row.get("thstrm_amount"))
            if amount is not None:
                return amount
    return None


def get_dart_financials(code: str, force: bool = False) -> Dict[str, Any]:
    code = str(code).zfill(6)
    cache_key = f"dart_financials:{code}"
    if not force:
        cached = cache_get(cache_key)
        if cached is not None:
            return dict(cached)

    corp_code = get_dart_corp_map(force=False).get(code)
    if not corp_code:
        raise ValueError("DART 기업코드를 찾지 못했습니다.")

    current_year = get_kst_now().year
    last_error: Optional[Exception] = None

    for year in range(current_year - 1, current_year - 5, -1):
        for fs_div in ("CFS", "OFS"):
            try:
                payload = _dart_request(
                    "fnlttSinglAcnt.json",
                    {
                        "corp_code": corp_code,
                        "bsns_year": str(year),
                        "reprt_code": "11011",
                        "fs_div": fs_div,
                    },
                )
                rows = payload.get("list") or []
                if not rows:
                    continue

                net_income = _pick_financial_amount(
                    rows,
                    ("당기순이익", "연결당기순이익", "지배기업소유주지분순이익"),
                )
                equity = _pick_financial_amount(
                    rows,
                    ("자본총계", "지배기업소유주지분"),
                )

                result = {
                    "corp_code": corp_code,
                    "year": year,
                    "fs_div": fs_div,
                    "net_income": net_income,
                    "equity": equity,
                }
                return cache_set(cache_key, result, DART_FINANCE_CACHE_SECONDS)
            except Exception as exc:
                last_error = exc

    if last_error:
        raise last_error
    raise ValueError("최근 사업보고서 재무자료를 찾지 못했습니다.")


def get_fair_value(code: str, name: str, current_price: Optional[float] = None, force: bool = False) -> Dict[str, Any]:
    code = str(code).zfill(6)
    if current_price is None:
        current_price = safe_float(get_price_data(code, 15, force=force).iloc[-1]["Close"])

    if current_price <= 0:
        raise ValueError("현재가를 확인하지 못했습니다.")

    finance = get_dart_financials(code, force=force)

    # DART 단일계정 API에는 발행주식수가 없을 수 있어 yfinance 보조 사용
    shares = None
    try:
        ticker = yf.Ticker(f"{code}.KS")
        info = ticker.fast_info
        shares = safe_float(getattr(info, "shares", None) or info.get("shares"), 0)
    except Exception:
        shares = 0

    if shares <= 0:
        try:
            ticker = yf.Ticker(f"{code}.KQ")
            info = ticker.fast_info
            shares = safe_float(getattr(info, "shares", None) or info.get("shares"), 0)
        except Exception:
            shares = 0

    net_income = finance.get("net_income")
    equity = finance.get("equity")

    eps = (net_income / shares) if net_income is not None and shares > 0 else None
    bps = (equity / shares) if equity is not None and shares > 0 else None

    per_value = eps * DART_TARGET_PER if eps is not None and eps > 0 else None
    pbr_value = bps * DART_TARGET_PBR if bps is not None and bps > 0 else None

    values = [v for v in (per_value, pbr_value) if v is not None and v > 0]
    fair_value = sum(values) / len(values) if values else None
    gap_percent = ((fair_value - current_price) / current_price * 100) if fair_value else None

    if gap_percent is None:
        attractiveness = "평가자료 부족"
    elif gap_percent >= 30:
        attractiveness = "저평가 가능성 높음"
    elif gap_percent >= 10:
        attractiveness = "저평가 가능성"
    elif gap_percent > -10:
        attractiveness = "적정가 부근"
    elif gap_percent > -25:
        attractiveness = "다소 고평가"
    else:
        attractiveness = "고평가 주의"

    return {
        "code": code,
        "name": name,
        "current_price": int(current_price),
        "year": finance.get("year"),
        "fs_div": finance.get("fs_div"),
        "shares": shares if shares > 0 else None,
        "eps": eps,
        "bps": bps,
        "per_target": DART_TARGET_PER,
        "pbr_target": DART_TARGET_PBR,
        "per_value": int(per_value) if per_value else None,
        "pbr_value": int(pbr_value) if pbr_value else None,
        "fair_value": int(fair_value) if fair_value else None,
        "gap_percent": gap_percent,
        "attractiveness": attractiveness,
    }


def format_dart_test_message() -> str:
    result = check_dart_connection()
    if result.get("success"):
        return (
            "✅ <b>[DART API 연결 성공]</b>\n\n"
            f"기업명: <b>{html.escape(str(result.get('corp_name')))}</b>\n"
            f"종목코드: <b>{html.escape(str(result.get('stock_code')))}</b>\n"
            f"상장사 코드 수: <b>{int(result.get('count', 0)):,}개</b>\n\n"
            "전자공시 재무데이터를 가져올 준비가 완료되었습니다."
        )
    return (
        "⚠️ <b>[DART API 연결 실패]</b>\n\n"
        f"내용: {html.escape(str(result.get('message', '알 수 없는 오류')))}"
    )


def format_fair_value_message(value: Dict[str, Any]) -> str:
    def money(v: Any) -> str:
        return f"{int(v):,}원" if v is not None else "계산 불가"

    gap = value.get("gap_percent")
    gap_text = f"{gap:+.1f}%" if gap is not None else "계산 불가"
    fs_label = "연결" if value.get("fs_div") == "CFS" else "별도"

    return (
        f"🏦 <b>[DART 기업가치 참고]</b>\n\n"
        f"📌 <b>{html.escape(str(value.get('name')))}</b> ({value.get('code')})\n"
        f"기준 재무제표: {value.get('year')}년 {fs_label}\n"
        f"현재가: <b>{money(value.get('current_price'))}</b>\n\n"
        f"PER 기준가: <b>{money(value.get('per_value'))}</b> "
        f"(목표 PER {value.get('per_target'):g}배)\n"
        f"PBR 기준가: <b>{money(value.get('pbr_value'))}</b> "
        f"(목표 PBR {value.get('pbr_target'):g}배)\n"
        f"평균 적정주가: <b>{money(value.get('fair_value'))}</b>\n"
        f"현재가 대비: <b>{gap_text}</b>\n"
        f"판정: <b>{html.escape(str(value.get('attractiveness')))}</b>\n\n"
        f"<i>DART 공시와 고정 목표배수를 이용한 참고값이며 투자수익을 보장하지 않습니다.</i>"
    )


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
    v_reasons: list[str]


def calculate_v_reversal_score(df: pd.DataFrame) -> Tuple[float, list[str]]:
    """기존 AI 점수와 분리된 V자 반등 보조 점수(0~10)."""
    if len(df) < 3:
        return 0.0, []

    row = df.iloc[-1]
    prev = df.iloc[-2]
    score = 0.0
    reasons: list[str] = []

    rsi = safe_float(row.get("RSI"), 50)
    prev_rsi = safe_float(prev.get("RSI"), 50)
    if prev_rsi < 35 <= rsi or (rsi <= 42 and rsi > prev_rsi):
        score += 2.0
        reasons.append("RSI 과매도권 반등")

    macd_hist = safe_float(row.get("MACD_HIST"))
    prev_macd_hist = safe_float(prev.get("MACD_HIST"))
    if macd_hist > prev_macd_hist:
        score += 2.0
        reasons.append("MACD 모멘텀 개선")

    avg_volume = max(safe_float(prev.get("VOL_MA20"), 1), 1)
    volume_ratio = safe_float(row.get("Volume")) / avg_volume * 100
    if volume_ratio >= 120:
        score += 2.0
        reasons.append("거래량 증가")
    elif volume_ratio >= 90:
        score += 1.0

    high = safe_float(row.get("High"))
    low = safe_float(row.get("Low"))
    close = safe_float(row.get("Close"))
    open_price = safe_float(row.get("Open"), close)
    candle_range = max(high - low, 0)
    if candle_range > 0:
        lower_wick = min(open_price, close) - low
        recovery = (close - low) / candle_range
        if lower_wick / candle_range >= 0.35:
            score += 2.0
            reasons.append("긴 아랫꼬리")
        if recovery >= 0.70:
            score += 1.0
            reasons.append("저점 대비 강한 회복")

    ema20 = safe_float(row.get("EMA20"))
    prev_close = safe_float(prev.get("Close"))
    if ema20 > 0 and prev_close < ema20 <= close:
        score += 1.0
        reasons.append("EMA20 재돌파")

    return round(min(score, 10.0), 1), reasons[:5]


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
    v_score, v_reasons = calculate_v_reversal_score(df)

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

    message = (
        f"🚨 <b>[뽕실로봇 V7 매수 후보]</b>\n"
        f"🧠 <b>AI 점수 {signal.score:.1f}/100 · {signal.grade}등급</b>\n"
        f"🔻 <b>V반등 점수 {signal.v_score:.1f}/10</b>\n\n"
        f"📌 <b>{html.escape(name)}</b> ({code})\n"
        f"💰 현재가: <b>{signal.current_price:,}원</b>\n"
        f"🎯 1차 목표가: <b>{signal.target_price:,}원</b> "
        f"({signal.upside:+.1f}%)\n"
        f"🛑 참고 손절가: <b>{signal.stop_price:,}원</b>\n\n"
        f"📊 RSI {signal.rsi:.1f} · ADX {signal.adx:.1f}\n"
        f"📈 예상 거래량 {signal.volume_ratio:.0f}% · "
        f"ATR {signal.atr_percent:.1f}%\n"
        f"🌏 시장강도 {market['score']:.1f}점 ({html.escape(market['detail'])})\n\n"
        f"<b>[가점 근거]</b>\n{reasons or '• 뚜렷한 가점 근거 없음'}"
    )
    if signal.v_reasons:
        v_text = "\n".join(f"• {html.escape(item)}" for item in signal.v_reasons)
        message += f"\n\n🔻 <b>[V반등 근거]</b>\n{v_text}"
    if warnings:
        message += f"\n\n⚠️ <b>[주의]</b>\n{warnings}"

    message += (
        f"\n\n💡 등록 명령어\n"
        f"• <code>/매수 {html.escape(name)} {signal.current_price} 단타</code>\n"
        f"• <code>/매수 {html.escape(name)} {signal.current_price} 스윙</code>\n\n"
        f"<i>AI 점수는 기술적 조건을 정량화한 참고값이며 수익을 보장하지 않습니다.</i>"
    )
    return message


# ----------------------------- portfolio -----------------------------

def get_trade_rules(trade_type: str, atr_percent: float) -> Dict[str, float]:
    """ATR를 반영하되 손절 폭에 명확한 상한을 둡니다."""
    atr_percent = max(0.8, min(atr_percent, 8.0))

    if trade_type == "스윙":
        stop_abs = min(max(4.0, atr_percent * 1.25), 8.0)
        return {
            "trigger": max(4.0, atr_percent * 1.35),
            "trailing": max(2.0, atr_percent * 0.75),
            "stop": -stop_abs,
        }

    stop_abs = min(max(2.5, atr_percent * 1.0), 5.5)
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
                if signal.score >= get_mode_min_ai_score():
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
                f"📊 <b>[{html.escape(name)}] V7 백테스트</b>\n"
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
            f"📊 <b>[{html.escape(name)}] V7 백테스트</b>\n"
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

                mode_min_score = get_mode_min_ai_score()
                if preliminary.score < mode_min_score - 5:
                    continue

                investor_ok = check_investor_buying(code)
                signal = calculate_ai_score(
                    df,
                    market["score"],
                    investor_ok=investor_ok,
                    now=now,
                )

                if signal.score >= mode_min_score:
                    candidates.append((signal.score, code, name, signal))

            except Exception as exc:
                logger.info("[%s %s] 분석 제외: %s", name, code, exc)

            time.sleep(0.05)

        candidates.sort(key=lambda item: item[0], reverse=True)

        # 한 번의 스캔에서 최대 10개만 전송해 알림 폭주 방지
        mode, mode_config = get_mode_settings()
        max_recommendations = int(mode_config["max_recommendations"])

        for _, code, name, signal in candidates[:max_recommendations]:
            send_telegram_msg(
                format_signal_message(code, name, signal, market),
                requested_chat_id,
            )
            with state_lock:
                sent_signals_today[code] = {
                    "name": name,
                    "time": now,
                    "score": signal.score,
                    "price": signal.current_price,
                    "target_price": signal.target_price,
                    "stop_price": signal.stop_price,
                    "v_score": signal.v_score,
                }
            append_signal_history({
                "date": today,
                "time": now.isoformat(),
                "code": code,
                "name": name,
                "price": signal.current_price,
                "score": signal.score,
                "grade": signal.grade,
                "v_score": signal.v_score,
                "target_price": signal.target_price,
                "stop_price": signal.stop_price,
                "market_score": market.get("score"),
            })
            signal_count += 1
            time.sleep(0.6)

        runtime_state["last_scan_at"] = now.isoformat()
        runtime_state["last_scan_count"] = scanned
        runtime_state["last_signal_count"] = signal_count
        runtime_state["today_scan_runs"] += 1
        runtime_state["today_analyzed_total"] += scanned
        runtime_state["today_signal_total"] += signal_count
        if candidates:
            best = max(item[0] for item in candidates)
            current_best = runtime_state.get("today_best_score")
            runtime_state["today_best_score"] = best if current_best is None else max(current_best, best)

        if requested_chat_id:
            send_telegram_msg(
                f"✅ 강제 스캔 완료\n"
                f"분석 {scanned}종목 · {mode_config['label']} 기준 "
                f"{float(mode_config['min_ai_score']):.0f}점 이상 "
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
        "🤖 <b>[뽕실로봇 V7 명령어]</b>\n\n"
        "• <code>/매수 종목명 단가 단타</code>\n"
        "• <code>/매수 종목명 단가 스윙</code>\n"
        "• <code>/수정 종목명 단가 단타</code>\n"
        "• <code>/매도완료 종목명</code>\n"
        "• <code>/목록</code>\n"
        "• <code>/점수 종목명</code>\n"
        "• <code>/백테스트 종목명</code>\n"
        "• <code>/다트테스트</code>\n"
        "• <code>/가치 종목명</code>\n"
        "• <code>/시장</code>\n"
        "• <code>/상태</code>\n"
        "• <code>/강제스캔</code>\n"
        "• <code>/모드</code>\n"
        "• <code>/모드 안전</code>\n"
        "• <code>/모드 일반</code>\n"
        "• <code>/모드 공격</code>\n"
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
            buy_price = safe_float(info.get("price"))
            trade_type = info.get("type", "단타")
            status = "트레일링 가동" if info.get("trailing_active") else "일반 감시"
            try:
                code = str(info.get("code", "")).zfill(6)
                df = add_indicators(get_price_data(code, 80))
                row = df.iloc[-1]
                current_price = int(row["Close"])
                atr_percent = safe_float(row["ATR"]) / max(current_price, 1) * 100
                rules = get_trade_rules(trade_type, atr_percent)
                profit_rate = (current_price - buy_price) / buy_price * 100 if buy_price else 0
                stop_price = int(buy_price * (1 + rules["stop"] / 100))
                trigger_price = int(buy_price * (1 + rules["trigger"] / 100))
                lines.append(
                    f"\n• <b>{html.escape(name)}</b> ({trade_type})\n"
                    f"  매수가 {int(buy_price):,}원 · 현재가 {current_price:,}원\n"
                    f"  수익률 <b>{profit_rate:+.2f}%</b>\n"
                    f"  손절가 {stop_price:,}원 · 트레일링 시작 {trigger_price:,}원\n"
                    f"  상태: {status}"
                )
            except Exception:
                lines.append(
                    f"\n• <b>{html.escape(name)}</b> ({trade_type})\n"
                    f"  매수가 {int(buy_price):,}원 · 현재가 확인 불가\n"
                    f"  상태: {status}"
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
            try:
                value = get_fair_value(
                    code,
                    name,
                    current_price=signal.current_price,
                    force=False,
                )
                send_telegram_msg(format_fair_value_message(value), chat_id)
            except Exception as dart_exc:
                logger.info("[%s] DART 가치평가 생략: %s", name, dart_exc)
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

    elif cmd == "/모드":
        if len(parts) == 1:
            send_telegram_msg(format_mode_status(), chat_id)
            return

        requested_mode = normalize_mode_name(parts[1])
        if not requested_mode:
            send_telegram_msg(
                "⚠️ 사용법: /모드 [안전/일반/공격]",
                chat_id,
            )
            return

        try:
            save_bot_mode(requested_mode)
            config = MODE_CONFIG[requested_mode]
            send_telegram_msg(
                f"✅ 추천모드를 <b>{config['label']}</b>로 변경했습니다.\n"
                f"AI 점수 기준: {float(config['min_ai_score']):.0f}점 이상\n"
                f"스캔당 추천 수: 최대 {int(config['max_recommendations'])}종목",
                chat_id,
            )
        except Exception as exc:
            logger.exception("추천모드 저장 실패")
            send_telegram_msg(
                f"⚠️ 추천모드 변경 실패: {html.escape(str(exc))}",
                chat_id,
            )

    elif cmd == "/다트테스트":
        send_telegram_msg("⏳ DART API 연결을 확인하고 있습니다.", chat_id)
        send_telegram_msg(format_dart_test_message(), chat_id)

    elif cmd == "/가치":
        if len(parts) < 2:
            send_telegram_msg("⚠️ 사용법: /가치 [종목명]", chat_id)
            return
        query = " ".join(parts[1:])
        code, name = resolve_stock(query)
        if not code or not name:
            send_telegram_msg("⚠️ 종목을 찾지 못했습니다.", chat_id)
            return
        send_telegram_msg(f"⏳ {html.escape(name)} DART 기업가치를 계산하고 있습니다.", chat_id)
        try:
            value = get_fair_value(code, name, force=True)
            send_telegram_msg(format_fair_value_message(value), chat_id)
        except Exception as exc:
            logger.exception("DART 기업가치 계산 실패")
            send_telegram_msg(
                f"⚠️ 기업가치 계산 실패: {html.escape(str(exc))}",
                chat_id,
            )

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
        started = datetime.fromisoformat(runtime_state["started_at"])
        uptime = datetime.now(KST) - started
        hours, remainder = divmod(int(uptime.total_seconds()), 3600)
        minutes = remainder // 60
        send_telegram_msg(
            f"🛠️ <b>[뽕실로봇 V{APP_VERSION} 상태]</b>\n\n"
            f"스캐너: {'가동 중' if runtime_state['scanner_running'] else '대기'}\n"
            f"추천모드: {get_mode_settings()[1]['label']}\n"
            f"추천기준: {get_mode_min_ai_score():.0f}점 이상 · 최대 {get_mode_max_recommendations()}종목\n"
            f"가동시간: {hours}시간 {minutes}분\n"
            f"마지막 스캔: {runtime_state['last_scan_at'] or '없음'}\n"
            f"최근 분석: {runtime_state['last_scan_count']}종목\n"
            f"최근 시그널: {runtime_state['last_signal_count']}종목\n"
            f"오늘 스캔: {runtime_state['today_scan_runs']}회\n"
            f"오늘 누적 분석: {runtime_state['today_analyzed_total']}종목\n"
            f"오늘 누적 시그널: {runtime_state['today_signal_total']}종목\n"
            f"오늘 최고 AI점수: {runtime_state['today_best_score'] or '없음'}\n"
            f"감시 종목: {len(portfolio)}종목\n"
            f"시장점수: {runtime_state['market_score'] or '미계산'}\n"
            f"기록파일: {html.escape(SIGNAL_HISTORY_FILE)}\n"
            f"DART API: {'설정됨' if DART_API_KEY else '미설정'}\n"
            f"오류: {html.escape(str(runtime_state['last_scan_error'] or '없음'))}",
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
        f"🌅 <b>[뽕실로봇 V7 장전 브리핑]</b>",
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
    today = now.strftime("%Y-%m-%d")
    history = [item for item in load_signal_history() if item.get("date") == today]

    if not history:
        message = (
            f"📋 <b>[{today} 장 마감]</b>\n\n"
            f"오늘 {get_mode_settings()[1]['label']} 기준 "
            f"AI 점수 {get_mode_min_ai_score():.0f}점 이상 시그널이 없었습니다.\n"
            f"누적 분석: {runtime_state['today_analyzed_total']}종목"
        )
        send_telegram_msg(message)
        return

    evaluated = []
    for item in history:
        try:
            df = get_price_data(str(item["code"]).zfill(6), 10, force=True)
            row = df.iloc[-1]
            entry = safe_float(item.get("price"))
            high = safe_float(row["High"])
            low = safe_float(row["Low"])
            close = safe_float(row["Close"])
            evaluated.append({
                **item,
                "close": close,
                "return": (close - entry) / entry * 100 if entry else 0,
                "max_return": (high - entry) / entry * 100 if entry else 0,
                "max_loss": (low - entry) / entry * 100 if entry else 0,
                "target_hit": high >= safe_float(item.get("target_price"), float("inf")),
                "stop_hit": low <= safe_float(item.get("stop_price"), 0),
            })
        except Exception as exc:
            logger.info("[%s] 장마감 성과 계산 실패: %s", item.get("name"), exc)

    if not evaluated:
        send_telegram_msg(f"📋 <b>[{today} 장 마감]</b>\n\n추천 기록은 있으나 성과 계산에 실패했습니다.")
        return

    avg_return = float(np.mean([x["return"] for x in evaluated]))
    winners = sum(1 for x in evaluated if x["return"] > 0)
    best = max(evaluated, key=lambda x: x["return"])
    worst = min(evaluated, key=lambda x: x["return"])
    target_hits = sum(1 for x in evaluated if x["target_hit"])
    stop_hits = sum(1 for x in evaluated if x["stop_hit"])
    rows = "\n".join(
        f"• {html.escape(x['name'])}: {x['return']:+.2f}% "
        f"(최대 {x['max_return']:+.2f}% / 최저 {x['max_loss']:+.2f}%)"
        for x in evaluated[:15]
    )
    message = (
        f"📋 <b>[{today} 장 마감 브리핑]</b>\n\n"
        f"분석 누적: <b>{runtime_state['today_analyzed_total']}종목</b>\n"
        f"추천: <b>{len(evaluated)}종목</b> · 상승 {winners} · 하락 {len(evaluated)-winners}\n"
        f"평균 수익률: <b>{avg_return:+.2f}%</b>\n"
        f"목표가 도달: {target_hits} · 손절가 도달: {stop_hits}\n"
        f"최고: {html.escape(best['name'])} {best['return']:+.2f}%\n"
        f"최저: {html.escape(worst['name'])} {worst['return']:+.2f}%\n\n"
        f"<b>[종목별 성과]</b>\n{rows}"
    )
    send_telegram_msg(message)


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
            "time_kst": get_kst_now().isoformat(),
            "recommendation_mode": get_mode_settings()[0],
            "mode_config": get_mode_settings()[1],
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
