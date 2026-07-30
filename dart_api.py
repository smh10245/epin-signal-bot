import os
from typing import Any, Dict

import requests


DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
DART_BASE_URL = "https://opendart.fss.or.kr/api"
REQUEST_TIMEOUT = 20


def check_dart_connection() -> Dict[str, Any]:
    """
    DART API 인증키와 실제 연결 상태를 확인합니다.
    인증키 자체는 출력하지 않습니다.
    """

    if not DART_API_KEY:
        return {
            "success": False,
            "message": "DART_API_KEY 환경변수가 없습니다.",
        }

    try:
        response = requests.get(
            f"{DART_BASE_URL}/company.json",
            params={
                "crtfc_key": DART_API_KEY,
                "corp_code": "00126380",
            },
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status()
        data = response.json()

    except requests.RequestException as error:
        return {
            "success": False,
            "message": f"DART 서버 요청 실패: {error}",
        }

    except ValueError:
        return {
            "success": False,
            "message": "DART 응답을 해석하지 못했습니다.",
        }

    status = str(data.get("status", "")).strip()
    message = str(data.get("message", "")).strip()

    if status != "000":
        return {
            "success": False,
            "status": status,
            "message": message or "알 수 없는 DART 오류",
        }

    return {
        "success": True,
        "message": "DART API 연결 성공",
        "corp_name": data.get("corp_name", ""),
        "stock_code": data.get("stock_code", ""),
    }