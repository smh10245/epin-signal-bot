import os
import requests

# DART API 설정
DART_API_KEY = os.getenv("DART_API_KEY")
BASE_URL = "https://opendart.fss.or.kr/api"


def check_dart_connection():
    """DART API Key 등록 여부 확인"""

    if not DART_API_KEY:
        return {
            "success": False,
            "message": "DART_API_KEY가 없습니다."
        }

    return {
        "success": True,
        "message": "DART API 연결 준비 완료"
    }