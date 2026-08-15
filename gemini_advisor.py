
from __future__ import annotations

"""
Gemini 보조 해석 모듈 v0.1.0
- 규칙 엔진이 계산한 compact payload만 전송
- AI는 수치/가격대를 새로 발명하지 못하도록 제한
- API 오류/무료쿼터 초과/키 미설정 시 호출 실패만 반환하고
  규칙 엔진 결과는 그대로 유지
"""

import json
import os
from typing import Any, Dict, Optional

import requests

VERSION = "0.1.0"


AI_SCHEMA = {
    "type": "object",
    "properties": {
        "verdict": {"type": "string"},
        "expert_summary": {"type": "string"},
        "bullish_case": {"type": "string"},
        "neutral_case": {"type": "string"},
        "bearish_case": {"type": "string"},
        "entry_comment": {"type": "string"},
        "invalidation_comment": {"type": "string"},
        "risk_flags": {
            "type": "array",
            "items": {"type": "string"}
        },
        "final_action": {"type": "string"}
    },
    "required": [
        "verdict", "expert_summary", "bullish_case", "neutral_case",
        "bearish_case", "entry_comment", "invalidation_comment",
        "risk_flags", "final_action"
    ]
}


SYSTEM_TEXT = """너는 한국 주식 단타·스윙용 '보조 해석기'다.
아래 규칙을 반드시 지킨다.

1. 제공된 수치와 가격대만 사용한다. 새로운 가격, 지지선, 저항선, 목표가를 임의로 만들지 않는다.
2. 미래 주가를 확정적으로 예언하지 않는다. '우세', '가능성', '조건부' 표현을 사용한다.
3. 상승·횡보·하락 세 시나리오를 모두 검토한다.
4. 고점 추격 위험과 지지선 무효 조건을 특히 중요하게 본다.
5. 규칙 엔진과 의견이 다르면 그 이유를 데이터 항목으로 설명한다.
6. 백테스트되지 않은 상대점수를 실제 확률이라고 부르지 않는다.
7. 자동매수/자동매도를 지시하지 않는다. 최종 의사결정은 사용자 몫임을 전제로 한다.
8. 설명은 한국어로 간결하고 실전 대응 중심으로 쓴다.
9. 내부 추론 과정이나 숨은 사고과정은 출력하지 않고, 결론과 근거만 쓴다.
"""


class GeminiAdvisor:
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        timeout: int = 25,
        session: Optional[requests.Session] = None,
    ):
        self.api_key = (api_key or os.getenv("GEMINI_API_KEY") or "").strip()
        self.model = (model or os.getenv("GEMINI_MODEL") or "gemini-3-flash-preview").strip()
        self.timeout = int(timeout)
        self.s = session or requests.Session()

    @property
    def enabled(self) -> bool:
        return bool(self.api_key and self.model)

    def build_request(self, rule_payload: Dict[str, Any]) -> Dict[str, Any]:
        compact = json.dumps(rule_payload, ensure_ascii=False, separators=(",", ":"))
        user_text = (
            "다음은 명하 규칙 엔진이 계산한 다음 거래일 분석 데이터다.\n"
            "수치를 바꾸거나 새 가격대를 만들지 말고 전문가 관점의 조건부 해석만 작성하라.\n\n"
            + compact
        )
        return {
            "system_instruction": {
                "parts": [{"text": SYSTEM_TEXT}]
            },
            "contents": [{
                "role": "user",
                "parts": [{"text": user_text}]
            }],
            "generationConfig": {
                "temperature": 0.2,
                "topP": 0.85,
                "maxOutputTokens": 900,
                "responseMimeType": "application/json",
                "responseJsonSchema": AI_SCHEMA,
            }
        }

    @staticmethod
    def _extract_text(data: Dict[str, Any]) -> str:
        try:
            candidates = data.get("candidates") or []
            if not candidates:
                return ""
            parts = ((candidates[0].get("content") or {}).get("parts") or [])
            return "".join(str(x.get("text") or "") for x in parts).strip()
        except Exception:
            return ""

    @staticmethod
    def _validate(obj: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(obj, dict):
            return None
        required = {
            "verdict", "expert_summary", "bullish_case", "neutral_case",
            "bearish_case", "entry_comment", "invalidation_comment",
            "risk_flags", "final_action"
        }
        if not required.issubset(obj):
            return None
        if not isinstance(obj.get("risk_flags"), list):
            return None
        # length guard
        out = dict(obj)
        for k in required - {"risk_flags"}:
            out[k] = str(out.get(k) or "")[:1200]
        out["risk_flags"] = [str(x)[:300] for x in out.get("risk_flags", [])[:8]]
        return out

    def advise(self, rule_payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.enabled:
            return {
                "ok": False,
                "error": "gemini_key_missing",
                "message": "GEMINI_API_KEY가 설정되지 않아 규칙 엔진 결과만 사용합니다.",
            }

        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self.model}:generateContent"
        )
        try:
            r = self.s.post(
                url,
                headers={
                    "x-goog-api-key": self.api_key,
                    "Content-Type": "application/json",
                },
                json=self.build_request(rule_payload),
                timeout=self.timeout,
            )
            if r.status_code == 429:
                return {
                    "ok": False,
                    "error": "free_tier_quota",
                    "message": "Gemini 무료 한도/속도 제한에 도달했습니다. 규칙 엔진 결과만 표시합니다.",
                    "status_code": 429,
                }
            if r.status_code >= 400:
                return {
                    "ok": False,
                    "error": "gemini_http_error",
                    "message": f"Gemini API 오류 HTTP {r.status_code}. 규칙 엔진 결과만 표시합니다.",
                    "status_code": r.status_code,
                }

            data = r.json()
            text = self._extract_text(data)
            if not text:
                return {
                    "ok": False,
                    "error": "empty_response",
                    "message": "Gemini 응답이 비어 있어 규칙 엔진 결과만 표시합니다.",
                }
            try:
                parsed = json.loads(text)
            except Exception:
                return {
                    "ok": False,
                    "error": "invalid_json",
                    "message": "Gemini 구조화 응답을 해석하지 못해 규칙 엔진 결과만 표시합니다.",
                }

            validated = self._validate(parsed)
            if not validated:
                return {
                    "ok": False,
                    "error": "schema_mismatch",
                    "message": "Gemini 응답 형식이 맞지 않아 규칙 엔진 결과만 표시합니다.",
                }

            return {
                "ok": True,
                "provider": "Google Gemini API",
                "model": self.model,
                "advisor_version": VERSION,
                "analysis": validated,
            }
        except requests.Timeout:
            return {
                "ok": False,
                "error": "timeout",
                "message": "Gemini 응답시간을 초과해 규칙 엔진 결과만 표시합니다.",
            }
        except Exception as e:
            return {
                "ok": False,
                "error": "gemini_exception",
                "message": f"Gemini 호출 실패: {str(e)[:120]}. 규칙 엔진 결과만 표시합니다.",
            }
