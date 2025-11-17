"""
데이터 캐싱 및 중복 감지 모듈

이전에 수집한 데이터와 현재 데이터를 비교하여 중복을 감지합니다.
1분 간격 호출 시 동일한 데이터가 반복 수집되는 것을 방지합니다.
"""

import hashlib
import json
import logging
from datetime import datetime, timedelta

# 메모리 기반 캐시 (간단한 구현)
# Azure Functions는 상태를 유지하지 않으므로 Azure Table Storage나 Redis를 사용하는 것이 이상적이지만,
# 여기서는 메모리 캐시로 간단하게 구현합니다.
_data_cache = {
    "last_hash": None,
    "last_collection_time": None,
    "last_update": None,
    "duplicate_count": 0
}


def calculate_data_hash(data):
    """
    데이터의 해시값을 계산합니다.

    Args:
        data (dict or list): 해시를 계산할 데이터

    Returns:
        str: SHA256 해시값 (16진수)
    """
    # JSON으로 직렬화하여 일관된 문자열 생성 (키 정렬)
    json_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
    hash_obj = hashlib.sha256(json_str.encode('utf-8'))
    return hash_obj.hexdigest()


def is_duplicate_data(message_data):
    """
    현재 데이터가 이전 데이터와 중복되는지 확인합니다.

    Args:
        message_data (dict): 배치 JSON 메시지

    Returns:
        tuple: (is_duplicate: bool, details: dict)
            - is_duplicate: 중복 여부
            - details: 중복 체크 상세 정보
    """
    global _data_cache

    current_hash = calculate_data_hash(message_data.get("data", []))
    current_collection_time = message_data.get("collection_time")

    details = {
        "current_hash": current_hash[:16],  # 처음 16자리만 로깅
        "last_hash": _data_cache["last_hash"][:16] if _data_cache["last_hash"] else None,
        "current_collection_time": current_collection_time,
        "last_collection_time": _data_cache["last_collection_time"],
        "duplicate_count": _data_cache["duplicate_count"]
    }

    # 첫 실행인 경우
    if _data_cache["last_hash"] is None:
        _data_cache["last_hash"] = current_hash
        _data_cache["last_collection_time"] = current_collection_time
        _data_cache["last_update"] = datetime.utcnow()
        _data_cache["duplicate_count"] = 0

        logging.info(f"첫 데이터 수집 - 해시: {current_hash[:16]}")
        return False, details

    # 해시 비교
    if current_hash == _data_cache["last_hash"]:
        _data_cache["duplicate_count"] += 1
        details["duplicate_count"] = _data_cache["duplicate_count"]

        logging.warning(
            f"중복 데이터 감지 - 해시: {current_hash[:16]}, "
            f"연속 중복: {_data_cache['duplicate_count']}회, "
            f"collection_time: {current_collection_time}"
        )
        return True, details

    # 새로운 데이터 - 캐시 업데이트
    _data_cache["last_hash"] = current_hash
    _data_cache["last_collection_time"] = current_collection_time
    _data_cache["last_update"] = datetime.utcnow()

    # 중복 카운트 초기화 (새 데이터가 들어왔으므로)
    if _data_cache["duplicate_count"] > 0:
        logging.info(f"새로운 데이터 수신 - 이전 {_data_cache['duplicate_count']}회 중복 종료")
        _data_cache["duplicate_count"] = 0

    details["duplicate_count"] = 0
    return False, details


def get_cache_stats():
    """
    캐시 통계 정보를 반환합니다.

    Returns:
        dict: 캐시 통계
    """
    return {
        "last_hash": _data_cache["last_hash"][:16] if _data_cache["last_hash"] else None,
        "last_collection_time": _data_cache["last_collection_time"],
        "last_update": _data_cache["last_update"].isoformat() if _data_cache["last_update"] else None,
        "duplicate_count": _data_cache["duplicate_count"]
    }


def should_send_duplicate(duplicate_count, max_skip=5):
    """
    중복 데이터라도 전송해야 하는지 결정합니다.
    너무 많은 중복이 쌓이면 데이터 손실로 오해할 수 있으므로
    일정 횟수마다 중복이라도 전송합니다.

    Args:
        duplicate_count (int): 연속 중복 횟수
        max_skip (int): 최대 스킵 횟수 (기본값: 5 = 5분)

    Returns:
        bool: 전송 여부
    """
    if duplicate_count >= max_skip:
        logging.info(
            f"중복 데이터이지만 {max_skip}회 연속 스킵되어 강제 전송합니다 "
            f"(데이터 파이프라인 유지 목적)"
        )
        return True
    return False
