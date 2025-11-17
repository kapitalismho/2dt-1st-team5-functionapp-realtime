import logging
import os
import json
import uuid
from datetime import datetime
import azure.functions as func
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.eventhub import EventHubProducerClient, EventData

# 추가 모듈 임포트
from circuit_breaker import get_circuit_breaker, CircuitBreakerOpenError
from data_cache import is_duplicate_data, should_send_duplicate, get_cache_stats

app = func.FunctionApp()

# ============================================================================
# Phase 2.1: API 호출 모듈
# ============================================================================

def _call_kma_api_internal():
    """
    기상청 API를 호출하여 CSV 데이터를 반환합니다.

    Returns:
        str: CSV 형식의 날씨 데이터

    Raises:
        requests.exceptions.RequestException: API 호출 실패 시
    """
    api_key = os.environ.get("KMA_API_KEY")
    if not api_key:
        raise ValueError("KMA_API_KEY environment variable is not set")

    # API URL 구성
    base_url = "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min"
    params = {
        "disp": "1",  # CSV 형식
        "help": "2",  # 데이터만 (헤더 제외)
        "stn": "98:99:102:108:112:119:201:202:203",  # 관측소 코드
        "authKey": api_key
    }

    # 재시도 로직 설정 (Phase 2.2)
    # Rate Limit (429) 및 서버 에러에 대한 재시도 추가
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,  # 2초, 4초, 8초
        status_forcelist=[429, 500, 502, 503, 504],  # 429 추가
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)

    try:
        response = session.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        logging.info(f"API 호출 성공: {len(response.text)} bytes 수신")
        return response.text
    except requests.exceptions.HTTPError as e:
        # HTTP 에러 상세 처리
        if e.response is not None:
            status_code = e.response.status_code
            if status_code == 429:
                # Rate Limit 에러
                retry_after = e.response.headers.get('Retry-After', '알 수 없음')
                logging.error(
                    f"⚠️ API Rate Limit 초과 (HTTP 429) - "
                    f"Retry-After: {retry_after}초. "
                    f"호출 빈도를 줄여야 합니다."
                )
            elif status_code in [500, 502, 503, 504]:
                logging.error(f"서버 에러 (HTTP {status_code}): {str(e)}")
            else:
                logging.error(f"HTTP 에러 (HTTP {status_code}): {str(e)}")
        raise
    except requests.exceptions.Timeout:
        logging.error("API 호출 타임아웃 (30초 초과)")
        raise
    except requests.exceptions.RequestException as e:
        logging.error(f"API 호출 실패: {str(e)}", exc_info=True)
        raise


def call_kma_api():
    """
    Circuit Breaker를 적용한 기상청 API 호출 래퍼 함수

    Returns:
        str: CSV 형식의 날씨 데이터

    Raises:
        CircuitBreakerOpenError: Circuit breaker가 열려있을 때
        requests.exceptions.RequestException: API 호출 실패 시
    """
    circuit_breaker = get_circuit_breaker()

    try:
        # Circuit breaker를 통해 API 호출
        return circuit_breaker.call(_call_kma_api_internal)
    except CircuitBreakerOpenError as e:
        # Circuit breaker가 열려있어 API 호출이 차단됨
        logging.error(f"Circuit breaker 차단: {str(e)}")
        raise


# ============================================================================
# Phase 2.3: 데이터 변환 모듈
# ============================================================================

def csv_to_batch_json(csv_text):
    """
    CSV 텍스트를 배치 JSON 형식으로 변환합니다.

    Args:
        csv_text (str): CSV 형식의 날씨 데이터

    Returns:
        dict: 배치 JSON 메시지
        {
            "collection_time": "YYMMDDHHMI",
            "station_count": int,
            "data": [...]
        }
    """
    lines = csv_text.strip().split('\n')
    if not lines:
        raise ValueError("CSV 데이터가 비어있습니다")

    data_list = []
    collection_time = None

    for line in lines:
        parts = line.strip().split(',')
        if len(parts) < 12:
            logging.warning(f"잘못된 CSV 행 (필드 수 부족): {line}")
            continue

        # 첫 번째 레코드에서 collection_time 추출
        if collection_time is None:
            collection_time = parts[0].strip()

        station_data = {
            "STN": parts[1].strip(),
            "TA": parts[2].strip(),
            "RE": parts[3].strip(),
            "RN-15m": parts[4].strip(),
            "RN-60m": parts[5].strip(),
            "RN-12H": parts[6].strip(),
            "RN-DAY": parts[7].strip(),
            "HM": parts[8].strip(),
            "PA": parts[9].strip(),
            "PS": parts[10].strip(),
            "TD": parts[11].strip()
        }
        data_list.append(station_data)

    message = {
        "collection_time": collection_time,
        "station_count": len(data_list),
        "data": data_list
    }

    logging.info(f"CSV 변환 완료: {len(data_list)}개 관측소")
    return message


# ============================================================================
# Phase 2.4: 데이터 검증 모듈
# ============================================================================

def validate_batch_message(message):
    """
    배치 메시지의 필수 필드를 검증합니다.

    Args:
        message (dict): 배치 JSON 메시지

    Returns:
        bool: 검증 성공 여부
    """
    if not message.get("collection_time"):
        logging.error("collection_time 필드 누락")
        return False

    valid_stations = []
    invalid_count = 0

    for station_data in message.get("data", []):
        if not station_data.get("STN"):
            logging.warning(f"STN 필드 누락: {station_data}")
            invalid_count += 1
            continue
        valid_stations.append(station_data)

    message["data"] = valid_stations
    message["station_count"] = len(valid_stations)

    if invalid_count > 0:
        logging.warning(f"검증 실패한 관측소 수: {invalid_count}")

    if len(valid_stations) == 0:
        logging.error("유효한 관측소 데이터가 없습니다")
        return False

    logging.info(f"검증 완료: {len(valid_stations)}개 관측소 유효")
    return True


# ============================================================================
# Phase 2.5: Event Hub 전송 모듈
# ============================================================================

def send_to_eventhub(message_data):
    """
    JSON 메시지를 Event Hub로 전송합니다.

    Args:
        message_data (dict): 전송할 JSON 메시지

    Raises:
        Exception: Event Hub 전송 실패 시
    """
    connection_string = os.environ.get("EventHubConnectionString")
    if not connection_string:
        raise ValueError("EventHubConnectionString environment variable is not set")

    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_string
        )

        with producer:
            event_data_batch = producer.create_batch()

            # JSON을 문자열로 직렬화
            message_json = json.dumps(message_data, ensure_ascii=False)
            message_size = len(message_json.encode('utf-8'))

            event_data_batch.add(EventData(message_json))
            producer.send_batch(event_data_batch)

            logging.info(f"Event Hub 전송 완료: 메시지 크기 {message_size / 1024:.2f}KB")

    except Exception as e:
        logging.error(f"Event Hub 전송 실패: {str(e)}", exc_info=True)
        raise


# ============================================================================
# Phase 3.1 & 3.2: 메인 함수 (구조화된 로깅 포함)
# ============================================================================

def log_structured(level, message, **kwargs):
    """구조화된 로그 출력"""
    log_data = {
        "message": message,
        "timestamp": datetime.utcnow().isoformat(),
        **kwargs
    }
    getattr(logging, level)(json.dumps(log_data, ensure_ascii=False))


@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def realtime_weather_APIcall_timer_trigger(myTimer: func.TimerRequest) -> None:
    """
    실시간 날씨 데이터 수집 타이머 트리거 함수

    매 1분마다 실행되며:
    1. Circuit Breaker 확인 (연속 실패 시 자동 차단)
    2. 기상청 API 호출 (Rate Limit 429 에러 처리 포함)
    3. CSV → JSON 변환
    4. 데이터 검증
    5. 중복 데이터 확인 (동일 데이터 반복 전송 방지)
    6. Event Hub 전송 (중복이 아닌 경우만)
    """
    correlation_id = str(uuid.uuid4())
    start_time = datetime.utcnow()

    # Circuit breaker 상태 확인
    circuit_breaker = get_circuit_breaker()
    cb_state = circuit_breaker.get_state()

    log_structured("info", "함수 실행 시작",
                   correlation_id=correlation_id,
                   function_name="realtime_weather_APIcall_timer_trigger",
                   circuit_breaker_state=cb_state["state"])

    if myTimer.past_due:
        log_structured("warning", "타이머가 예정 시간을 초과했습니다",
                       correlation_id=correlation_id)

    try:
        # 1. API 호출
        log_structured("info", "기상청 API 호출 중...",
                       correlation_id=correlation_id)
        csv_data = call_kma_api()

        # 2. 데이터 변환
        log_structured("info", "CSV → JSON 변환 중...",
                       correlation_id=correlation_id)
        message = csv_to_batch_json(csv_data)

        # 3. 데이터 검증
        log_structured("info", "데이터 검증 중...",
                       correlation_id=correlation_id)
        if not validate_batch_message(message):
            raise ValueError("데이터 검증 실패")

        # 3.5. 중복 데이터 체크
        log_structured("info", "중복 데이터 확인 중...",
                       correlation_id=correlation_id)
        is_duplicate, duplicate_details = is_duplicate_data(message)

        if is_duplicate:
            # 중복 데이터이지만 일정 횟수마다 강제 전송
            if should_send_duplicate(duplicate_details["duplicate_count"], max_skip=5):
                log_structured("warning",
                               "중복 데이터이지만 파이프라인 유지를 위해 전송합니다",
                               correlation_id=correlation_id,
                               duplicate_count=duplicate_details["duplicate_count"],
                               **duplicate_details)
            else:
                # 중복 데이터 - 전송 스킵
                log_structured("info",
                               "중복 데이터 감지 - Event Hub 전송 스킵",
                               correlation_id=correlation_id,
                               duplicate_count=duplicate_details["duplicate_count"],
                               collection_time=message["collection_time"],
                               station_count=message["station_count"])

                # 함수 성공 종료 (에러가 아님)
                end_time = datetime.utcnow()
                duration = (end_time - start_time).total_seconds()

                log_structured("info", "함수 실행 완료 (중복 데이터 - 전송 스킵됨)",
                               correlation_id=correlation_id,
                               duration_seconds=duration,
                               data_skipped=True)
                return

        # 4. Event Hub 전송
        log_structured("info", "Event Hub 전송 중...",
                       correlation_id=correlation_id)
        send_to_eventhub(message)

        # 성공 로깅
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        log_structured("info", "함수 실행 완료",
                       correlation_id=correlation_id,
                       station_count=message["station_count"],
                       duration_seconds=duration,
                       collection_time=message["collection_time"])

    except CircuitBreakerOpenError as e:
        # Circuit breaker가 열려있어 API 호출이 차단됨
        log_structured("error", f"Circuit Breaker 차단: {str(e)}",
                       correlation_id=correlation_id,
                       error_type="CircuitBreakerOpenError",
                       circuit_breaker_state=circuit_breaker.get_state())
        # Circuit breaker 에러는 재시도하지 않음 (복구 대기 중)
        raise

    except Exception as e:
        log_structured("error", f"함수 실행 실패: {str(e)}",
                       correlation_id=correlation_id,
                       error_type=type(e).__name__,
                       circuit_breaker_state=circuit_breaker.get_state(),
                       exc_info=True)
        raise