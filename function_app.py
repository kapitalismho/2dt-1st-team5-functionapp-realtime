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

app = func.FunctionApp()

# ============================================================================
# Phase 2.1: API 호출 모듈
# ============================================================================

def call_kma_api():
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
        "stn": "0",   # 모든 관측소
        "authKey": api_key
    }

    # 재시도 로직 설정 (Phase 2.2)
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,  # 2초, 4초, 8초
        status_forcelist=[500, 502, 503, 504],
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
    except requests.exceptions.RequestException as e:
        logging.error(f"API 호출 실패: {str(e)}", exc_info=True)
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
    connection_string = os.environ.get("EVENT_HUB_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("EVENT_HUB_CONNECTION_STRING environment variable is not set")

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


@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def realtime_weather_APIcall_timer_trigger(myTimer: func.TimerRequest) -> None:
    """
    실시간 날씨 데이터 수집 타이머 트리거 함수

    매 5분마다 실행되며:
    1. 기상청 API 호출
    2. CSV → JSON 변환
    3. 데이터 검증
    4. Event Hub 전송
    """
    correlation_id = str(uuid.uuid4())
    start_time = datetime.utcnow()

    log_structured("info", "함수 실행 시작",
                   correlation_id=correlation_id,
                   function_name="realtime_weather_APIcall_timer_trigger")

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

    except Exception as e:
        log_structured("error", f"함수 실행 실패: {str(e)}",
                       correlation_id=correlation_id,
                       error_type=type(e).__name__,
                       exc_info=True)
        raise