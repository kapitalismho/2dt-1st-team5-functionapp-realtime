"""
Circuit Breaker 패턴 구현

API 호출이 연속으로 실패할 경우 일정 시간 동안 호출을 중단하여
시스템 과부하를 방지하고 복구 시간을 제공합니다.

상태:
- CLOSED: 정상 작동 (API 호출 허용)
- OPEN: 회로 차단 (API 호출 차단, 빠른 실패 반환)
- HALF_OPEN: 테스트 상태 (제한적 API 호출 허용, 복구 확인)
"""

import logging
from datetime import datetime, timedelta
from enum import Enum


class CircuitState(Enum):
    """Circuit Breaker 상태"""
    CLOSED = "CLOSED"         # 정상 작동
    OPEN = "OPEN"             # 회로 차단
    HALF_OPEN = "HALF_OPEN"   # 테스트 상태


class CircuitBreaker:
    """
    Circuit Breaker 패턴 구현

    연속 실패 횟수가 임계값을 초과하면 회로를 차단(OPEN)하고,
    일정 시간 후 복구를 시도합니다(HALF_OPEN).
    """

    def __init__(self, failure_threshold=5, timeout_seconds=300, half_open_max_calls=3):
        """
        Circuit Breaker 초기화

        Args:
            failure_threshold (int): 회로를 차단할 연속 실패 횟수 (기본값: 5)
            timeout_seconds (int): 회로가 열린 후 복구를 시도할 시간(초) (기본값: 300 = 5분)
            half_open_max_calls (int): HALF_OPEN 상태에서 허용할 최대 호출 횟수 (기본값: 3)
        """
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.half_open_max_calls = half_open_max_calls

        # 상태 관리
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.opened_time = None
        self.half_open_calls = 0

    def call(self, func, *args, **kwargs):
        """
        Circuit Breaker를 통해 함수를 호출합니다.

        Args:
            func: 호출할 함수
            *args, **kwargs: 함수에 전달할 인자

        Returns:
            함수 실행 결과

        Raises:
            CircuitBreakerOpenError: 회로가 열려있을 때
            원본 함수의 예외: 함수 실행 실패 시
        """
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._transition_to_half_open()
            else:
                raise CircuitBreakerOpenError(
                    f"Circuit breaker is OPEN. "
                    f"연속 실패: {self.failure_count}회. "
                    f"다음 재시도: {self._get_next_retry_time()}"
                )

        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_calls >= self.half_open_max_calls:
                raise CircuitBreakerOpenError(
                    f"Circuit breaker HALF_OPEN 상태에서 최대 호출 횟수 초과"
                )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result

        except Exception as e:
            self._on_failure(e)
            raise

    def _on_success(self):
        """호출 성공 시 처리"""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            self.half_open_calls += 1

            logging.info(
                f"Circuit breaker HALF_OPEN 성공: {self.success_count}/{self.half_open_max_calls}"
            )

            # 충분한 성공 후 회로를 닫음
            if self.success_count >= self.half_open_max_calls:
                self._transition_to_closed()

        elif self.state == CircuitState.CLOSED:
            # 정상 상태에서 성공 - 실패 카운트 초기화
            if self.failure_count > 0:
                logging.info(
                    f"Circuit breaker: 이전 {self.failure_count}회 실패 후 성공 - 카운터 초기화"
                )
                self.failure_count = 0
                self.last_failure_time = None

    def _on_failure(self, exception):
        """호출 실패 시 처리"""
        self.last_failure_time = datetime.utcnow()

        if self.state == CircuitState.HALF_OPEN:
            # HALF_OPEN 상태에서 실패하면 다시 OPEN으로
            self.half_open_calls += 1
            logging.warning(
                f"Circuit breaker HALF_OPEN 실패 - OPEN으로 전환: {str(exception)}"
            )
            self._transition_to_open()

        elif self.state == CircuitState.CLOSED:
            self.failure_count += 1
            logging.warning(
                f"Circuit breaker 실패 카운트: {self.failure_count}/{self.failure_threshold}"
            )

            # 임계값 초과 시 회로 차단
            if self.failure_count >= self.failure_threshold:
                self._transition_to_open()

    def _should_attempt_reset(self):
        """회로 복구를 시도할 시간인지 확인"""
        if self.opened_time is None:
            return True

        elapsed = (datetime.utcnow() - self.opened_time).total_seconds()
        return elapsed >= self.timeout_seconds

    def _get_next_retry_time(self):
        """다음 재시도 가능 시간 반환"""
        if self.opened_time is None:
            return "즉시"

        next_retry = self.opened_time + timedelta(seconds=self.timeout_seconds)
        remaining = (next_retry - datetime.utcnow()).total_seconds()

        if remaining <= 0:
            return "즉시"

        return f"{int(remaining)}초 후"

    def _transition_to_open(self):
        """OPEN 상태로 전환"""
        self.state = CircuitState.OPEN
        self.opened_time = datetime.utcnow()

        logging.error(
            f"⚠️ Circuit breaker OPEN - API 호출 중단 "
            f"(연속 실패: {self.failure_count}회, "
            f"복구 시도: {self.timeout_seconds}초 후)"
        )

    def _transition_to_half_open(self):
        """HALF_OPEN 상태로 전환"""
        self.state = CircuitState.HALF_OPEN
        self.half_open_calls = 0
        self.success_count = 0

        logging.info(
            f"Circuit breaker HALF_OPEN - 복구 테스트 시작 "
            f"(최대 {self.half_open_max_calls}회 시도)"
        )

    def _transition_to_closed(self):
        """CLOSED 상태로 전환"""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.opened_time = None
        self.half_open_calls = 0

        logging.info("✅ Circuit breaker CLOSED - 정상 작동 복구")

    def get_state(self):
        """
        현재 Circuit Breaker 상태 반환

        Returns:
            dict: 상태 정보
        """
        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "failure_threshold": self.failure_threshold,
            "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None,
            "opened_time": self.opened_time.isoformat() if self.opened_time else None,
            "next_retry": self._get_next_retry_time() if self.state == CircuitState.OPEN else None,
            "half_open_calls": self.half_open_calls if self.state == CircuitState.HALF_OPEN else 0
        }

    def reset(self):
        """Circuit Breaker 강제 리셋 (테스트/관리 목적)"""
        logging.warning("Circuit breaker 강제 리셋")
        self._transition_to_closed()


class CircuitBreakerOpenError(Exception):
    """Circuit Breaker가 OPEN 상태일 때 발생하는 예외"""
    pass


# 전역 Circuit Breaker 인스턴스
# Azure Functions는 인스턴스 간 상태를 공유하지 않으므로
# 각 인스턴스마다 독립적인 Circuit Breaker가 생성됩니다.
_circuit_breaker = CircuitBreaker(
    failure_threshold=5,      # 5회 연속 실패 시 차단
    timeout_seconds=300,      # 5분 후 복구 시도
    half_open_max_calls=3     # 복구 테스트 시 3회 시도
)


def get_circuit_breaker():
    """전역 Circuit Breaker 인스턴스 반환"""
    return _circuit_breaker
