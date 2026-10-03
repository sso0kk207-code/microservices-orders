import time
from collections.abc import Callable


class CircuitBreaker:
    """Если сервис подряд отвечает ошибками, перестаём к нему ходить на cooldown секунд: быстрый отказ вместо ожидания таймаутов.

    closed -> (N ошибок подряд) -> open -> (cooldown прошёл) -> half-open: пропускаем один пробный запрос
    успех = closed, ошибка = снова open.
    """

    def __init__(self, threshold: int = 3, cooldown: float = 10.0, clock: Callable[[], float] = time.monotonic):
        self.threshold, self.cooldown, self.clock = threshold, cooldown, clock
        self.failures, self.opened_at = 0, None

    @property
    def state(self) -> str:
        if self.opened_at is None:
            return "closed"
        return "half-open" if self.clock() - self.opened_at >= self.cooldown else "open"

    def allow(self) -> bool:
        return self.state != "open"

    def success(self) -> None:
        self.failures, self.opened_at = 0, None

    def failure(self) -> None:
        self.failures += 1
        if self.failures >= self.threshold or self.state == "half-open":
            self.opened_at = self.clock()
