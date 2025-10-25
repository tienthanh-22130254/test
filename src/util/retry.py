import time
import random
import functools
from typing import Callable, Iterable, Tuple, Optional, Type


def retry(
    exceptions: Tuple[Type[BaseException], ...],
    tries: int = 5,
    delay: float = 1.0,
    backoff: float = 2.0,
    jitter: float = 0.3,
    timeout: Optional[float] = 15.0,
    on_backoff: Optional[Callable[[int, BaseException], None]] = None,
):
    """
    Decorator for retrying a function call with exponential backoff and jitter.

    Parameters:
    - exceptions: tuple of exception classes to trigger retry
    - tries: max tries (including the first attempt)
    - delay: initial delay between retries (seconds)
    - backoff: exponential multiplier for delay
    - jitter: random jitter added to delay [0, jitter]
    - timeout: if provided and not present in kwargs, injects timeout=<timeout>
    - on_backoff: optional callback, called as on_backoff(attempt_number, exception)
    """

    if tries < 1:
        raise ValueError("tries must be >= 1")
    if delay < 0:
        raise ValueError("delay must be >= 0")
    if backoff < 1:
        raise ValueError("backoff must be >= 1")
    if jitter < 0:
        raise ValueError("jitter must be >= 0")

    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            attempt = 1
            last_exc: Optional[BaseException] = None

            while attempt <= tries:
                try:
                    call_kwargs = dict(kwargs)
                    if timeout is not None and "timeout" not in call_kwargs:
                        call_kwargs["timeout"] = timeout
                    return func(*args, **call_kwargs)
                except exceptions as exc:  # type: ignore[misc]
                    last_exc = exc
                    if attempt == tries:
                        raise
                    if on_backoff:
                        try:
                            on_backoff(attempt, exc)
                        except Exception:
                            # Don't let callback errors break retry
                            pass
                    sleep_for = current_delay + (random.uniform(0, jitter) if jitter else 0.0)
                    time.sleep(max(0.0, sleep_for))
                    current_delay *= backoff
                    attempt += 1
            # Should not reach here
            if last_exc:
                raise last_exc
        return wrapper
    return decorator