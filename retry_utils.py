from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Iterable

RETRY_DELAYS_SHORT = [1, 2, 4]
RETRY_DELAYS_LONG = [1, 2, 4, 8, 12]
RETRY_DELAYS_SEC = RETRY_DELAYS_LONG


async def retry_async(
    task: Callable[[], Awaitable[object]],
    *,
    delays: Iterable[int] | None = None,
    logger: logging.Logger | None = None,
    label: str = "send message",
    retry_exceptions: tuple[type[BaseException], ...] = (asyncio.TimeoutError,),
) -> bool:
    delays_list = list(delays or RETRY_DELAYS_LONG)
    for attempt, delay in enumerate(delays_list, start=1):
        try:
            await task()
            return True
        except retry_exceptions as exc:
            if logger:
                logger.warning("Failed to %s (attempt %d): %s", label, attempt, exc)
            await asyncio.sleep(delay)
        except Exception as exc:
            if logger:
                logger.warning("Failed to %s: %s", label, exc)
            return False
    return False
