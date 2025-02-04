from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable


class BrokerError(Exception):
    pass


class QueueNotFoundError(BrokerError):
    def __init__(self, queue_name: str) -> None:
        super().__init__(f"queue not found: {queue_name!r}")


@dataclass(frozen=True)
class Message:
    body: Any
    message_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    enqueued_at: float = field(default_factory=time.time)
    delivery_count: int = 0

    def with_delivery(self) -> "Message":
        return Message(
            body=self.body,
            message_id=self.message_id,
            enqueued_at=self.enqueued_at,
            delivery_count=self.delivery_count + 1,
        )


@dataclass(frozen=True)
class Receipt:
    queue: str
    message: Message
    receipt_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])


VisibilityPolicy = Callable[[int], float]


def exponential_visibility(base_seconds: float = 2.0, maximum: float = 60.0) -> VisibilityPolicy:
    def policy(delivery_count: int) -> float:
        return min(maximum, base_seconds * (2 ** max(0, delivery_count - 1)))
    return policy


def fixed_visibility(seconds: float) -> VisibilityPolicy:
    def policy(delivery_count: int) -> float:
        return seconds
    return policy


class Queue:
    def __init__(self, name: str, visibility_policy: VisibilityPolicy | None = None,
                 max_retries: int = 3, dead_letter: "Queue | None" = None) -> None:
        self.name = name
        self._messages: list[Message] = []
        self._inflight: dict[str, tuple[Message, float]] = {}
        self._lock = threading.Lock()
        self._visibility = visibility_policy or fixed_visibility(30.0)
        self._max_retries = max_retries
        self.dead_letter = dead_letter

    def send(self, body: Any) -> Message:
        message = Message(body=body)
        with self._lock:
            self._messages.append(message)
        return message

    def _release_expired(self, now: float) -> None:
        expired_keys = [
            receipt_id for receipt_id, (message, visible_at)
            in self._inflight.items() if now >= visible_at
        ]
        for receipt_id in expired_keys:
            message, _ = self._inflight.pop(receipt_id)
            if message.delivery_count >= self._max_retries and self.dead_letter is not None:
                self.dead_letter.send(message.body)
                continue
            self._messages.append(message)

    def receive(self) -> Receipt | None:
        with self._lock:
            self._release_expired(time.monotonic())
            if not self._messages:
                return None
            original = self._messages.pop(0)
        delivered = original.with_delivery()
        timeout = self._visibility(delivered.delivery_count)
        with self._lock:
            self._inflight[str(uuid.uuid4())] = (delivered, time.monotonic() + timeout)
            receipt_id = list(self._inflight.keys())[-1]
        return Receipt(queue=self.name, message=delivered, receipt_id=receipt_id)

    def acknowledge(self, receipt: Receipt) -> bool:
        with self._lock:
            _, removed = self._inflight.pop(receipt.receipt_id, (None, None))
            return removed is not None

    def depth(self) -> int:
        with self._lock:
            self._release_expired(time.monotonic())
            return len(self._messages)

    def inflight_count(self) -> int:
        with self._lock:
            return len(self._inflight)


class MessageBroker:
    def __init__(self) -> None:
        self._queues: dict[str, Queue] = {}

    def declare_queue(self, name: str, visibility_policy: VisibilityPolicy | None = None,
                      max_retries: int = 3, dead_letter_name: str | None = None) -> Queue:
        dead_letter = (
            self.declare_queue(dead_letter_name) if dead_letter_name else None
        )
        existing = self._queues.get(name)
        if existing is not None:
            return existing
        queue = Queue(name, visibility_policy, max_retries, dead_letter)
        self._queues[name] = queue
        return queue

    def get_queue(self, name: str) -> Queue:
        queue = self._queues.get(name)
        if queue is None:
            raise QueueNotFoundError(name)
        return queue

    def publish(self, queue_name: str, body: Any) -> Message:
        queue = self.get_queue(queue_name)
        return queue.send(body)

    def consume(self, queue_name: str) -> Receipt | None:
        return self.get_queue(queue_name).receive()

    def acknowledge(self, receipt: Receipt) -> bool:
        queue = self._queues.get(receipt.queue)
        if queue is None:
            return False
        return queue.acknowledge(receipt)

    def inflight_count(self, queue_name: str) -> int:
        return self.get_queue(queue_name).inflight_count()

    def depth(self, queue_name: str) -> int:
        return self.get_queue(queue_name).depth()

    def queue_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._queues))
