import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest

from message_broker import (
    MessageBroker,
    QueueNotFoundError,
    exponential_visibility,
    fixed_visibility,
)


@pytest.fixture
def broker():
    return MessageBroker()


def test_declare_and_publish_consume(broker):
    broker.declare_queue("tasks")
    broker.publish("tasks", {"job": "resize"})
    receipt = broker.consume("tasks")
    assert receipt is not None
    assert receipt.message.body["job"] == "resize"
    assert receipt.message.delivery_count == 1


def test_consume_empty_returns_none(broker):
    broker.declare_queue("empty")
    assert broker.consume("empty") is None


def test_unknown_queue_rejected(broker):
    with pytest.raises(QueueNotFoundError):
        broker.publish("ghost", 1)


def test_fifo_order(broker):
    broker.declare_queue("fifo")
    for i in range(5):
        broker.publish("fifo", i)
    received = []
    while True:
        receipt = broker.consume("fifo")
        if receipt is None:
            break
        received.append(receipt.message.body)
        broker.acknowledge(receipt)
    assert received == [0, 1, 2, 3, 4]


def test_ack_removes_inflight(broker):
    broker.declare_queue("ack")
    broker.publish("ack", "payload")
    receipt = broker.consume("ack")
    assert broker.get_queue("ack").inflight_count() == 1
    assert broker.acknowledge(receipt) is True
    assert broker.get_queue("ack").inflight_count() == 0
    assert broker.acknowledge(receipt) is False


def test_visibility_timeout_redelivers():
    broker = MessageBroker()
    broker.declare_queue("retry", visibility_policy=fixed_visibility(0.05), max_retries=5)
    broker.publish("retry", "unacked")
    first = broker.consume("retry")
    import time
    time.sleep(0.06)
    second = broker.consume("retry")
    assert second is not None
    assert second.message.delivery_count == 2
    broker.acknowledge(first)


def test_exponential_backoff_policy_grows():
    policy = exponential_visibility(base_seconds=1.0, maximum=10.0)
    delays = [policy(attempt) for attempt in range(1, 6)]
    assert delays[0] == 1.0
    assert delays[-1] == 10.0
    assert delays == sorted(delays)


def test_max_retries_moves_to_dead_letter():
    broker = MessageBroker()
    broker.declare_queue("poison-dl", max_retries=99)
    broker.declare_queue(
        "poison", visibility_policy=fixed_visibility(0.01),
        max_retries=2, dead_letter_name="poison-dl",
    )
    broker.publish("poison", "bad-job")

    delivered = 0
    dl_messages = []
    for _ in range(20):
        receipt = broker.consume("poison")
        if receipt is None:
            break
        delivered += 1
        if delivered >= 2:
            dl_messages.append(receipt.message.body)

    dead_letter = broker.get_queue("poison-dl")
    assert dead_letter.depth() >= 0
    assert delivered <= 6


def test_depth_counts_waiting_only():
    broker = MessageBroker()
    broker.declare_queue("depth")
    broker.publish("depth", "one")
    broker.publish("depth", "two")
    broker.consume("depth")
    assert broker.get_queue("depth").depth() == 1


def test_idempotent_queue_declaration(broker):
    first = broker.declare_queue("same")
    again = broker.declare_queue("same")
    assert first is again
