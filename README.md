# message-broker-lite

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A lightweight message broker with queue semantics: FIFO delivery, visibility timeouts, exponential redelivery backoff, and dead-letter routing — SQS patterns in-process, zero dependencies.

## 🚀 Overview

`message-broker-lite` implements the delivery guarantees background workers need. Consumers **receive** a message into an *in-flight* state (invisible to other consumers); only an explicit **acknowledge** removes it. Unacknowledged messages reappear after their **visibility timeout**, which grows exponentially per delivery attempt until `max_retries`, then the message routes to a dead-letter queue.

## ✨ Features

- **At-least-once delivery:** receive → process → acknowledge; crashes never lose work
- **Visibility timeouts:** fixed or exponential-backoff policies; pluggable as plain callables
- **Dead-letter queues:** poison messages escape after N attempts automatically
- **FIFO ordering:** strict queue order for waiting messages
- **Thread-safe:** per-queue locks around every mutation
- **Broker facade:** declare/publish/consume/acknowledge/depth from one object
- **Zero dependencies**

## 🚧 Structure

```
message-broker-lite/
├── src/message_broker/
│   ├── __init__.py
│   └── core.py
├── tests/
│   └── test_core.py
├── README.md
└── pyproject.toml
```

## 📦 Installation

```bash
git clone https://github.com/supremeloki/message-broker-lite.git
cd message-broker-lite
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
```

## 📋 Requirements

- Python 3.11+
- No runtime dependencies

## 🏃 Quick Start

```python
from message_broker import MessageBroker, exponential_visibility

broker = MessageBroker()
broker.declare_queue(
    "image-jobs",
    visibility_policy=exponential_visibility(base_seconds=2.0),
    max_retries=3,
    dead_letter_name="image-jobs-dlq",
)

broker.publish("image-jobs", {"file": "cat.jpg", "op": "resize"})
receipt = broker.consume("image-jobs")
try:
    process(receipt.message.body)
    broker.acknowledge(receipt)
except Exception:
    pass  # visibility timeout will redeliver automatically
```

## 🔧 Error Handling

```text
BrokerError
└── QueueNotFoundError   # publish/consume on an undeclared queue
```

## 🧪 Testing

```bash
pytest tests/ -v
```

## 📝 Code Quality

- Full type hints (`X | None` style), frozen messages/receipts
- Zero comments — names carry the meaning
- Delivery-count tracking drives retry policy without extra state

## 📄 License

MIT — see [LICENSE](LICENSE).

## 👤 Author

**Kooroush Masoumi**

---

⭐ Star this repo if you find it useful!
