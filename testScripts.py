import asyncio
import json
import time
import websockets

WS_URL = "ws://localhost:8080/api/v1/stream"

TEST_MESSAGES = [
    {
        "account_id": "ACC-99210",
        "destination_account_id": "ACC-11029",
        "amount": 25000.00,
        "currency": "USD",
        "timestamp": "2026-06-10T11:00:00Z",
        "ip_address": "197.34.22.105",
        "device_id": "dv_mac_88290",
    },
    {
        "account_id": "ACC-00123",
        "destination_account_id": "ACC-00456",
        "amount": 1500.00,
        "currency": "EUR",
        "timestamp": "2026-06-10T11:00:01Z",
        "ip_address": "41.20.11.200",
        "device_id": "dv_win_00321",
    },
    {
        "account_id": "ACC-77777",
        "destination_account_id": "ACC-88888",
        "amount": 99999.99,
        "currency": "USD",
        "timestamp": "2026-06-10T11:00:02Z",
        "ip_address": "102.55.66.77",
        "device_id": "dv_mob_55512",
    },
]


async def run_test():
    print(f"Connecting to {WS_URL} ...")

    try:
        async with websockets.connect(WS_URL) as ws:
            print(" Connected\n")

            for i, payload in enumerate(TEST_MESSAGES, 1):
                msg = json.dumps(payload)
                print(f"[{i}] Sending → {msg}")
                await ws.send(msg)

                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    print(f"[{i}] Received ← {response}")
                except asyncio.TimeoutError:
                    print(
                        f"[{i}] No response within 5s (server may be fire-and-forget)"
                    )

                await asyncio.sleep(0.5)

            print("\nAll messages sent. Check Kafka topic for produced records.")

    except ConnectionRefusedError:
        print("Connection refused — is the Go server running on port 8080?")
    except websockets.exceptions.InvalidURI as e:
        print(f"Invalid URI: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


def verify_kafka(
    topic: str = "transactions.raw",
    broker: str = "127.0.0.1:9092",
    timeout_s: float = 10.0,
):
    try:
        from confluent_kafka import Consumer, KafkaException
    except ImportError:
        print("\nconfluent-kafka not installed — skipping Kafka verification.")
        print("   Run: pip install confluent-kafka")
        return

    conf = {
        "bootstrap.servers": broker,
        "group.id": "test-verifier",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])
    print(f"\nListening on Kafka topic '{topic}' for {timeout_s}s …")

    deadline = time.time() + timeout_s
    received = 0

    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            received += 1
            print(f"  Kafka ← [{received}] {msg.value().decode()}")
    finally:
        consumer.close()

    if received:
        print(f"\nVerified {received} message(s) in Kafka.")
    else:
        print("\nNo messages found in Kafka within the timeout window.")


if __name__ == "__main__":
    asyncio.run(run_test())
    verify_kafka()
