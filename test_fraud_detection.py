import asyncio
import json
import time
import sys
import websockets

WS_URL = "ws://localhost:8080/api/v1/stream"

RUN_ID = str(int(time.time()))[-6:]

TEST_TRANSACTIONS = [
    {
        "label": "NORMAL — small amount, regular account",
        "payload": {
            "account_id": f"TEST-NORM1-{RUN_ID}",
            "destination_account_id": "ACC-00456",
            "amount": 45.00,
            "currency": "USD",
            "timestamp": "2026-06-10T12:00:00Z",
            "ip_address": "192.168.1.10",
            "device_id": "dv_web_normal_01",
        },
    },
    {
        "label": "NORMAL — moderate amount",
        "payload": {
            "account_id": f"TEST-NORM2-{RUN_ID}",
            "destination_account_id": "ACC-00789",
            "amount": 250.00,
            "currency": "EUR",
            "timestamp": "2026-06-10T12:00:01Z",
            "ip_address": "10.0.0.50",
            "device_id": "dv_mob_normal_02",
        },
    },
    {
        "label": "SUSPICIOUS — very high amount",
        "payload": {
            "account_id": f"TEST-SUS1-{RUN_ID}",
            "destination_account_id": "ACC-88888",
            "amount": 99999.99,
            "currency": "USD",
            "timestamp": "2026-06-10T12:00:02Z",
            "ip_address": "102.55.66.77",
            "device_id": "dv_mob_55512",
        },
    },
    {
        "label": "SUSPICIOUS — large transfer",
        "payload": {
            "account_id": f"TEST-SUS2-{RUN_ID}",
            "destination_account_id": "ACC-11029",
            "amount": 50000.00,
            "currency": "USD",
            "timestamp": "2026-06-10T12:00:03Z",
            "ip_address": "197.34.22.105",
            "device_id": "dv_mac_88290",
        },
    },
    {
        "label": "EDGE CASE — zero amount",
        "payload": {
            "account_id": f"TEST-EDGE-{RUN_ID}",
            "destination_account_id": "ACC-00001",
            "amount": 0.00,
            "currency": "USD",
            "timestamp": "2026-06-10T12:00:04Z",
            "ip_address": "127.0.0.1",
            "device_id": "dv_test_edge",
        },
    },
]

SEPARATOR = "=" * 70


def get_test_account_ids() -> set:
    return {t["payload"]["account_id"] for t in TEST_TRANSACTIONS}


def find_label_by_account(account_id: str) -> str:
    for t in TEST_TRANSACTIONS:
        if t["payload"]["account_id"] == account_id:
            return t["label"]
    return "UNKNOWN"


async def send_transactions():
    print(f"\n{SEPARATOR}")
    print("PHASE 1: Sending test transactions via WebSocket")
    print(SEPARATOR)
    print(f"Connecting to {WS_URL} ...")
    print(f"Run ID: {RUN_ID}\n")

    try:
        async with websockets.connect(WS_URL) as ws:
            print("✓ Connected to Go ingestion server\n")

            for i, txn in enumerate(TEST_TRANSACTIONS, 1):
                msg = json.dumps(txn["payload"])

                print(f"  [{i}] {txn['label']}")
                print(f"      Amount: ${txn['payload']['amount']:,.2f}")
                print(f"      Account: {txn['payload']['account_id']}")
                await ws.send(msg)

                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    print(f"      Server ACK: {response}")
                except asyncio.TimeoutError:
                    print(f"      (no ACK — fire-and-forget mode)")

                await asyncio.sleep(0.3)

            print(f"\n✓ Sent {len(TEST_TRANSACTIONS)} transactions")

    except ConnectionRefusedError:
        print("✗ Connection refused — is the Go server running on port 8080?")
        print("  Run: docker compose up -d")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        sys.exit(1)


def verify_fraud_scores(
    broker: str = "127.0.0.1:9092",
    timeout_s: float = 15.0,
):
    print(f"\n{SEPARATOR}")
    print("PHASE 2: Verifying ML fraud scores on 'transactions.evaluated'")
    print(SEPARATOR)

    try:
        from confluent_kafka import Consumer, KafkaException
    except ImportError:
        print("✗ confluent-kafka not installed")
        print("  Run: pip install confluent-kafka")
        return

    test_account_ids = get_test_account_ids()

    conf = {
        "bootstrap.servers": broker,
        "group.id": f"fraud-test-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    consumer.subscribe(["transactions.evaluated"])
    print(f"Listening on 'transactions.evaluated' for up to {timeout_s}s ...")
    print(f"Looking for account_ids: {test_account_ids}\n")

    deadline = time.time() + timeout_s
    results = []
    matched_ids = set()
    total_messages = 0

    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            total_messages += 1
            data = json.loads(msg.value().decode("utf-8"))
            account_id = data.get("account_id", "")

            if account_id not in test_account_ids:
                continue

            fraud_score = data.get("fraud_score")
            amount = data.get("amount", 0)
            label = find_label_by_account(account_id)

            results.append(
                {
                    "label": label,
                    "account_id": account_id,
                    "amount": amount,
                    "fraud_score": fraud_score,
                    "correlation_id": data.get("correlation_id", "?"),
                }
            )
            matched_ids.add(account_id)

            if matched_ids == test_account_ids:
                break

    finally:
        consumer.close()

    print(f"\n{SEPARATOR}")
    print("RESULTS")
    print(SEPARATOR)

    if not results:
        print(f"\n✗ NO scored transactions found for this test run (Run ID: {RUN_ID})")
        print(f"  (scanned {total_messages} total messages on the topic)")
        print("  This means the ML service is NOT processing transactions.")
        print("\n  Troubleshooting:")
        print("    1. Check ML service is running: docker compose ps")
        print("    2. Check ML logs: docker logs finguard-ml")
        print("    3. Check ML health: curl http://localhost:8000/health")
        print("    4. Verify Kafka connectivity from ML container")
        print("    5. Wait a few seconds and try again.")
        return

    print(f"\n{'Label':<40} {'Amount':>12} {'Fraud Score':>14} {'Verdict'}")
    print("-" * 85)

    for r in sorted(results, key=lambda x: x.get("fraud_score", 0), reverse=True):
        score = r["fraud_score"]
        if score is None:
            verdict = "⚠ MISSING SCORE"
        elif score >= 0.7:
            verdict = "🚨 FRAUD"
        elif score >= 0.4:
            verdict = "⚠ SUSPICIOUS"
        else:
            verdict = "✓ CLEAN"

        score_str = f"{score:.4f}" if score is not None else "N/A"
        print(f"  {r['label']:<38} ${r['amount']:>10,.2f} {score_str:>14} {verdict}")

    missing = test_account_ids - matched_ids
    print(f"\n{SEPARATOR}")
    print("SUMMARY")
    print(SEPARATOR)
    print(f"  Transactions sent:       {len(test_account_ids)}")
    print(f"  Transactions scored:     {len(results)}")
    print(f"  Transactions missing:    {len(missing)}")
    print(f"  Total messages on topic: {total_messages}")

    scores = [r["fraud_score"] for r in results if r["fraud_score"] is not None]
    if scores:
        print(f"  Score range:             {min(scores):.4f} — {max(scores):.4f}")

        all_same = len(set(f"{s:.4f}" for s in scores)) == 1
        if all_same and len(scores) > 1:
            print("\n  ⚠ WARNING: All fraud scores are identical!")
            print("    The model may not be differentiating between transactions.")
            if scores[0] == 0.5:
                print("    Score of 0.5 = fallback default (model is throwing exceptions).")
                print("    Check ML logs: docker logs finguard-ml")

        low_scores = [r["fraud_score"] for r in results if r["amount"] < 1000 and r["fraud_score"] is not None]
        high_scores = [r["fraud_score"] for r in results if r["amount"] >= 10000 and r["fraud_score"] is not None]

        if low_scores and high_scores:
            avg_low = sum(low_scores) / len(low_scores)
            avg_high = sum(high_scores) / len(high_scores)
            print(f"\n  Model differentiation analysis:")
            print(f"    Avg score (amount < $1,000):    {avg_low:.4f}")
            print(f"    Avg score (amount >= $10,000):   {avg_high:.4f}")

            if abs(avg_high - avg_low) < 0.01:
                print("\n  ⚠ WARNING: The model scores high and low amounts nearly the same.")
                print("    The fraud model may not be using 'amount' as a useful signal,")
                print("    or the feature vector may not be constructed correctly.")

    if missing:
        print(f"\n  ⚠ These account IDs were never scored:")
        for aid in missing:
            print(f"    - {aid}")

    print()


def verify_raw_kafka(
    broker: str = "127.0.0.1:9092",
    timeout_s: float = 5.0,
):
    print(f"\n{SEPARATOR}")
    print("PHASE 1.5: Quick check on 'transactions.raw'")
    print(SEPARATOR)

    try:
        from confluent_kafka import Consumer, KafkaException
    except ImportError:
        print("✗ confluent-kafka not installed — skipping")
        return

    test_account_ids = get_test_account_ids()

    conf = {
        "bootstrap.servers": broker,
        "group.id": f"raw-test-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    consumer.subscribe(["transactions.raw"])

    deadline = time.time() + timeout_s
    count = 0
    our_count = 0

    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                break
            count += 1
            try:
                data = json.loads(msg.value().decode("utf-8"))
                if data.get("account_id") in test_account_ids:
                    our_count += 1
            except Exception:
                pass
    finally:
        consumer.close()

    if count > 0:
        print(f"  ✓ Found {count} total message(s) on 'transactions.raw'")
        print(f"    Of which {our_count} belong to this test run (Run ID: {RUN_ID})")
        print(f"    Go server → Kafka ingestion is working.")
    else:
        print(f"  ✗ No messages found on 'transactions.raw'")
        print(f"    The Go server may not be producing to Kafka.")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("  FINGUARD — END-TO-END FRAUD DETECTION VERIFICATION")
    print("=" * 70)

    asyncio.run(send_transactions())
    verify_raw_kafka()

    print(f"\n  ⏳ Waiting 5s for ML service to process transactions...")
    time.sleep(5)

    verify_fraud_scores()
