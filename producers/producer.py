
import os
import json
import time
import uuid
import random
from datetime import datetime, timezone
from collections import deque
from confluent_kafka import Producer

TOPIC = "banking_transactions"
DELAY_SECONDS = 0.5
ANOMALY_SCENARIO_RATE = 0.10

producer = Producer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"]
})

CUSTOMERS = {
    f"CUST{i:04d}": {
        "home_city": random.choice([
            "Hyderabad", "Mumbai", "Bangalore", "Chennai"
        ]),
        "usual_amount": random.randint(500, 5000)
    }
    for i in range(1, 101)
}

MERCHANTS = [
    "Amazon", "Flipkart", "Swiggy",
    "Zomato", "DMart", "Reliance Digital"
]

pending_transactions = deque()


def current_timestamp():
    return datetime.now(timezone.utc).isoformat()


def create_transaction(customer_id=None):
    customer_id = customer_id or random.choice(list(CUSTOMERS))
    customer = CUSTOMERS[customer_id]

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "timestamp": current_timestamp(),
        "amount": round(
            random.uniform(100, customer["usual_amount"] * 1.5), 2
        ),
        "currency": "INR",
        "merchant": random.choice(MERCHANTS),
        "location": customer["home_city"],
        "status": "SUCCESS"
    }


def normal_transaction():
    return [create_transaction()]


def large_transaction():
    tx = create_transaction()
    tx["amount"] = round(random.uniform(100000, 500000), 2)
    return [tx]


def rapid_transactions():
    customer_id = random.choice(list(CUSTOMERS))
    transactions = []

    for _ in range(8):
        tx = create_transaction(customer_id)
        tx["amount"] = round(random.uniform(1000, 15000), 2)
        transactions.append(tx)

    return transactions


def distant_location_transactions():
    customer_id = random.choice(list(CUSTOMERS))

    tx1 = create_transaction(customer_id)
    tx2 = create_transaction(customer_id)

    tx1["location"] = "Hyderabad"
    tx2["location"] = "London"

    return [tx1, tx2]


def repeated_declines():
    customer_id = random.choice(list(CUSTOMERS))
    transactions = []

    for i in range(6):
        tx = create_transaction(customer_id)
        tx["status"] = "DECLINED" if i < 5 else "SUCCESS"
        transactions.append(tx)

    return transactions


def unusual_transaction():
    tx = create_transaction()
    tx["amount"] = round(random.uniform(50000, 200000), 2)
    tx["location"] = random.choice([
        "London", "Dubai", "Singapore"
    ])
    return [tx]


SCENARIOS = [
    large_transaction,
    rapid_transactions,
    distant_location_transactions,
    repeated_declines,
    unusual_transaction
]


def delivery_callback(err, msg):
    if err:
        print(f"Delivery failed: {err}")


def publish(tx):
    # Set the actual event time when the record is published.
    tx["timestamp"] = current_timestamp()

    payload = json.dumps(tx).encode("utf-8")

    while True:
        try:
            producer.produce(
                topic=TOPIC,
                key=tx["customer_id"].encode("utf-8"),
                value=payload,
                callback=delivery_callback
            )
            break
        except BufferError:
            producer.poll(1)

    producer.poll(0)
    print(json.dumps(tx))


def main():
    print("Starting banking transaction producer...")

    try:
        while True:
            if not pending_transactions:
                if random.random() < ANOMALY_SCENARIO_RATE:
                    scenario = random.choice(SCENARIOS)
                    pending_transactions.extend(scenario())
                else:
                    pending_transactions.extend(normal_transaction())

            transaction = pending_transactions.popleft()
            publish(transaction)

            time.sleep(DELAY_SECONDS)

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush(30)


if __name__ == "__main__":
    main()
