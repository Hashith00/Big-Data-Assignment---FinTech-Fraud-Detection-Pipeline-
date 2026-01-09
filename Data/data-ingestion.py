import json
import random
import time
from datetime import datetime, timezone, timedelta
from uuid import uuid4

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "transactions"

# Tune these:
EVENTS_PER_SECOND = 2          # frequency control
FRAUD_RATE = 0.03              # 3% of events are fraud-ish (occasional)
IMPOSSIBLE_TRAVEL_RATE = 0.02  # 2% trigger "two countries within 10 mins"
HIGH_VALUE_RATE = 0.02         # 2% amount > 5000

MERCHANT_CATEGORIES = [
    "Grocery", "Electronics", "Fuel", "Travel", "Gaming", "Restaurants", "Pharmacy"
]

COUNTRIES = ["Sri Lanka", "India", "Singapore", "UAE", "UK", "Germany", "USA"]

# Track last country + timestamp per user to sometimes create "impossible travel"
last_seen = {}  # user_id -> (country, event_time)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def make_user_id():
    # Keep user IDs repeatable-ish so Spark can detect patterns per user
    return f"U{random.randint(100, 999)}"

def generate_normal_tx(user_id: str):
    country = random.choice(COUNTRIES[:3])  # bias to nearby/normal region (SL/India/Singapore)
    amount = round(random.uniform(5, 500), 2)
    return user_id, country, amount

def inject_high_value(amount):
    return round(random.uniform(5000.01, 12000), 2)

def generate_transaction():
    user_id = make_user_id()
    event_time = datetime.now(timezone.utc)

    # Base normal transaction
    user_id, country, amount = generate_normal_tx(user_id)
    merchant = random.choice(MERCHANT_CATEGORIES)

    # Decide if we inject a fraud pattern occasionally
    r = random.random()

    if r < HIGH_VALUE_RATE:
        amount = inject_high_value(amount)  # triggers "amount > 5000"
    elif r < HIGH_VALUE_RATE + IMPOSSIBLE_TRAVEL_RATE:
        # Try to force a country change within 10 minutes
        prev = last_seen.get(user_id)
        if prev:
            prev_country, prev_time = prev
            # Set current time within 10 minutes of prev_time
            event_time = prev_time + timedelta(minutes=random.randint(1, 9))
            # Force different country
            country = random.choice([c for c in COUNTRIES if c != prev_country])
        else:
            # If no history, just pick a far country so next tx can be made impossible
            country = random.choice(COUNTRIES[3:])
    # else: normal

    tx = {
        "transaction_id": str(uuid4()),
        "user_id": user_id,
        "timestamp": event_time.isoformat(),        # event-time timestamp
        "merchant_category": merchant,
        "amount": amount,
        "location": country
    }

    # update last seen
    last_seen[user_id] = (country, event_time)

    return tx

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    sleep_time = 1.0 / EVENTS_PER_SECOND

    print(f"Producing to topic '{TOPIC}' at ~{EVENTS_PER_SECOND} msg/sec ...")
    while True:
        tx = generate_transaction()
        producer.send(TOPIC, tx)
        print(tx)
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
