import json, time, random, rstr
from datetime import datetime, timezone, timedelta

from click import DateTime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_event():
    state:str=random.choice(["ENUGU", "KANO", "RIVERS","LAGOS","Federal Capital Territory"])
    year=datetime.now().year
    start=datetime.now().strftime("%m/%d/%Y %H:%M")
    date_obj = datetime.strptime(start, "%m/%d/%Y %H:%M")
    duration=random.randint(1,20)
    end=date_obj+timedelta(hours=duration)
    prefix=state[:2]
    return {
        # 1. Exact Regex Pattern (e.g., "ORD-2026-AB1234")
        "CardholderID":  f"{prefix}-{random.randint(0, 999):03d}-{random.randint(0, 9999):04d}",

        # 2. Custom Prefix + Zero-Padded Sequence/Random (e.g., "USR_00482")
        "TransactionTypeID": random.randint(1, 4),

        # 3. Weighted Enum / Specific Allowed Values
        "TransactionID": f"{prefix}-{year}-{random.randint(0, 999999999):010d}",

        # 4. Formatted Device/MAC/IP Pattern
        "TransactionStartDateTime": start,

        # 5. Constrained Numeric Precision
        "TransactionEndDateTime":  end,

        # 6. Strict ISO-8601 UTC Timestamp
        "Location Name": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",

        # 7. Nested Object Pattern
        "City": state[:1].upper() + state[1:]  ,
        "State": {
            "region": random.choice(["eu-west-1", "us-east-1", "af-south-1"]),
            "version": rstr.xeger(r"^v[1-3]\.\d\.\d$")
        },
        "duration_sec":"",
        "TransactionTypeName":"",
        "Gender":"",
        "ATMID":"",
        "Occupation":"",
        "AccountType":random.choices(["Current", "Savings"], weights=[0.4, 0.6])[0]
    }


while True:
    event = generate_event()
    producer.send("transactions", value=event)
    print("Sent:", event)
    time.sleep(0.5)
