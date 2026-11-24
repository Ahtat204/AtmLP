import json, time, random, rstr
from datetime import datetime, timezone, timedelta

from utils import location, extract_location, extract_occupation
from faker import Faker
from kafka import KafkaProducer

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)



def generate_event():
    prefix = rstr.xeger(r"(RI|FC|LA|EN|KN)")
    locationid = f"{prefix}-{random.randint(0, 10):03d}"
    location = extract_location(locationid)
    state=location["State"].to_string(index=False)
    ##state:str=random.choice(["ENUGU", "KANO", "RIVERS","LAGOS","Federal Capital Territory"])
    year=datetime.now().year
    start=datetime.now().strftime("%m/%d/%Y %H:%M")
    date_obj = datetime.strptime(start, "%m/%d/%Y %H:%M")
    duration=random.randint(1,20)
    end_date=(date_obj+timedelta(minutes=duration))
    end = end_date.strftime("%m/%d/%Y %H:%M")
    tr_type=random.choice(["Withdrawal","Deposit","Balance Inquiry","Transfer"])
    gender=random.choice(["M","F"])
    atm_id=f"{prefix}-{random.randint(0, 15):03d}"
    return {
        "CardholderID":  f"{prefix}-{random.randint(0, 999):03d}-{random.randint(0, 9999):04d}",
        "TransactionTypeID": random.randint(1, 4),
        "TransactionID": f"{prefix}-{year}-{random.randint(0, 999999999):010d}",
        "TransactionStartDateTime": start,
        "TransactionEndDateTime":  end,
        "TransactionAmount": random.randint(0,9)*10**random.randint(2,9),
        "Location Name": location["Location Name"].to_string(index=False),
        "City": location["City"].to_string(index=False)  ,
        "State": state,
        "duration_sec":duration,
        "TransactionTypeName":tr_type,
        "Gender":gender,
        "ATMID":atm_id,
        "Occupation":extract_occupation(atm_id),
        "AccountType":random.choices(["Current", "Savings"], weights=[0.4, 0.6])[0]
    }


while True:

    event = generate_event()
    producer.send("transactions", value=event)
    print("Sent:", event)
    time.sleep(0.5)
