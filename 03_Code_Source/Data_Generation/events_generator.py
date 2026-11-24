import json, time, random, rstr
from datetime import datetime, timezone, timedelta
import pandas as pd
from cryptography.hazmat.asn1.asn1 import NoneType
from kafka import KafkaProducer
from pandas import DataFrame

location:DataFrame = pd.read_csv("../../04_Datasets/Historical_Data/atm_location_lookup.csv")
customer :DataFrame = pd.read_csv("../../04_Datasets/Historical_Data/customers_lookup.csv")


def extract_location(locationid: str):
    tmp= location[location['LocationID'] == locationid]
    if tmp.size == 0:
        return None
    return tmp

def extract_occupation(atm_id: str):
    temp= customer[customer['ATMID'] == atm_id]
    if temp.empty:
        return None
    return temp['Occupation'].iloc[0]
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode(encoding='utf-8')
)

def generate_event():
    prefix = rstr.xeger(r"(RI|FC|LA|EN|KN)")
    locationid = f"{prefix}-{random.randint(1, 10):03d}"
    location = extract_location(locationid)
    if location is not NoneType:

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
    return None

while True:

    event = generate_event()
    if event is not None:
        producer.send("transactions", value=event)
        print("Sent:", event)
        time.sleep(0.5)
