import random

import pandas as pd
import rstr

location = pd.read_csv("../../04_Datasets/Historical_Data/atm_location_lookup.csv")
customer = pd.read_csv("../../04_Datasets/Historical_Data/customers_lookup.csv")


def extract_location(locationid: str):
    return location[location['LocationID'] == locationid]


def extract_occupation(atm_id: str):
    temp= customer[customer['ATMID'] == atm_id]
    if temp.empty:
        return None
    return temp['Occupation'].iloc[0]


# prefix = rstr.xeger(r"(RI|FC|LA|EN|KN)")
# locationId = f"{prefix}-{random.randint(0, 10):03d}"
# print(locationId)
# print(extract_location(locationId))
