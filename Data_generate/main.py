# This code generates the required dataset sample for the tables
import csv
import random
import uuid
from datetime import datetime
from faker import Faker

fake = Faker()

# Generating customers.csv with 1000 records
customers = []
for i in range(1, 1001):
    customers.append({
        'id': i,
        'name': fake.company(),
        'active': random.choice([0, 1])
    })

with open('customers.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['id', 'name', 'active'])
    writer.writeheader()
    writer.writerows(customers)

# Generating ip_blacklist.csv with 1000 records
ip_blacklist = []
for _ in range(1000):
    ip_blacklist.append({'ip': fake.ipv4()})

with open('ip_blacklist.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['ip'])
    writer.writeheader()
    writer.writerows(ip_blacklist)

# Generating ua_blacklist.csv with 1000 records
ua_blacklist = []
for _ in range(1000):
    ua_blacklist.append({'ua': fake.user_agent()})

with open('ua_blacklist.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['ua'])
    writer.writeheader()
    writer.writerows(ua_blacklist)

# Generating requests.csv with 10000 records.
requests = []
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

for _ in range(10000):
    timestamp = fake.date_time_between(start_date=start_date, end_date=end_date)
    requests.append({
        'customerID': random.choice(range(1, 1001)),
        'tagID': random.randint(1, 100),
        'userID': str(uuid.uuid4()),
        'remoteIP': fake.ipv4(),
        'timestamp': int(timestamp.timestamp()),
        'userAgent': fake.user_agent()
    })

with open('requests.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['customerID', 'tagID', 'userID', 'remoteIP', 'timestamp', 'userAgent'])
    writer.writeheader()
    writer.writerows(requests)
