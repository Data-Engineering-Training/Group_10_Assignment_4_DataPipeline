from faker import Faker
import pandas as pd
import random

fake = Faker('tw_GH')

# Generate 100,000 records
print("Generating 100,000 records of data. Please a moment...")
records = []
for _ in range(100000):
    individual = {
        'customer_name': fake.name(),
        'address': fake.address(),
        'email': fake.email(),
        'telephone': fake.phone_number(),
        'country': 'Ghana',
        'contact_preference': random.choice(['SMS', 'Email', 'Call']),
        'transaction_activity': fake.random_int(min=0, max=100000),
        'customer_preference': random.choice(['App', 'Website']),
        'communication_method': random.choice(['SMS', 'Email', 'Call'])
    }

    records.append(individual)

# convert records to a dataframe
data = pd.DataFrame(records)
# print len of data
print("Done!\nNumber of customer records: ", len(data))
# get an overview of the data
data.head()
# save the data
data.to_csv('star_assurance_ltd.csv', index=False)
