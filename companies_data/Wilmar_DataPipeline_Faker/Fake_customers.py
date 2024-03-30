# Importing necessary libraries
import pandas as pd  
from faker import Faker  
import random

# Creating Faker instance
fake = Faker('tw_GH')

# Generating fake data for 100000 records
records = []
for _ in range(100000):
    fake_records = {
        'customer_name': fake.name(),
        'address': fake.address(),
        'email': fake.email(),
        'telephone': fake.phone_number(),
        'country': 'Ghana',
        'contact_preference': random.choice(['sms', 'email', 'call']),

        # Example transaction activity
        'transaction_activity': fake.random_int(min=0, max=100),
        'customer_preference': random.choice(['App', 'Website']),

        # Default communication method
        'communication_method': random.choice(['SMS', 'Email', 'Call'])

    }

    records.append(fake_records)

# Converting fake records into DataFrame
df = pd.DataFrame(records)

# Displaying and validating the DataFrame
print(df.head())
print(len(df))

# save the data
Wilmar_Fake_data = df.to_csv('WilmarGH_fakedata.csv', index = False)

