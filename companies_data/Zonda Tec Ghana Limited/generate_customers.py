# Importing necessary libraries
import pandas as pd  
from faker import Faker  
import random

# Creating Faker instance
fake = Faker('tw_GH')

# Generating fake data for 100000 records
records = []
for i in range(100000):
    fake_records = {
        'customer_name': fake.name(),
        'address': fake.address(),
        'email': fake.email(),
        'phone_number': fake.phone_number(),
        'contact_preference': random.choice(['sms', 'email', 'call']),

        'transaction_activity': fake.random_int(min=0, max=100000),
        'customer_preference': random.choice(['App', 'Website']),
        'transaction_location': random.choice(['Tema', 'Accra', 'Nsawam'])
    }

    records.append(fake_records)

# Converting fake records into DataFrame
df = pd.DataFrame(records)

# Displaying and validating the DataFrame
print(df.head())
print(len(df))

# save the data
zonda_tec_fake_data = df.to_csv('Zonda_Tec_fake_customer_data.csv', index = False)

