"""
    Generating Faker Data and Ingesting It into Database
    by : John Tamakloe
"""

import csv
import pandas 
import psycopg2
from tqdm import tqdm
from faker import Faker
from random import choice, randint


def main():
    """Main function"""
    gen_data()


def gen_data(amount:int = 100000, country_code: str = 'tw_GH'):
    """Generates Fake Customer Data
    Args:
        amount:int : This is the total data to be generated
        country_code:str : This is the local country to generate data for
        
    Returns:
        csv file of the generated data
    """
    fake = Faker(country_code)
    Faker.seed(10) # this allows for generating same data 
    #Generator Expressions are used for generating data for performance boost
    name = (fake.name() for _ in range(amount))
    email = (fake.email() for _ in range(amount))
    phone = (fake.phone_number() for _ in range(amount))
    address = ((fake.address()).replace('\n','') for _ in range(amount)) # the replace method removes weird newlines from address
    country = ('Ghana' for _ in range(amount))
    contact_preference = (choice(['Email', 'SMS', 'Call']) for _ in range(amount))
    transaction_activity = (randint(1,80) for _ in range(amount))
    customer_preference = (choice(['App', 'Website']) for _ in range(amount))

    # combining data 
    data = zip(name, email, phone, address, country,
               contact_preference, transaction_activity,
               customer_preference)
    
    # Writing Data to File
    with open('tropical_cable.csv', mode='w+t',encoding = 'utf-8',newline='') as file:
        csv_writer = csv.writer(file)
        try:
            for _,item in data:
                csv_writer.writerow(item)
        except Exception as e:
            print(f'Error Writing to file: {e}')


# Ingesting Data Into Database
def ingest_data(data_file, **kwargs):
    """Ingests Data Into a dedicated database
    Args:
        data_file:str: realpath of data to ingest into database
        **kwargs:{str:str}: login credentials of database
    """
    pass


#runs when file is runned directly in terminal
if __name__ == '__main__':
    main()