import random
from faker import Faker
import psycopg2
import pandas as pd


class DataPipeline:
    def __init__(self, dbname, user, password, host):
        """
        Initializes DataPipeline object with database credentials.

        Parameters:
            dbname (str): Name of the database.
            user (str): Username for database authentication.
            password (str): Password for database authentication.
            host (str): Hostname where the database is hosted.
        """
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host

    def create_connection(self):
        """
        Creates a connection to the PostgreSQL database.

        Returns:
            psycopg2.extensions.connection: Connection object.
        """
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host
        )

    def create_table(self, cursor):
        """
        Creates 'customers' table in the database if not exists.

        Parameters:
            cursor (psycopg2.extensions.cursor): Database cursor object.
        """
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                id SERIAL PRIMARY KEY,
                customer_id VARCHAR(10),
                name VARCHAR(100),
                address TEXT,
                email VARCHAR(100),
                telephone VARCHAR(20),
                contact_preference VARCHAR(10),
                transaction_activity INTEGER,
                customer_preference VARCHAR(10),
                communication_method VARCHAR(10)
            )
        ''')

    def ingest_data(self, cursor, records):
        """
        Ingests data into the 'customers' table.

        Parameters:
            cursor (psycopg2.extensions.cursor): Database cursor object.
            records (list): List of tuples containing customer records.
        """
        for record in records:
            cursor.execute('''
                INSERT INTO customers (customer_id, name, address, email, telephone, contact_preference, transaction_activity, customer_preference, communication_method)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', record)

    def run_pipeline(self, records):
        """
        Runs the data pipeline to create table and ingest data.

        Parameters:
            records (list): List of tuples containing customer records.
        """
        conn = self.create_connection()
        cursor = conn.cursor()

        try:
            self.create_table(cursor)
            self.ingest_data(cursor, records)
            conn.commit()
            print("Data ingestion successful.")
        except Exception as e:
            conn.rollback()
            print(f"Error during data ingestion: {str(e)}")
        finally:
            cursor.close()
            conn.close()


def generate_records():
    """
    Generates fake customer records using Faker library.

    Returns:
        list: List of tuples containing customer records.
    """
    fake = Faker('tw_GH')
    records = []
    for _ in range(100000):
        customer_id = f"NWI{fake.random_number(digits=6)}"
        name = fake.name()
        address = fake.address()
        email = fake.email()
        telephone = fake.phone_number()
        contact_preference = random.choice(['SMS', 'Email', 'Call'])
        transaction_activity = fake.random_int(min=0, max=100000)
        customer_preference = random.choice(['App', 'Website'])
        communication_method = random.choice(['SMS', 'Email', 'Call'])

        records.append((customer_id, name, address, email, telephone, contact_preference,
                       transaction_activity, customer_preference, communication_method))

        # # incase you want to save the customer record to a csv or parquet, etc and return it. you uncomment this code to do that
        # #convert records into a data frame
        # df = pd.DataFrame(records, columns=['customer_id', 'name', 'address', 'email', 'telephone', 'contact_preference', 'transaction_activity', 'customer_preference', 'communication_method'])
        # #save the data frame into a csv file in in the companies_data folder
        # df.to_csv('customer_record.csv', index=False)

    return records


if __name__ == "__main__":
    # now, provide database credentials
    dbname = "your database name"
    user = "your username"
    password = "your password"
    host = "localhost"

    # Generate records
    records = generate_records()

    # Initialize pipeline and run
    pipeline = DataPipeline(dbname, user, password, host)
    pipeline.run_pipeline(records)
