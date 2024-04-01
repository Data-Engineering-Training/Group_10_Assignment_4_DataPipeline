import random
from faker import Faker
import csv
import psycopg2

print('Generating data...\nPlease wait a moment...')


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

    def create_table(self, cursor, company_name):
        """
        Creates a table for the specified company in the database if not exists.

        Parameters:
            cursor (psycopg2.extensions.cursor): Database cursor object.
            company_name (str): Name of the company.
        """
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {company_name.lower().replace(' ', '_')} (
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

    def ingest_data(self, cursor, company_name, records):
        """
        Ingests data into the table for the specified company.

        Parameters:
            cursor (psycopg2.extensions.cursor): Database cursor object.
            company_name (str): Name of the company.
            records (list): List of tuples containing customer records for the company.
        """
        for record in records:
            cursor.execute(f'''
                INSERT INTO {company_name.lower().replace(' ', '_')} (customer_id, name, address, email, telephone, contact_preference, transaction_activity, customer_preference, communication_method)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', record)

    def run_pipeline(self, company_data):
        """
        Runs the data pipeline to create tables, ingest data, and save to CSV files.

        Parameters:
            company_data (dict): Dictionary containing company names as keys and their respective records as values.
        """
        conn = self.create_connection()
        cursor = conn.cursor()

        try:
            for company_name, records in company_data.items():
                self.create_table(cursor, company_name)
                self.ingest_data(cursor, company_name, records)
                self.save_to_csv(company_name, records)
            conn.commit()
            print("Data ingestion and CSV export successful\nDone!.")
        except Exception as e:
            conn.rollback()
            print(f"Error during data ingestion: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    def save_to_csv(self, company_name, company_records):
        """
        Saves data for a company into a CSV file.

        Parameters:
            company_name (str): Name of the company.
            company_records (list): List of tuples containing customer records for the company.
        """
        # you can change the folder path to your desired location
        filename = f"Group_10_Assignment_4_DataPipeline/companies_data/{
            company_name.lower().replace(' ', '_')}_data.csv"

        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['customer_id', 'name', 'address', 'email', 'telephone', 'contact_preference',
                            'transaction_activity', 'customer_preference', 'communication_method'])
            writer.writerows(company_records)
        print(f"\nGenerated Data for {company_name} saved to {
              filename} for quick overview")


def generate_company_records(company_name, num_records):
    """
    Generates fake customer records for a single company using Faker library.

    Parameters:
        company_name (str): Name of the company.
        num_records (int): Number of records to generate.

    Returns:
        list: List of tuples containing customer records.
    """
    fake = Faker('tw_GH')
    company_records = []
    for _ in range(num_records):
        customer_id = f"{company_name[:3].upper()}{
            fake.random_number(digits=6)}"
        name = fake.name()
        address = fake.address()
        email = fake.email()
        telephone = fake.phone_number()
        contact_preference = random.choice(['SMS', 'Email', 'Call'])
        transaction_activity = fake.random_int(min=0, max=100000)
        customer_preference = random.choice(['App', 'Website'])
        communication_method = random.choice(['SMS', 'Email', 'Call'])

        company_records.append((customer_id, name, address, email, telephone, contact_preference,
                                transaction_activity, customer_preference, communication_method))

    return company_records


if __name__ == "__main__":
    # Provide database credentials
    dbname = "etl"
    user = "postgres"
    password = "post123"
    host = "localhost"

    # Specify company names and the number of records per company
    company_names = [
        "Nationwide Medical Insurance",
        "Imperial Homes Limited",
        "Microfin Rural Bank Limited",
        "Wilmar Africa Ltd",
        "Tropical Cable and Conductor Ltd",
        "DHL Ghana Limited",
        "Ghandour Cosmetics",
        "Star Assurance Limited Company",
        "Zonda Tec Ghana Limited",
        "Bayport Savings And Loans Plc"
    ]
    num_records_per_company = 1000

    # Generate data for all companies
    company_data = {}
    for company_name in company_names:
        company_data[company_name] = generate_company_records(
            company_name, num_records_per_company)

    # Initialize pipeline and run
    pipeline = DataPipeline(dbname, user, password, host)
    pipeline.run_pipeline(company_data)
