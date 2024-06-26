# Group_10_Assignment_4_DataPipeline

## Data Pipeline for Customer Record Ingestion
GENERATING 100K USER DATA EACH  FOR CREATING DATA PIPELINE FOR TEN COMPANIES

## Table of Contents
  -  [Overview](#overview)
  -  [Features](#Features)
  -  [Details](#Details)
  -  [Prerequisites](#Prerequisites)
  -  [Installation](#Installation)
  -  [Usage](#Usage)
  -  [Code](#Code)
  -  [Acknowledgements](#Acknowledgements)
  -  [Lead Trainers](#LeadTrainers)
  -  [Group Members](#GroupMembers)


# Overview
This project implements an ETL (Extract, Transform, Load) pipeline for ingesting synthetic data into a PostgreSQL database for  fictional 10 companies, based in Ghana. The pipeline generates simulated customer data using the Faker library, creates a relational database schema, and loads the generated data into the database. Additionally, it provides functionality to query the data and save the queries into a file.

# Features

- Generates synthetic customer data including demographics, transaction activity, customer preferences, and communication methods.
- Ingests the generated data into a PostgreSQL database.
- Provides Python scripts to execute various data processing tasks and SQL queries.
- Dockerized for easy deployment and reproducibility.

# Details:
- Data Generation: Fake customer records are generated using the Faker library to simulate realistic data.

- Database Interaction: The pipeline establishes a connection to a PostgreSQL database and creates a table (customers) to store the generated records.

- Data Ingestion: Generated customer records are ingested into the database table.

- Error Handling: The pipeline includes error handling mechanisms to ensure data integrity and reliability during ingestion.

# Prerequisites

- [Python 3.0 and above](https://www.python.org)
- [PostgreSQL](https://www.postgresql.org)
- [psycopg2 library](https://pypi.org/project/psycopg2/)
- [Docker](https://www.docker.com)
- [Faker](https://faker.readthedocs.io/en/master/)

# Installation

1. Clone the repository:
   
```git clone https://github.com/Data-Engineering-Training/Group_10_Assignment_4_DataPipeline.git```

# Usage
Modify the database credentials in the main.py file:

dbname = ```"your_database_name"```

user = ```"your_database_user"```

password = ```"your_database_password"```

host = ```"your_database_host"```

Modify the company_names list in main.py to include the names of companies for which you want to generate data.

Set the num_records_per_company variable in main.py to specify the number of records to generate for each company.

Run the main.py script:

# Code 
## Example: Run the pipeline:
Database credentials

dbname = ```"etl"```

user = ```"postgres"```

password = ```"post123"```

host = ```"localhost"```

# Generate records

records = ```generate_records()```

# Initialize pipeline and run

```pipeline = DataPipeline(dbname, user, password, host)```

```pipeline.run_pipeline(records)```

# Acknowledgements

## Trestle Academy Ghana
Thanks to Trestle Academy for their effort in creating talents in the tech space

To know about trestle academy, visit: 

- ```website:``` https://www.trestleacademyghana.org

- ```Mail:``` info@trestleacademyghana.org

## Lead Trainers:
1. DEREK DEGBEDZUI
2. THEOPHILUS AKUGRE

# GroupMembers

- ISHMAEL  ABAYATEYE KABU
- ABIGAIL ODONKOR
- WILFRED OWUSU-BONSU
- JOHN TAMAKLOE
- JUSTICE OHENE AMOFA
- PHILIP NARTEY
- KODJOTSE SYLVESTER
- PETER KOBENA EDUAH
- FREDERICK OTU-AFRO
- ERIC AKWETE AJAVON
- FRANCIS TAWIAH
- KWAME OSEI TUTU AGYEMAN
