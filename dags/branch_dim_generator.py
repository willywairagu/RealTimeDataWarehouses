import random
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

start_date = datetime(2024, 10, 7)
default_args = {
    'owner': 'wairagu',
    'depends_on_past': False,
    'backfill': False,
}

# Parameters
num_rows = 100  # Number of rows to generate
output_file = './branch_dim_large_data.csv'

# List of sample UK cities and regions for realistic data generation
cities = ["London", "Manchester", "Birmingham", "Glasgow", "Edinburgh"]
regions = ["London", "Greater Manchester", "West Midlands", "Scotland", "Scotland"]
postcodes = ["EC1A 1BB", "M1 1AE", "B1 1AA", "G1 1AA", "EH1 1AA"]


# Function to generate random branch data
def generate_random_data(row_num):
    branch_id = f"B{row_num:05d}"
    branch_name = f"Branch {row_num}"
    branch_address = f"{random.randint(1, 999)} {random.choice(['High St', 'King St', 'Queen St', 'Church Rd', 'Market St'])}"
    city = random.choice(cities)
    region = random.choice(regions)
    postcode = random.choice(postcodes)

    # Generate opening date in milliseconds
    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0, 3650))  # Random date within the last 10 years
    opening_date_millis = int(random_date.timestamp() * 1000)  # Convert to milliseconds since epoch

    return branch_id, branch_name, branch_address, city, region, postcode, opening_date_millis


# Initialize lists to store data
branch_ids = []
branch_names = []
branch_addresses = []
cities_list = []
regions_list = []
postcodes_list = []
opening_dates = []


def generate_branch_dim_data():
    # Generate data using a while loop
    row_num = 1
    while row_num <= num_rows:
        data = generate_random_data(row_num)
        branch_ids.append(data[0])
        branch_names.append(data[1])
        branch_addresses.append(data[2])
        cities_list.append(data[3])
        regions_list.append(data[4])
        postcodes_list.append(data[5])
        opening_dates.append(data[6])
        row_num += 1

    # Create a DataFrame
    df = pd.DataFrame({
        "branch_id": branch_ids,
        "branch_name": branch_names,
        "branch_address": branch_addresses,
        "city": cities_list,
        "region": regions_list,
        "postcode": postcodes_list,
        "opening_date": opening_dates
    })

    # Save DataFrame to CSV
    df.to_csv(output_file, index=False)

    print(f"CSV file '{output_file}' with {num_rows} rows has been generated successfully.")


with DAG('branch_dim_generator',
         default_args=default_args,
         description='Generate large branch dimension data CSV file',
         schedule_interval=timedelta(days=1),
         start_date=start_date,
         tags=['schema']) as dag:
    start = EmptyOperator(
        task_id='start_task',
    )

    generate_branch_dim_data = PythonOperator(
        task_id='generate_branch_dim_data',
        python_callable=generate_branch_dim_data
    )

    end = EmptyOperator(
        task_id='end_task',
    )

    start >> generate_branch_dim_data >> end