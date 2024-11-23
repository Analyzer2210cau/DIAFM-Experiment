import pandas as pd
import numpy as np
from faker import Faker
import random
import os

# Initialize Faker
fake = Faker()

# Number of records per schema
num_records = 1_000_000

# Directory to save the datasets
save_directory = "E:\Research-Articles\Increamental Frequent Itemset(Un-published)\Journal-Mathemtics-MDPI-Review\Data-Sets"

# Ensure the directory exists
os.makedirs(save_directory, exist_ok=True)

# Schema 1: E-commerce Transactions
def generate_ecommerce_data(n):
    return pd.DataFrame({
        "TransactionID": [f"TXN{str(i).zfill(7)}" for i in range(1, n + 1)],
        "CustomerID": [f"CUST{random.randint(1, 100_000)}" for _ in range(n)],
        "ProductID": [f"PROD{random.randint(1, 500)}" for _ in range(n)],
        "TransactionDate": [fake.date_time_between(start_date="-2y", end_date="now") for _ in range(n)],
        "TransactionAmount": [round(random.uniform(10.0, 1000.0), 2) for _ in range(n)],
        "PaymentMethod": [random.choice(["Credit Card", "PayPal", "Cash", "Bank Transfer"]) for _ in range(n)],
        "Status": [random.choice(["Success", "Failed"]) for _ in range(n)],
    })

# Schema 2: Bank Transactions
def generate_bank_transactions(n):
    return pd.DataFrame({
        "TransactionID": [f"TXN{str(i).zfill(7)}" for i in range(1, n + 1)],
        "AccountID": [f"ACC{random.randint(1, 100_000)}" for _ in range(n)],
        "TransactionDate": [fake.date_time_between(start_date="-1y", end_date="now") for _ in range(n)],
        "TransactionAmount": [round(random.uniform(100.0, 10_000.0), 2) for _ in range(n)],
        "TransactionType": [random.choice(["Deposit", "Withdrawal", "Transfer"]) for _ in range(n)],
        "Balance": [round(random.uniform(1_000.0, 50_000.0), 2) for _ in range(n)],
        "Branch": [random.choice(["New York", "London", "Mumbai", "Tokyo", "Berlin"]) for _ in range(n)],
    })

# Schema 3: Healthcare Appointments
def generate_healthcare_appointments(n):
    return pd.DataFrame({
        "AppointmentID": [f"APT{str(i).zfill(7)}" for i in range(1, n + 1)],
        "PatientID": [f"PAT{random.randint(1, 50_000)}" for _ in range(n)],
        "DoctorID": [f"DOC{random.randint(1, 1_000)}" for _ in range(n)],
        "AppointmentDate": [fake.date_time_between(start_date="-1y", end_date="now") for _ in range(n)],
        "Specialization": [random.choice(["Cardiology", "Orthopedics", "Pediatrics", "Dermatology"]) for _ in range(n)],
        "Fee": [round(random.uniform(50.0, 500.0), 2) for _ in range(n)],
        "Status": [random.choice(["Scheduled", "Completed", "Cancelled"]) for _ in range(n)],
    })

# Schema 4: Retail Store Sales
def generate_retail_sales(n):
    df = pd.DataFrame({
        "SaleID": [f"SALE{str(i).zfill(7)}" for i in range(1, n + 1)],
        "StoreID": [f"STORE{random.randint(1, 100)}" for _ in range(n)],
        "ProductID": [f"PROD{random.randint(1, 500)}" for _ in range(n)],
        "SaleDate": [fake.date_time_between(start_date="-6m", end_date="now") for _ in range(n)],
        "Quantity": [random.randint(1, 20) for _ in range(n)],
        "PricePerUnit": [round(random.uniform(5.0, 200.0), 2) for _ in range(n)],
        "Discount": [round(random.uniform(0.0, 30.0), 2) for _ in range(n)],
    })
    df["TotalAmount"] = df["Quantity"] * df["PricePerUnit"]
    return df

# Generate and Save Data
schemas = {
    "ecommerce_transactions.csv": generate_ecommerce_data,
    "bank_transactions.csv": generate_bank_transactions,
    "healthcare_appointments.csv": generate_healthcare_appointments,
    "retail_sales.csv": generate_retail_sales,
}

for filename, generator in schemas.items():
    print(f"Generating {filename}...")
    df = generator(num_records)
    file_path = os.path.join(save_directory, filename)
    df.to_csv(file_path, index=False)
    print(f"Saved {filename} at {file_path}.")
