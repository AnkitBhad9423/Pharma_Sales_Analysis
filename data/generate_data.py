import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration
NUM_REPS = 50
NUM_DOCTORS = 500
NUM_PRODUCTS = 20
NUM_TERRITORIES = 25
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 12, 31)

# Generate dim_date
def generate_dim_date():
    dates = pd.date_range(start=START_DATE, end=END_DATE, freq='D')
    dim_date = pd.DataFrame({
        'date_key': range(1, len(dates) + 1),
        'date': dates,
        'day': dates.day,
        'month': dates.month,
        'quarter': dates.quarter,
        'year': dates.year,
        'day_of_week': dates.dayofweek,
        'month_name': dates.strftime('%B'),
        'is_weekend': dates.dayofweek >= 5
    })
    return dim_date

# Generate dim_sales_rep
def generate_dim_sales_rep():
    regions = ['North', 'South', 'East', 'West', 'Central']
    teams = ['Team A', 'Team B', 'Team C', 'Team D']
    
    dim_sales_rep = pd.DataFrame({
        'rep_key': range(1, NUM_REPS + 1),
        'rep_name': [f'Rep_{i}' for i in range(1, NUM_REPS + 1)],
        'region': np.random.choice(regions, NUM_REPS),
        'team': np.random.choice(teams, NUM_REPS),
        'hire_date': pd.date_range(start='2018-01-01', periods=NUM_REPS, freq='15D'),
        'experience_years': np.random.randint(1, 8, NUM_REPS),
        'performance_tier': np.random.choice(['Top', 'Medium', 'Low'], NUM_REPS, p=[0.2, 0.6, 0.2])
    })
    return dim_sales_rep

# Generate dim_doctor
def generate_dim_doctor():
    specialties = ['Cardiology', 'Oncology', 'Neurology', 'Endocrinology', 'General Practice']
    
    dim_doctor = pd.DataFrame({
        'doctor_key': range(1, NUM_DOCTORS + 1),
        'doctor_name': [f'Dr_{i}' for i in range(1, NUM_DOCTORS + 1)],
        'specialty': np.random.choice(specialties, NUM_DOCTORS),
        'hospital': [f'Hospital_{np.random.randint(1, 51)}' for _ in range(NUM_DOCTORS)],
        'city': [f'City_{np.random.randint(1, 101)}' for _ in range(NUM_DOCTORS)],
        'prescription_volume': np.random.choice(['High', 'Medium', 'Low'], NUM_DOCTORS, p=[0.3, 0.5, 0.2])
    })
    return dim_doctor

# Generate dim_product
def generate_dim_product():
    categories = ['Cardiovascular', 'Diabetes', 'Pain Management', 'Antibiotics', 'Oncology']
    
    dim_product = pd.DataFrame({
        'product_key': range(1, NUM_PRODUCTS + 1),
        'product_name': [f'Drug_{chr(65+i)}' for i in range(NUM_PRODUCTS)],
        'category': np.random.choice(categories, NUM_PRODUCTS),
        'unit_price': np.random.uniform(50, 500, NUM_PRODUCTS).round(2),
        'launch_date': pd.date_range(start='2020-01-01', periods=NUM_PRODUCTS, freq='60D'),
        'patent_status': np.random.choice(['Active', 'Expiring Soon'], NUM_PRODUCTS, p=[0.7, 0.3])
    })
    return dim_product

# Generate dim_territory
def generate_dim_territory():
    regions = ['North', 'South', 'East', 'West', 'Central']
    states = ['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
    
    dim_territory = pd.DataFrame({
        'territory_key': range(1, NUM_TERRITORIES + 1),
        'territory_name': [f'Territory_{i}' for i in range(1, NUM_TERRITORIES + 1)],
        'region': np.random.choice(regions, NUM_TERRITORIES),
        'state': np.random.choice(states, NUM_TERRITORIES),
        'population': np.random.randint(100000, 5000000, NUM_TERRITORIES),
        'market_potential': np.random.choice(['High', 'Medium', 'Low'], NUM_TERRITORIES, p=[0.3, 0.5, 0.2])
    })
    return dim_territory

# Generate fact_sales
def generate_fact_sales(dim_date, dim_sales_rep, dim_doctor, dim_product, dim_territory):
    NUM_TRANSACTIONS = 50000
    
    fact_sales = pd.DataFrame({
        'sale_id': range(1, NUM_TRANSACTIONS + 1),
        'date_key': np.random.choice(dim_date['date_key'], NUM_TRANSACTIONS),
        'rep_key': np.random.choice(dim_sales_rep['rep_key'], NUM_TRANSACTIONS),
        'doctor_key': np.random.choice(dim_doctor['doctor_key'], NUM_TRANSACTIONS),
        'product_key': np.random.choice(dim_product['product_key'], NUM_TRANSACTIONS),
        'territory_key': np.random.choice(dim_territory['territory_key'], NUM_TRANSACTIONS),
        'quantity_sold': np.random.randint(1, 100, NUM_TRANSACTIONS),
        'discount_percent': np.random.uniform(0, 15, NUM_TRANSACTIONS).round(2),
        'marketing_spend': np.random.uniform(100, 5000, NUM_TRANSACTIONS).round(2)
    })
    
    # Calculate revenue (join with product to get unit_price)
    fact_sales = fact_sales.merge(dim_product[['product_key', 'unit_price']], on='product_key')
    fact_sales['revenue'] = (fact_sales['quantity_sold'] * fact_sales['unit_price'] * 
                             (1 - fact_sales['discount_percent']/100)).round(2)
    fact_sales = fact_sales.drop('unit_price', axis=1)
    
    return fact_sales

# Execute data generation
print("Generating dimensional data...")
dim_date = generate_dim_date()
dim_sales_rep = generate_dim_sales_rep()
dim_doctor = generate_dim_doctor()
dim_product = generate_dim_product()
dim_territory = generate_dim_territory()
fact_sales = generate_fact_sales(dim_date, dim_sales_rep, dim_doctor, dim_product, dim_territory)

# Save to CSV
dim_date.to_csv('data/dim_date.csv', index=False)
dim_sales_rep.to_csv('data/dim_sales_rep.csv', index=False)
dim_doctor.to_csv('data/dim_doctor.csv', index=False)
dim_product.to_csv('data/dim_product.csv', index=False)
dim_territory.to_csv('data/dim_territory.csv', index=False)
fact_sales.to_csv('data/fact_sales.csv', index=False)

print("✅ Data generation complete!")
print(f"Generated {len(fact_sales)} sales transactions")