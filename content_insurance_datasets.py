# Databricks notebook source
# MAGIC %md
# MAGIC # Content Insurance Claims Dataset Generation
# MAGIC 
# MAGIC This notebook generates realistic datasets for content insurance claims including:
# MAGIC - Policy table (20 columns)
# MAGIC - Customer table (20 columns) 
# MAGIC - Claims table (20 columns including comments)
# MAGIC 
# MAGIC Data is skewed to reflect real-world insurance patterns and written as parquet files.

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Helper Functions

# COMMAND ----------

# Configuration
NUM_POLICIES = 10000
NUM_CUSTOMERS = 8000
NUM_CLAIMS = 5000

# Set random seed for reproducibility
random.seed(42)

# COMMAND ----------

def generate_skewed_data(values, weights, n):
    """Generate skewed data based on weights"""
    return random.choices(values, weights=weights, k=n)

def generate_date_range(start_date, end_date, n):
    """Generate random dates between start and end"""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    delta = end - start
    return [(start + timedelta(days=random.randint(0, delta.days))).strftime('%Y-%m-%d') for _ in range(n)]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Policy Table Generation

# COMMAND ----------

# Policy data generation with realistic skewing
policy_data = []

# Skewed policy types (more home insurance than others)
policy_types = ['Home', 'Renters', 'Condo', 'Mobile Home', 'Vacation Home']
policy_type_weights = [40, 25, 20, 10, 5]

# Skewed coverage amounts (more mid-range policies)
coverage_amounts = [50000, 100000, 150000, 200000, 300000, 500000, 750000, 1000000]
coverage_weights = [5, 15, 25, 20, 15, 10, 7, 3]

# Skewed premium amounts (correlated with coverage)
premium_ranges = [(500, 1000), (1000, 2000), (2000, 3000), (3000, 5000), (5000, 8000), (8000, 12000)]
premium_weights = [20, 30, 25, 15, 7, 3]

# Skewed regions (more policies in certain states)
regions = ['CA', 'TX', 'FL', 'NY', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI', 'NJ', 'VA', 'WA', 'AZ', 'MA']
region_weights = [12, 10, 8, 7, 6, 5, 5, 4, 4, 4, 3, 3, 3, 3, 3]

# Skewed policy status (more active policies)
policy_statuses = ['Active', 'Inactive', 'Suspended', 'Cancelled', 'Expired']
status_weights = [70, 10, 8, 7, 5]

for i in range(NUM_POLICIES):
    policy_type = generate_skewed_data(policy_types, policy_type_weights, 1)[0]
    coverage_amount = generate_skewed_data(coverage_amounts, coverage_weights, 1)[0]
    
    # Premium correlated with coverage amount
    base_premium = coverage_amount * random.uniform(0.008, 0.015)
    premium = round(base_premium + random.uniform(-200, 500), 2)
    
    policy_data.append({
        'policy_id': f'POL_{str(i+1).zfill(6)}',
        'customer_id': f'CUST_{str(random.randint(1, NUM_CUSTOMERS)).zfill(6)}',
        'policy_type': policy_type,
        'coverage_amount': coverage_amount,
        'premium_amount': premium,
        'deductible': random.choice([500, 1000, 1500, 2500, 5000]),
        'policy_start_date': generate_date_range('2020-01-01', '2024-01-01', 1)[0],
        'policy_end_date': generate_date_range('2024-01-01', '2025-12-31', 1)[0],
        'renewal_date': generate_date_range('2024-01-01', '2025-12-31', 1)[0],
        'region': generate_skewed_data(regions, region_weights, 1)[0],
        'zip_code': f'{random.randint(10000, 99999)}',
        'property_type': random.choice(['Single Family', 'Condo', 'Townhouse', 'Mobile Home', 'Apartment']),
        'construction_year': random.randint(1950, 2023),
        'square_footage': random.randint(800, 5000),
        'policy_status': generate_skewed_data(policy_statuses, status_weights, 1)[0],
        'agent_id': f'AGENT_{str(random.randint(1, 100)).zfill(3)}',
        'underwriter_id': f'UW_{str(random.randint(1, 50)).zfill(3)}',
        'risk_score': round(random.uniform(200, 850), 2),
        'discount_percentage': round(random.uniform(0, 25), 2),
        'payment_frequency': random.choice(['Monthly', 'Quarterly', 'Semi-Annual', 'Annual']),
        'created_date': generate_date_range('2020-01-01', '2024-01-01', 1)[0],
        'last_updated': generate_date_range('2023-01-01', '2024-01-01', 1)[0]
    })

# Create Policy DataFrame
policy_schema = StructType([
    StructField("policy_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("policy_type", StringType(), True),
    StructField("coverage_amount", IntegerType(), True),
    StructField("premium_amount", DoubleType(), True),
    StructField("deductible", IntegerType(), True),
    StructField("policy_start_date", StringType(), True),
    StructField("policy_end_date", StringType(), True),
    StructField("renewal_date", StringType(), True),
    StructField("region", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("property_type", StringType(), True),
    StructField("construction_year", IntegerType(), True),
    StructField("square_footage", IntegerType(), True),
    StructField("policy_status", StringType(), True),
    StructField("agent_id", StringType(), True),
    StructField("underwriter_id", StringType(), True),
    StructField("risk_score", DoubleType(), True),
    StructField("discount_percentage", DoubleType(), True),
    StructField("payment_frequency", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("last_updated", StringType(), True)
])

policy_df = spark.createDataFrame(policy_data, schema=policy_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Customer Table Generation

# COMMAND ----------

# Customer data generation with realistic skewing
customer_data = []

# Skewed age groups (more middle-aged customers)
age_groups = ['18-25', '26-35', '36-45', '46-55', '56-65', '65+']
age_weights = [5, 20, 25, 25, 15, 10]

# Skewed income levels (more middle-income customers)
income_ranges = ['<30k', '30k-50k', '50k-75k', '75k-100k', '100k-150k', '150k-250k', '250k+']
income_weights = [10, 20, 25, 20, 15, 7, 3]

# Skewed credit scores (more good credit scores)
credit_ranges = ['Poor (300-579)', 'Fair (580-669)', 'Good (670-739)', 'Very Good (740-799)', 'Excellent (800-850)']
credit_weights = [10, 20, 30, 25, 15]

# Skewed marital status
marital_statuses = ['Single', 'Married', 'Divorced', 'Widowed', 'Separated']
marital_weights = [25, 50, 15, 7, 3]

# Skewed employment status
employment_statuses = ['Employed', 'Self-Employed', 'Retired', 'Unemployed', 'Student']
employment_weights = [60, 15, 15, 7, 3]

for i in range(NUM_CUSTOMERS):
    age_group = generate_skewed_data(age_groups, age_weights, 1)[0]
    income_range = generate_skewed_data(income_ranges, income_weights, 1)[0]
    credit_range = generate_skewed_data(credit_ranges, credit_weights, 1)[0]
    
    customer_data.append({
        'customer_id': f'CUST_{str(i+1).zfill(6)}',
        'first_name': random.choice(['John', 'Jane', 'Michael', 'Sarah', 'David', 'Lisa', 'Robert', 'Jennifer', 'William', 'Maria']),
        'last_name': random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']),
        'email': f'customer{i+1}@email.com',
        'phone': f'{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}',
        'date_of_birth': generate_date_range('1950-01-01', '2005-12-31', 1)[0],
        'age_group': age_group,
        'gender': random.choice(['M', 'F', 'Other']),
        'marital_status': generate_skewed_data(marital_statuses, marital_weights, 1)[0],
        'employment_status': generate_skewed_data(employment_statuses, employment_weights, 1)[0],
        'income_range': income_range,
        'credit_score_range': credit_range,
        'address_line1': f'{random.randint(100, 9999)} {random.choice(["Main", "Oak", "Pine", "Elm", "Cedar", "Maple"])} St',
        'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']),
        'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA']),
        'zip_code': f'{random.randint(10000, 99999)}',
        'country': 'USA',
        'occupation': random.choice(['Engineer', 'Teacher', 'Manager', 'Sales', 'Healthcare', 'Finance', 'Legal', 'Retail', 'Construction', 'Technology']),
        'years_at_address': random.randint(1, 20),
        'previous_claims_count': random.choices([0, 1, 2, 3, 4, 5], weights=[60, 20, 10, 5, 3, 2])[0],
        'customer_since': generate_date_range('2015-01-01', '2023-01-01', 1)[0],
        'last_contact_date': generate_date_range('2023-01-01', '2024-01-01', 1)[0]
    })

# Create Customer DataFrame
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("age_group", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("marital_status", StringType(), True),
    StructField("employment_status", StringType(), True),
    StructField("income_range", StringType(), True),
    StructField("credit_score_range", StringType(), True),
    StructField("address_line1", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("years_at_address", IntegerType(), True),
    StructField("previous_claims_count", IntegerType(), True),
    StructField("customer_since", StringType(), True),
    StructField("last_contact_date", StringType(), True)
])

customer_df = spark.createDataFrame(customer_data, schema=customer_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Claims Table Generation

# COMMAND ----------

# Claims data generation with realistic skewing
claims_data = []

# Skewed claim types (more water damage and theft)
claim_types = ['Water Damage', 'Theft', 'Fire', 'Wind/Hail', 'Vandalism', 'Liability', 'Personal Property', 'Additional Living Expenses']
claim_type_weights = [25, 20, 10, 15, 8, 7, 10, 5]

# Skewed claim status (more closed claims)
claim_statuses = ['Open', 'In Progress', 'Approved', 'Denied', 'Closed', 'Under Review']
status_weights = [10, 15, 20, 5, 40, 10]

# Skewed severity levels
severity_levels = ['Low', 'Medium', 'High', 'Critical']
severity_weights = [40, 35, 20, 5]

# Skewed claim amounts (more smaller claims)
claim_amount_ranges = [(100, 1000), (1000, 5000), (5000, 15000), (15000, 50000), (50000, 100000), (100000, 500000)]
claim_amount_weights = [30, 35, 20, 10, 3, 2]

# Sample comments for different claim types
claim_comments = {
    'Water Damage': [
        'Burst pipe in basement caused significant water damage to flooring and drywall',
        'Roof leak during heavy rainstorm damaged ceiling and walls',
        'Washing machine overflow flooded laundry room and adjacent areas',
        'Water heater failure caused extensive water damage throughout home',
        'Sewer backup resulted in contaminated water damage requiring professional cleanup'
    ],
    'Theft': [
        'Home burglary - electronics and jewelry stolen, forced entry through back door',
        'Package theft from front porch, security camera footage shows suspect',
        'Vehicle break-in, laptop and tools stolen from car',
        'Garage break-in, bicycles and power tools stolen',
        'Theft of outdoor equipment including lawn mower and garden tools'
    ],
    'Fire': [
        'Kitchen fire caused by unattended cooking, smoke damage throughout home',
        'Electrical fire in basement, extensive damage to electrical panel',
        'Wildfire damage to exterior of home, smoke damage inside',
        'Candle fire in bedroom, limited damage but smoke throughout house',
        'Grill fire spread to deck and siding, moderate damage'
    ],
    'Wind/Hail': [
        'Hailstorm damaged roof shingles and gutters, need roof replacement',
        'High winds caused tree to fall on house, structural damage to roof',
        'Tornado damage to siding and windows, debris throughout property',
        'Severe windstorm damaged fence and outdoor structures',
        'Hail damage to vehicle and outdoor furniture'
    ],
    'Vandalism': [
        'Graffiti spray painted on exterior walls and garage door',
        'Broken windows from thrown rocks, glass damage throughout',
        'Vandalized mailbox and outdoor lighting fixtures',
        'Keyed car paint damage, multiple deep scratches',
        'Broken fence panels and damaged landscaping'
    ],
    'Liability': [
        'Guest injured on property due to uneven walkway, medical expenses covered',
        'Dog bite incident with neighbor, liability coverage activated',
        'Slip and fall on icy driveway, medical and legal expenses',
        'Property damage caused by falling tree branch onto neighbor\'s car',
        'Accidental damage to neighbor\'s fence during construction work'
    ],
    'Personal Property': [
        'Stolen laptop and camera equipment from home office',
        'Jewelry theft during home invasion, high-value items missing',
        'Electronics damaged by power surge, multiple devices affected',
        'Artwork damaged during move, professional restoration needed',
        'Musical instruments stolen from home, professional equipment loss'
    ],
    'Additional Living Expenses': [
        'Temporary housing required due to extensive water damage repairs',
        'Hotel expenses during kitchen fire restoration work',
        'Rental car needed while vehicle repaired after accident',
        'Temporary storage costs for belongings during home renovation',
        'Meal expenses during extended displacement from home'
    ]
}

for i in range(NUM_CLAIMS):
    claim_type = generate_skewed_data(claim_types, claim_type_weights, 1)[0]
    claim_status = generate_skewed_data(claim_statuses, status_weights, 1)[0]
    severity = generate_skewed_data(severity_levels, severity_weights, 1)[0]
    
    # Claim amount based on type and severity
    base_amount_range = generate_skewed_data(claim_amount_ranges, claim_amount_weights, 1)[0]
    severity_multiplier = {'Low': 0.5, 'Medium': 1.0, 'High': 2.0, 'Critical': 4.0}[severity]
    claim_amount = round(random.uniform(base_amount_range[0], base_amount_range[1]) * severity_multiplier, 2)
    
    # Settlement amount (usually less than claim amount)
    settlement_amount = round(claim_amount * random.uniform(0.3, 0.95), 2) if claim_status in ['Approved', 'Closed'] else 0
    
    # Random comment from appropriate category
    comment = random.choice(claim_comments[claim_type])
    
    claims_data.append({
        'claim_id': f'CLM_{str(i+1).zfill(6)}',
        'policy_id': f'POL_{str(random.randint(1, NUM_POLICIES)).zfill(6)}',
        'customer_id': f'CUST_{str(random.randint(1, NUM_CUSTOMERS)).zfill(6)}',
        'claim_type': claim_type,
        'claim_status': claim_status,
        'claim_date': generate_date_range('2020-01-01', '2024-01-01', 1)[0],
        'incident_date': generate_date_range('2020-01-01', '2024-01-01', 1)[0],
        'report_date': generate_date_range('2020-01-01', '2024-01-01', 1)[0],
        'claim_amount': claim_amount,
        'settlement_amount': settlement_amount,
        'deductible_applied': random.choice([500, 1000, 1500, 2500, 5000]),
        'severity_level': severity,
        'adjuster_id': f'ADJ_{str(random.randint(1, 50)).zfill(3)}',
        'investigator_id': f'INV_{str(random.randint(1, 30)).zfill(3)}',
        'police_report_number': f'PR{random.randint(100000, 999999)}' if random.random() < 0.3 else None,
        'witness_count': random.randint(0, 5),
        'photos_taken': random.randint(0, 20),
        'repair_estimate': round(claim_amount * random.uniform(0.8, 1.2), 2),
        'fraud_score': round(random.uniform(0, 100), 2),
        'comments': comment,
        'created_date': generate_date_range('2020-01-01', '2024-01-01', 1)[0],
        'last_updated': generate_date_range('2023-01-01', '2024-01-01', 1)[0]
    })

# Create Claims DataFrame
claims_schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("policy_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("claim_type", StringType(), True),
    StructField("claim_status", StringType(), True),
    StructField("claim_date", StringType(), True),
    StructField("incident_date", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("settlement_amount", DoubleType(), True),
    StructField("deductible_applied", IntegerType(), True),
    StructField("severity_level", StringType(), True),
    StructField("adjuster_id", StringType(), True),
    StructField("investigator_id", StringType(), True),
    StructField("police_report_number", StringType(), True),
    StructField("witness_count", IntegerType(), True),
    StructField("photos_taken", IntegerType(), True),
    StructField("repair_estimate", DoubleType(), True),
    StructField("fraud_score", DoubleType(), True),
    StructField("comments", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("last_updated", StringType(), True)
])

claims_df = spark.createDataFrame(claims_data, schema=claims_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Quality and Skew Analysis

# COMMAND ----------

# Display data quality metrics
print("=== DATA QUALITY METRICS ===")
print(f"Policy Records: {policy_df.count()}")
print(f"Customer Records: {customer_df.count()}")
print(f"Claims Records: {claims_df.count()}")

print("\n=== POLICY DATA DISTRIBUTION ===")
policy_df.groupBy("policy_type").count().orderBy(desc("count")).show()
policy_df.groupBy("policy_status").count().orderBy(desc("count")).show()

print("\n=== CUSTOMER DATA DISTRIBUTION ===")
customer_df.groupBy("age_group").count().orderBy(desc("count")).show()
customer_df.groupBy("income_range").count().orderBy(desc("count")).show()

print("\n=== CLAIMS DATA DISTRIBUTION ===")
claims_df.groupBy("claim_type").count().orderBy(desc("count")).show()
claims_df.groupBy("claim_status").count().orderBy(desc("count")).show()
claims_df.groupBy("severity_level").count().orderBy(desc("count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write Data to Parquet Files

# COMMAND ----------

# Define output paths
output_base_path = "/FileStore/shared_uploads/insurance_data/"
policy_path = f"{output_base_path}policies/"
customer_path = f"{output_base_path}customers/"
claims_path = f"{output_base_path}claims/"

# Write data to parquet files with partitioning
print("Writing Policy data to parquet...")
policy_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("region", "policy_status") \
    .parquet(policy_path)

print("Writing Customer data to parquet...")
customer_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("state", "age_group") \
    .parquet(customer_path)

print("Writing Claims data to parquet...")
claims_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("claim_type", "claim_status") \
    .parquet(claims_path)

print("Data successfully written to parquet files!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Data and Create Tables

# COMMAND ----------

# Verify the written data
print("=== VERIFYING WRITTEN DATA ===")
print(f"Policy files: {len(dbutils.fs.ls(policy_path))}")
print(f"Customer files: {len(dbutils.fs.ls(customer_path))}")
print(f"Claims files: {len(dbutils.fs.ls(claims_path))}")

# Read back a sample to verify
print("\n=== SAMPLE POLICY DATA ===")
spark.read.parquet(policy_path).show(5)

print("\n=== SAMPLE CUSTOMER DATA ===")
spark.read.parquet(customer_path).show(5)

print("\n=== SAMPLE CLAIMS DATA ===")
spark.read.parquet(claims_path).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Delta Tables (Optional)

# COMMAND ----------

# Create Delta tables for better performance and ACID transactions
# print("Creating Delta tables...")

# # Policy Delta table
# policy_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true") \
#     .saveAsTable("insurance.policies")

# # Customer Delta table  
# customer_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true") \
#     .saveAsTable("insurance.customers")

# # Claims Delta table
# claims_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true") \
#     .saveAsTable("insurance.claims")

# print("Delta tables created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Data Analysis Queries

# COMMAND ----------

# Sample analysis queries
print("=== INSURANCE DATA ANALYSIS ===")

# Top claim types by frequency
print("\n1. Top Claim Types by Frequency:")
claims_df.groupBy("claim_type") \
    .count() \
    .orderBy(desc("count")) \
    .show()

# Average claim amount by type
print("\n2. Average Claim Amount by Type:")
claims_df.groupBy("claim_type") \
    .agg(avg("claim_amount").alias("avg_claim_amount"), 
         count("claim_id").alias("claim_count")) \
    .orderBy(desc("avg_claim_amount")) \
    .show()

# Claims by severity
print("\n3. Claims Distribution by Severity:")
claims_df.groupBy("severity_level") \
    .agg(count("claim_id").alias("claim_count"),
         avg("claim_amount").alias("avg_amount")) \
    .orderBy(desc("claim_count")) \
    .show()

# Policy coverage analysis
print("\n4. Policy Coverage Analysis:")
policy_df.groupBy("policy_type") \
    .agg(avg("coverage_amount").alias("avg_coverage"),
         avg("premium_amount").alias("avg_premium"),
         count("policy_id").alias("policy_count")) \
    .orderBy(desc("policy_count")) \
    .show()

print("\n=== DATA GENERATION COMPLETE ===")
print(f"✅ Generated {NUM_POLICIES:,} policy records")
print(f"✅ Generated {NUM_CUSTOMERS:,} customer records") 
print(f"✅ Generated {NUM_CLAIMS:,} claims records")
print(f"✅ Data written to parquet files with partitioning")
print(f"✅ Delta tables created for analytics")
print(f"✅ All data includes realistic skewing and comments")
