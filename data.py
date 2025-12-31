import snowflake.connector
import pandas as pd
import os

# Connect to bootcamp Snowflake
conn = snowflake.connector.connect(
    user='dataexpert_student',
    password='DataExpert123!',
    account='aab46027.us-west-2',
    warehouse='COMPUTE_WH',
    database='DATAEXPERT_STUDENT',
    schema='bootcamp'
)

tables = [
    'raw_customers',
    'raw_customer_feedbacks',
    'raw_haunted_houses',
    'raw_haunted_house_tickets'
]

# Create export directory
os.makedirs('exported_data', exist_ok=True)

for table in tables:
    print(f"Exporting {table}...")
    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, conn)
    df.to_csv(f'exported_data/{table}.csv', index=False)
    print(f"Exported {len(df)} rows to exported_data/{table}.csv")

conn.close()
print("✅ All tables exported!")