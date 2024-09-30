import pandas as pd

# Step 1: Read the Excel file (adjust the file path as necessary)
file_path = '/Users/venateshwarlu/Documents/tets_Data.xlsx'  # Replace with the actual file path
df = pd.read_excel(file_path)

# Clean column names if there are leading/trailing spaces
df.columns = df.columns.str.strip()

# Step 2: Group by PayrollProcessingID and collect AOID for each PayrollProcessingID
grouped_data = df.groupby('PayrollProcessingID')['AOID'].apply(list).reset_index()

# Step 3: Construct SQL queries
sql_queries = []
for index, row in grouped_data.iterrows():
    aoid_list = ', '.join(f"'{aoid}'" for aoid in row['AOID'])  # Prepare list of AOIDs for SQL IN clause
    payroll_processing_id = row['PayrollProcessingID']
    
    # Construct SQL query
    query = f"""
    UPDATE public."WAGE_BREAKOUT"
    SET "WAGE_AMOUNT" = 0
    WHERE "ASSOCIATEOID" IN ({aoid_list})
    AND "WAGE_TYPE" = 'GrossYTD'
    AND "PAYROLL_PROCESSING_JOBID" = '{payroll_processing_id}';
    """
    sql_queries.append(query)

# Output the generated queries
for query in sql_queries:
    print(query)

# Optionally, write queries to a file
with open('output_queries.sql', 'w') as f:
    for query in sql_queries:
        f.write(query + '\n')
