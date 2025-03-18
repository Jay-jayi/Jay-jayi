import pandas as pd
import socket
import sqlite3

# Read CSV files
customers = pd.read_csv('customer.csv')
orders = pd.read_csv('orders.csv')

# Calculate total sales
orders['TotalSales'] = orders['Quantity'] * orders['Price']
orders['OrderDate'] = pd.to_datetime(orders['OrderDate'])

# Filter high-value orders
filtered_orders = orders[orders['TotalSales'] > 1000]

# Merge data
result = pd.merge(filtered_orders, customers, on='CustomerID', how='left')

# Print user information and aggregated table
print("Your Name:", "John Doe")
print("IP Address:", socket.gethostbyname(socket.gethostname()))
print("Machine Name:", socket.gethostname())
print("\nAggregated Table:")
print(result)

# Save the result to a CSV file
result.to_csv('aggregated_orders.csv', index=False)

# Connect to the SQLite database
conn = sqlite3.connect('output.db')

# Create a table
create_table_query = '''
CREATE TABLE IF NOT EXISTS aggregated_orders (
    OrderID INTEGER,
    CustomerID INTEGER,
    Quantity INTEGER,
    Price REAL,
    OrderDate TEXT,
    TotalSales REAL,
    -- Assume the customers table has Name and Email columns
    Name TEXT,
    Email TEXT
)
'''
conn.execute(create_table_query)

# Load data into the database
result.to_sql('aggregated_orders', conn, if_exists='replace', index=False)

# Query results from the database and print them
query_result = conn.execute('SELECT * FROM aggregated_orders')
for row in query_result.fetchall():
    print(row)

# Close the connection
conn.close()

print("ETL process completed successfully!")