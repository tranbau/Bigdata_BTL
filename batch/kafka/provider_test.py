import os
import pandas as pd

# Get the current directory of the provider.py file
current_dir = os.path.dirname(os.path.abspath(__file__))

# Navigate up one directory to reach the 'data' directory
data_dir = os.path.join(current_dir, '..', 'data')

print(data_dir)

# # Path to the CSV file
csv_file_path = os.path.join(data_dir, 'stock.csv')

# # Read the CSV file using pandas
data = pd.read_csv(csv_file_path)

# # Now 'data' contains the contents of the CSV file
# print(data.head())  # Example: printing the first few rows