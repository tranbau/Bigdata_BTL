import pandas as pd
import os

current_directory = os.getcwd()
csv_file_path = os.path.join(current_directory, 'data', 'data.csv')
# Read the CSV file
data = pd.read_csv(csv_file_path)

# Drop the index column
data.rename(columns=lambda x: x.lower(), inplace=True)

# Save the modified data to a new CSV file
new_csv_file_path = os.path.join(current_directory, 'data', 'data.csv')
data.to_csv(new_csv_file_path, index=False)
