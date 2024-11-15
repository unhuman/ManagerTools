# This script will process a file that contains usernames to team groups from a csv file

# This script was generated by OpenAI

import sys  
import pandas as pd  
  
def generate_groups(file_path):  
    # Read CSV file  
    df = pd.read_csv(file_path)  
  
    # Display list of columns  
    columns = df.columns.tolist()  
    print("Columns:")  
    for i, column in enumerate(columns):  
        print(f"{i+1}. {column}")  
  
    # Prompt for key column  
    key_column_index = int(input("Enter the index of the column to use as the team: ")) - 1  
    key_column = columns[key_column_index]  
  
    # Prompt for grouping column  
    grouping_column_index = int(input("Enter the index of the column to group usernames: ")) - 1  
    grouping_column = columns[grouping_column_index]  
  
    # Group values based on the selected columns  
    grouped_data = df.groupby(key_column)[grouping_column].apply(list).reset_index()  
  
    # Generate and sort the groups  
    groups = {}  
    for index, row in grouped_data.iterrows():  
        group_name = row[key_column]  
        values = sorted(set(str(value) for value in row[grouping_column]), key=lambda x: x.lower())
        groups[group_name] = values  
  
    # Sort the groups by keys  
    sorted_groups = dict(sorted(groups.items(), key=lambda x: x[0].lower()))  
  
    return sorted_groups  
  
# Get the file path from the command line arguments  
if len(sys.argv) < 2:  
    print("Please provide the path to the CSV file as a command line argument.")  
    sys.exit(1)  
  
file_path = sys.argv[1]  
  
# Generate and display the groups  
groups = generate_groups(file_path)  
print("\nGenerated Groups:")  
for group_name, values in groups.items():  
    print(f"\"{group_name}\": \"{', '.join(values)}\",")  