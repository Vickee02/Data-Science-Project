import os
import csv
import subprocess
import requests
import json
import pandas as pd

repository_url = 'https://github.com/PhonePe/pulse.git'

# Clone the Github repository using the git command
os.system(f'git clone {repository_url}')

root_dir = r'C:\Users\admin\pulse\data'

# Initialize empty list to hold dictionaries of data for each JSON file
data_list = []

# Loop over all the state folders
for state_dir in os.listdir(os.path.join(root_dir, 'aggregated/transaction/country/india/state')):
    state_path = os.path.join(root_dir, 'aggregated/transaction/country/india/state', state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        quarter = int(json_file.split('.')[0])  # Extract quarter from filename
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)

                            for transaction_data in data['data']['transactionData']:
                                row_dict = {
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarters': quarter,  # Use the extracted quarter
                                    'Transaction_Type': transaction_data['name'],
                                    'Transaction_Count': transaction_data['paymentInstruments'][0]['count'],
                                    'Transaction_Amount': transaction_data['paymentInstruments'][0]['amount']
                                }
                                data_list.append(row_dict)

# Convert list of dictionaries to dataframe
df1 = pd.DataFrame(data_list)
df1

root_dir = r'C:\Users\admin\pulse\data'
data_list_1 = []
for state_dir in os.listdir(os.path.join(root_dir, 'aggregated/user/country/india/state')):
    state_path = os.path.join(root_dir, 'aggregated/user/country/india/state', state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        quarter = int(json_file.split('.')[0])  # Extract quarter from filename

                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            # for user in data['data']['key']['transactionData']:
                            for district, values in data['data'].items():
                                # Extract data as before
                                row_dict = {

                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarter': int(json_file.split('.')[0]),
                                    'District': district,
                                    'RegisteredUsers': 'registeredUsers',
                                }
                                data_list_1.append(row_dict)

# Convert list of dictionaries to dataframe
df2 = pd.DataFrame(data_list_1)
df2

root_dir = r'C:\Users\admin\pulse\data'

# Initialize empty list to hold dictionaries of data for each JSON file
data_list_3 = []

# Loop over all the state folders
for state_dir in os.listdir(os.path.join(root_dir, 'map\\transaction\\hover\\country\\india\\state')):
    state_path = os.path.join(root_dir, 'map\\transaction\\hover\\country\\india\\state', state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        quarter = int(json_file.split('.')[0])  # Extract quarter from filename
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)

                        for hoverDataList in data['data']['hoverDataList']:
                            row_dict = {
                                'States': state_dir,
                                'Transaction_Year': year_dir,
                                'Quarters': int(json_file.split('.')[0]),
                                'District': hoverDataList['name'],
                                'Transaction_Type': hoverDataList['metric'][0]['type'],
                                'Transaction_Count': hoverDataList['metric'][0]['amount']
                            }
                            data_list_3.append(row_dict)

                        # Convert list of dictionaries to dataframe
df3 = pd.DataFrame(data_list_3)
df3