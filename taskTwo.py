import pandas as pd

filePath = "/Users/keshavsaraogi/Desktop/BU/sem3/big-data/ASSIGNMENTS/ASSIGNMENT_1/taxi-data-sorted-small.csv"
data = pd.read_csv(filePath, header=None)

print(data.head())

print(data.shape)
print(data.isnull().sum())
print(data.dtypes)

# Assign column names
column_names = [
    'medallion', 'hack_license', 'pickup_datetime', 'dropoff_datetime',
    'trip_time_in_secs', 'trip_distance', 'pickup_longitude', 'pickup_latitude',
    'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount',
    'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount'
]
data.columns = column_names

import pandas as pd
import time

start_time = time.time()

# Droping rows with missing values
data_cleaned = data.dropna()

# Group by medallion and count unique hack_license
top_taxis = data_cleaned.groupby('medallion')['hack_license'].nunique().sort_values(ascending=False).head(10)

top_taxis_df = pd.DataFrame(top_taxis).reset_index()
top_taxis_df.columns = ['Taxi (medallion)', 'Number of Drivers']
print(top_taxis_df)

end_time = time.time()
processing_time = end_time - start_time
print(f"Processing time: {processing_time:.2f} seconds")

import pandas as pd
import time


start_time = time.time()

cleaned_data = data.dropna(subset=['hack_license', 'total_amount', 'trip_time_in_secs'])
cleaned_data = cleaned_data[cleaned_data['trip_time_in_secs'] > 0]
cleaned_data['money_per_minute'] = cleaned_data['total_amount'] / (cleaned_data['trip_time_in_secs'] / 60)

driver_earnings = cleaned_data.groupby('hack_license')['money_per_minute'].mean()
top_10_drivers = driver_earnings.nlargest(10)

processing_time = time.time() - start_time

print("Top 10 best drivers (hack_license, average money per minute):")
print(top_10_drivers)
print(f"Processing time: {processing_time:.4f} seconds")


import pandas as pd
import time

start_time = time.time()

cleaned_data = data.dropna(subset=['surcharge', 'trip_distance', 'pickup_datetime'])
cleaned_data = cleaned_data[cleaned_data['trip_distance'] > 0]

cleaned_data['pickup_datetime'] = pd.to_datetime(cleaned_data['pickup_datetime'])
cleaned_data['hour'] = cleaned_data['pickup_datetime'].dt.hour

cleaned_data['profit_ratio'] = cleaned_data['surcharge'] / cleaned_data['trip_distance']

hourly_profit = cleaned_data.groupby('hour')['profit_ratio'].mean()

# Find the hour with the highest profit ratio
best_hour = hourly_profit.idxmax()
best_hour_value = hourly_profit.max()

processing_time = time.time() - start_time

print(f"The best time of day to work (hour {best_hour}) with the highest profit ratio: {best_hour_value:.4f}")
print(f"Processing time: {processing_time:.4f} seconds")