"""
Traffic Stops Data Preprocessing Script
Removes columns with all missing values and handles NaN values
"""

import pandas as pd
import warnings

warnings.filterwarnings('ignore')

# Load data
file_path = 'traffic_stops - traffic_stops_with_vehicle_number.csv'
df = pd.read_csv(file_path)
print(f"Original shape: {df.shape}")

# Remove columns with ALL missing values
all_missing_cols = df.columns[df.isnull().all()].tolist()
df_clean = df.drop(columns=all_missing_cols)
print(f"Removed {len(all_missing_cols)} empty columns")
print(f"Shape after removing empty columns: {df_clean.shape}")

# Handle NaN values in remaining columns
print(f"\nHandling missing values...")
for col in df_clean.columns:
    if df_clean[col].isnull().any():
        if df_clean[col].dtype in ['object', 'category']:
            df_clean[col] = df_clean[col].fillna('Unknown')
        elif df_clean[col].dtype in ['int64', 'float64']:
            df_clean[col] = df_clean[col].fillna(df_clean[col].median())
        elif df_clean[col].dtype == 'bool':
            df_clean[col] = df_clean[col].fillna(False)
        else:
            df_clean[col] = df_clean[col].fillna('Unknown')

print(f"Missing values remaining: {df_clean.isnull().sum().sum()}")

# Export cleaned data
output_file = 'traffic_stops_cleaned.csv'
df_clean.to_csv(output_file, index=False)
print(f"\nCleaned data saved to: {output_file}")
print(f"Final shape: {df_clean.shape}")
