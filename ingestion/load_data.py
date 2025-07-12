import pandas as pd
import psycopg2
print("Script started.")
from psycopg2 import sql

# 1. Database connection settings
DB_NAME = "healthguardian"
DB_USER = "postgres"
DB_PASS = "Akash@2001"
DB_HOST = "localhost"
DB_PORT = "5432"

# 2. Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()

# 3. Load patients.csv
df_patients = pd.read_csv('../data/patients.csv')

# Clean: keep relevant columns and rename
df_patients = df_patients.rename(columns={
    'Id': 'id',
    'BIRTHDATE': 'birthdate',
    'DEATHDATE': 'deathdate',
    'GENDER': 'gender',
    'RACE': 'race',
    'ETHNICITY': 'ethnicity'
})
df_patients = df_patients[['id', 'birthdate', 'deathdate', 'gender', 'race', 'ethnicity']]

# 4. Insert patients into the table
for _, row in df_patients.iterrows():
    # Convert NaN to None for SQL compatibility
    values = [
        row['id'],
        row['birthdate'],
        None if pd.isna(row['deathdate']) else row['deathdate'],
        row['gender'],
        row['race'],
        row['ethnicity']
    ]
    cursor.execute("""
        INSERT INTO patients (id, birthdate, deathdate, gender, race, ethnicity)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """, values)

conn.commit()
print("Inserted patients data.")

# 5. Load encounters.csv
df_encounters = pd.read_csv('../data/encounters.csv')

# Clean and rename columns
df_encounters = df_encounters.rename(columns={
    'Id': 'id',
    'PATIENT': 'patient_id',
    'START': 'start',
    'STOP': 'stop',
    'ENCOUNTERCLASS': 'encounter_class',
    'DESCRIPTION': 'description'
})
df_encounters = df_encounters[['id', 'patient_id', 'start', 'stop', 'encounter_class', 'description']]

# 6. Insert encounters
for _, row in df_encounters.iterrows():
    cursor.execute("""
        INSERT INTO encounters (id, patient_id, start, stop, encounter_class, description)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """, tuple(row))

conn.commit()
print("Inserted encounters data.")

# 7. Load conditions.csv
df_conditions = pd.read_csv('../data/conditions.csv')

# Print to verify columns
print(df_conditions.columns)

# Clean and rename
df_conditions = df_conditions.rename(columns={
    'PATIENT': 'patient_id',
    'START': 'start',
    'STOP': 'stop',
    'DESCRIPTION': 'description'
})

# Keep only these columns
df_conditions = df_conditions[['patient_id', 'start', 'stop', 'description']]

# 8. Insert conditions
for _, row in df_conditions.iterrows():
    values = [
        row['patient_id'],
        row['start'],
        None if pd.isna(row['stop']) else row['stop'],
        row['description']
    ]
    cursor.execute("""
        INSERT INTO conditions (patient_id, start, stop, description)
        VALUES (%s, %s, %s, %s);
    """, values)

print("Inserted conditions data.")

# 9. Close connection
cursor.close()
conn.close()


print("All data loaded successfully.")
print("Script finished.")