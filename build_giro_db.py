import os
import sqlite3
import pandas as pd

# Define folder and database paths
csv_folder = "giro2025"
db_path = "giro_2025.db"

# Connect to SQLite
conn = sqlite3.connect(db_path)
cursor = conn.cursor()


def normalize_spaces(series):
    return series.astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()


# Helper: load CSV into SQLite, optionally processing rider info
def load_csv_to_sql(filename, table_name, process_riders=False):
    filepath = os.path.join(csv_folder, filename)
    if not os.path.exists(filepath):
        print(f"Skipped (not found): {filename}")
        return

    print(f"Loading {filename} into table: {table_name}")
    df = pd.read_csv(filepath)

    if process_riders:
        # Normalize name and team_name
        if "name" in df.columns:
            df["name"] = normalize_spaces(df["name"])
        if "team_name" in df.columns:
            df["team_name"] = normalize_spaces(df["team_name"])

        # Extract numeric fields
        if "weight" in df.columns:
            df["weight_kg"] = df["weight"].astype(str).str.extract(r"([\d.]+)").astype(float)

        if "height" in df.columns:
            df["height_m"] = df["height"].astype(str).str.extract(r"([\d.]+)").astype(float)

        # Add rider_id as PRIMARY KEY
        df.insert(0, "rider_id", range(1, len(df) + 1))

        # Create table manually with correct types
        cursor.execute("DROP TABLE IF EXISTS riders")
        cursor.execute("""
            CREATE TABLE riders (
                rider_id INTEGER PRIMARY KEY,
                name TEXT,
                age INTEGER,
                weight TEXT,
                height TEXT,
                nationality TEXT,
                team_name TEXT,
                team_url TEXT,
                rider_url TEXT,
                weight_kg REAL,
                height_m REAL
            )
        """)
        df.to_sql("riders", conn, if_exists="append", index=False)
        return

    # All other tables: automatic import
    df.to_sql(table_name, conn, if_exists="replace", index=False)


# Load riders with processing
load_csv_to_sql("giro2025_all_riders.csv", "riders", process_riders=True)

# Load main race tables
load_csv_to_sql("giro2025_gc.csv", "gc")
load_csv_to_sql("giro2025_stages.csv", "stages")
load_csv_to_sql("giro2025_startlist.csv", "startlist")

# Load results for 21 stages
for i in range(1, 22):
    filename = f"stage_{i}_results.csv"
    table_name = f"stage_{i}_results"
    load_csv_to_sql(filename, table_name)

# Load all team_*.csv files into a unified 'teams' table
team_rows = []

for fname in os.listdir(csv_folder):
    if fname.startswith("team_") and fname.endswith(".csv"):
        path = os.path.join(csv_folder, fname)
        team_df = pd.read_csv(path)

        # Normalize rider name and team name
        if "name" in team_df.columns:
            team_df["name"] = normalize_spaces(team_df["name"])
        if "team_name" in team_df.columns:
            team_df["team_name"] = normalize_spaces(team_df["team_name"])
        else:
            # Infer from filename if not in file
            inferred_team = fname.removeprefix("team_").removesuffix(".csv").replace("_", " ")
            team_df["team_name"] = inferred_team

        team_rows.append(team_df)

if team_rows:
    all_teams = pd.concat(team_rows, ignore_index=True)

    # Drop existing table and create new one
    cursor.execute("DROP TABLE IF EXISTS teams")
    all_teams.to_sql("teams", conn, if_exists="replace", index=False)
    print(f"Loaded {len(all_teams)} rows into 'teams' table")
else:
    print("No team CSV files found.")

conn.commit()
conn.close()
print(f"SQLite database created: {db_path}")
