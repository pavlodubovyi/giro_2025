import os
import sqlite3
import pandas as pd

csv_folder = "giro2025"
db_path = "giro_2025.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()


def normalize_spaces(series):
    return series.astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()


# Step 1: Load riders and build lookup
def load_riders():
    filepath = os.path.join(csv_folder, "giro2025_all_riders.csv")
    df = pd.read_csv(filepath)
    df["name"] = normalize_spaces(df["name"])
    df["team_name"] = normalize_spaces(df["team_name"])

    if "rider_number" not in df.columns:
        raise ValueError("Column 'rider_number' is missing in giro2025_all_riders.csv")
    # Move the column to the first position
    cols = ["rider_number"] + [col for col in df.columns if col != "rider_number"]
    df = df[cols]

    cursor.execute("DROP TABLE IF EXISTS riders")
    cursor.execute("""
        CREATE TABLE riders (
            rider_number INTEGER PRIMARY KEY,
            name TEXT,
            birthdate TEXT,
            weight TEXT,
            height TEXT,
            nationality TEXT,
            team_name TEXT,
            team_url TEXT,
            rider_url TEXT
        )
    """)
    df.to_sql("riders", conn, if_exists="append", index=False)

    df["name_clean"] = df["name"].str.lower().str.strip()
    return dict(zip(df["name_clean"], df["rider_number"]))


# Step 2: Load race-related tables and join rider_number
def load_table_with_rider_number(filename, table_name, name_to_number):
    filepath = os.path.join(csv_folder, filename)
    if not os.path.exists(filepath):
        print(f"Skipped (not found): {filename}")
        return

    print(f"Loading {filename} into table: {table_name}")
    df = pd.read_csv(filepath)

    if "name" in df.columns:
        df["name_clean"] = df["name"].astype(str).str.lower().str.strip()
        df["rider_number"] = df["name_clean"].map(name_to_number)
        df.rename(columns={"name": "rider_name"}, inplace=True)
        df.drop(columns=["name_clean"], inplace=True)

    df.to_sql(table_name, conn, if_exists="replace", index=False)


# Step 3: Load team files into unified 'teams' table
def load_teams(name_to_number):
    team_rows = []

    for fname in os.listdir(csv_folder):
        if fname.startswith("team_") and fname.endswith(".csv"):
            path = os.path.join(csv_folder, fname)
            df = pd.read_csv(path)

            if "name" in df.columns:
                df["name"] = normalize_spaces(df["name"])
                df["name_clean"] = df["name"].str.lower().str.strip()
                df["rider_number"] = df["name_clean"].map(name_to_number)
                df.rename(columns={"name": "rider_name"}, inplace=True)
                df.drop(columns=["name_clean"], inplace=True)

            if "team_name" in df.columns:
                df["team_name"] = normalize_spaces(df["team_name"])
            else:
                inferred_team = fname.removeprefix("team_").removesuffix(".csv").replace("_", " ")
                df["team_name"] = inferred_team

            columns_to_drop = [col for col in ["rider_id", "weight_kg", "height_m", "age"] if col in df.columns]
            df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

            team_rows.append(df)

    if team_rows:
        all_teams = pd.concat(team_rows, ignore_index=True)
        cursor.execute("DROP TABLE IF EXISTS teams")
        all_teams.to_sql("teams", conn, if_exists="replace", index=False)
        print(f"Loaded {len(all_teams)} rows into 'teams' table")
    else:
        print("No team CSV files found.")


# Main workflow
name_to_number = load_riders()

# Load GC table
load_table_with_rider_number("giro2025_gc.csv", "gc", name_to_number)

# Load all stage results
for i in range(1, 22):
    load_table_with_rider_number(f"stage_{i}_results.csv", f"stage_{i}_results", name_to_number)

# Load stages table (doesn't need riders)
stages_path = os.path.join(csv_folder, "giro2025_stages.csv")
if os.path.exists(stages_path):
    pd.read_csv(stages_path).to_sql("stages", conn, if_exists="replace", index=False)

# Load teams
load_teams(name_to_number)

# Finalize
conn.commit()
conn.close()
print(f"SQLite database created: {db_path}")
