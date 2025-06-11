import os
import re
import pandas as pd
import requests
from io import StringIO
from procyclingstats import Race, Stage, RaceStartlist, Rider


def save_csv(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pd.DataFrame(data).to_csv(path, index=False)
    print(f"Saved {path}")


def sanitize_filename(name):
    name = re.sub(r'[\s/]+', '_', name.strip())
    name = re.sub(r'_+', '_', name)
    return name


def scrape_classification(relative_url, filename):
    url = f"https://www.procyclingstats.com/{relative_url}"
    response = requests.get(url)
    if response.status_code == 200:
        html_content = StringIO(response.text)
        tables = pd.read_html(html_content)
        if tables:
            save_csv(tables[0], os.path.join("giro2025", filename))


def main():
    base_folder = "giro2025"
    race = Race("race/giro-d-italia/2025")

    # Save stage list
    stages = race.stages()
    save_csv(stages, f"{base_folder}/giro2025_stages.csv")

    # Save all stage results
    for stage_info in stages:
        url = stage_info["stage_url"]
        st = Stage(url)
        try:
            results = st.results()
            num = stage_info["stage_name"].split()[1]
            save_csv(results, f"{base_folder}/stage_{num}_results.csv")
        except Exception as e:
            print(f"Error getting results for {url}: {e}")

    # Get riders and teams
    try:
        sl = RaceStartlist("race/giro-d-italia/2025/startlist")
        riders = sl.startlist("rider_name", "rider_url", "team_name", "team_url", "rider_number")
    except Exception as e:
        print(f"Error fetching startlist: {e}")
        riders = []

    all_riders = []
    for rider in riders:
        try:
            r = Rider(rider["rider_url"])
            info = r.parse()

            # Safely get birthdate
            try:
                birthdate = r.birthdate()
            except Exception:
                birthdate = None

            all_riders.append({
                "rider_number": rider.get("rider_number"),
                "name": info.get("name", rider.get("rider_name")),
                "birthdate": birthdate,
                "weight": info.get("weight"),
                "height": info.get("height"),
                "nationality": info.get("nationality"),
                "team_name": rider.get("team_name"),
                "team_url": rider.get("team_url"),
                "rider_url": rider.get("rider_url")
            })
        except Exception as e:
            print(f"Failed to fetch full profile for {rider.get('rider_name')}: {e}")

    print("Creating giro2025_all_riders.csv")
    save_csv(all_riders, f"{base_folder}/giro2025_all_riders.csv")

    # Save per-team CSVs
    team_dict = {}
    for r in all_riders:
        team = r.get("team_name", "Unknown")
        team_dict.setdefault(team, []).append(r)

    for team, members in team_dict.items():
        filename = f"{base_folder}/team_{sanitize_filename(team)}.csv"
        save_csv(members, filename)


if __name__ == "__main__":
    main()
