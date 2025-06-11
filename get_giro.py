import os
import re
import pandas as pd
from procyclingstats import Race, Stage, RaceStartlist, Rider


def save_csv(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pd.DataFrame(data).to_csv(path, index=False)
    print(f"Saved {path}")


def sanitize_filename(name):
    """
    Replaces all spaces and multiple underscores with a single underscore.
    Removes problematic characters like slashes.
    """
    name = re.sub(r'[\s/]+', '_', name.strip())  # Replace spaces and slashes with _
    name = re.sub(r'_+', '_', name)              # Replace multiple _ with a single one
    return name


def main():
    base_folder = "giro2025"
    race = Race("race/giro-d-italia/2025")

    # Fetch all stages
    stages = race.stages()
    save_csv(stages, f"{base_folder}/giro2025_stages.csv")

    # Fetch results for each stage
    for stage_info in stages:
        url = stage_info["stage_url"]
        st = Stage(url)
        try:
            results = st.results()
            num = stage_info["stage_name"].split()[1]
            save_csv(results, f"{base_folder}/stage_{num}_results.csv")
        except Exception as e:
            print(f"Error getting results for {url}: {e}")

    # Get General Classification (GC) from the last stage
    try:
        final_stage_url = stages[-1]["stage_url"]
        final_stage = Stage(final_stage_url)
        gc = final_stage.results()
        save_csv(gc, f"{base_folder}/giro2025_gc.csv")
    except Exception as e:
        print(f"Error fetching GC: {e}")

    # Fetch startlist with team information
    try:
        sl = RaceStartlist("race/giro-d-italia/2025/startlist")
        riders = sl.startlist("rider_name", "rider_url", "team_name", "team_url")
        save_csv(riders, f"{base_folder}/giro2025_startlist.csv")
    except Exception as e:
        print(f"Error fetching startlist: {e}")
        riders = []

    # Fetch detailed profile for each rider into a single file
    all_riders = []
    for rider in riders:
        rider_name = rider.get("rider_name", "Unknown")
        try:
            r = Rider(rider["rider_url"])
            info = r.parse()

            filtered_info = {
                "name": info.get("name", rider_name),
                "age": info.get("age"),
                "weight": info.get("weight"),
                "height": info.get("height"),
                "nationality": info.get("nationality"),
                "team_name": rider.get("team_name"),
                "team_url": rider.get("team_url"),
                "rider_url": rider.get("rider_url"),
            }

        except Exception as e:
            print(f"Failed to fetch full profile for {rider_name}: {e}")
            filtered_info = {
                "name": rider_name,
                "age": None,
                "weight": None,
                "height": None,
                "nationality": None,
                "team_name": rider.get("team_name"),
                "team_url": rider.get("team_url"),
                "rider_url": rider.get("rider_url"),
            }

        all_riders.append(filtered_info)

    print("Creating a single file with all riders...")
    save_csv(all_riders, f"{base_folder}/giro2025_all_riders.csv")
    print("giro2025_all_riders.csv created")

    # Group riders by team and save team rosters
    team_dict = {}
    for r in all_riders:
        team = r.get("team_name", "Unknown")
        team_dict.setdefault(team, []).append(r)

    for team, members in team_dict.items():
        filename = f"{base_folder}/team_{sanitize_filename(team)}.csv"
        save_csv(members, filename)


if __name__ == "__main__":
    main()
