# Preparation
First I scrapped the data from [ProCyclingStats](https://www.procyclingstats.com/), using Python (_pandas_ and _procyclingstats_ libraties). *get_giro.py* is the script, all the data is saved into *giro2025* folder.

Somehow there were errors fetching full profile for Simon Yates (the winner!) and Diego Ulissi. I have to rename the files manually to make them unified, and to search for information online in order to complete them.

The SQLite database is built with build_giro_db.py script.