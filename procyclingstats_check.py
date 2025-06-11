from procyclingstats import Race, Rider

# race = Race("race/giro-d-italia/2025")
# print([attr for attr in dir(race) if not attr.startswith("_")])

from procyclingstats import Rider

r = Rider("rider/tadej-pogacar")

# Update HTML to make sure the data is loaded
r.update_html()

# Try accessing birthdate as a property
print("Birthdate:", r.birthdate)

