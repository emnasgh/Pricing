import requests
import numpy as np

API_URL = "https://prices.openfoodfacts.org/api/v1/products"

total_pages = requests.get(API_URL, params={"page": 1, "size": 100}).json()["pages"]
pages = list(np.linspace(1, total_pages, 100, dtype=int))  # 100 pages dispersées

all_items = []
for i, page in enumerate(pages):
    r = requests.get(API_URL, params={"page": page, "size": 100})
    all_items.extend(r.json()["items"])
    print(f"{i+1}/100 ✅", end="\r")

# chercher non-food
found = set()
for item in all_items:
    for cat in item.get("categories_tags", []):
        if any(x in cat for x in ["beauty", "cosmetic", "pet", "hygiene"]):
            found.add(cat)

print(f"\nCatégories non-food trouvées : {found}")