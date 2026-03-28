import requests
import pandas as pd
import random

API_URL  = "https://prices.openfoodfacts.org/api/v1/prices"
PAGE_SIZE = 50

# ── 1. Stats globales sans télécharger ──
stats = requests.get("https://prices.openfoodfacts.org/api/v1/stats").json()
print("Stats globales :", stats)

# ── 2. Taille réelle de l'API ──
r = requests.get(API_URL, params={"page": 1, "size": 1}).json()
total_items = r["total"]
total_pages = (total_items // PAGE_SIZE) + 1
print(f"Total prices : {total_items:,}  |  Pages : {total_pages:,}")

# ── 3. Sonder 50% → pages réparties uniformément (pas aléatoires) ──
# Uniformément = couvre tout le dataset, pas de biais
step = 2  # 1 page sur 2 = 50%
sampled_pages = list(range(1, total_pages + 1, step))
print(f"Pages à sonder : {len(sampled_pages):,}  (~{100/step:.0f}% du total)")

# ── 4. Téléchargement par chunks → évite de saturer la RAM ──
CHUNK_SIZE = 1000  # analyser par lots de 1000 prices
null_counts = {}
total_seen  = 0

for i in range(0, len(sampled_pages), CHUNK_SIZE // PAGE_SIZE):
    chunk_pages = sampled_pages[i : i + CHUNK_SIZE // PAGE_SIZE]
    chunk_data  = []

    for page in chunk_pages:
        try:
            items = requests.get(API_URL, params={"page": page, "size": PAGE_SIZE}, timeout=15).json().get("items", [])
            chunk_data.extend(items)
        except Exception as e:
            print(f"  ❌ page {page} : {e}")

    if not chunk_data:
        continue

    df_chunk = pd.json_normalize(chunk_data)
    total_seen += len(df_chunk)

    # Accumuler les null counts
    for col in df_chunk.columns:
        null_counts[col] = null_counts.get(col, 0) + df_chunk[col].isnull().sum()

    print(f"  Chunk {i} → {total_seen:,} prices traités")

# ── 5. Résultat final ──
null_pct = pd.Series({col: (n / total_seen * 100) for col, n in null_counts.items()})
print(f"\n Analysé : {total_seen:,} prices ({total_seen/total_items*100:.1f}% de l'API)\n")
print(null_pct.sort_values(ascending=True).to_string())