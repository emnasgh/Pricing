"""
Analyse PRO des % NULL dans products_raw
VERSION FINALE ROBUSTE

- Sampling SYSTEM avec fallback
- Protection lignes corrompues (HTML au lieu de JSON)
- Tri croissant par Null %
- Host configurable via variable d'environnement DB_HOST
  → Dans Docker  : host = "postgres"  (par défaut)
  → Depuis Windows : set DB_HOST=localhost puis lancer le script
"""

import json
import os
import psycopg2

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG DB
# Le host est configurable via variable d'environnement DB_HOST.
# Par défaut "postgres" (nom du service Docker).
# Depuis Windows : $env:DB_HOST="localhost" puis relancer le script.
# ─────────────────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "postgres"),   # ← configurable
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "mydb"),       # ← adapter à ton projet
    "user":     os.getenv("DB_USER",     "airflow"),    # ← adapter à ton projet
    "password": os.getenv("DB_PASSWORD", "airflow"),    # ← adapter à ton projet
}

SAMPLE_PCT      = 10      # % de la table à échantillonner via TABLESAMPLE
MIN_SAMPLE_ROWS = 1000    # fallback si TABLESAMPLE renvoie trop peu de lignes
TOP_FIELDS      = 30      # nombre de champs à afficher


def analyze_products_null():

    try:
        print(f"Connexion à {DB_CONFIG['host']}:{DB_CONFIG['port']} / {DB_CONFIG['dbname']}\n")

        conn   = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # ─────────────────────────────────────────────────────────────────────
        # ÉTAPE 1 : récupérer un échantillon de raw_data
        # On ne caste PAS ::jsonb en SQL — certaines lignes contiennent du HTML
        # invalide qui ferait crasher le cast. On parse en Python ensuite.
        # ─────────────────────────────────────────────────────────────────────

        print(f"Sampling TABLESAMPLE SYSTEM({SAMPLE_PCT}%)...\n")

        cursor.execute(f"""
            SELECT raw_data
            FROM products_raw
            TABLESAMPLE SYSTEM ({SAMPLE_PCT})
            WHERE raw_data IS NOT NULL
              AND length(raw_data) > 2
        """)
        rows = cursor.fetchall()

        # Fallback si TABLESAMPLE renvoie trop peu de lignes
        if len(rows) < 10:
            print(f"⚠️  TABLESAMPLE a renvoyé {len(rows)} lignes — fallback sur LIMIT {MIN_SAMPLE_ROWS}\n")
            cursor.execute(f"""
                SELECT raw_data
                FROM products_raw
                WHERE raw_data IS NOT NULL
                  AND length(raw_data) > 2
                LIMIT {MIN_SAMPLE_ROWS}
            """)
            rows = cursor.fetchall()

        cursor.close()
        conn.close()

        print(f"Lignes récupérées : {len(rows):,}\n")

        if not rows:
            print("❌ Aucune ligne trouvée dans products_raw.")
            return

        # ─────────────────────────────────────────────────────────────────────
        # ÉTAPE 2 : parser chaque ligne en Python
        # json.loads() dans un try/except → les lignes HTML sont ignorées
        # proprement sans crasher le script
        # ─────────────────────────────────────────────────────────────────────

        field_seen     = {}   # field → nombre de fois vu
        field_non_null = {}   # field → nombre de fois rempli
        skipped        = 0
        EMPTY_VALUES   = {"", "[]", "{}", "null"}

        for (raw,) in rows:
            try:
                data = json.loads(raw)              # crash si HTML → except ci-dessous
                if not isinstance(data, dict):
                    skipped += 1
                    continue
                for key, value in data.items():
                    field_seen[key] = field_seen.get(key, 0) + 1
                    str_val   = str(value).strip() if value is not None else ""
                    is_filled = (value is not None and str_val not in EMPTY_VALUES)
                    if is_filled:
                        field_non_null[key] = field_non_null.get(key, 0) + 1

            except (json.JSONDecodeError, TypeError):
                skipped += 1    # ligne corrompue → skip silencieux
                continue

        print(f"Lignes corrompues ignorées : {skipped:,}\n")

        if not field_seen:
            print("❌ Aucun champ trouvé — vérifier le contenu de products_raw.")
            return

        # ─────────────────────────────────────────────────────────────────────
        # ÉTAPE 3 : calculer les stats et trier
        # ─────────────────────────────────────────────────────────────────────

        stats = []
        for field, seen in field_seen.items():
            non_null   = field_non_null.get(field, 0)
            filled_pct = round(100 * non_null / seen, 2)
            null_pct   = round(100 - filled_pct, 2)
            stats.append((field, seen, non_null, null_pct, filled_pct))

        # Tri croissant par null_pct → les plus remplis en premier
        stats.sort(key=lambda x: x[3])
        stats = stats[:TOP_FIELDS]

        # ─────────────────────────────────────────────────────────────────────
        # ÉTAPE 4 : affichage
        # ─────────────────────────────────────────────────────────────────────

        print(f"{'':2} {'Champ':<35} {'Rempli %':>10} {'Null %':>10} {'Vu':>10}   Barre")
        print("─" * 85)

        for field, seen, non_null, null_pct, filled_pct in stats:
            bar    = "█" * int(filled_pct / 5)
            status = (
                "✅" if filled_pct >= 80
                else "⚠️ " if filled_pct >= 40
                else "❌"
            )
            print(f"{status} {field:<35} {filled_pct:>9}% {null_pct:>9}% {seen:>10,}   {bar}")

        print("\n✅ Analyse terminée.")

    except psycopg2.OperationalError as e:
        print(f"\n❌ Impossible de se connecter à la base : {e}")
        print("─" * 60)
        print("💡 Si tu lances ce script depuis Windows (hors Docker) :")
        print("   1. Vérifie que le port 5432 est exposé dans docker-compose.yml")
        print("      ports:")
        print('        - "5432:5432"')
        print("   2. Lance le script avec DB_HOST=localhost :")
        print("      PowerShell : $env:DB_HOST='localhost'; python dags\\analyze_products_null.py")
        print("      CMD        : set DB_HOST=localhost && python dags\\analyze_products_null.py")
        print("─" * 60)

    except Exception as e:
        print(f"❌ ERREUR : {e}")
        raise


if __name__ == "__main__":
    analyze_products_null()