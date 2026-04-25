"""
update_records.py

Incrementally fetches GBIF records interpreted since the last pull
and appends new ones to the SQLite database.

Run on a schedule via Windows Task Scheduler.
"""

import os
import sqlite3
import time
import logging
from datetime import date
from dotenv import load_dotenv
import pandas as pd
from pygbif import species as gbif_species
from pygbif import occurrences

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

DB_PATH = "data/gbif_records.db"
LOG_DIR = "logs"
BBOX    = "POLYGON((-3.7 56.85,-1.75 56.85,-1.75 57.75,-3.7 57.75,-3.7 56.85))"

KEEP_COLS = [
    "key", "decimalLatitude", "decimalLongitude",
    "year", "month", "basisOfRecord",
    "datasetName", "coordinateUncertaintyInMeters",
    "lastInterpreted",
]

# ── Logging ───────────────────────────────────────────────────────────────────

os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "update.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_taxon_key():
    match = gbif_species.name_backbone(
        scientificName="Sciurus vulgaris", taxonRank="species"
    )
    return int(match["usage"]["key"])


def check_schema(conn):
    cols = [r[1] for r in conn.execute("PRAGMA table_info(red_squirrel)").fetchall()]
    missing = [c for c in ("key", "lastInterpreted") if c not in cols]
    if missing:
        raise SystemExit(
            f"Database missing columns: {missing}. Re-run notebook 01 to rebuild."
        )


def get_last_interpreted(conn):
    row = conn.execute(
        "SELECT MAX(lastInterpreted) FROM red_squirrel"
    ).fetchone()
    val = row[0]
    if val is None:
        raise SystemExit("No records in database. Run notebook 01 first.")
    return val[:10]  # trim to YYYY-MM-DD


def fetch_since(taxon_key, since_date):
    date_filter = f"{since_date},{date.today().isoformat()}"
    records, offset, limit = [], 0, 300

    while True:
        batch = occurrences.search(
            taxonKey           = taxon_key,
            geometry           = BBOX,
            hasCoordinate      = True,
            hasGeospatialIssue = False,
            lastInterpreted    = date_filter,
            limit              = limit,
            offset             = offset,
        )
        records.extend(batch["results"])
        log.info(f"  Fetched {len(records):,} / {batch['count']:,}")
        if batch["endOfRecords"]:
            break
        offset += limit
        time.sleep(0.3)

    return records


def append_new(conn, df):
    existing_keys = set(
        r[0] for r in conn.execute("SELECT key FROM red_squirrel").fetchall()
    )
    new = df[~df["key"].isin(existing_keys)]
    if len(new) > 0:
        new.to_sql("red_squirrel", conn, if_exists="append", index=False)
    return len(new), len(df) - len(new)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Update started")

    with sqlite3.connect(DB_PATH) as conn:
        check_schema(conn)
        since = get_last_interpreted(conn)
        log.info(f"Last interpreted date in database: {since}")

        taxon_key = get_taxon_key()
        log.info(f"Taxon key: {taxon_key}")

        records = fetch_since(taxon_key, since)

        if not records:
            log.info("No new records found. Database is up to date.")
        else:
            df = pd.DataFrame(records)
            cols = [c for c in KEEP_COLS if c in df.columns]
            df   = df[cols].dropna(subset=["decimalLatitude", "decimalLongitude"])

            added, skipped = append_new(conn, df)
            log.info(f"Added {added:,} new records ({skipped:,} duplicates skipped)")

    log.info("Update complete")
