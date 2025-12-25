import csv
import time
import hashlib
import requests
import psycopg2
from datetime import date
from tqdm import tqdm
import os

# ---------------- CONFIG ----------------
BATCH_SIZE = 100
SLEEP = 2
HEADERS = {"User-Agent": "ISSN-Metadata-Bot/1.0"}

# ---------------- DB ----------------
conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    port=os.environ["DB_PORT"],
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"]
)
cur = conn.cursor()

# ---------------- CREATE TABLES ----------------
cur.execute("""
CREATE TABLE IF NOT EXISTS issn_metadata_fact (
    issn TEXT,
    journal_title TEXT,
    publisher TEXT,
    doi_prefix TEXT,
    country TEXT,
    open_access BOOLEAN,
    fetch_date DATE,
    record_hash TEXT
)
""")
conn.commit()

# ---------------- LOAD ISSNS ----------------
issns = []
with open("issn_master.csv", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        issns.append(row["issn"])

# ---------------- FUNCTIONS ----------------
def fetch_crossref(issn):
    try:
        r = requests.get(
            f"https://api.crossref.org/journals/{issn}",
            headers=HEADERS,
            timeout=20
        )
        if r.status_code == 200:
            return r.json()["message"]
    except:
        pass
    return {}

def fetch_openalex(issn):
    try:
        r = requests.get(
            "https://api.openalex.org/sources",
            params={"filter": f"issn:{issn}"},
            headers=HEADERS,
            timeout=20
        )
        if r.status_code == 200 and r.json()["results"]:
            return r.json()["results"][0]
    except:
        pass
    return {}

def hash_row(values):
    return hashlib.sha256("|".join(values).encode()).hexdigest()

# ---------------- MAIN LOOP ----------------
today = date.today()

for i in tqdm(range(0, len(issns), BATCH_SIZE)):
    batch = issns[i:i+BATCH_SIZE]

    for issn in batch:
        cr = fetch_crossref(issn)
        oa = fetch_openalex(issn)

        journal = cr.get("title")
        publisher = cr.get("publisher")
        prefix = cr.get("prefix")
        country = oa.get("country_code")
        oa_flag = oa.get("is_oa")

        row_hash = hash_row([
            issn,
            str(journal),
            str(publisher),
            str(prefix),
            str(country),
            str(oa_flag)
        ])

        cur.execute("""
        SELECT 1 FROM issn_metadata_fact
        WHERE issn=%s AND record_hash=%s
        """, (issn, row_hash))

        if not cur.fetchone():
            cur.execute("""
            INSERT INTO issn_metadata_fact
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                issn, journal, publisher, prefix,
                country, oa_flag, today, row_hash
            ))
            conn.commit()

    time.sleep(SLEEP)

print("DONE")
