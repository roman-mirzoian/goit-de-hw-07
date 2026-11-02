"""Load the course dataset into olympic_dataset.athlete_event_results.

The CSV is the same file the course uses (https://ftp.goit.study/neoversity/).
"""
import csv
import os
import urllib.request

from db import SOURCE_TABLE, get_connection

URL = "https://ftp.goit.study/neoversity/athlete_event_results.csv"
CSV_PATH = os.path.join("data", "athlete_event_results.csv")
BATCH_SIZE = 5000

COLUMNS = [
    "edition", "edition_id", "country_noc", "sport", "event", "result_id",
    "athlete", "athlete_id", "pos", "medal", "isTeamSport",
]
INT_COLUMNS = {"edition_id", "result_id", "athlete_id"}

if not os.path.exists(CSV_PATH):
    os.makedirs("data", exist_ok=True)
    print(f"Downloading {URL}")
    urllib.request.urlretrieve(URL, CSV_PATH)


def to_value(column, value):
    if value == "":
        return None
    return int(value) if column in INT_COLUMNS else value


connection = get_connection()
with connection.cursor() as cursor:
    cursor.execute("CREATE DATABASE IF NOT EXISTS olympic_dataset")
    cursor.execute(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")
    cursor.execute(f"""
        CREATE TABLE {SOURCE_TABLE} (
            edition VARCHAR(100),
            edition_id INT,
            country_noc VARCHAR(10),
            sport VARCHAR(100),
            event VARCHAR(255),
            result_id INT,
            athlete VARCHAR(255),
            athlete_id INT,
            pos VARCHAR(50),
            medal VARCHAR(10),
            isTeamSport VARCHAR(10)
        )
    """)

    insert_sql = (
        f"INSERT INTO {SOURCE_TABLE} ({', '.join(COLUMNS)}) "
        f"VALUES ({', '.join(['%s'] * len(COLUMNS))})"
    )

    total = 0
    batch = []
    with open(CSV_PATH, encoding="utf-8-sig", newline="") as csv_file:
        for row in csv.DictReader(csv_file):
            batch.append([to_value(column, row[column]) for column in COLUMNS])
            if len(batch) == BATCH_SIZE:
                cursor.executemany(insert_sql, batch)
                total += len(batch)
                batch = []
                print(f"Inserted {total} rows", end="\r")
        if batch:
            cursor.executemany(insert_sql, batch)
            total += len(batch)

    print(f"Inserted {total} rows into {SOURCE_TABLE}")
    cursor.execute(f"SELECT medal, COUNT(*) FROM {SOURCE_TABLE} WHERE medal IS NOT NULL GROUP BY medal")
    for medal, count in cursor.fetchall():
        print(f"  {medal}: {count}")

connection.close()
