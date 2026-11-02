import os

import pymysql

ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

RESULTS_TABLE = "roman_mirzoian.hw_dag_results"
SOURCE_TABLE = "olympic_dataset.athlete_event_results"


def load_env(path=ENV_FILE):
    """Read KEY=VALUE pairs from .env into os.environ (without overriding existing variables)."""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as env_file:
        for line in env_file:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


def get_connection():
    load_env()
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        autocommit=True,
    )
