import random
import time

from prefect import flow, get_run_logger, task
from prefect.states import Completed

from db import RESULTS_TABLE, SOURCE_TABLE, get_connection

MEDALS = ["Bronze", "Silver", "Gold"]

SENSOR_MAX_AGE_SECONDS = 30
SENSOR_POKE_INTERVAL_SECONDS = 5
SENSOR_TIMEOUT_SECONDS = 20


class SensorCheckFailed(Exception):
    """The newest record did not become fresh enough before the sensor timeout."""


def execute(sql, params=None, fetch=False):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone() if fetch else None
    finally:
        connection.close()


# 1. Create the results table
@task(name="create_table")
def create_table():
    execute("CREATE DATABASE IF NOT EXISTS roman_mirzoian")
    execute(f"""
        CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        )
    """)
    get_run_logger().info(f"Table {RESULTS_TABLE} is ready")


# 2. Pick a random medal
@task(name="pick_medal")
def pick_medal():
    medal = random.choice(MEDALS)
    get_run_logger().info(f"Picked medal: {medal}")
    return medal


# 3. Branching: decide which calc task has to run
@task(name="pick_medal_task")
def pick_medal_task(medal):
    branch = f"calc_{medal}"
    get_run_logger().info(f"Branch to run: {branch}")
    return branch


# 4. Count records with the medal and store the result; tasks that are not on the
# chosen branch finish in the "Skipped" state, like skipped tasks in Airflow
@task
def calc_medal(medal, branch):
    logger = get_run_logger()
    if branch != f"calc_{medal}":
        logger.info(f"Branch {branch} was chosen, skipping calc_{medal}")
        return Completed(name="Skipped", message=f"Branch {branch} was chosen")

    execute(
        f"""
        INSERT INTO {RESULTS_TABLE} (medal_type, count, created_at)
        SELECT %s, COUNT(*), NOW()
        FROM {SOURCE_TABLE}
        WHERE medal = %s
        """,
        (medal, medal),
    )
    count = execute(f"SELECT count FROM {RESULTS_TABLE} ORDER BY id DESC LIMIT 1", fetch=True)[0]
    logger.info(f"{medal}: {count} records saved to {RESULTS_TABLE}")
    return count


# 5. Delay before the check, runs after the calc task finished successfully
@task(name="generate_delay")
def generate_delay(seconds):
    get_run_logger().info(f"Sleeping for {seconds} seconds")
    time.sleep(seconds)


# 6. Sensor: the newest record must not be older than 30 seconds
@task(name="check_for_correctness")
def check_for_correctness():
    logger = get_run_logger()
    started = time.monotonic()
    while True:
        age = execute(
            f"SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) FROM {RESULTS_TABLE}",
            fetch=True,
        )[0]
        logger.info(f"Newest record is {age} seconds old (limit {SENSOR_MAX_AGE_SECONDS})")
        if age is not None and age <= SENSOR_MAX_AGE_SECONDS:
            logger.info("Check passed: the record was written recently")
            return age
        if time.monotonic() - started >= SENSOR_TIMEOUT_SECONDS:
            raise SensorCheckFailed(
                f"Sensor timed out: newest record is {age} seconds old, "
                f"limit is {SENSOR_MAX_AGE_SECONDS} seconds"
            )
        time.sleep(SENSOR_POKE_INTERVAL_SECONDS)


@flow(name="medal_count_flow", log_prints=True)
def medal_count_flow(delay_seconds: int = 5):
    create_table()
    medal = pick_medal()
    branch = pick_medal_task(medal)

    calc_states = [
        calc_medal.with_options(name=f"calc_{name}")(name, branch, return_state=True)
        for name in MEDALS
    ]
    # Runs only when the chosen calc task succeeded
    chosen_state = next(state for state, name in zip(calc_states, MEDALS) if branch == f"calc_{name}")
    if not chosen_state.is_completed():
        raise RuntimeError(f"{branch} did not finish successfully")

    generate_delay(delay_seconds)
    check_for_correctness()


if __name__ == "__main__":
    import sys

    medal_count_flow(delay_seconds=int(sys.argv[1]) if len(sys.argv) > 1 else 5)
