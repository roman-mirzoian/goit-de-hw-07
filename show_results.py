"""Print the results table, like SELECT * FROM roman_mirzoian.hw_dag_results in a SQL client."""
from db import RESULTS_TABLE, get_connection

connection = get_connection()
with connection.cursor() as cursor:
    cursor.execute(f"SELECT id, medal_type, count, created_at FROM {RESULTS_TABLE} ORDER BY id")
    rows = cursor.fetchall()
connection.close()

print(f"SELECT * FROM {RESULTS_TABLE};\n")
header = ("id", "medal_type", "count", "created_at")
widths = [max(len(str(value)) for value in column) for column in zip(header, *rows)]
line = "+" + "+".join("-" * (width + 2) for width in widths) + "+"
print(line)
print("| " + " | ".join(name.ljust(width) for name, width in zip(header, widths)) + " |")
print(line)
for row in rows:
    print("| " + " | ".join(str(value).ljust(width) for value, width in zip(row, widths)) + " |")
print(line)
print(f"{len(rows)} rows")
