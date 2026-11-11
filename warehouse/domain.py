from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal


def overview(connection):
    rows = [dict(record) for record in connection.execute('SELECT partition_day, COUNT(*) AS rows, SUM(bytes) AS bytes FROM scenes GROUP BY partition_day ORDER BY partition_day')]
    return rows
