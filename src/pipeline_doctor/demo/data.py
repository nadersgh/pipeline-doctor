import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
from loguru import logger

_FIRST_NAMES = [
    "Ava",
    "Liam",
    "Noah",
    "Emma",
    "Mia",
    "Leo",
    "Zoe",
    "Omar",
    "Ines",
    "Yuki",
    "Raj",
    "Sara",
    "Theo",
    "Lina",
]
_LAST_NAMES = ["Martin", "Tremblay", "Nguyen", "Garcia", "Smith", "Roy", "Khan", "Lee", "Silva", "Dubois", "Cohen"]
_ORDER_STATUSES = (
    (["completed"] * 60) + (["shipped"] * 15) + (["placed"] * 10) + (["returned"] * 10) + ["return_pending"] * 5
)
_PAYMENT_METHODS = (["credit_card"] * 55) + (["bank_transfer"] * 20) + (["coupon"] * 15) + (["gift_card"] * 10)


@dataclass(frozen=True)
class DemoDataShape:
    customers: int = 400
    orders: int = 2000
    history_days: int = 60
    seed: int = 7


def _loaded_at(now: datetime, days_ago: int) -> datetime:
    return now - timedelta(days=days_ago, hours=1)


def generate_raw_data(duckdb_path: Path, shape: DemoDataShape | None = None) -> None:
    """Creates the raw.customers, raw.orders and raw.payments landing tables with daily load batches."""
    shape = shape or DemoDataShape()
    rng = random.Random(shape.seed)
    now = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    customers, customer_days = _build_customers(rng, shape, now)
    orders = _build_orders(rng, shape, now, customer_days)
    payments = _build_payments(rng, orders)

    logger.info(f"write raw demo data {duckdb_path}")
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute("create schema if not exists raw")
        connection.execute(
            "create or replace table raw.customers (id integer, first_name varchar, last_name varchar, "
            "email varchar, created_at timestamp, _loaded_at timestamp)"
        )
        connection.execute(
            "create or replace table raw.orders (id integer, customer_id integer, order_date date, "
            "status varchar, _loaded_at timestamp)"
        )
        connection.execute(
            "create or replace table raw.payments (id integer, order_id integer, payment_method varchar, "
            "amount integer, _loaded_at timestamp)"
        )
        connection.executemany("insert into raw.customers values (?, ?, ?, ?, ?, ?)", customers)
        connection.executemany("insert into raw.orders values (?, ?, ?, ?, ?)", [row[:5] for row in orders])
        connection.executemany("insert into raw.payments values (?, ?, ?, ?, ?)", payments)


def _build_customers(
    rng: random.Random, shape: DemoDataShape, now: datetime
) -> tuple[list[tuple[object, ...]], dict[int, int]]:
    rows: list[tuple[object, ...]] = []
    signup_days: dict[int, int] = {}
    for customer_id in range(1, shape.customers + 1):
        days_ago = shape.history_days * 2 - (customer_id * shape.history_days * 2) // shape.customers
        first, last = rng.choice(_FIRST_NAMES), rng.choice(_LAST_NAMES)
        created_at = now - timedelta(days=days_ago, hours=rng.randint(2, 20))
        email = f"{first.lower()}.{last.lower()}{customer_id}@example.com"
        rows.append((customer_id, first, last, email, created_at, _loaded_at(now, days_ago)))
        signup_days[customer_id] = days_ago
    return rows, signup_days


def _build_orders(
    rng: random.Random, shape: DemoDataShape, now: datetime, signup_days: dict[int, int]
) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for order_id in range(1, shape.orders + 1):
        days_ago = shape.history_days - (order_id * shape.history_days) // shape.orders
        eligible = [customer for customer, signup in signup_days.items() if signup >= days_ago]
        customer_id = rng.choice(eligible)
        order_date: date = (now - timedelta(days=days_ago)).date()
        status = rng.choice(_ORDER_STATUSES) if days_ago > 3 else rng.choice(["placed", "shipped", "completed"])
        rows.append((order_id, customer_id, order_date, status, _loaded_at(now, days_ago), days_ago))
    return rows


def _build_payments(rng: random.Random, orders: list[tuple[object, ...]]) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    payment_id = 1
    for order_id, _customer, _date, status, loaded_at, _days in orders:
        if status == "placed" and rng.random() < 0.5:
            continue
        payment_count = rng.choices([1, 2, 3], weights=[70, 22, 8])[0]
        for _ in range(payment_count):
            amount_cents = rng.randint(300, 40000) // payment_count
            rows.append((payment_id, order_id, rng.choice(_PAYMENT_METHODS), amount_cents, loaded_at))
            payment_id += 1
    return rows
