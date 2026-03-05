"""Create and seed the benchmark SQLite database with deterministic synthetic data."""

import random
import sqlite3
from datetime import date, timedelta

from llm2sql.config import DATA_DIR, DB_PATH
from llm2sql.schema import DDL
from llm2sql.queries import QUERIES

random.seed(42)

# --- Helpers ---

def _rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 0)))


def _today() -> date:
    return date(2026, 3, 4)


# --- Data generators ---

REGIONS = ["EMEA", "APAC", "Americas", "Nordics"]
COUNTRIES = ["Finland", "Sweden", "Germany", "USA", "Japan", "UK", "France", "Australia"]
SEGMENTS = ["enterprise", "mid_market", "smb"]

PRODUCT_CATALOG = [
    # (name, category, list_price, product_type)
    ("CNC Mill X200", "Milling", 85000, "equipment"),
    ("CNC Mill X500", "Milling", 120000, "equipment"),
    ("Laser Cutter L10", "Cutting", 65000, "equipment"),
    ("Laser Cutter L30", "Cutting", 95000, "equipment"),
    ("3D Printer P100", "Additive", 45000, "equipment"),
    ("Drill Press D50", "Drilling", 32000, "equipment"),
    ("X200 Spindle Assembly", "Milling", 4500, "spare_part"),
    ("X500 Coolant Pump", "Milling", 2800, "spare_part"),
    ("L10 Lens Module", "Cutting", 3200, "spare_part"),
    ("L30 Power Supply", "Cutting", 5100, "spare_part"),
    ("P100 Print Head", "Additive", 6000, "spare_part"),
    ("D50 Chuck Assembly", "Drilling", 1800, "spare_part"),
    ("Universal Filter Set", "General", 950, "spare_part"),
    ("Basic Maintenance Plan", "Service", 8000, "service_package"),
    ("Premium Support Plan", "Service", 18000, "service_package"),
]


def create_db() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(DDL)
    return conn


def seed_db(conn: sqlite3.Connection) -> None:
    today = _today()

    # --- Account managers (5) ---
    ams = [
        (1, "Alice Martin", "alice@company.com", "EMEA"),
        (2, "Bob Chen", "bob@company.com", "APAC"),
        (3, "Carol Smith", "carol@company.com", "Americas"),
        (4, "David Kim", "david@company.com", "Nordics"),
        (5, "Eva Johansson", "eva@company.com", "EMEA"),
    ]
    conn.executemany("INSERT INTO account_managers VALUES (?,?,?,?)", ams)

    # --- Products (15) ---
    for i, (name, cat, price, ptype) in enumerate(PRODUCT_CATALOG, 1):
        conn.execute("INSERT INTO products VALUES (?,?,?,?,?)", (i, name, cat, price, ptype))

    # --- Customers (30) ---
    # Design customers with specific characteristics for query coverage:
    # C1-C5: active, recent orders (healthy)
    # C6-C10: active, old orders, have machines (at-risk)
    # C11-C15: active, some orders, mixed signals
    # C16-C20: active, equipment buyers (some with spare parts, some without)
    # C21-C25: active, various penetration levels
    # C26-C28: churned/inactive
    # C29-C30: active, no orders ever but have machines

    customers = []
    for i in range(1, 31):
        if i <= 25:
            status = "active"
        elif i <= 27:
            status = "churned"
        elif i == 28:
            status = "inactive"
        else:
            status = "active"

        customers.append((
            i,
            f"Customer_{i:02d}",
            random.choice(COUNTRIES),
            random.choice(SEGMENTS),
            random.choice([1, 2, 3, 4, 5]),
            str(_rand_date(date(2020, 1, 1), date(2024, 6, 1))),
            status,
        ))
    conn.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?,?)", customers)

    # --- Machines (60+) ---
    machine_id = 0
    equipment_ids = list(range(1, 7))  # products 1-6 are equipment

    # Every active customer gets 1-4 machines
    for cid in range(1, 30):  # customers 1-29 (not 30, keep one machineless)
        n_machines = random.randint(1, 4)
        for _ in range(n_machines):
            machine_id += 1
            pid = random.choice(equipment_ids)
            install_date = str(_rand_date(date(2021, 1, 1), date(2025, 6, 1)))
            status = "active" if random.random() < 0.85 else "decommissioned"
            conn.execute(
                "INSERT INTO machines VALUES (?,?,?,?,?,?)",
                (machine_id, cid, pid, f"SN-{machine_id:04d}", install_date, status),
            )
    # Customer 30 gets 2 active machines (for at-risk with no orders)
    for _ in range(2):
        machine_id += 1
        conn.execute(
            "INSERT INTO machines VALUES (?,?,?,?,?,?)",
            (machine_id, 30, random.choice(equipment_ids), f"SN-{machine_id:04d}", "2023-06-15", "active"),
        )

    total_machines = machine_id

    # --- Service contracts ---
    # Give some machines active contracts, some expired, some none
    sc_id = 0
    # Full coverage for customers 1-3
    rows = conn.execute(
        "SELECT id, customer_id FROM machines WHERE customer_id IN (1,2,3) AND status='active'"
    ).fetchall()
    for mid, cid in rows:
        sc_id += 1
        conn.execute(
            "INSERT INTO service_contracts VALUES (?,?,?,?,?,?,?)",
            (sc_id, cid, mid, "2025-01-01", "2026-12-31", random.choice([8000, 18000]), "active"),
        )

    # Partial coverage for customers 4-5, 11-15
    for cid in [4, 5, 11, 12, 13, 14, 15]:
        rows = conn.execute(
            "SELECT id FROM machines WHERE customer_id=? AND status='active'", (cid,)
        ).fetchall()
        for mid_row in rows[:1]:  # contract on first machine only
            sc_id += 1
            conn.execute(
                "INSERT INTO service_contracts VALUES (?,?,?,?,?,?,?)",
                (sc_id, cid, mid_row[0], "2025-03-01", "2026-03-01", 8000, "active"),
            )

    # Expired contracts for customers 6-7
    for cid in [6, 7]:
        rows = conn.execute(
            "SELECT id FROM machines WHERE customer_id=? AND status='active'", (cid,)
        ).fetchall()
        for mid_row in rows[:1]:
            sc_id += 1
            conn.execute(
                "INSERT INTO service_contracts VALUES (?,?,?,?,?,?,?)",
                (sc_id, cid, mid_row[0], "2023-01-01", "2024-12-31", 8000, "expired"),
            )

    # No contracts for customers 8-10, 16-25, 29-30 (cross-sell candidates)

    # --- Orders ---
    order_id = 0
    line_id = 0
    spare_part_ids = list(range(7, 14))  # products 7-13

    def add_order(cid, am_id, odate, status, products_list):
        nonlocal order_id, line_id
        order_id += 1
        total = 0
        lines = []
        for pid in products_list:
            line_id += 1
            price = PRODUCT_CATALOG[pid - 1][2]
            qty = random.randint(1, 3)
            lv = price * qty
            total += lv
            lines.append((line_id, order_id, pid, qty, price, lv))
        conn.execute(
            "INSERT INTO orders VALUES (?,?,?,?,?,?)",
            (order_id, cid, am_id, str(odate), total, status),
        )
        conn.executemany("INSERT INTO order_lines VALUES (?,?,?,?,?,?)", lines)

    # Customers 1-5: recent orders (healthy) — orders in last 2 months
    for cid in range(1, 6):
        am = customers[cid - 1][4]
        # Recent order
        add_order(cid, am, _rand_date(today - timedelta(days=60), today - timedelta(days=5)),
                  "completed", [random.choice(equipment_ids)])
        # Also a spare part order
        add_order(cid, am, _rand_date(today - timedelta(days=90), today - timedelta(days=10)),
                  "completed", [random.choice(spare_part_ids)])
        # Historical orders for churn signal baseline
        for _ in range(random.randint(1, 3)):
            add_order(cid, am, _rand_date(today - timedelta(days=365), today - timedelta(days=180)),
                      "completed", [random.choice(equipment_ids + spare_part_ids)])

    # Customers 6-10: old orders only (at-risk) — last order > 180 days ago
    for cid in range(6, 11):
        am = customers[cid - 1][4]
        add_order(cid, am, _rand_date(date(2025, 1, 1), date(2025, 6, 1)),
                  "completed", [random.choice(equipment_ids)])
        # Another old order
        add_order(cid, am, _rand_date(date(2024, 6, 1), date(2024, 12, 1)),
                  "completed", [random.choice(spare_part_ids)])

    # Customers 11-15: mixed — some recent, some old, declining pattern
    for cid in range(11, 16):
        am = customers[cid - 1][4]
        # More orders in prior 6 months than recent 6 months (churn signal)
        # Prior period: 3 orders
        for _ in range(3):
            add_order(cid, am, _rand_date(today - timedelta(days=365), today - timedelta(days=181)),
                      "completed", [random.choice(equipment_ids + spare_part_ids)])
        # Recent period: 1 order
        add_order(cid, am, _rand_date(today - timedelta(days=170), today - timedelta(days=10)),
                  "completed", [random.choice(spare_part_ids)])

    # Customers 16-20: equipment buyers, some without spare parts (for Q12)
    for cid in range(16, 21):
        am = customers[cid - 1][4]
        add_order(cid, am, _rand_date(today - timedelta(days=300), today - timedelta(days=30)),
                  "completed", [random.choice(equipment_ids)])
        if cid <= 18:  # 16-18 also buy spare parts
            add_order(cid, am, _rand_date(today - timedelta(days=200), today - timedelta(days=20)),
                      "completed", [random.choice(spare_part_ids)])

    # Customers 21-25: various orders for coverage
    for cid in range(21, 26):
        am = customers[cid - 1][4]
        n_orders = random.randint(2, 5)
        for _ in range(n_orders):
            add_order(cid, am, _rand_date(date(2024, 1, 1), today - timedelta(days=5)),
                      "completed", [random.choice(equipment_ids + spare_part_ids)])

    # Customer 29-30: no orders (at-risk, have machines)
    # No orders added for these

    # Add some cancelled/pending orders for realism
    for cid in [1, 3, 5]:
        am = customers[cid - 1][4]
        add_order(cid, am, _rand_date(today - timedelta(days=30), today),
                  "pending", [random.choice(equipment_ids)])
    for cid in [2, 4]:
        am = customers[cid - 1][4]
        add_order(cid, am, _rand_date(today - timedelta(days=60), today - timedelta(days=30)),
                  "cancelled", [random.choice(spare_part_ids)])

    # --- Orders for Q4 (revenue this year = 2026) ---
    # Ensure some customers have completed orders in 2026
    for cid in range(1, 6):
        am = customers[cid - 1][4]
        add_order(cid, am, _rand_date(date(2026, 1, 5), today - timedelta(days=1)),
                  "completed", [random.choice(equipment_ids)])

    # --- Orders for Q18 (declining quarters) ---
    # Customer 11: create clear declining pattern across 4 quarters
    am = customers[10][4]
    # Q2-2025: 4 orders
    for _ in range(4):
        add_order(11, am, _rand_date(date(2025, 4, 1), date(2025, 6, 28)),
                  "completed", [random.choice(spare_part_ids)])
    # Q3-2025: 3 orders
    for _ in range(3):
        add_order(11, am, _rand_date(date(2025, 7, 1), date(2025, 9, 28)),
                  "completed", [random.choice(spare_part_ids)])
    # Q4-2025: 2 orders
    for _ in range(2):
        add_order(11, am, _rand_date(date(2025, 10, 1), date(2025, 12, 28)),
                  "completed", [random.choice(spare_part_ids)])

    # --- Quotes ---
    quote_id = 0
    ql_id = 0

    def add_quote(cid, am_id, created, sent, expiry, status, products_list):
        nonlocal quote_id, ql_id
        quote_id += 1
        total = 0
        lines = []
        for pid in products_list:
            ql_id += 1
            price = PRODUCT_CATALOG[pid - 1][2]
            qty = random.randint(1, 2)
            total += price * qty
            lines.append((ql_id, quote_id, pid, qty, price))
        conn.execute(
            "INSERT INTO quotes VALUES (?,?,?,?,?,?,?,?)",
            (quote_id, cid, am_id, str(created), str(sent) if sent else None,
             str(expiry) if expiry else None, total, status),
        )
        conn.executemany("INSERT INTO quote_lines VALUES (?,?,?,?,?)", lines)

    # Stale quotes (sent > 45 days ago, status='sent') — for Q2
    for cid in [6, 8, 10, 15, 20]:
        am = customers[cid - 1][4]
        sent_d = today - timedelta(days=random.randint(50, 90))
        add_quote(cid, am, sent_d - timedelta(days=5), sent_d,
                  sent_d + timedelta(days=30), "sent", [random.choice(equipment_ids)])

    # Active/draft quotes (pipeline) — for Q11
    for cid in [1, 2, 3, 7, 12]:
        am = customers[cid - 1][4]
        add_quote(cid, am, today - timedelta(days=10), None, None,
                  "draft", [random.choice(equipment_ids)])

    # Sent recent quotes (not stale)
    for cid in [4, 5, 14]:
        am = customers[cid - 1][4]
        sent_d = today - timedelta(days=random.randint(5, 30))
        add_quote(cid, am, sent_d - timedelta(days=3), sent_d,
                  sent_d + timedelta(days=30), "sent", [random.choice(equipment_ids)])

    # Won/lost quotes — for Q14
    for cid in range(1, 16):
        am = customers[cid - 1][4]
        status = "won" if random.random() < 0.6 else "lost"
        d = _rand_date(date(2025, 1, 1), today - timedelta(days=30))
        add_quote(cid, am, d, d + timedelta(days=2), d + timedelta(days=30),
                  status, [random.choice(equipment_ids + spare_part_ids)])

    # Extra won/lost to ensure all AMs have data
    for am_id in range(1, 6):
        for _ in range(2):
            cid = random.choice([c[0] for c in customers if c[4] == am_id and c[6] == "active"][:3] or [1])
            status = "won" if random.random() < 0.5 else "lost"
            d = _rand_date(date(2025, 3, 1), today - timedelta(days=20))
            add_quote(cid, am_id, d, d + timedelta(days=1), d + timedelta(days=30),
                      status, [random.choice(spare_part_ids)])

    conn.commit()


def verify_queries(conn: sqlite3.Connection) -> list[tuple[int, bool, int]]:
    """Run all 20 ground truth queries and check they return rows."""
    results = []
    for q in QUERIES:
        try:
            rows = conn.execute(q["sql"]).fetchall()
            results.append((q["id"], True, len(rows)))
        except Exception as e:
            results.append((q["id"], False, 0))
            print(f"  Q{q['id']} FAILED: {e}")
    return results


def setup() -> None:
    """Full setup: create, seed, verify."""
    print("Creating database...")
    conn = create_db()
    print("Seeding data...")
    seed_db(conn)
    print("Verifying ground truth queries...")
    results = verify_queries(conn)

    all_ok = True
    for qid, ok, nrows in results:
        status = f"OK ({nrows} rows)" if ok and nrows > 0 else f"PROBLEM ({'error' if not ok else '0 rows'})"
        if not ok or nrows == 0:
            all_ok = False
        print(f"  Q{qid:2d}: {status}")

    conn.close()
    if all_ok:
        print(f"\nDatabase ready at {DB_PATH}")
    else:
        print("\nWARNING: Some queries returned no rows — review seed data.")


if __name__ == "__main__":
    setup()
