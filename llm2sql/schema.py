DDL = """\
CREATE TABLE IF NOT EXISTS account_managers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    region TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT NOT NULL,
    segment TEXT NOT NULL CHECK (segment IN ('enterprise', 'mid_market', 'smb')),
    account_manager_id INTEGER REFERENCES account_managers(id),
    acquisition_date TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'inactive', 'churned'))
);

CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    list_price REAL NOT NULL,
    product_type TEXT NOT NULL CHECK (product_type IN ('equipment', 'spare_part', 'service_package'))
);

CREATE TABLE IF NOT EXISTS machines (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    product_id INTEGER REFERENCES products(id),
    serial_number TEXT NOT NULL UNIQUE,
    installation_date TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'decommissioned'))
);

CREATE TABLE IF NOT EXISTS service_contracts (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    machine_id INTEGER REFERENCES machines(id),
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    annual_value REAL NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'expired', 'cancelled'))
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    account_manager_id INTEGER REFERENCES account_managers(id),
    order_date TEXT NOT NULL,
    total_value REAL NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'cancelled'))
);

CREATE TABLE IF NOT EXISTS order_lines (
    id INTEGER PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    line_value REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS quotes (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    account_manager_id INTEGER REFERENCES account_managers(id),
    created_date TEXT NOT NULL,
    sent_date TEXT,
    expiry_date TEXT,
    total_value REAL NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('draft', 'sent', 'won', 'lost', 'expired'))
);

CREATE TABLE IF NOT EXISTS quote_lines (
    id INTEGER PRIMARY KEY,
    quote_id INTEGER REFERENCES quotes(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL
);
"""

BUSINESS_CONTEXT = """\
at_risk_account     = customer with status='active' AND no order in last 180 days
                      AND has at least 1 active machine in installed base

stale_quote         = quote with status='sent' AND sent_date < today - 45 days

service_penetration = count(machines with active service contract)
                      / count(total active machines) × 100  [per customer]

high_value_account  = customer in top 20% by lifetime order total_value

cross_sell_candidate = customer with ≥1 active machine AND service_penetration = 0

churn_signal        = order count in last 6 months < order count in same period prior year

expansion_potential = count(active machines) × (1 - service_penetration / 100)
                      [higher = more untapped service revenue]

active_account      = at least 1 completed order in last 12 months

revenue_at_risk     = sum of avg annual spend for all at_risk_accounts
"""
