## Synthetic Data Seeder

This folder contains a generic MySQL seeder for local development.

It is designed for the common case in this repo:

- you already have a schema with multiple related tables
- you want fake rows quickly
- you do not want to hand-write `INSERT` statements

The script reads your schema from `information_schema`, orders tables using
foreign-key dependencies where possible, and generates simple synthetic values
based on column names and MySQL data types. If the target database is empty,
it can also create a small demo schema for you.

### Setup

Use a write-enabled MySQL user for seeding. The pipeline itself can stay
read-only.

Add these optional variables to `.env` if your seed credentials differ from the
query/pipeline credentials:

```env
MYSQL_SEED_HOST=localhost
MYSQL_SEED_PORT=3306
MYSQL_SEED_USER=your_write_user
MYSQL_SEED_PASSWORD=your_write_password
MYSQL_SEED_DATABASE=your_dev_database
```

If `MYSQL_SEED_*` variables are not set, the script falls back to `MYSQL_*`.

### Usage

Seed every table in the configured database:

```bash
uv run python synthetic_data/seed_mysql.py
```

If the database is empty, create a demo schema first and then seed it:

```bash
uv run python synthetic_data/seed_mysql.py --create-demo-schema
```

Seed a smaller dataset:

```bash
uv run python synthetic_data/seed_mysql.py --rows-per-table 10
```

Seed only selected tables:

```bash
uv run python synthetic_data/seed_mysql.py --tables customers,orders,order_items
```

Preview what would happen without writing rows:

```bash
uv run python synthetic_data/seed_mysql.py --dry-run
```

Use a fixed seed for repeatable output:

```bash
uv run python synthetic_data/seed_mysql.py --seed 42
```

Preview what would happen for an empty database without writing tables or rows:

```bash
uv run python synthetic_data/seed_mysql.py --create-demo-schema --dry-run
```

### Notes

- The script skips `AUTO_INCREMENT` and generated columns.
- It handles typical single-column foreign keys by sampling existing or newly
  inserted parent values.
- The built-in demo schema contains `customers`, `products`, `orders`, and
  `order_items`.
- `--create-demo-schema` only works when the target database has no tables.
- It works best on ordinary business schemas. Highly custom constraints,
  composite foreign keys, or strict domain rules may still require manual
  adjustments.
- If some tables already contain rows, the script will append more rows.
