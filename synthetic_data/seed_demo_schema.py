from __future__ import annotations

import mysql.connector

DEMO_TABLE_NAMES = [
    "customers",
    "products",
    "orders",
    "order_items",
]


def create_demo_schema(conn: mysql.connector.MySQLConnection) -> None:
    cursor = conn.cursor()
    statements = [
        """
        CREATE TABLE `customers` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `full_name` VARCHAR(255) NOT NULL,
            `email` VARCHAR(255) NOT NULL,
            `city` VARCHAR(120) NOT NULL,
            `status` VARCHAR(50) NOT NULL,
            `created_at` DATETIME NOT NULL,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uq_customers_email` (`email`)
        )
        """,
        """
        CREATE TABLE `products` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(255) NOT NULL,
            `sku` VARCHAR(64) NOT NULL,
            `category` VARCHAR(100) NOT NULL,
            `price` DECIMAL(10, 2) NOT NULL,
            `created_at` DATETIME NOT NULL,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uq_products_sku` (`sku`)
        )
        """,
        """
        CREATE TABLE `orders` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `customer_id` BIGINT NOT NULL,
            `order_date` DATE NOT NULL,
            `status` VARCHAR(50) NOT NULL,
            `total_amount` DECIMAL(10, 2) NOT NULL,
            PRIMARY KEY (`id`),
            KEY `idx_orders_customer_id` (`customer_id`),
            CONSTRAINT `fk_orders_customer`
                FOREIGN KEY (`customer_id`) REFERENCES `customers` (`id`)
        )
        """,
        """
        CREATE TABLE `order_items` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `order_id` BIGINT NOT NULL,
            `product_id` BIGINT NOT NULL,
            `quantity` INT NOT NULL,
            `unit_price` DECIMAL(10, 2) NOT NULL,
            PRIMARY KEY (`id`),
            KEY `idx_order_items_order_id` (`order_id`),
            KEY `idx_order_items_product_id` (`product_id`),
            CONSTRAINT `fk_order_items_order`
                FOREIGN KEY (`order_id`) REFERENCES `orders` (`id`),
            CONSTRAINT `fk_order_items_product`
                FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)
        )
        """,
    ]

    try:
        for statement in statements:
            cursor.execute(statement)
        conn.commit()
    finally:
        cursor.close()


def parse_demo_selected_tables(raw_tables: str) -> set[str]:
    if not raw_tables.strip():
        return set(DEMO_TABLE_NAMES)

    requested = {table.strip() for table in raw_tables.split(",") if table.strip()}
    unknown = sorted(requested - set(DEMO_TABLE_NAMES))
    if unknown:
        raise ValueError(
            "Unknown table(s) for demo schema: " + ", ".join(unknown)
        )
    return requested
