from __future__ import annotations

import mysql.connector

import src.core.config as config


def connect() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DATABASE,
    )

