from __future__ import annotations

import os

import mysql.connector
from dotenv import load_dotenv

import src.core.config as config


def connect_for_seed() -> mysql.connector.MySQLConnection:
    load_dotenv()
    return mysql.connector.connect(
        host=_seed_value("MYSQL_SEED_HOST", config.MYSQL_HOST),
        port=int(_seed_value("MYSQL_SEED_PORT", str(config.MYSQL_PORT))),
        user=_seed_value("MYSQL_SEED_USER", config.MYSQL_USER),
        password=_seed_value("MYSQL_SEED_PASSWORD", config.MYSQL_PASSWORD),
        database=_seed_value("MYSQL_SEED_DATABASE", config.MYSQL_DATABASE),
    )


def _seed_value(name: str, fallback: str | None) -> str | None:
    return os.getenv(name, fallback)
