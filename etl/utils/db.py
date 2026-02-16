import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine() -> Engine:
    """
    Create a SQLAlchemy Engine using environment variables from a local .env file.

    Required env vars (see .env.example):
      DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
    """
    load_dotenv()

    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    db = os.getenv("DB_NAME", "faf_dw")

    # Important: encode password to safely handle special characters like @, #, %
    password_encoded = quote_plus(password)

    url = f"mysql+pymysql://{user}:{password_encoded}@{host}:{port}/{db}"

    return create_engine(
        url,
        pool_pre_ping=True,
        future=True,
        hide_parameters=True,   # prevents huge SQL dumps in errors/logs
    )
