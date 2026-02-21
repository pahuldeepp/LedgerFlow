import psycopg
from psycopg.rows import dict_row
from .settings import settings

def get_conn():
    return psycopg.connect(settings.database_url, row_factory=dict_row)