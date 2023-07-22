import datetime
import json
import logging
import ssl
from email.mime.text import MIMEText
from typing import Tuple
import psycopg2 as psycopg2

from sms import SMS


class DBDelivery:
    def __init__(
        self, dsn: str
    ) -> None:

        self.dsn = dsn
        self.l = logging.getLogger("DatabaseDelivery")

    def _create_connection(self) -> None:
        self.conn = psycopg2.connect(self.dsn)

    def query(self, jsonb: dict) -> bool:
        self.conn.autocommit = True
        cursor = self.conn.cursor()
        sql = '''SELECT ub.api_call(%s)'''.format(json.dumps(jsonb))
        cursor.execute(sql)
        return True

