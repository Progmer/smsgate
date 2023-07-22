import json
import logging
import queue
import time
import traceback
import threading
from typing import Tuple

import psycopg2 as psycopg2

class DBDelivery:
    def __init__(
        self, dsn: str
    ) -> None:
        self.queue = queue.Queue()
        self.dsn = dsn
        self.l = logging.getLogger("DatabaseDelivery")

        self.thread = threading.Thread(target=self.do)
        self.thread.start()

        self.conn = psycopg2.connect(self.dsn)

    def do(self):
        """
        Internal method that checks the delivery queue for outgoing SMS that should be sent to DB.
        """
        while True:
            try:
                _sms = self.queue.get(timeout=10)
                self.l.info(f"[{_sms.get_id()}] Event in SMS-to-DB delivery queue.")
                if _sms:
                    self.l.info(f"[{_sms.get_id()}] Try to deliver SMS to DB.")

                    if self.query({
                        'sms_id': _sms.get_id,
                        'recipient': _sms.recipient,
                        'text': _sms.text,
                        'timestamp': _sms.timestamp,
                        'sender': _sms.sender,
                        'receiving_modem': _sms.receiving_modem,
                    }):
                        self.l.info(f"[{_sms.get_id()}] SMS sended to DB.")
                    else:
                        self.l.info(f"[{_sms.get_id()}] There was an error delivering the SMS. Put SMS back into "
                                    "queue and wait.")
                        self.queue.put(_sms)

                        time.sleep(30)
            except queue.Empty:
                self.l.debug(
                    "db_delivery.do(): No SMS in queue. Checking if health check should be run."
                )
                self.do_health_check()

            except:
                self.l.warning("DBDelivery.do(): Unknown exception.")
                traceback.print_exc()

    def query(self, jsonb: dict) -> bool:
        self.conn.autocommit = True
        cursor = self.conn.cursor()
        jsonb['api_call'] = '/parser/sms'
        sql = '''SELECT ub.api_call(%s)'''.format(json.dumps(jsonb))
        self.l.debug('DB Query: ' + sql)
        cursor.execute(sql)
        return True

    def do_health_check(self) -> Tuple[str, str]:
        pass