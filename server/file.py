import json
import logging
import queue
import time
import traceback
import threading
from typing import Tuple

class FileDelivery:
    def __init__(
        self, filepath: str
    ) -> None:
        self.queue = queue.Queue()
        self.filepath = filepath
        self.l = logging.getLogger("FileDelivery")

        self.thread = threading.Thread(target=self.do)
        self.thread.start()

    def do(self):
        """
        Internal method that checks the delivery queue for outgoing SMS that should be sent to File.
        """
        while True:
            try:
                _sms = self.queue.get(timeout=10)
                self.l.info(f"[{_sms.get_id()}] Event in SMS-to-File delivery queue.")
                if _sms:
                    self.l.info(f"[{_sms.get_id()}] Try to deliver SMS to File.")

                    if self.write({
                        'sms_id': _sms.get_id,
                        'recipient': _sms.recipient,
                        'text': _sms.text,
                        'timestamp': _sms.timestamp,
                        'sender': _sms.sender,
                        'receiving_modem': _sms.receiving_modem,
                    }):
                        self.l.info(f"[{_sms.get_id()}] SMS sended to File.")
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
                self.l.warning("FileDelivery.do(): Unknown exception.")
                traceback.print_exc()

    def write(self, jsonb: dict) -> bool:
        fp = open(self.filepath, 'w')
        fp.write(json.dumps(jsonb))
        return True

    def do_health_check(self) -> Tuple[str, str]:
        pass