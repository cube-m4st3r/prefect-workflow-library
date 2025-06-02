import logging
import time
import json
import threading
import requests
from prefect.variables import Variable
from .filters import ContextFilter

loki_json = Variable.get("lokiapiurl")


class LokiHandler(logging.Handler):
    def __init__(self, url, labels=None, auth=None):
        super().__init__()
        self.url = url
        self.labels = labels or {}
        self.auth = auth

    def emit(self, record):
        try:
            log_entry = self.format(record)
            timestamp_ns = str(int(time.time() * 1e9))  # nanoseconds

            stream = {
                "stream": {
                    **self.labels,
                    **getattr(record, "tags", {}),
                    "level": record.levelname.lower(),
                    "logger": record.name
                },
                "values": [[timestamp_ns, log_entry]]
            }

            headers = {"Content-Type": "application/json"}
            payload = {"streams": [stream]}

            threading.Thread(
                target=requests.post,
                args=(self.url,),
                kwargs={"data": json.dumps(payload), "headers": headers, "auth": self.auth},
                daemon=True
            ).start()

        except Exception:
            self.handleError(record)



def get_logger(
    name: str,
    *,
    url: str = loki_json["url"],
    labels: dict = None,
    auth: tuple = None,
    level: str = "info"
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not any(isinstance(h, LokiHandler) for h in logger.handlers):
        handler = LokiHandler(url=url, labels=labels, auth=auth)
        handler.setLevel(getattr(logging, level.upper(), logging.INFO))

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)

        handler.addFilter(ContextFilter())

        logger.addHandler(handler)

    return logger