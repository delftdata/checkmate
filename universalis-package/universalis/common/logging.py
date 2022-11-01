import datetime
import logging

logging.Formatter.formatTime = (lambda self, record, datefmt: datetime.datetime.
                                fromtimestamp(record.created, datetime.timezone.utc).astimezone().isoformat())

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
                    level=logging.WARNING)
