from datetime import date, datetime, timedelta
import uuid
import time

HOUR = 'hour'
DAY = 'day'
MONTH = 'month'

class Timeline(object):

    def __init__(self, column_family, split_by='day'):
        self.column_family = column_family
        self.split_by = split_by
        if self.split_by is HOUR:
            self.split_delta = timedelta(hours=1)
        if self.split_by is DAY:
            self.split_delta = timedelta(days=1)
        if self.split_by is MONTH:
            self.split_delta = timedelta(month=1)

    def _get_key(self, time):
        if hasattr(time, 'strftime'):
            current_date = time
        else:
            current_date = datetime.fromtimestamp(timestamp)
        if self.split_by is HOUR:
            return current_date.strftime('%Y%m%d%H')
        elif self.split_by is DAY:
            return current_date.strftime('%Y%m%d')
        elif self.split_by is MONTH:
            return current_date.strftime('%Y%m')

    def append(self, entry):
        ts = time.time()
        self.insert(entry, ts)

    def insert(self, entry, timestamp):
        key = self._get_key(timestamp)
        self.column_family.insert(key, {timestamp: entry})

    def get_last(self, count=1):
        results = []
        ts = ColumnFamily.gm_timestamp()
        while len(result) < count):
            key = self._get_key(ts)
            try:
                results.extend(column_family.get(key, column_reversed=True, column_count=count).items())
            except NotFoundException:
                pass
        return results

    def get_between(self, start, end=None):
        if not hasattr(start, 'timetuple'):
            start = datetime.fromtimestamp(start)
        if end is None:
            end = datetime.utcnow()
        else:
            if not hasattr(end, 'timetuple'):
                end = datetime.fromtimestamp()
        results = []
        current = start
        while current < end:
            results.extend[self.column_family.get(self._get_key(current), column_start=start, column_finish=finish).items()]
            current += self.split_delta
        return results

