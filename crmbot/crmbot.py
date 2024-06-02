import logging

import asyncio

from datetime import datetime, date, timedelta, time

if __name__ == '__main__':
    today = date.today()
    print(today)
    next_day = today + timedelta(days=1)
    print(next_day)
    next_day_time = datetime.combine(next_day, time(23, 0))
    print(next_day_time)
    delta_t = next_day_time - datetime.now()
    print(delta_t.total_seconds())

    next_day_time = datetime.combine(date.today() + timedelta(days=1), time(23, 0))
    pass
    delta_t = next_day_time - datetime.now()
    print(delta_t.total_seconds())
