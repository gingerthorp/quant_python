import datetime
import schedule
import time

def job():
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(now)

schedule.every(1).minutes.do(job)

while 1:
    schedule.run_pending()
    time.sleep(60)