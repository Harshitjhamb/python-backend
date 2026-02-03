import time
import schedule
from app import app, sync_external_data
def job():
    print("🔄 Running hourly sync...")
    with app.app_context():
        sync_external_data()
print("⏳ Scheduler started")
with app.app_context():
    sync_external_data()
schedule.every().hour.at(":15").do(job)
while True:
    schedule.run_pending()
    time.sleep(1)