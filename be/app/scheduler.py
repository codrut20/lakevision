import time
from datetime import datetime, timezone
from croniter import croniter
from insights.runner import execute_job
from insights.job_schedule import JobSchedule
import os
from storage import get_storage

os.environ['DATABASE_URL'] = "postgresql://db_user:testpass@192.168.1.101:5432/lakevision"
job_storage = get_storage(model=JobSchedule)
job_storage.connect()
job_storage.ensure_table()

def run_scheduler_cycle():
    """
    This function runs once to check for and trigger due jobs.
    """
    print(f"[{datetime.now()}] Scheduler checking for due jobs...")
    
    # 1. Find all active jobs that are due to run.
    now = datetime.now(timezone.utc)
    # This query is conceptual; your storage implementation would handle it.
    schedules_to_run = job_storage.query("SELECT * FROM jobschedules WHERE is_enabled = TRUE AND next_run_timestamp <= ?", (now,))

    for schedule in schedules_to_run:
        print(f"Triggering job for schedule: {schedule.id}")
        
        # 2. Trigger the job execution (ideally asynchronously).
        # This function (defined in the next section) does the actual work.
        execute_job(schedule) 
        
        # 3. Update the schedule for its next run.
        base_time = now
        iterator = croniter(schedule.cron_schedule, base_time)
        schedule.next_run_timestamp = iterator.get_next(datetime)
        schedule.last_run_timestamp = now
        
        job_storage.save(schedule) # Save the updated timestamps

print(__name__)

if __name__ == "__main__":
    # This loop makes the script run forever.
    # In production, you'd use a real daemon or a cron job to run it.
    while True:
        run_scheduler_cycle()
        time.sleep(60) # Wait for 60 seconds