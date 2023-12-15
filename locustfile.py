import time
import resource
from locust import HttpUser, task, between

# Set a higher limit for the number of open files
try:
    resource.setrlimit(resource.RLIMIT_NOFILE, (1000000, 1000000)) 
except:
    print("Couldn't raise resource limit")

class DramatiqUser(HttpUser):
    wait_time = between(4, 7)

    @task
    def enqueue_priority_tasks(self):
        self.client.post("/enqueue_task/proc_confirm_email", json={"task": "proc_confirm_email"})
        time.sleep(1)  # Introduce a 1-second delay
        self.client.post("/enqueue_task/proc_send_email", json={"task": "proc_send_email"})
        time.sleep(1)  # Introduce a 1-second delay
        self.client.post("/enqueue_task/proc_async_scan", json={"task": "proc_async_scan"})
        time.sleep(1)  # Introduce a 1-second delay
        self.client.post("/enqueue_task/proc_update_user", json={"task": "proc_update_user"})
        time.sleep(1)  # Introduce a 1-second delay
        self.client.post("/enqueue_task/is_healthy", json={"task": "is_healthy"})
        time.sleep(1)  # Introduce a 1-second delay
        self.client.post("/enqueue_task/search_task_names", json={"task": "search_task_names"})
        time.sleep(1)  # Introduce a 1-second delay
        self.client.post("/enqueue_task/scrap_role_galaxy", json={"task": "scrap_role_galaxy"})
        time.sleep(1)  # Introduce a 1-second delay
        self.client.post("/enqueue_task/scrap_repository", json={"task": "scrap_repository"})
        time.sleep(1)  # Introduce a 1-second delay

    @task
    def enqueue_rate_limited_task(self):
        self.client.post("/enqueue_task/rate_limit_process", json={"task": "rate_limit_process"})
        time.sleep(1)  # Introduce a 1-second delay

    @task
    def enqueue_singleton_task(self):
        self.client.post("/enqueue_task/matcher_model_usage", json={"task": "matcher_model_usage", "message": "Test Message"})
        time.sleep(1)  # Introduce a 1-second delay
