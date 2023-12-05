from locust import HttpUser, task, between

class DramatiqUser(HttpUser):
    wait_time = between(1, 5)

    @task
    def enqueue_priority_tasks(self):
        self.client.post("/enqueue_task", json={"task": "proc_confirm_email"})
        self.client.post("/enqueue_task", json={"task": "proc_send_email"})
        self.client.post("/enqueue_task", json={"task": "proc_async_scan"})
        self.client.post("/enqueue_task", json={"task": "proc_update_user"})
        self.client.post("/enqueue_task", json={"task": "is_healthy"})
        self.client.post("/enqueue_task", json={"task": "search_task_names"})
        self.client.post("/enqueue_task", json={"task": "scrap_role_galaxy"})
        self.client.post("/enqueue_task", json={"task": "scrap_repository"})

    @task
    def enqueue_rate_limited_task(self):
        self.client.post("/enqueue_task", json={"task": "rate_limit_process"})

    @task
    def enqueue_singleton_task(self):
        self.client.post("/enqueue_task", json={"task": "matcher_model_usage", "message": "Test Message"})
