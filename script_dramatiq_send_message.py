import dramatiq
from script_dramatiq_worker import (
    proc_confirm_email,
    proc_send_email,
    proc_async_scan,
    proc_update_user,
    is_healthy,
    search_task_names,
    scrap_role_galaxy,
    scrap_repository,
    rate_limit_process,
    matcher_model_usage,
)

if __name__ == "__main__":
    # Enqueue tasks to be processed by the Dramatiq workers
    proc_confirm_email.send()
    proc_send_email.send()
    proc_async_scan.send()
    proc_update_user.send()
    is_healthy.send()
    search_task_names.send()
    scrap_role_galaxy.send()
    scrap_repository.send()

    for _ in range(10):
        rate_limit_process.send()

    for _ in range(10):
        matcher_model_usage.send("My message for testing..")
