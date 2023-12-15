import resource
from flask import Flask, request
import script_dramatiq_worker 

# Set a higher limit for the number of open files
resource.setrlimit(resource.RLIMIT_NOFILE, (999999, 999999))

app = Flask(__name__)

@app.route('/enqueue_task', methods=['POST'])
def enqueue_task():
    data = request.json
    task_name = data.get("task")
    message = data.get("message", None)

    if task_name == "proc_confirm_email":
        script_dramatiq_worker.proc_confirm_email.send()
    elif task_name == "proc_send_email":
        script_dramatiq_worker.proc_send_email.send()
    elif task_name == "proc_async_scan":
        script_dramatiq_worker.proc_async_scan.send()
    elif task_name == "proc_update_user":
        script_dramatiq_worker.proc_update_user.send()
    elif task_name == "is_healthy":
        script_dramatiq_worker.is_healthy.send()
    elif task_name == "search_task_names":
        script_dramatiq_worker.search_task_names.send()
    elif task_name == "scrap_role_galaxy":
        script_dramatiq_worker.scrap_role_galaxy.send()
    elif task_name == "scrap_repository":
        script_dramatiq_worker.scrap_repository.send()
    elif task_name == "rate_limit_process":
        script_dramatiq_worker.rate_limit_process.send()
    elif task_name == "matcher_model_usage" and message:
        script_dramatiq_worker.matcher_model_usage.send(message)

    return {"status": "task enqueued"}

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)
