"""
Author: Sam
Name: Dramatiq Worker, Single Container Architecture POC.

Description:
This script is created to provide a Proof of Concept for Dramatiq Worker Priority implementation and
single container architecture feasibility. This script is also using the Dramatiq task distribution library in Python
Here, the script focuses on bringing into list the execution and implementation feasibility of Task Prioritization,
Rate limiting and solving the "only load matcher model once".

This script is to be used within a docker container to run as a part of the container application to simulate the
way we would want to run the application in prod.
"""

import dramatiq
import coloredlogs
import logging

# import time
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.results import Results
from dramatiq.rate_limits import ConcurrentRateLimiter
from dramatiq.rate_limits.backends import RedisBackend

coloredlogs.install(
    level="INFO",
    fmt="%(asctime)s [%(levelname)s] %(message)s",
    level_styles={"info": {"color": "blue"}},
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("dramatiq_log.log"), logging.StreamHandler()],
)

LOADED_MODEL = "Great ! Matcher Model Already Loaded."
# Let's set up the RabbitMQ broker
rabbitmq_broker = RabbitmqBroker(url="amqp://guest:guest@rabbitmq:5672")
dramatiq.set_broker(rabbitmq_broker)


# Let's set up the rate limiter and create the broker/define the actor
redis_backend = RedisBackend(url="redis://redis:6379/0")
rabbitmq_broker.add_middleware(Results(backend=redis_backend))
RATE_LIMIT_CONTROLLER = ConcurrentRateLimiter(
    redis_backend, "rate-limit-controller", limit=1
)


# Creating the matcher model Singleton to load it only once
class MatcherModelSingleton:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = MatcherModelSingleton()
        return cls._instance

    def load_matcher_model(self, message):
        logging.info("Loading the matcher model.")
        return f"Processing message: {message}"


@dramatiq.actor(store_results=True, queue_name="matcher_model_queue")
def matcher_model_process(message):
    model = MatcherModelSingleton.get_instance()
    result = model.load_matcher_model(message)
    return result


# Let's create the tasks for the application
@dramatiq.actor(
    queue_name="proc_conf_email_queue", priority=10, max_retries=3, min_backoff=1000
)
def proc_confirm_email():
    # logging.info("Task: Confirming Email. Priority High. Task Started.")
    # time.sleep(25)
    logging.info("Task: Confirming Email Process. Priority High. Task Executed.")


@dramatiq.actor(
    queue_name="proc_send_email_queue", priority=10, max_retries=3, min_backoff=1000
)
def proc_send_email():
    # logging.info("Task: Sending Email. Priority High. Task Started.")
    # time.sleep(25)
    logging.info("Task: Sending Email. Priority High. Task Executed.")


@dramatiq.actor(
    queue_name="proc_async_scan_queue", priority=25, max_retries=5, min_backoff=1000
)
def proc_async_scan():
    # logging.info("Task: Processing Async Scan. Priority Medium-High. Task Started.")
    # time.sleep(5)
    logging.info("Task: Processing Async Scan. Priority Medium-High. Task Executed.")


@dramatiq.actor(
    queue_name="proc_update_user_queue", priority=40, max_retries=3, min_backoff=1000
)
def proc_update_user():
    logging.info("Task: Processing Update User. Priority Medium. Task Executed.")


@dramatiq.actor(
    queue_name="is_healthy_queue", priority=15, max_retries=3, min_backoff=1000
)
def is_healthy():
    # logging.info("Task: Checking if Healthy. Priority High. Task Started.")
    # time.sleep(35)
    logging.info("Task: Checking if Healthy. Priority High. Task Executed.")


@dramatiq.actor(
    queue_name="search_task_names_queue", priority=50, max_retries=3, min_backoff=1000
)
def search_task_names():
    # logging.info("Task: Search Task Names. Priority Medium. Task Started.")
    # time.sleep(15)
    logging.info("Task: Search Task Names. Priority Medium. Task Executed.")


@dramatiq.actor(
    queue_name="scrap_role_galaxy_queue", priority=80, max_retries=5, min_backoff=1000
)
def scrap_role_galaxy():
    # logging.info("Task: Scrapping Role Galaxy. Priority Low. Task Started.")
    # time.sleep(10)
    logging.info("Task: Scrapping Role Galaxy. Priority Low. Task Executed.")


@dramatiq.actor(
    queue_name="scrap_repository_queue", priority=75, max_retries=3, min_backoff=1000
)
def scrap_repository():
    logging.info("Task: Scrap Repository. Priority Low. Task Executed.")


@dramatiq.actor
def rate_limit_process():
    with RATE_LIMIT_CONTROLLER.acquire():
        logging.info("Task: Executing Rate Limiting.")


@dramatiq.actor(queue_name="matcher_model_usage_queue")
def matcher_model_usage(message):
    message = matcher_model_process.send(message)
    result = message.get_result(block=True, timeout=10000)
    logging.info(f"Result from the Matcher Model: {result}")


if __name__ == "__main__":
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
