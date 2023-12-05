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
import time
import redis
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.results import Results
from dramatiq.rate_limits import ConcurrentRateLimiter
from dramatiq.rate_limits.backends import RedisBackend
import dramatiq.results.backends as result_backend
from dramatiq.middleware import GroupCallbacks
import dramatiq.rate_limits.backends as rate_limit


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

LOADED_MODEL = "Matcher Model Already Loaded."

# RABBITMQ_HOST = "rabbitmq"
# REDIS_HOST = "redis"

RABBITMQ_HOST = "127.0.0.1"
REDIS_HOST = "127.0.0.1"

# Let's set up the RabbitMQ broker
rabbitmq_broker = RabbitmqBroker(url=f"amqp://guest:guest@{RABBITMQ_HOST}:5672")

redis_backend = result_backend.RedisBackend(url=f"redis://{REDIS_HOST}:6379/0")
rabbitmq_broker.add_middleware(Results(backend=redis_backend))

rate_limiter = rate_limit.RedisBackend(url=f"redis://{REDIS_HOST}:6379/0")
rabbitmq_broker.add_middleware(GroupCallbacks(rate_limiter))

# Let's set up the rate limiter and create the broker/define the actor
redis_backend = RedisBackend(url=f"redis://{REDIS_HOST}:6379/0")
# rabbitmq_broker.add_middleware(Results(backend=redis_backend))
dramatiq.set_broker(rabbitmq_broker)

RATE_LIMIT_CONTROLLER = ConcurrentRateLimiter(
    redis_backend, "rate-limit-controller", limit=1
)


# Creating the matcher model Singleton to load it only once
class MatcherModelSingleton:
    _instance = None
    _lock_model_key = "model_lock_load"
    _redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            lock_acquired = cls._redis_client.set(
                cls._lock_model_key, "1", nx=True, ex=60
            )
            if lock_acquired:
                logging.info("Lock Acquired, Loading the matcher model.")
                cls._instance = cls._create_instance()
                cls._redis_client.delete(cls._lock_model_key)
            else:
                logging.info("Lock already acquired, skipping matcher model.")
                while cls._instance is None:
                    time.sleep(1)

        return cls._instance

    @classmethod
    def _create_instance(cls):
        instance = super(MatcherModelSingleton, cls).__new__(cls)
        instance.model_loaded = False
        cls._load_ai_model(instance)
        return instance

    @staticmethod
    def _load_ai_model(instance):
        time.sleep(10)
        instance.model = "My matcher model."
        instance.model_loaded = True
        logging.info("AI matcher model loaded successfully....")

    def load_matcher_model(self, message):
        if self.model_loaded:
            return f"Processing message for {self.model}: {message}"
        else:
            return "AI model not loaded...Sorry..."


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
    messages = matcher_model_process.send(message)
    logging.info(f"Result from the Matcher Model: {messages}")


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
