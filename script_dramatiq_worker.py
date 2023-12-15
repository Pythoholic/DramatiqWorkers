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
import os
import dramatiq
import coloredlogs
import logging
import time
import redis
import pika
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.results import Results
from dramatiq.rate_limits import ConcurrentRateLimiter
from dramatiq.rate_limits.backends import RedisBackend
import dramatiq.results.backends as result_backend
from dramatiq.middleware import GroupCallbacks
import dramatiq.rate_limits.backends as rate_limit
from dramatiq.middleware import Middleware
from contextlib import contextmanager


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Process ID %(process)d] %(message)s",
    handlers=[logging.FileHandler("dramatiq_log.log"), logging.StreamHandler()],
)

coloredlogs.install(
    level="INFO",
    fmt="%(asctime)s [%(levelname)s] [Process ID %(process)d] %(message)s",
    level_styles={"info": {"color": "blue"}},
)


LOADED_MODEL = "Matcher Model Already Loaded."


class BrokerFlushMiddleware(Middleware):
    def after_worker_boot(self, broker, worker):
        broker.flush_all()
        logging.info("Broker flushed successfully.")


class ConnectionClose(Middleware):
    def after_declare_actor(self, broker, _actor):
        broker.close()


# RABBITMQ_HOST = "rabbitmq"
# REDIS_HOST = "redis"

# RabbitMQ and Redis configuration
RABBITMQ_HOST = "127.0.0.1"
REDIS_HOST = "127.0.0.1"

# Connection Pool for Redis
redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=6379, db=0)


# Context manager to create and close RabbitMQ connections
@contextmanager
def create_rabbitmq_connection():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    try:
        yield connection  # Provide the connection to the caller
    finally:
        connection.close()  # Ensure the connection is always closed


# Let's set up the RabbitMQ broker
rabbitmq_broker = RabbitmqBroker(url=f"amqp://guest:guest@{RABBITMQ_HOST}:5672")
rabbitmq_broker.add_middleware(
    Results(backend=result_backend.RedisBackend(url=f"redis://{REDIS_HOST}:6379/0"))
)

rabbitmq_broker.add_middleware(BrokerFlushMiddleware())

# Add the Connection Close middleware
rabbitmq_broker.add_middleware(ConnectionClose())

rate_limiter = rate_limit.RedisBackend(url=f"redis://{REDIS_HOST}:6379/0")
rabbitmq_broker.add_middleware(GroupCallbacks(rate_limiter))
dramatiq.set_broker(rabbitmq_broker)
RATE_LIMIT_CONTROLLER = ConcurrentRateLimiter(
    RedisBackend(url=f"redis://{REDIS_HOST}:6379/0"), "rate-limit-controller", limit=1
)


# Creating the matcher model Singleton to load it only once
class MatcherModelSingleton:
    _instance = None
    _lock_model_key = "model_lock_load"

    # _redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)

    @classmethod
    def get_instance(cls):
        with redis.Redis(connection_pool=redis_pool) as client:  # Using connection pool
            if cls._instance is None:
                lock_acquired = client.set(cls._lock_model_key, "1", nx=True, ex=60)
                if lock_acquired:
                    logging.info("Lock Acquired, Loading the matcher model.")
                    cls._instance = cls._create_instance()
                    client.delete(cls._lock_model_key)
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
        time.sleep(5)  # Simulate model loading time
        instance.model = "My matcher model."
        instance.model_loaded = True
        process_id = os.getpid()
        logging.info(
            f"AI matcher model loaded successfully in process {process_id}...."
        )

    def load_matcher_model(self, message):
        if self.model_loaded:
            return f"Processing message for {self.model}: {message}"
        else:
            return "AI model not loaded...Sorry..."


@dramatiq.actor(store_results=True, queue_name="matcher_model_queue")
def matcher_model_process(message):
    with create_rabbitmq_connection() as connection:
        model = MatcherModelSingleton.get_instance()
        result = model.load_matcher_model(message)
        return result


# Let's create the tasks for the application
@dramatiq.actor(
    queue_name="proc_conf_email_queue", priority=10, max_retries=3, min_backoff=1000
)
def proc_confirm_email():
    with create_rabbitmq_connection() as connection:
        try:
            logging.info(
                "Task: Confirming Email Process. Priority High. Task Executed."
            )
        except Exception as e:
            logging.error(f"proc_confirm_email encountered an error: {e}. Retrying...")
            raise


@dramatiq.actor(
    queue_name="proc_send_email_queue", priority=10, max_retries=3, min_backoff=1000
)
def proc_send_email():
    with create_rabbitmq_connection() as connection:
        try:
            logging.info("Task: Sending Email. Priority High. Task Executed.")
        except Exception as e:
            logging.error(f"proc_send_email encountered an error: {e}. Retrying...")
            raise


@dramatiq.actor(
    queue_name="proc_async_scan_queue", priority=25, max_retries=5, min_backoff=1000
)
def proc_async_scan():
    with create_rabbitmq_connection() as connection:
        try:
            logging.info(
                "Task: Processing Async Scan. Priority Medium-High. Task Executed."
            )
        except Exception as e:
            logging.error(f"proc_async_scan encountered an error: {e}. Retrying...")
            raise


@dramatiq.actor(
    queue_name="proc_update_user_queue", priority=40, max_retries=3, min_backoff=1000
)
def proc_update_user():
    with create_rabbitmq_connection() as connection:
        try:
            logging.info(
                "Task: Processing Update User. Priority Medium. Task Executed."
            )
        except Exception as e:
            logging.error(f"proc_update_user encountered an error: {e}. Retrying...")
            raise


@dramatiq.actor(
    queue_name="is_healthy_queue", priority=15, max_retries=3, min_backoff=1000
)
def is_healthy():
    with create_rabbitmq_connection() as connection:
        try:
            logging.info("Task: Checking if Healthy. Priority High. Task Executed.")
        except Exception as e:
            logging.error(f"is_healthy encountered an error: {e}. Retrying...")
            raise


@dramatiq.actor(
    queue_name="search_task_names_queue", priority=50, max_retries=3, min_backoff=1000
)
def search_task_names():
    with create_rabbitmq_connection() as connection:
        try:
            logging.info("Task: Search Task Names. Priority Medium. Task Executed.")
        except Exception as e:
            logging.error(f"search_task_names encountered an error: {e}. Retrying...")
            raise


@dramatiq.actor(
    queue_name="scrap_role_galaxy_queue", priority=80, max_retries=5, min_backoff=1000
)
def scrap_role_galaxy():
    with create_rabbitmq_connection() as connection:
        try:
            logging.info("Task: Scrapping Role Galaxy. Priority Low. Task Executed.")
        except Exception as e:
            logging.error(f"scrap_role_galaxy encountered an error: {e}. Retrying...")
            raise


@dramatiq.actor(
    queue_name="scrap_repository_queue", priority=75, max_retries=3, min_backoff=1000
)
def scrap_repository():
    with create_rabbitmq_connection() as connection:
        try:
            logging.info("Task: Scrap Repository. Priority Low. Task Executed.")
        except Exception as e:
            logging.error(f"scrap_repository encountered an error: {e}. Retrying...")
            raise


@dramatiq.actor
def rate_limit_process():
    with RATE_LIMIT_CONTROLLER.acquire():
        logging.info("Task: Executing Rate Limiting.")


@dramatiq.actor(queue_name="matcher_model_usage_queue")
def matcher_model_usage(message):
    with create_rabbitmq_connection() as connection:
        messages = matcher_model_process.send(message)
        logging.info(f"Result from the Matcher Model: {messages}")


if __name__ == "__main__":
    pass
