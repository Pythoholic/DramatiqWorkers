import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker

# Set up the RabbitMQ broker
rabbitmq_broker = RabbitmqBroker(url="amqp://guest:guest@localhost:5672")
dramatiq.set_broker(rabbitmq_broker)


class AIModel:
    # Singleton pattern implementation
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = AIModel()
        return cls._instance


    def process_data(self, data):
        # Process data with the AI model
        return f"Processed: {data}"


# Actor for model management
@dramatiq.actor(queue_name="ai_model_queue")
def ai_model_process(data):
    model = AIModel.get_instance()
    result = model.process_data(data)
    return result


# Other worker tasks
@dramatiq.actor(queue_name="task_queue")
def some_task(data):
    # This task needs AI model processing
    result = ai_model_process.send(data)
    print(f"Result from AI model: {result.get_result()}")


if __name__ == "__main__":
    # Example of sending a task
    for _ in range(10):
        some_task.send("sample data")