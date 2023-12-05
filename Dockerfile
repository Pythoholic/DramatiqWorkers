FROM python:3.8

# Install ping and telnet
RUN apt-get update && apt-get install -y iputils-ping telnet

WORKDIR /worker_app
COPY script_dramatiq_worker.py /worker_app
COPY requirements.txt /worker_app
COPY execute_things.sh /worker_app
COPY script_dramatiq_send_message.py /worker_app

RUN pip install -r requirements.txt
RUN chmod +x /worker_app/execute_things.sh

CMD ["/worker_app/execute_things.sh"]
