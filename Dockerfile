FROM python:3.8

RUN apt-get update && apt-get install -y iputils-ping telnet

WORKDIR /worker_app
COPY script_dramatiq_worker.py /worker_app
COPY requirements.txt /worker_app
COPY script_dramatiq_send_message.py /worker_app
COPY execute_script.sh /worker_app

RUN pip install -r requirements.txt
RUN chmod +x /worker_app/execute_script.sh

EXPOSE 80

CMD ["/worker_app/execute_script.sh"]