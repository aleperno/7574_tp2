FROM python:3.9.7-slim
RUN pip3 install --user pika retry
COPY src /src/
RUN python3 /src/setup.py install
ENTRYPOINT ["/bin/sh"]
