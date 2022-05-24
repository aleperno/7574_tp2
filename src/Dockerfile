FROM python:3.9.7-slim
RUN pip3 install --user pika
RUN pip3 install --user retry
RUN pip3 install --user pandas
RUN pip3 install --user more_itertools
COPY src /src/
RUN python3 /src/setup.py install
ENTRYPOINT ["/bin/sh"]
