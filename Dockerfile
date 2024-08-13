# FROM spark:3.5.2-python3
FROM docker.io/bitnami/spark:3.5

USER root
RUN pip install pyspark==3.5.2

WORKDIR /app
COPY pismo.py /app/pismo.py

CMD [ "python3", "/app/pismo.py" ]
