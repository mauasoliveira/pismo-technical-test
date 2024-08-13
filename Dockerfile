FROM spark:3.5.2-python3

USER root
RUN pip install pyspark==3.5.2

USER spark
WORKDIR /app
COPY pismo.py /app/pismo.py

CMD [ "python3", "/app/pismo.py" ]
