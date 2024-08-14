# Pismo Technical Test

This is the repository for the technical test for Pismo Data Engineering application.

This used the fake data generator found on the repository: https://github.com/eder1985/pismo_recruiting_technical_case

# Requirements

* Docker compose configured
* Docker environment
* Local port 4040, 8080, 8088, 8888 and 9870 available to Cluster Application

# Usage

To use this project, just initialize the environment with:

```bash

docker compose up

```

Some applications will be available:

* [Spark Master](http://localhost:8080)
* [Spark Job](http://localhost:4040)
* [HDFS Namenode](http://localhost:9870)
* [HDFS Resource Manager](http://localhost:8088)
* [Jupyter environment](http://localhost:8888)

The full application and documentation is on the notebook [Pismo_DE_Test](http://localhost:8888/notebooks/work/Pismo_DE_Test.ipynb)
