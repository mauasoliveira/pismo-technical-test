# Pismo Technical Test

This is the repository for the technical test for Pismo Data Engineering application.

This used the fake data generator found on the repository: https://github.com/eder1985/pismo_recruiting_technical_case

# Requirements

* Docker environment
* Docker compose configured
* Local port 4040 available to Spark Application

# Usage

To use this project, just initialize the environment with:

```bash

docker compose up

```

A Spark Application UI will be available via [port-forward](http://localhost:4040)

The environment will create new test data - using [Fake Data Generator](./data/Local_Fake_Data_Generator.py) - and execute the test.

The Spark application executes the following:

1) Load data from volume `pismo-data`
2) Adds domain and unique identifiers columns ( `unique_event` and `domain_id` )
3) Deletes duplicated data using windowed partition and row_number
4) Partition by domain and date information
5) Save the result on path `./output/pismo-data/`

# Images

The project consists on three base images:

* Python (alpine): to generate data
* Custom image based on Bitnami Spark to execute Pismo application

Bitnami Spark image was chosen due to the easy of use and pre-configuration.
