# This Dockerfile is used for development purposes for the alocateam.

FROM openjdk:15-slim

RUN apt update && \
    apt install python3-pip -y

# parquet-tools csv data/experiments.parquet > ./data/experiments.csv
RUN pip3 install parquet-tools

CMD bash
