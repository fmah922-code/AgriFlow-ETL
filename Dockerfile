# Dockerfile -> Blueprint for making images
# Images -> Running containers
# Container -> Running process with applciation in it

#docker run --env-file ./.env agriflow

FROM openjdk:11-slim

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

ENV JAVA_HOME=/usr/local/openjdk-11

COPY . /AgriFlow
RUN pip install -r /AgriFlow/requirements.txt
CMD ["python3","/AgriFlow/AgriFlow.py"]