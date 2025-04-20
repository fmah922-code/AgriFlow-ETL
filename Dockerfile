# Dockerfile -> Blueprint for making images
# Images -> Running containers
# Container -> Running process with applciation in it

FROM  python:3.11.9
COPY . /AgriFlow
RUN pip install -r /AgriFlow/requirements.txt
CMD ["python3","/AgriFlow/AgriFlow.py"]