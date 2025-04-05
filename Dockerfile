FROM python
COPY ./requirements.txt /requirements.txt
# ADD https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.10.txt /constraints.txt
RUN pip install -r /requirements.txt
WORKDIR /AgriFlow
COPY . /AgriFlow
CMD ["python3","AgriFlow.py"]