FROM apache/airflow:2.10.2-python3.11

USER ${AIRFLOW_UID:-50000}:0

COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

EXPOSE 5432 5050 8080