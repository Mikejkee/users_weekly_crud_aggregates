FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update
RUN apt install -y default-jdk
RUN apt-get autoremove -yqq --purge
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

USER ${AIRFLOW_UID:-50000}:0

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
#RUN export JAVA_HOME


COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

EXPOSE 5432 5050 8080