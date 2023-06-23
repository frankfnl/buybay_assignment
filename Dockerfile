FROM apache/airflow:2.6.2
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==2.6.2" -r /requirements.txt
ADD data /opt/airflow/data