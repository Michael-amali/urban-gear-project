FROM apache/airflow:2.8.0

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    pandas \
    pyarrow \
    sqlalchemy \
    psycopg2-binary \
    python-dotenv \
    boto3==1.34.0 \
    dbt-core \
    dbt-postgres \
    apache-airflow-providers-slack
