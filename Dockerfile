FROM apache/airflow:2.8.0
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         gcc \
         python3-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt
ENV PATH="/home/airflow/.local/bin:${PATH}"
ENV PYTHONPATH="/home/airflow/.local/lib/python3.8/site-packages:${PYTHONPATH}"

