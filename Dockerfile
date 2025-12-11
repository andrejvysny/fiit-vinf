FROM apache/spark-py:v3.4.0

# Ensure we can write to site-packages during build
USER root

WORKDIR /opt/app

# Pre-install Python dependencies to avoid runtime pip installs.
# requirements.txt is copied to leverage Docker layer caching when unchanged.
COPY requirements.txt /opt/app/requirements.txt

ENV PIP_ROOT_USER_ACTION=ignore
RUN pip install --no-cache-dir --disable-pip-version-check -r /opt/app/requirements.txt

# Default env so mounted code is importable without extra flags.
ENV PYTHONPATH=/opt/app

# Create writable temp dirs used by Spark
RUN mkdir -p /tmp/spark /tmp/spark_checkpoints && chmod -R 777 /tmp/spark /tmp/spark_checkpoints

# Entrypoint is defined by docker-compose per service via spark-submit command.
